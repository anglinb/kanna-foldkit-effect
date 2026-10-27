import { ByteSize, Effect, FileSystem, Option, PlatformError, PubSub, Stream } from "effect"
import mime from "mime-types"
import { FileSystemExtras, type PreciseFileInfo } from "./file-system-extras"

/**
 * The slice of a SQLite connection this needs: a Durable Object's
 * `ctx.storage.sql` has it, and so does a `bun:sqlite` database behind a
 * one-line adapter (tests).
 */
export interface SqlRunner {
  exec(query: string, ...bindings: Array<any>): Iterable<Record<string, unknown>>
}

/** DO SQLite caps a value at 2 MB; file bodies are stored in chunks well under it. */
const CHUNK_BYTES = 512 * 1024

interface Entry {
  readonly path: string
  readonly kind: "file" | "dir"
  readonly mode: number
  readonly size: number
  readonly mtime_ms: number
  readonly atime_ms: number
  readonly birthtime_ms: number
  readonly ino: number
}

const MODULE = "SqliteFileSystem"

const fail = (tag: PlatformError.SystemErrorTag, method: string, path: string, description?: string) =>
  PlatformError.systemError({ _tag: tag, module: MODULE, method, pathOrDescriptor: path, description, syscall: method })

const unsupported = (method: string, path: string) =>
  PlatformError.badArgument({ module: MODULE, method, description: `${method} isn't supported on Kanna's stored files (${path})` })

function normalize(path: string) {
  const parts: string[] = []
  for (const part of path.split("/")) {
    if (!part || part === ".") continue
    if (part === "..") parts.pop()
    else parts.push(part)
  }
  return `/${parts.join("/")}`
}

const parentOf = (path: string) => {
  const index = path.lastIndexOf("/")
  return index <= 0 ? "/" : path.slice(0, index)
}

/** The range of paths strictly under `dir`: `dir/` up to, not including, `dir0` ('0' follows '/'). */
const descendantRange = (dir: string) => dir === "/" ? ["/", "0"] as const : [`${dir}/`, `${dir}0`] as const

function concat(chunks: ReadonlyArray<Uint8Array>) {
  if (chunks.length === 1) return chunks[0]!
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
  const out = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    out.set(chunk, offset)
    offset += chunk.length
  }
  return out
}

const asBytes = (value: unknown) => value instanceof Uint8Array ? value : new Uint8Array(value as ArrayBuffer)

/**
 * Kanna's own state as files in SQLite: a Durable Object's data dir. Entries
 * (files and directories) in one table, file bodies as ordered chunks in
 * another, so an append (how the event logs grow) is one insert.
 *
 * It keeps the file semantics Kanna's state modules rely on: parents must
 * exist, `wx` refuses to overwrite, rename replaces, remove needs
 * `recursive` for a non-empty directory, byte-range reads through `open`.
 * Links aren't supported; nothing in the data dir uses them. `watch` sees
 * this store's own writes, which hosted are the only writes there are.
 */
export const makeSqliteFileSystem = Effect.fn("SqliteFileSystem.make")(function* (sql: SqlRunner, options: { readonly now?: () => number } = {}) {
  const now = options.now ?? Date.now
  sql.exec(`CREATE TABLE IF NOT EXISTS kanna_fs_entries (
    path TEXT PRIMARY KEY, kind TEXT NOT NULL, mode INTEGER NOT NULL, size INTEGER NOT NULL,
    mtime_ms REAL NOT NULL, atime_ms REAL NOT NULL, birthtime_ms REAL NOT NULL, ino INTEGER NOT NULL)`)
  sql.exec("CREATE TABLE IF NOT EXISTS kanna_fs_chunks (path TEXT NOT NULL, seq INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (path, seq))")
  const changes = yield* PubSub.unbounded<FileSystem.WatchEvent>()
  const rows = <Row>(query: string, ...bindings: Array<unknown>) => [...sql.exec(query, ...bindings)] as Array<Row>

  const run = <A>(method: string, path: string, body: () => A) => Effect.try({
    try: body,
    catch: (cause) => cause instanceof PlatformError.PlatformError ? cause : PlatformError.systemError({
      _tag: "Unknown", module: MODULE, method, pathOrDescriptor: path, description: cause instanceof Error ? cause.message : String(cause), cause,
    }),
  })
  const throwing = (error: PlatformError.PlatformError): never => {
    throw error
  }
  const emit = (event: FileSystem.WatchEvent) => PubSub.publishUnsafe(changes, event)

  const get = (path: string): Entry | undefined => rows<Entry>("SELECT * FROM kanna_fs_entries WHERE path = ?", path)[0]
  const nextIno = () => Number(rows<{ ino: number }>("SELECT COALESCE(MAX(ino), 0) + 1 AS ino FROM kanna_fs_entries")[0]!.ino)
  const children = (dir: string) => {
    const [low, high] = descendantRange(dir)
    return rows<{ path: string }>("SELECT path FROM kanna_fs_entries WHERE path >= ? AND path < ? ORDER BY path", low, high)
      .map((row) => row.path)
  }
  const chunksOf = (path: string) => rows<{ data: unknown }>("SELECT data FROM kanna_fs_chunks WHERE path = ? ORDER BY seq", path).map((row) => asBytes(row.data))
  const contents = (path: string) => concat(chunksOf(path))

  const requireDirectory = (path: string, method: string) => {
    if (path === "/") return
    const entry = get(path)
    if (!entry) throwing(fail("NotFound", method, path))
    else if (entry.kind !== "dir") throwing(fail("BadResource", method, path, "not a directory"))
  }
  const requireEntry = (path: string, method: string) => get(path) ?? throwing(fail("NotFound", method, path))
  const requireFile = (path: string, method: string) => {
    const entry = requireEntry(path, method)
    return entry.kind === "file" ? entry : throwing(fail("BadResource", method, path, "is a directory"))
  }

  const appendChunks = (path: string, data: Uint8Array, firstSeq: number) => {
    let seq = firstSeq
    for (let offset = 0; offset < data.length; offset += CHUNK_BYTES) {
      sql.exec("INSERT INTO kanna_fs_chunks (path, seq, data) VALUES (?, ?, ?)", path, seq, data.subarray(offset, offset + CHUNK_BYTES))
      seq += 1
    }
  }
  const nextSeq = (path: string) => Number(rows<{ seq: number }>("SELECT COALESCE(MAX(seq), -1) + 1 AS seq FROM kanna_fs_chunks WHERE path = ?", path)[0]!.seq)

  /** Replaces a file's whole body, creating it if needed (parent must exist). */
  const putFile = (path: string, data: Uint8Array, mode = 0o644) => {
    const existing = get(path)
    if (existing?.kind === "dir") throwing(fail("BadResource", "writeFile", path, "is a directory"))
    if (!existing) requireDirectory(parentOf(path), "writeFile")
    const time = now()
    sql.exec("DELETE FROM kanna_fs_chunks WHERE path = ?", path)
    appendChunks(path, data, 0)
    if (existing) sql.exec("UPDATE kanna_fs_entries SET size = ?, mtime_ms = ? WHERE path = ?", data.length, time, path)
    else sql.exec("INSERT INTO kanna_fs_entries VALUES (?, 'file', ?, ?, ?, ?, ?, ?)", path, mode, data.length, time, time, time, nextIno())
    emit({ _tag: existing ? "Update" : "Create", path })
  }

  const appendFile = (path: string, data: Uint8Array, mode = 0o644) => {
    const existing = get(path)
    if (!existing) return putFile(path, data, mode)
    if (existing.kind === "dir") throwing(fail("BadResource", "writeFile", path, "is a directory"))
    appendChunks(path, data, nextSeq(path))
    sql.exec("UPDATE kanna_fs_entries SET size = size + ?, mtime_ms = ? WHERE path = ?", data.length, now(), path)
    emit({ _tag: "Update", path })
  }

  const makeDirectory = (path: string, recursive: boolean, mode = 0o755) => {
    if (path === "/") return
    const existing = get(path)
    if (existing) {
      if (existing.kind === "dir" && recursive) return
      throwing(fail("AlreadyExists", "makeDirectory", path))
    }
    const parent = parentOf(path)
    if (recursive) makeDirectory(parent, true)
    else requireDirectory(parent, "makeDirectory")
    const time = now()
    sql.exec("INSERT INTO kanna_fs_entries VALUES (?, 'dir', ?, 0, ?, ?, ?, ?)", path, mode, time, time, time, nextIno())
    emit({ _tag: "Create", path })
  }

  const removeEntry = (path: string, recursive: boolean, force: boolean) => {
    const entry = get(path)
    if (!entry) {
      if (force) return
      throwing(fail("NotFound", "remove", path))
      return
    }
    if (entry.kind === "dir") {
      const below = children(path)
      if (below.length > 0 && !recursive) throwing(fail("Busy", "remove", path, "directory not empty"))
      const [low, high] = descendantRange(path)
      sql.exec("DELETE FROM kanna_fs_chunks WHERE path >= ? AND path < ?", low, high)
      sql.exec("DELETE FROM kanna_fs_entries WHERE path >= ? AND path < ?", low, high)
    }
    sql.exec("DELETE FROM kanna_fs_chunks WHERE path = ?", path)
    sql.exec("DELETE FROM kanna_fs_entries WHERE path = ?", path)
    emit({ _tag: "Remove", path })
  }

  const renameEntry = (from: string, to: string) => {
    const entry = requireEntry(from, "rename")
    if (from === to) return
    requireDirectory(parentOf(to), "rename")
    const target = get(to)
    if (target) {
      if (target.kind === "dir" && entry.kind !== "dir") throwing(fail("BadResource", "rename", to, "is a directory"))
      removeEntry(to, true, true)
    }
    const move = (oldPath: string, newPath: string) => {
      sql.exec("UPDATE kanna_fs_entries SET path = ? WHERE path = ?", newPath, oldPath)
      sql.exec("UPDATE kanna_fs_chunks SET path = ? WHERE path = ?", newPath, oldPath)
    }
    if (entry.kind === "dir") for (const child of children(from)) move(child, to + child.slice(from.length))
    move(from, to)
    emit({ _tag: "Remove", path: from })
    emit({ _tag: "Create", path: to })
  }

  const info = (entry: Entry): FileSystem.File.Info => ({
    type: entry.kind === "dir" ? "Directory" : "File",
    mtime: Option.some(new Date(entry.mtime_ms)),
    atime: Option.some(new Date(entry.atime_ms)),
    birthtime: Option.some(new Date(entry.birthtime_ms)),
    dev: 0n,
    ino: Option.some(BigInt(entry.ino)),
    mode: entry.mode,
    nlink: Option.some(1n),
    uid: Option.none(),
    gid: Option.none(),
    rdev: Option.none(),
    size: ByteSize.bytes(entry.size),
    blksize: Option.none(),
    blocks: Option.none(),
  })
  const preciseInfo = (entry: Entry): PreciseFileInfo => ({
    kind: entry.kind === "dir" ? "Directory" : "File",
    size: entry.size,
    mtimeMs: entry.mtime_ms,
    ctimeMs: entry.mtime_ms,
    atimeMs: entry.atime_ms,
    ino: entry.ino,
    dev: 0,
    mode: entry.mode,
  })

  const readRange = (path: string, position: number, length: number) => contents(path).slice(position, position + length)

  const open = (rawPath: string, openOptions?: { readonly flag?: FileSystem.OpenFlag | undefined; readonly mode?: number | undefined }) => run("open", rawPath, () => {
    const path = normalize(rawPath)
    const flag = openOptions?.flag ?? "r"
    const existing = get(path)
    if (flag.startsWith("r")) requireFile(path, "open")
    if (flag.includes("x") && existing) throwing(fail("AlreadyExists", "open", path))
    if (flag.startsWith("w")) putFile(path, new Uint8Array(0), openOptions?.mode)
    else if (flag.startsWith("a") && !existing) putFile(path, new Uint8Array(0), openOptions?.mode)
    const append = flag.startsWith("a")
    let position = 0
    const size = () => requireFile(path, "read").size
    const read = (length: number, at?: bigint) => {
      const from = at === undefined ? position : Number(at)
      const bytes = readRange(path, from, length)
      if (at === undefined) position += bytes.length
      return bytes
    }
    const write = (buffer: Uint8Array) => {
      if (append || position === size()) {
        appendFile(path, buffer)
      } else {
        const body = contents(path)
        const next = new Uint8Array(Math.max(body.length, position + buffer.length))
        next.set(body)
        next.set(buffer, position)
        putFile(path, next)
      }
      position = append ? size() : position + buffer.length
      return buffer.length
    }
    return {
      [FileSystem.FileTypeId]: FileSystem.FileTypeId,
      stat: run("stat", path, () => info(requireFile(path, "stat"))),
      seek: (offset: bigint, from: FileSystem.SeekMode) => run("seek", path, () => {
        position = from === "start" ? Number(offset) : position + Number(offset)
        return BigInt(position)
      }),
      sync: Effect.void,
      read: (buffer: Uint8Array, readOptions?: FileSystem.File.ReadOptions) => run("read", path, () => {
        const bytes = read(buffer.length, readOptions?.position)
        buffer.set(bytes)
        return bytes.length
      }),
      readAlloc: (length: number, readOptions?: FileSystem.File.ReadOptions) => run("read", path, () => {
        const bytes = read(length, readOptions?.position)
        return bytes.length === 0 ? Option.none() : Option.some(bytes)
      }),
      truncate: (length?: number) => run("truncate", path, () => putFile(path, contents(path).slice(0, length ?? 0))),
      write: (buffer: Uint8Array) => run("write", path, () => write(buffer)),
      writeAll: (buffer: Uint8Array) => run("write", path, () => {
        write(buffer)
      }),
    } as FileSystem.File
  })

  const tempName = (prefix = "") => `${prefix}${crypto.randomUUID().slice(0, 8)}`
  const tempDirectory = (directory: string | undefined, prefix: string | undefined) => {
    const base = normalize(directory ?? "/tmp")
    makeDirectory(base, true)
    const path = `${base}/${tempName(prefix)}`
    makeDirectory(path, false)
    return path
  }

  const copyFileSync = (from: string, to: string) => {
    requireFile(from, "copyFile")
    putFile(to, contents(from))
  }
  const copyTree = (from: string, to: string) => {
    const entry = requireEntry(from, "copy")
    if (entry.kind === "file") return copyFileSync(from, to)
    makeDirectory(to, true)
    for (const child of children(from)) {
      const target = to + child.slice(from.length)
      if (get(child)!.kind === "dir") makeDirectory(target, true)
      else copyFileSync(child, target)
    }
  }

  const fs = FileSystem.make({
    access: (path) => run("access", path, () => {
      requireEntry(normalize(path), "access")
    }),
    copy: (from, to) => run("copy", from, () => copyTree(normalize(from), normalize(to))),
    copyFile: (from, to) => run("copyFile", from, () => copyFileSync(normalize(from), normalize(to))),
    chmod: (path, mode) => run("chmod", path, () => {
      requireEntry(normalize(path), "chmod")
      sql.exec("UPDATE kanna_fs_entries SET mode = ? WHERE path = ?", mode, normalize(path))
    }),
    chown: (path) => run("chown", path, () => {
      requireEntry(normalize(path), "chown")
    }),
    glob: (pattern) => Effect.fail(unsupported("glob", pattern)),
    link: (from) => Effect.fail(unsupported("link", from)),
    makeDirectory: (path, options) => run("makeDirectory", path, () => makeDirectory(normalize(path), options?.recursive ?? false, options?.mode)),
    makeTempDirectory: (options) => run("makeTempDirectory", options?.directory ?? "/tmp", () => tempDirectory(options?.directory, options?.prefix)),
    makeTempDirectoryScoped: (options) => Effect.acquireRelease(
      run("makeTempDirectory", options?.directory ?? "/tmp", () => tempDirectory(options?.directory, options?.prefix)),
      (path) => run("remove", path, () => removeEntry(path, true, true)).pipe(Effect.ignore),
    ),
    makeTempFile: (options) => run("makeTempFile", options?.directory ?? "/tmp", () => {
      const dir = tempDirectory(options?.directory, options?.prefix)
      const path = `${dir}/${tempName()}${options?.suffix ?? ""}`
      putFile(path, new Uint8Array(0))
      return path
    }),
    makeTempFileScoped: (options) => Effect.acquireRelease(
      run("makeTempFile", options?.directory ?? "/tmp", () => {
        const dir = tempDirectory(options?.directory, options?.prefix)
        const path = `${dir}/${tempName()}${options?.suffix ?? ""}`
        putFile(path, new Uint8Array(0))
        return path
      }),
      (path) => run("remove", path, () => removeEntry(parentOf(path), true, true)).pipe(Effect.ignore),
    ),
    open,
    readDirectory: (rawPath, options) => run("readDirectory", rawPath, () => {
      const path = normalize(rawPath)
      requireDirectory(path, "readDirectory")
      const prefix = path === "/" ? "/" : `${path}/`
      const names = children(path).map((child) => child.slice(prefix.length))
      return options?.recursive ? names : names.filter((name) => !name.includes("/"))
    }),
    readFile: (path) => run("readFile", path, () => {
      requireFile(normalize(path), "readFile")
      return contents(normalize(path))
    }),
    readLink: (path) => Effect.fail(unsupported("readLink", path)),
    realPath: (path) => run("realPath", path, () => {
      requireEntry(normalize(path), "realPath")
      return normalize(path)
    }),
    remove: (path, options) => run("remove", path, () => removeEntry(normalize(path), options?.recursive ?? false, options?.force ?? false)),
    rename: (from, to) => run("rename", from, () => renameEntry(normalize(from), normalize(to))),
    stat: (path) => run("stat", path, () => info(requireEntry(normalize(path), "stat"))),
    symlink: (from) => Effect.fail(unsupported("symlink", from)),
    truncate: (path, length) => run("truncate", path, () => {
      requireFile(normalize(path), "truncate")
      putFile(normalize(path), contents(normalize(path)).slice(0, length ?? 0))
    }),
    utimes: (path, atime, mtime) => run("utimes", path, () => {
      requireEntry(normalize(path), "utimes")
      const ms = (value: Date | number) => typeof value === "number" ? value : value.getTime()
      sql.exec("UPDATE kanna_fs_entries SET atime_ms = ?, mtime_ms = ? WHERE path = ?", ms(atime), ms(mtime), normalize(path))
    }),
    watch: (rawPath, options) => {
      const path = normalize(rawPath)
      const prefix = path === "/" ? "/" : `${path}/`
      // Subscribed before `onReady`, so no write after it is missed.
      return Stream.unwrap(Effect.map(PubSub.subscribe(changes), (subscription) => {
        options?.onReady?.()
        return Stream.fromSubscription(subscription)
      })).pipe(Stream.filter((event) => {
        if (event.path === path) return true
        if (!event.path.startsWith(prefix)) return false
        return options?.recursive === true || !event.path.slice(prefix.length).includes("/")
      }))
    },
    writeFile: (path, data, options) => run("writeFile", path, () => {
      const target = normalize(path)
      const flag = options?.flag ?? "w"
      if (flag.includes("x") && get(target)) throwing(fail("AlreadyExists", "writeFile", target))
      if (flag.startsWith("a")) appendFile(target, data, options?.mode)
      else putFile(target, data, options?.mode)
    }),
  })

  const extras = FileSystemExtras.of({
    stat: (path) => run("stat", path, () => preciseInfo(requireEntry(normalize(path), "stat"))),
    lstat: (path) => run("lstat", path, () => preciseInfo(requireEntry(normalize(path), "lstat"))),
    readDirectoryEntries: (rawPath) => run("readDirectory", rawPath, () => {
      const path = normalize(rawPath)
      requireDirectory(path, "readDirectory")
      const prefix = path === "/" ? "/" : `${path}/`
      return children(path)
        .filter((child) => !child.slice(prefix.length).includes("/"))
        .map((child) => ({ name: child.slice(prefix.length), kind: get(child)!.kind === "dir" ? "Directory" as const : "File" as const }))
    }),
    isExecutableFile: (path) => Effect.sync(() => {
      const entry = get(normalize(path))
      return entry?.kind === "file" && (entry.mode & 0o111) !== 0
    }),
    readFileHead: (path, maxBytes) => run("readFileHead", path, () => {
      const entry = requireEntry(normalize(path), "readFileHead")
      return { info: preciseInfo(entry), head: entry.kind === "file" ? readRange(normalize(path), 0, maxBytes) : new Uint8Array(0) }
    }),
    contentType: (path) => mime.contentType(path.split("/").pop() ?? "") || "",
    fileBody: (path) => Effect.sync(() => {
      const body = get(normalize(path))?.kind === "file" ? contents(normalize(path)) : new Uint8Array(0)
      return new Blob([body as Uint8Array<ArrayBuffer>], { type: mime.contentType(path.split("/").pop() ?? "") || "" })
    }),
  })

  return { fs, extras }
})
