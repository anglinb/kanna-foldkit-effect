import { Effect, FileSystem, Layer, PlatformError, Stream } from "effect"
import { FileSystemExtras } from "./file-system-extras"

/**
 * Which file system a path belongs to, when Kanna's own state and the
 * machine's files live in different places (the server in a Durable Object,
 * the repos in a container).
 *
 * `hostRoots` are Kanna's: the data dir, the built client it serves. Within
 * them, `machineRoots` go back to the machine anyway, because processes on
 * the machine read them: the account config dirs a CLI gets as
 * `CLAUDE_CONFIG_DIR` / `CODEX_HOME`.
 */
export interface PathRoutes {
  readonly hostRoots: ReadonlyArray<string>
  readonly machineRoots?: ReadonlyArray<string>
}

const within = (root: string, path: string) => {
  const base = root.endsWith("/") ? root.slice(0, -1) : root
  return path === base || path.startsWith(`${base}/`)
}

export function isHostPath(routes: PathRoutes, path: string) {
  return routes.hostRoots.some((root) => within(root, path))
    && !(routes.machineRoots ?? []).some((root) => within(root, path))
}

interface Side {
  readonly fs: FileSystem.FileSystem
  readonly extras: FileSystemExtras["Service"]
}

const crossDevice = (method: string, from: string, to: string) =>
  PlatformError.systemError({
    _tag: "BadResource",
    module: "RoutedFileSystem",
    method,
    description: `${from} and ${to} are on different machines`,
    pathOrDescriptor: from,
  })

/**
 * One `FileSystem` and `FileSystemExtras` over two: each call goes to the
 * side its path is on. Copies and renames between the sides move the bytes
 * through the host; links between them fail, as they would across devices.
 */
export function makeRoutedFileSystem(routes: PathRoutes, host: Side, machine: Side) {
  const side = (path: string) => isHostPath(routes, path) ? host : machine
  const fsOf = (path: string) => side(path).fs
  const sameSide = (from: string, to: string) => side(from) === side(to)

  const copyAcross = (from: string, to: string) => Effect.gen(function* () {
    const info = yield* fsOf(from).stat(from)
    if (info.type !== "File") return yield* crossDevice("copy", from, to)
    yield* fsOf(to).writeFile(to, yield* fsOf(from).readFile(from))
  })

  const fs = FileSystem.make({
    access: (path, options) => fsOf(path).access(path, options),
    copy: (from, to, options) => sameSide(from, to) ? fsOf(from).copy(from, to, options) : copyAcross(from, to),
    copyFile: (from, to) => sameSide(from, to) ? fsOf(from).copyFile(from, to) : copyAcross(from, to),
    chmod: (path, mode) => fsOf(path).chmod(path, mode),
    chown: (path, uid, gid) => fsOf(path).chown(path, uid, gid),
    glob: (pattern, options) => fsOf(options?.root ?? pattern).glob(pattern, options),
    link: (from, to) => sameSide(from, to) ? fsOf(from).link(from, to) : Effect.fail(crossDevice("link", from, to)),
    makeDirectory: (path, options) => fsOf(path).makeDirectory(path, options),
    // Temporary files are scratch space for processes: the machine's, unless
    // the caller names a directory.
    makeTempDirectory: (options) => (options?.directory ? fsOf(options.directory) : machine.fs).makeTempDirectory(options),
    makeTempDirectoryScoped: (options) => (options?.directory ? fsOf(options.directory) : machine.fs).makeTempDirectoryScoped(options),
    makeTempFile: (options) => (options?.directory ? fsOf(options.directory) : machine.fs).makeTempFile(options),
    makeTempFileScoped: (options) => (options?.directory ? fsOf(options.directory) : machine.fs).makeTempFileScoped(options),
    open: (path, options) => fsOf(path).open(path, options),
    readDirectory: (path, options) => fsOf(path).readDirectory(path, options),
    readFile: (path) => fsOf(path).readFile(path),
    readLink: (path) => fsOf(path).readLink(path),
    realPath: (path) => fsOf(path).realPath(path),
    remove: (path, options) => fsOf(path).remove(path, options),
    rename: (from, to) => sameSide(from, to)
      ? fsOf(from).rename(from, to)
      : copyAcross(from, to).pipe(Effect.andThen(fsOf(from).remove(from))),
    stat: (path) => fsOf(path).stat(path),
    symlink: (from, to) => fsOf(to).symlink(from, to),
    truncate: (path, length) => fsOf(path).truncate(path, length),
    utimes: (path, atime, mtime) => fsOf(path).utimes(path, atime, mtime),
    watch: (path, options) => Stream.suspend(() => fsOf(path).watch(path, options)),
    writeFile: (path, data, options) => fsOf(path).writeFile(path, data, options),
  })

  const extras = FileSystemExtras.of({
    stat: (path) => side(path).extras.stat(path),
    lstat: (path) => side(path).extras.lstat(path),
    readDirectoryEntries: (path) => side(path).extras.readDirectoryEntries(path),
    isExecutableFile: (path) => side(path).extras.isExecutableFile(path),
    readFileHead: (path, maxBytes) => side(path).extras.readFileHead(path, maxBytes),
    contentType: (path) => side(path).extras.contentType(path),
    fileBody: (path) => side(path).extras.fileBody(path),
  })

  return {
    fs: FileSystem.FileSystem.of({
      ...fs,
      // Each side streams its own way (the runner over one RPC stream).
      stream: (path, options) => Stream.suspend(() => fsOf(path).stream(path, options)),
      sink: (path, options) => fsOf(path).sink(path, options),
      readFileString: (path, encoding) => fsOf(path).readFileString(path, encoding),
      writeFileString: (path, data, options) => fsOf(path).writeFileString(path, data, options),
      exists: (path) => fsOf(path).exists(path),
    }),
    extras,
  }
}

/** The routed pair as a layer over two already-built sides. */
export const RoutedFileSystemLayer = (routes: PathRoutes, host: Side, machine: Side) => {
  const routed = makeRoutedFileSystem(routes, host, machine)
  return Layer.merge(
    Layer.succeed(FileSystem.FileSystem, routed.fs),
    Layer.succeed(FileSystemExtras, routed.extras),
  )
}
