import { Database } from "bun:sqlite"
import { describe, expect, test } from "bun:test"
import { Effect, Fiber, Option, type Scope, Stream } from "effect"
import { makeSqliteFileSystem, type SqlRunner } from "./sqlite-file-system"

const bunSql = (db: Database): SqlRunner => ({
  exec: (query, ...bindings) => db.query(query).all(...(bindings as Array<string | number | Uint8Array>)) as never,
})

const run = <A, E>(body: (store: Effect.Success<ReturnType<typeof makeSqliteFileSystem>>) => Effect.Effect<A, E, Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(Effect.flatMap(makeSqliteFileSystem(bunSql(new Database(":memory:"))), body)))

describe("SqliteFileSystem", () => {
  test("writes, appends and reads files, in chunks past the chunk size", () => run(({ fs }) => Effect.gen(function* () {
    yield* fs.makeDirectory("/home/u/.kanna/data", { recursive: true })
    yield* fs.writeFileString("/home/u/.kanna/data/chats.jsonl", "a\n")
    yield* fs.writeFileString("/home/u/.kanna/data/chats.jsonl", "b\n", { flag: "a" })
    expect(yield* fs.readFileString("/home/u/.kanna/data/chats.jsonl")).toBe("a\nb\n")
    expect(Number((yield* fs.stat("/home/u/.kanna/data/chats.jsonl")).size)).toBe(4)

    const big = new Uint8Array(1_300_000).map((_, index) => index % 251)
    yield* fs.writeFile("/home/u/.kanna/data/big.bin", big)
    yield* fs.writeFile("/home/u/.kanna/data/big.bin", new Uint8Array([7]), { flag: "a" })
    const back = yield* fs.readFile("/home/u/.kanna/data/big.bin")
    expect(back.length).toBe(1_300_001)
    expect(back[1_299_999]).toBe(big[1_299_999]!)
    expect(back[1_300_000]).toBe(7)
  })))

  test("keeps the errors callers catch by reason", () => run(({ fs }) => Effect.gen(function* () {
    expect((yield* Effect.flip(fs.readFileString("/missing"))).reason._tag).toBe("NotFound")
    expect(yield* fs.exists("/missing")).toBe(false)
    // A parent has to exist, as on disk.
    expect((yield* Effect.flip(fs.writeFileString("/no/such/dir/file", "x"))).reason._tag).toBe("NotFound")
    yield* fs.writeFileString("/once", "1")
    expect((yield* Effect.flip(fs.writeFileString("/once", "2", { flag: "wx" }))).reason._tag).toBe("AlreadyExists")
    yield* fs.makeDirectory("/dir")
    yield* fs.writeFileString("/dir/f", "x")
    expect((yield* Effect.flip(fs.remove("/dir"))).reason._tag).toBe("Busy")
    yield* fs.remove("/dir", { recursive: true })
    expect(yield* fs.exists("/dir/f")).toBe(false)
    yield* fs.remove("/dir", { force: true })
  })))

  test("lists direct children, and renames directories with what they hold", () => run(({ fs, extras }) => Effect.gen(function* () {
    yield* fs.makeDirectory("/d/sub", { recursive: true })
    yield* fs.writeFileString("/d/a.txt", "a")
    yield* fs.writeFileString("/d/sub/b.txt", "b")
    yield* fs.writeFileString("/d0", "not a child of /d")
    expect((yield* fs.readDirectory("/d")).sort()).toEqual(["a.txt", "sub"])
    expect((yield* fs.readDirectory("/d", { recursive: true })).sort()).toEqual(["a.txt", "sub", "sub/b.txt"])
    expect((yield* extras.readDirectoryEntries("/d")).map((entry) => `${entry.name}:${entry.kind}`).sort()).toEqual(["a.txt:File", "sub:Directory"])

    yield* fs.rename("/d", "/e")
    expect(yield* fs.readFileString("/e/sub/b.txt")).toBe("b")
    expect(yield* fs.exists("/d")).toBe(false)

    // Rename replaces a file, as the atomic tmp-then-rename writes need.
    yield* fs.writeFileString("/e/a.txt.tmp", "new")
    yield* fs.rename("/e/a.txt.tmp", "/e/a.txt")
    expect(yield* fs.readFileString("/e/a.txt")).toBe("new")
  })))

  test("reads byte ranges through an open file", () => run(({ fs }) => Effect.gen(function* () {
    yield* fs.writeFileString("/t.jsonl", "0123456789")
    const file = yield* fs.open("/t.jsonl")
    expect(new TextDecoder().decode(Option.getOrThrow(yield* file.readAlloc(4, { position: 6n })))).toBe("6789")
    expect(Option.isNone(yield* file.readAlloc(4, { position: 10n }))).toBe(true)
    expect(Number((yield* file.stat).size)).toBe(10)

    const appender = yield* fs.open("/log", { flag: "a" })
    yield* appender.writeAll(new TextEncoder().encode("x"))
    yield* appender.writeAll(new TextEncoder().encode("y"))
    expect(yield* fs.readFileString("/log")).toBe("xy")
  })))

  test("watch sees its own writes under the path", () => run(({ fs }) => Effect.gen(function* () {
    yield* fs.makeDirectory("/w")
    let ready = false
    const seen = yield* fs.watch("/w", { onReady: () => (ready = true) }).pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
    // Callers wait for onReady before they rely on the watch (keybindings, settings).
    while (!ready) yield* Effect.yieldNow
    yield* fs.writeFileString("/w/settings.json", "{}")
    yield* fs.writeFileString("/elsewhere", "{}")
    yield* fs.writeFileString("/w/settings.json", "{\"a\":1}")
    const events = yield* Fiber.join(seen)
    expect([...events]).toEqual([{ _tag: "Create", path: "/w/settings.json" }, { _tag: "Update", path: "/w/settings.json" }])
  })))
})
