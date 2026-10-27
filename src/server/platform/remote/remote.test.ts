import { describe, expect, test } from "bun:test"
import { BunHttpServer } from "@effect/platform-bun"
import { Effect, FileSystem, Layer, Option, type Scope, Stream } from "effect"
import { HttpServer } from "effect/http"
import { BunPlatform } from "../bun"
import { FileSystemExtras } from "../file-system-extras"
import { Machine } from "../machine"
import { ProcessSpawner } from "../process-spawner"
import { PtySpawner } from "../pty"
import { RemoteMachineLayer } from "./connection"
import { serveRunner } from "./runner"

const TOKEN = "test-token"
const decoder = new TextDecoder()

/** A runner on a free port, on this computer, and the host's view of it. */
const remote = Layer.unwrap(Effect.gen(function* () {
  yield* serveRunner({ token: TOKEN })
  const server = yield* HttpServer.HttpServer
  const port = (server.address as { readonly port: number }).port
  return RemoteMachineLayer({ url: `ws://127.0.0.1:${port}`, token: TOKEN })
})).pipe(Layer.provide(Layer.merge(BunHttpServer.layer({ port: 0, hostname: "127.0.0.1" }), BunPlatform)))

type Remote = Layer.Success<typeof remote>

const run = <A, E>(effect: Effect.Effect<A, E, Remote | Scope.Scope>) =>
  Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(remote)))

const collect = (stream: Stream.Stream<Uint8Array, unknown>) =>
  Stream.runCollect(stream).pipe(Effect.map((chunks) => decoder.decode(Buffer.concat([...chunks]))))

describe("remote machine", () => {
  test("reports the runner's machine", () => run(Effect.gen(function* () {
    const machine = yield* Machine
    expect(machine.homeDirectory).toBe(require("node:os").homedir())
    expect(machine.env.get("PATH")).toBe(process.env.PATH)
    expect(yield* machine.which("sh")).toBe(Bun.which("sh"))
  })))

  test("runs a process and collects its output and exit", () => run(Effect.gen(function* () {
    const spawner = yield* ProcessSpawner
    const proc = yield* spawner.spawn(["sh", "-c", "echo out; echo err >&2; exit 3"], { stdout: "pipe", stderr: "pipe" })
    const [stdout, stderr] = yield* Effect.all([collect(proc.stdout), collect(proc.stderr)], { concurrency: 2 })
    const exit = yield* proc.exited
    expect(stdout).toBe("out\n")
    expect(stderr).toBe("err\n")
    expect(exit).toEqual({ code: 3, exitCode: 3, signalCode: null })
    expect(yield* proc.running).toBe(false)
  })))

  test("pipes stdin", () => run(Effect.gen(function* () {
    const proc = yield* (yield* ProcessSpawner).spawn(["cat"], { stdin: "pipe", stdout: "pipe" })
    yield* proc.writeStdin("hello ")
    yield* proc.writeStdin(new TextEncoder().encode("world"))
    yield* proc.endStdin
    expect(yield* collect(proc.stdout)).toBe("hello world")
  })))

  test("fails a missing command with the platform's own message", () => run(Effect.gen(function* () {
    const error = yield* Effect.flip((yield* ProcessSpawner).spawn(["kanna-no-such-command"]))
    expect(error._tag).toBe("@kanna/server/process-utils/CommandSpawnError")
    expect(error.message).toContain("kanna-no-such-command")
  })))

  test("kills a process, and closing the scope kills one left running", () => run(Effect.gen(function* () {
    const spawner = yield* ProcessSpawner
    const proc = yield* spawner.spawn(["sleep", "30"])
    yield* proc.kill("SIGTERM")
    expect((yield* proc.exited).signalCode).toBe("SIGTERM")

    const pid = yield* Effect.scoped(Effect.map(spawner.spawn(["sleep", "30"]), (left) => left.pid!))
    yield* Effect.sleep("200 millis")
    const machine = yield* Machine
    const gone = yield* Effect.flip(machine.signalProcess(pid, 0))
    expect(gone.code).toBe("ESRCH")
  })))

  test("runs a PTY", () => run(Effect.gen(function* () {
    const ptys = yield* PtySpawner
    expect(ptys.available).toBe(true)
    let output = ""
    const pty = yield* ptys.spawn(["sh", "-c", "printf 'tty:'; test -t 0 && echo yes"], {
      cols: 80,
      rows: 24,
      onData: (data) => {
        output += decoder.decode(data)
      },
    })
    expect((yield* pty.exited).exitCode).toBe(0)
    yield* Effect.sleep("50 millis")
    expect(output).toContain("tty:yes")
  })))

  test("reads and writes files, keeping error reasons", () => run(Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const extras = yield* FileSystemExtras
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "kanna-remote-" })
    const file = `${dir}/a.txt`
    yield* fs.writeFileString(file, "hello")
    expect(yield* fs.readFileString(file)).toBe("hello")
    expect(yield* fs.readDirectory(dir)).toEqual(["a.txt"])
    const info = yield* fs.stat(file)
    expect(info.type).toBe("File")
    expect(Number(info.size)).toBe(5)
    expect(Option.isSome(info.mtime)).toBe(true)
    expect((yield* extras.stat(file)).size).toBe(5)
    expect(extras.contentType(file)).toContain("text/plain")

    const missing = yield* Effect.flip(fs.readFileString(`${dir}/missing`))
    expect(missing._tag).toBe("PlatformError")
    expect(missing.reason._tag).toBe("NotFound")
    expect(yield* fs.exists(`${dir}/missing`)).toBe(false)

    expect(yield* collect(fs.stream(file, { offset: 1 }))).toBe("ello")
    const body = yield* extras.fileBody(file)
    expect(yield* Effect.promise(() => new Response(body).text())).toBe("hello")
  })))

  test("opens a file and reads it by position", () => run(Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "kanna-remote-" })
    yield* fs.writeFileString(`${dir}/b.txt`, "0123456789")
    const file = yield* fs.open(`${dir}/b.txt`)
    const chunk = yield* file.readAlloc(4, { position: 3n })
    expect(decoder.decode(Option.getOrThrow(chunk))).toBe("3456")
    expect(Number((yield* file.stat).size)).toBe(10)
  })))

  test("rejects a host without the token", async () => {
    const program = Effect.gen(function* () {
      yield* serveRunner({ token: TOKEN })
      const server = yield* HttpServer.HttpServer
      const port = (server.address as { readonly port: number }).port
      return yield* Effect.promise(() => fetch(`http://127.0.0.1:${port}/machine?token=wrong`).then((response) => response.status))
    })
    const status = await Effect.runPromise(program.pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(BunHttpServer.layer({ port: 0, hostname: "127.0.0.1" }), BunPlatform)),
    ))
    expect(status).toBe(401)
  })
})
