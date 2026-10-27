import { ByteSize, Cause, Context, Deferred, Effect, FileSystem, Option, Queue, Ref, Stream } from "effect"
import type { RpcClient, RpcClientError, RpcGroup } from "effect/rpc"
import mime from "mime-types"
import { FileSystemExtras } from "../file-system-extras"
import { Machine, type MachineEnv, ProcessSignalError } from "../machine"
import {
  CommandSpawnError,
  ProcessSpawner,
  type ProcessExit,
  type ProcessSpawnOptions,
  type ProcessStdin,
  type SpawnedProcess,
} from "../process-spawner"
import { PtySpawner, type PtyProcess, type PtySpawnOptions } from "../pty"
import { definedEnv, fromWireExit, fromWireFileInfo, platformErrorOf, unwireFs } from "./codec"
import type { MachineRpcs, WireStdin } from "./protocol"

export type MachineClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof MachineRpcs>, RpcClientError.RpcClientError>

/** What the runner reports about its machine (`MachineInfo`), plus whether it has PTYs. */
export interface MachineFacts {
  readonly platform: string
  readonly arch: string
  readonly homeDirectory: string
  readonly tmpDirectory: string
  readonly workingDirectory: string
  readonly hostname: string
  readonly env: Readonly<Record<string, string>>
  readonly ptyAvailable: boolean
}

/**
 * The host's side of a runner connection (`connection.ts`): the current
 * client, the machine's facts and its environment. While the runner is
 * unreachable, `client`'s calls fail at once with `MachineOfflineError`
 * and the facts are the last ones it reported.
 */
export interface RunnerLink {
  readonly client: MachineClient
  readonly facts: () => MachineFacts
  /** The host's copy of the machine's environment, kept in step with the runner's. */
  readonly env: MachineEnv
}

/** The runner can't be reached right now. */
export class MachineOfflineError extends Error {
  constructor(readonly operation: string) {
    super("The machine is offline")
    this.name = "MachineOfflineError"
  }
}

/**
 * What a host sees when the machine is a runner somewhere else: the same
 * `Machine`, `ProcessSpawner`, `PtySpawner`, `FileSystem` and
 * `FileSystemExtras` the Bun bindings provide, each call forwarded over the
 * link's current connection.
 */
export type RemoteMachineServices = Machine | ProcessSpawner | PtySpawner | FileSystem.FileSystem | FileSystemExtras

/** The connection dropped (or the runner restarted) while a process was running. */
const LOST_EXIT: ProcessExit = { code: 137, exitCode: null, signalCode: "SIGKILL" }

const encoder = new TextEncoder()

const lostConnection = (command: string) => (cause: unknown) =>
  new CommandSpawnError({
    command,
    message: cause instanceof MachineOfflineError
      ? `Can't run ${command}: the machine is offline`
      : `Lost the connection to the machine running ${command}`,
    cause,
  })

/** A process RPC's error as the `CommandSpawnError` local spawns fail with. */
const spawnFailure = (command: string) => (error: unknown) =>
  error instanceof CommandSpawnError ? error : lostConnection(command)(error)

function toWireStdin(stdin: ProcessStdin | undefined): WireStdin {
  if (stdin === undefined) return "ignore"
  if (stdin === "ignore" || stdin === "inherit" || stdin === "pipe") return stdin
  return typeof stdin === "string" ? { text: stdin } : { bytes: stdin }
}

export function makeRemoteMachineContext(link: RunnerLink): Context.Context<RemoteMachineServices> {
  const { client, env } = link
  const facts = link.facts

  // Getters: a runner that reconnects after a restart can report new facts.
  const machine = Machine.of({
    remote: true,
    get platform() {
      return facts().platform as NodeJS.Platform
    },
    get arch() {
      return facts().arch
    },
    get homeDirectory() {
      return facts().homeDirectory
    },
    get tmpDirectory() {
      return facts().tmpDirectory
    },
    get workingDirectory() {
      return facts().workingDirectory
    },
    hostname: () => facts().hostname,
    env,
    which: (command, options) => client.MachineWhich(options?.PATH !== undefined ? { command, PATH: options.PATH } : { command }).pipe(
      Effect.orElseSucceed(() => null),
    ),
    userShell: client.MachineUserShell().pipe(Effect.orElseSucceed(() => null)),
    signalProcess: (pid, signal) => client.MachineSignalProcess({ pid, signal }).pipe(
      Effect.mapError((error) => error instanceof ProcessSignalError ? error : new ProcessSignalError({ pid, cause: error })),
    ),
  })

    const spawner = ProcessSpawner.of({
      spawn: Effect.fn("RemoteProcessSpawner.spawn")(function* (argv: ReadonlyArray<string>, options: ProcessSpawnOptions = {}) {
        const command = argv[0] ?? ""
        yield* Effect.annotateCurrentSpan("command", command)
        const fail = spawnFailure(command)
        const events = yield* client.ProcessSpawn({
          argv: [...argv],
          ...(options.cwd !== undefined ? { cwd: options.cwd } : {}),
          env: definedEnv(options.env ?? env.snapshot()),
          stdin: toWireStdin(options.stdin),
          stdout: options.stdout ?? "ignore",
          stderr: options.stderr ?? "ignore",
        }, { asQueue: true }).pipe(Effect.mapError(fail))
        const started = yield* Queue.take(events).pipe(Effect.mapError(fail))
        if (started._tag !== "Started") return yield* fail(new Error(`expected Started, got ${started._tag}`))
        const { handle } = started

        const stdout = yield* Queue.unbounded<Uint8Array, CommandSpawnError | Cause.Done>()
        const stderr = yield* Queue.unbounded<Uint8Array, CommandSpawnError | Cause.Done>()
        const exit = yield* Deferred.make<ProcessExit>()
        const running = yield* Ref.make(true)
        const finish = (result: ProcessExit) => Effect.gen(function* () {
          yield* Ref.set(running, false)
          yield* Queue.end(stdout)
          yield* Queue.end(stderr)
          yield* Deferred.succeed(exit, result)
        })

        yield* Effect.gen(function* () {
          while (true) {
            const event = yield* Queue.take(events)
            if (event._tag === "Stdout") yield* Queue.offer(stdout, event.data)
            else if (event._tag === "Stderr") yield* Queue.offer(stderr, event.data)
            else if (event._tag === "Exit") return yield* finish(fromWireExit(event.exit))
          }
        }).pipe(
          Effect.catch((error) => Effect.gen(function* () {
            const failure = fail(error)
            yield* Queue.fail(stdout, failure)
            yield* Queue.fail(stderr, failure)
            yield* finish(LOST_EXIT)
          })),
          Effect.forkScoped,
        )

        const isRunning = Ref.get(running)
        return {
          command,
          pid: started.pid ?? undefined,
          stdout: Stream.fromQueue(stdout),
          stderr: Stream.fromQueue(stderr),
          writeStdin: (chunk) => client.ProcessWriteStdin({ handle, data: typeof chunk === "string" ? encoder.encode(chunk) : chunk }).pipe(
            Effect.mapError(fail),
          ),
          endStdin: client.ProcessEndStdin({ handle }).pipe(Effect.ignore),
          exited: Deferred.await(exit),
          running: isRunning,
          kill: (signal) => Effect.flatMap(isRunning, (alive) => alive ? client.ProcessKill(signal ? { handle, signal } : { handle }).pipe(Effect.ignore) : Effect.void),
        } satisfies SpawnedProcess
      }),
      spawnDetached: (command, args) => client.ProcessSpawnDetached({ command, args: [...args] }).pipe(Effect.mapError(spawnFailure(command))),
    })

    const ptys = PtySpawner.of({
      get available() {
        return facts().ptyAvailable
      },
      spawn: Effect.fn("RemotePtySpawner.spawn")(function* (argv: ReadonlyArray<string>, options: PtySpawnOptions) {
        const command = argv[0] ?? ""
        yield* Effect.annotateCurrentSpan("command", command)
        const fail = spawnFailure(command)
        const events = yield* client.PtySpawn({
          argv: [...argv],
          ...(options.cwd !== undefined ? { cwd: options.cwd } : {}),
          env: definedEnv(options.env ?? env.snapshot()),
          cols: options.cols,
          rows: options.rows,
          ...(options.name !== undefined ? { name: options.name } : {}),
        }, { asQueue: true }).pipe(Effect.mapError(fail))
        const started = yield* Queue.take(events).pipe(Effect.mapError(fail))
        if (started._tag !== "Started") return yield* fail(new Error(`expected Started, got ${started._tag}`))
        const { handle } = started

        const exit = yield* Deferred.make<ProcessExit, CommandSpawnError>()
        const running = yield* Ref.make(true)
        yield* Effect.gen(function* () {
          while (true) {
            const event = yield* Queue.take(events)
            if (event._tag === "Data") options.onData(event.data)
            else if (event._tag === "Exit") return yield* Deferred.succeed(exit, fromWireExit(event.exit))
            else if (event._tag === "ExitFailed") return yield* Deferred.fail(exit, new CommandSpawnError({ command, message: event.message, cause: null }))
          }
        }).pipe(
          Effect.catch(() => Deferred.succeed(exit, LOST_EXIT)),
          Effect.ensuring(Ref.set(running, false)),
          Effect.forkScoped,
        )

        const isRunning = Ref.get(running)
        return {
          pid: started.pid ?? undefined,
          write: (data) => client.PtyWrite({ handle, data }).pipe(Effect.mapError(fail)),
          resize: (cols, rows) => client.PtyResize({ handle, cols, rows }).pipe(Effect.ignore),
          signalGroup: (signal) => client.PtySignalGroup({ handle, signal }).pipe(Effect.orElseSucceed(() => false)),
          kill: (signal) => Effect.flatMap(isRunning, (alive) => alive ? client.PtyKill(signal ? { handle, signal } : { handle }).pipe(Effect.ignore) : Effect.void),
          exited: Deferred.await(exit),
          running: isRunning,
          close: client.PtyClose({ handle }).pipe(Effect.ignore),
        } satisfies PtyProcess
      }),
    })

    const open = Effect.fn("RemoteFileSystem.open")(function* (
      path: string,
      options?: { readonly flag?: FileSystem.OpenFlag | undefined; readonly mode?: number | undefined; readonly noFollow?: boolean | undefined },
    ) {
      const opened = yield* unwireFs(client.FsOpen({ path, ...options }, { asQueue: true }))
      const { handle } = yield* unwireFs(Queue.take(opened))
      const readAlloc = (size: number, readOptions?: FileSystem.File.ReadOptions) =>
        unwireFs(client.FileRead({
          handle,
          length: size,
          ...(readOptions?.position !== undefined ? { position: String(readOptions.position) } : {}),
        })).pipe(Effect.map((bytes) => bytes.length === 0 ? Option.none() : Option.some(bytes)))
      const write = (buffer: Uint8Array) => unwireFs(client.FileWrite({ handle, data: buffer }))
      return {
        [FileSystem.FileTypeId]: FileSystem.FileTypeId,
        stat: unwireFs(client.FileStat({ handle })).pipe(Effect.map(fromWireFileInfo)),
        seek: (offset, from) => unwireFs(client.FileSeek({ handle, offset: String(offset), from })).pipe(Effect.map(BigInt)),
        sync: unwireFs(client.FileSync({ handle })),
        read: (buffer, readOptions) => readAlloc(buffer.length, readOptions).pipe(Effect.map(Option.match({
          onNone: () => 0,
          onSome: (bytes) => {
            buffer.set(bytes)
            return bytes.length
          },
        }))),
        readAlloc,
        truncate: (length) => unwireFs(client.FileTruncate(length !== undefined ? { handle, length } : { handle })),
        write,
        writeAll: (buffer) => Effect.gen(function* () {
          let offset = 0
          while (offset < buffer.length) offset += yield* write(buffer.subarray(offset))
        }),
      } as FileSystem.File
    })

    const stream = (path: string, options?: {
      readonly bytesToRead?: ByteSize.Input | undefined
      readonly chunkSize?: number | undefined
      readonly offset?: ByteSize.Input | undefined
    }) => client.FsStream({
      path,
      ...(options?.offset !== undefined ? { offset: String(options.offset) } : {}),
      ...(options?.bytesToRead !== undefined ? { bytesToRead: String(options.bytesToRead) } : {}),
      ...(options?.chunkSize !== undefined ? { chunkSize: options.chunkSize } : {}),
    }).pipe(Stream.mapError(platformErrorOf))

    const makeTempDirectory = (options?: { readonly directory?: string | undefined; readonly prefix?: string | undefined }) =>
      unwireFs(client.FsMakeTempDirectory({ ...options }))
    const makeTempFile = (options?: { readonly directory?: string | undefined; readonly prefix?: string | undefined; readonly suffix?: string | undefined }) =>
      unwireFs(client.FsMakeTempFile({ ...options }))
    const remove = (path: string, options?: { readonly recursive?: boolean | undefined; readonly force?: boolean | undefined }) =>
      unwireFs(client.FsRemove({ path, ...options }))

    const fs = FileSystem.make({
      access: (path, options) => unwireFs(client.FsAccess({ path, ...options })),
      copy: (from, to, options) => unwireFs(client.FsCopy({ from, to, ...options })),
      copyFile: (from, to) => unwireFs(client.FsCopyFile({ from, to })),
      chmod: (path, mode) => unwireFs(client.FsChmod({ path, mode })),
      chown: (path, uid, gid) => unwireFs(client.FsChown({ path, uid, gid })),
      glob: (pattern, options) => unwireFs(client.FsGlob({ pattern, ...options })).pipe(Effect.map((paths) => [...paths])),
      link: (from, to) => unwireFs(client.FsLink({ from, to })),
      makeDirectory: (path, options) => unwireFs(client.FsMakeDirectory({ path, ...options })),
      makeTempDirectory,
      makeTempDirectoryScoped: (options) => Effect.acquireRelease(makeTempDirectory(options), (path) => remove(path, { recursive: true }).pipe(Effect.ignore)),
      makeTempFile,
      makeTempFileScoped: (options) => Effect.acquireRelease(makeTempFile(options), (path) => remove(path).pipe(Effect.ignore)),
      open,
      readDirectory: (path, options) => unwireFs(client.FsReadDirectory({ path, ...options })).pipe(Effect.map((names) => [...names])),
      readFile: (path) => unwireFs(client.FsReadFile({ path })),
      readLink: (path) => unwireFs(client.FsReadLink({ path })),
      realPath: (path) => unwireFs(client.FsRealPath({ path })),
      remove,
      rename: (from, to) => unwireFs(client.FsRename({ from, to })),
      stat: (path) => unwireFs(client.FsStat({ path })).pipe(Effect.map(fromWireFileInfo)),
      symlink: (from, to) => unwireFs(client.FsSymlink({ from, to })),
      truncate: (path, length) => unwireFs(client.FsTruncate(length !== undefined ? { path, length } : { path })),
      utimes: (path, atime, mtime) => unwireFs(client.FsUtimes({
        path,
        atimeMs: typeof atime === "number" ? atime : atime.getTime(),
        mtimeMs: typeof mtime === "number" ? mtime : mtime.getTime(),
      })),
      watch: (path, options) => client.FsWatch({ path, ...(options?.recursive !== undefined ? { recursive: options.recursive } : {}) }).pipe(
        Stream.mapError(platformErrorOf),
        Stream.filter((event): event is FileSystem.WatchEvent => {
          if (event._tag !== "Ready") return true
          options?.onReady?.()
          return false
        }),
      ),
      writeFile: (path, data, options) => unwireFs(client.FsWriteFile({ path, data, ...options })),
    })

    const extras = FileSystemExtras.of({
      stat: (path) => unwireFs(client.FsxStat({ path })),
      lstat: (path) => unwireFs(client.FsxLstat({ path })),
      readDirectoryEntries: (path) => unwireFs(client.FsxReadDirectoryEntries({ path })),
      isExecutableFile: (path) => client.FsxIsExecutableFile({ path }).pipe(Effect.orElseSucceed(() => false)),
      readFileHead: (path, maxBytes) => unwireFs(client.FsxReadFileHead({ path, maxBytes })),
      // By name alone, as Bun's `Bun.file(path).type` is.
      contentType: (path) => mime.contentType(path.split("/").pop() ?? "") || "",
      fileBody: (path) => Effect.sync(() => Stream.toReadableStream(stream(path))),
    })

    return Context.make(Machine, machine).pipe(
      Context.add(ProcessSpawner, spawner),
      Context.add(PtySpawner, ptys),
      Context.add(FileSystem.FileSystem, FileSystem.FileSystem.of({ ...fs, stream })),
      Context.add(FileSystemExtras, extras),
    )
}
