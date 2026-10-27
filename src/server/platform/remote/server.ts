import { Cause, Effect, FileSystem, Option, Queue, Stream } from "effect"
import { FileSystemExtras } from "../file-system-extras"
import { Machine } from "../machine"
import { CommandSpawnError, ProcessSpawner, type ProcessSignal, type ProcessStdin, type SpawnedProcess } from "../process-spawner"
import { PtySpawner, type PtyProcess } from "../pty"
import { definedEnv, toWireExit, toWireFileInfo, toWirePlatformError, wireFs } from "./codec"
import { MachineRpcs, type ProcessEvent, type PtyEvent, type WireStdin, WireWatchEvent } from "./protocol"

/**
 * The runner's half of `MachineRpcs`: each call answered with this
 * machine's own platform services. Nothing here is Bun-specific; the runner
 * entry (`runner.ts`) provides the Bun bindings.
 *
 * Processes, PTYs and open files are keyed by a handle the host gets in the
 * first element of their stream. Each lives exactly as long as that stream:
 * when the host closes its scope (or its connection drops), the RPC server
 * interrupts the stream and the resource's own finalizer kills or closes it.
 */
export const MachineRpcHandlers = MachineRpcs.toLayer(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const extras = yield* FileSystemExtras
  const machine = yield* Machine
  const spawner = yield* ProcessSpawner
  const ptys = yield* PtySpawner

  let nextHandle = 1
  const processes = new Map<number, SpawnedProcess>()
  const terminals = new Map<number, PtyProcess>()
  const files = new Map<number, FileSystem.File>()

  /** Registers `value` under a fresh handle until the current scope closes. */
  const register = <A>(registry: Map<number, A>, value: A) => Effect.gen(function* () {
    const handle = nextHandle++
    registry.set(handle, value)
    yield* Effect.addFinalizer(() => Effect.sync(() => registry.delete(handle)))
    return handle
  })

  const lookup = <A>(registry: Map<number, A>, handle: number, what: string) =>
    Effect.suspend(() => {
      const value = registry.get(handle)
      return value === undefined
        ? Effect.fail(new CommandSpawnError({ command: what, message: `No ${what} with handle ${handle} (it has exited)`, cause: null }))
        : Effect.succeed(value)
    })

  const file = (handle: number) => Effect.suspend(() => {
    const open = files.get(handle)
    return open
      ? Effect.succeed(open)
      : Effect.die(new Error(`No open file with handle ${handle}`))
  })

  const stdinOf = (stdin: WireStdin): ProcessStdin =>
    typeof stdin === "string" ? stdin : "text" in stdin ? stdin.text : stdin.bytes

  return MachineRpcs.of({
    FsAccess: ({ path, ...options }) => wireFs(fs.access(path, options)),
    FsCopy: ({ from, to, ...options }) => wireFs(fs.copy(from, to, options)),
    FsCopyFile: ({ from, to }) => wireFs(fs.copyFile(from, to)),
    FsChmod: ({ path, mode }) => wireFs(fs.chmod(path, mode)),
    FsChown: ({ path, uid, gid }) => wireFs(fs.chown(path, uid, gid)),
    FsGlob: ({ pattern, ...options }) => wireFs(fs.glob(pattern, options)),
    FsLink: ({ from, to }) => wireFs(fs.link(from, to)),
    FsMakeDirectory: ({ path, ...options }) => wireFs(fs.makeDirectory(path, options)),
    FsMakeTempDirectory: (options) => wireFs(fs.makeTempDirectory(options)),
    FsMakeTempFile: (options) => wireFs(fs.makeTempFile(options)),
    FsReadDirectory: ({ path, ...options }) => wireFs(fs.readDirectory(path, options)),
    FsReadFile: ({ path }) => wireFs(fs.readFile(path)),
    FsReadLink: ({ path }) => wireFs(fs.readLink(path)),
    FsRealPath: ({ path }) => wireFs(fs.realPath(path)),
    FsRemove: ({ path, ...options }) => wireFs(fs.remove(path, options)),
    FsRename: ({ from, to }) => wireFs(fs.rename(from, to)),
    FsStat: ({ path }) => wireFs(Effect.map(fs.stat(path), toWireFileInfo)),
    FsSymlink: ({ from, to }) => wireFs(fs.symlink(from, to)),
    FsTruncate: ({ path, length }) => wireFs(fs.truncate(path, length)),
    FsUtimes: ({ path, atimeMs, mtimeMs }) => wireFs(fs.utimes(path, atimeMs, mtimeMs)),
    FsWriteFile: ({ path, data, ...options }) => wireFs(fs.writeFile(path, data, options)),
    FsStream: ({ path, offset, bytesToRead, chunkSize }) => fs.stream(path, {
      ...(offset !== undefined ? { offset: BigInt(offset) } : {}),
      ...(bytesToRead !== undefined ? { bytesToRead: BigInt(bytesToRead) } : {}),
      ...(chunkSize !== undefined ? { chunkSize } : {}),
    }).pipe(Stream.mapError(toWirePlatformError)),
    FsWatch: ({ path, recursive }) => Stream.unwrap(Effect.gen(function* () {
      // The host's `onReady` fires when this `Ready` arrives: after the
      // runner's own watcher is installed, not just when the call went out.
      const ready = yield* Queue.unbounded<typeof WireWatchEvent.Type, Cause.Done>()
      const changes = fs.watch(path, { recursive, onReady: () => Queue.offerUnsafe(ready, { _tag: "Ready", path }) }).pipe(
        Stream.map((event): typeof WireWatchEvent.Type => ({ _tag: event._tag, path: event.path })),
        Stream.mapError(toWirePlatformError),
      )
      return Stream.merge(Stream.fromQueue(ready), changes, { haltStrategy: "right" })
    })),
    FsOpen: ({ path, ...options }) => Stream.unwrap(wireFs(Effect.gen(function* () {
      const opened = yield* fs.open(path, options)
      const handle = yield* register(files, opened)
      // The handle stays open until the host interrupts this stream.
      return Stream.concat(Stream.make({ handle }), Stream.never)
    }))),
    FileRead: ({ handle, length, position }) => wireFs(Effect.gen(function* () {
      const open = yield* file(handle)
      const read = yield* open.readAlloc(length, position !== undefined ? { position: BigInt(position) } : undefined)
      return Option.getOrElse(read, () => new Uint8Array(0))
    })),
    FileWrite: ({ handle, data }) => wireFs(Effect.flatMap(file(handle), (open) => open.write(data))),
    FileSeek: ({ handle, offset, from }) => wireFs(Effect.flatMap(file(handle), (open) => Effect.map(open.seek(BigInt(offset), from), String))),
    FileStat: ({ handle }) => wireFs(Effect.flatMap(file(handle), (open) => Effect.map(open.stat, toWireFileInfo))),
    FileTruncate: ({ handle, length }) => wireFs(Effect.flatMap(file(handle), (open) => open.truncate(length))),
    FileSync: ({ handle }) => wireFs(Effect.flatMap(file(handle), (open) => open.sync)),

    FsxStat: ({ path }) => wireFs(extras.stat(path)),
    FsxLstat: ({ path }) => wireFs(extras.lstat(path)),
    FsxReadDirectoryEntries: ({ path }) => wireFs(extras.readDirectoryEntries(path)),
    FsxIsExecutableFile: ({ path }) => extras.isExecutableFile(path),
    FsxReadFileHead: ({ path, maxBytes }) => wireFs(extras.readFileHead(path, maxBytes)),

    MachineInfo: () => Effect.sync(() => ({
      platform: machine.platform,
      arch: machine.arch,
      homeDirectory: machine.homeDirectory,
      tmpDirectory: machine.tmpDirectory,
      workingDirectory: machine.workingDirectory,
      hostname: machine.hostname(),
      env: definedEnv(machine.env.snapshot()),
    })),
    MachineSetEnv: ({ name, value }) => Effect.sync(() => {
      if (value === undefined) machine.env.unset(name)
      else machine.env.set(name, value)
    }),
    MachineWhich: ({ command, PATH }) => machine.which(command, PATH !== undefined ? { PATH } : undefined),
    MachineUserShell: () => machine.userShell,
    MachineSignalProcess: ({ pid, signal }) => machine.signalProcess(pid, signal as ProcessSignal | 0),

    ProcessSpawn: ({ argv, cwd, env, stdin, stdout, stderr }) => Stream.unwrap(Effect.gen(function* () {
      const proc = yield* spawner.spawn(argv, {
        ...(cwd !== undefined ? { cwd } : {}),
        ...(env !== undefined ? { env } : {}),
        stdin: stdinOf(stdin),
        stdout,
        stderr,
      })
      const handle = yield* register(processes, proc)
      const output = Stream.merge(
        proc.stdout.pipe(Stream.map((data): ProcessEvent => ({ _tag: "Stdout", data }))),
        proc.stderr.pipe(Stream.map((data): ProcessEvent => ({ _tag: "Stderr", data }))),
      )
      // Exit goes last, after both pipes drained, so the host never sees an
      // exit before the output that preceded it.
      return Stream.succeed<ProcessEvent>({ _tag: "Started", handle, pid: proc.pid ?? null }).pipe(
        Stream.concat(output),
        Stream.concat(Stream.fromEffect(Effect.map(proc.exited, (exit): ProcessEvent => ({ _tag: "Exit", exit: toWireExit(exit) })))),
      )
    })),
    ProcessWriteStdin: ({ handle, data }) => Effect.flatMap(lookup(processes, handle, "process"), (proc) => proc.writeStdin(data)),
    ProcessEndStdin: ({ handle }) => Effect.suspend(() => processes.get(handle)?.endStdin ?? Effect.void),
    ProcessKill: ({ handle, signal }) => Effect.suspend(() => processes.get(handle)?.kill(signal as ProcessSignal | undefined) ?? Effect.void),
    ProcessSpawnDetached: ({ command, args }) => spawner.spawnDetached(command, args),

    PtySpawn: ({ argv, cwd, env, cols, rows, name }) => Stream.unwrap(Effect.gen(function* () {
      const events = yield* Queue.unbounded<PtyEvent, Cause.Done>()
      const pty = yield* ptys.spawn(argv, {
        ...(cwd !== undefined ? { cwd } : {}),
        ...(env !== undefined ? { env } : {}),
        ...(name !== undefined ? { name } : {}),
        cols,
        rows,
        // Called outside any fiber, as each read lands.
        onData: (data) => {
          Queue.offerUnsafe(events, { _tag: "Data", data })
        },
      })
      const handle = yield* register(terminals, pty)
      yield* pty.exited.pipe(
        Effect.map((exit): PtyEvent => ({ _tag: "Exit", exit: toWireExit(exit) })),
        Effect.catch((error) => Effect.succeed<PtyEvent>({ _tag: "ExitFailed", message: error.message })),
        Effect.flatMap((event) => Queue.offer(events, event)),
        Effect.andThen(Queue.end(events)),
        Effect.forkScoped,
      )
      return Stream.succeed<PtyEvent>({ _tag: "Started", handle, pid: pty.pid ?? null }).pipe(Stream.concat(Stream.fromQueue(events)))
    })),
    PtyAvailable: () => Effect.succeed(ptys.available),
    PtyWrite: ({ handle, data }) => Effect.flatMap(lookup(terminals, handle, "terminal"), (pty) => pty.write(data)),
    PtyResize: ({ handle, cols, rows }) => Effect.suspend(() => terminals.get(handle)?.resize(cols, rows) ?? Effect.void),
    PtySignalGroup: ({ handle, signal }) => Effect.suspend(() => terminals.get(handle)?.signalGroup(signal as ProcessSignal) ?? Effect.succeed(false)),
    PtyKill: ({ handle, signal }) => Effect.suspend(() => terminals.get(handle)?.kill(signal as ProcessSignal | undefined) ?? Effect.void),
    PtyClose: ({ handle }) => Effect.suspend(() => terminals.get(handle)?.close ?? Effect.void),
  })
}))
