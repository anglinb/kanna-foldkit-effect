import { Schema } from "effect"
import { Rpc, RpcGroup } from "effect/rpc"
import { CommandSpawnError } from "../process-spawner"
import { ProcessSignalError } from "../machine"

/**
 * The machine, as one RPC group: what a host (Kanna on Bun with its repos
 * elsewhere, or a Durable Object) asks of the runner on the machine that
 * holds the repos and the agent CLIs. The runner (`runner.ts`) answers it
 * with its own platform bindings; `client.ts` turns it back into the same
 * platform services, so no module above `platform/` can tell.
 *
 * Bytes are `Uint8Array`; the JSON serialization carries them as base64.
 * Big integers (inode, size) travel as decimal strings.
 */

// ── File system ─────────────────────────────────────────────────────────

/**
 * A `PlatformError` across the wire. `PlatformError` is a plain tagged
 * error, not a schema, so it is flattened here and rebuilt on the other
 * side with the same reason tag: callers catch `NotFound` by that tag.
 */
export class WirePlatformError extends Schema.TaggedError<WirePlatformError>()("@kanna/remote/WirePlatformError", {
  kind: Schema.Literals(["SystemError", "BadArgument"]),
  reason: Schema.String,
  module: Schema.String,
  method: Schema.String,
  description: Schema.optional(Schema.String),
  syscall: Schema.optional(Schema.String),
  pathOrDescriptor: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
}) {}

const NullableNumber = Schema.NullOr(Schema.Number)
const NullableBigInt = Schema.NullOr(Schema.String)

/** `FileSystem.File.Info`, with dates as epoch ms and bigints as strings. */
export const WireFileInfo = Schema.Struct({
  type: Schema.Literals(["File", "Directory", "SymbolicLink", "BlockDevice", "CharacterDevice", "FIFO", "Socket", "Unknown"]),
  mtimeMs: NullableNumber,
  atimeMs: NullableNumber,
  birthtimeMs: NullableNumber,
  dev: Schema.String,
  ino: NullableBigInt,
  mode: Schema.Number,
  nlink: NullableBigInt,
  uid: NullableNumber,
  gid: NullableNumber,
  rdev: NullableBigInt,
  size: Schema.String,
  blksize: NullableBigInt,
  blocks: NullableBigInt,
})
export type WireFileInfo = typeof WireFileInfo.Type

/** `FileSystemExtras.PreciseFileInfo`: already plain numbers. */
export const WirePreciseFileInfo = Schema.Struct({
  kind: Schema.Literals(["File", "Directory", "SymbolicLink", "Other"]),
  size: Schema.Number,
  mtimeMs: Schema.Number,
  ctimeMs: Schema.Number,
  atimeMs: Schema.Number,
  ino: Schema.Number,
  dev: Schema.Number,
  mode: Schema.Number,
})

const OpenFlag = Schema.Literals(["r", "r+", "w", "wx", "w+", "wx+", "a", "ax", "a+", "ax+"])

const fs = <const Tag extends string, Payload extends Schema.Struct.Fields, Success extends Schema.Top = typeof Schema.Void>(
  tag: Tag,
  payload: Payload,
  success?: Success,
) => Rpc.make(tag, { payload, success: (success ?? Schema.Void) as Success, error: WirePlatformError })

export const FsAccess = fs("FsAccess", { path: Schema.String, ok: Schema.optional(Schema.Boolean), readable: Schema.optional(Schema.Boolean), writable: Schema.optional(Schema.Boolean) })
export const FsCopy = fs("FsCopy", { from: Schema.String, to: Schema.String, overwrite: Schema.optional(Schema.Boolean), preserveTimestamps: Schema.optional(Schema.Boolean) })
export const FsCopyFile = fs("FsCopyFile", { from: Schema.String, to: Schema.String })
export const FsChmod = fs("FsChmod", { path: Schema.String, mode: Schema.Number })
export const FsChown = fs("FsChown", { path: Schema.String, uid: Schema.Number, gid: Schema.Number })
export const FsGlob = fs("FsGlob", { pattern: Schema.String, root: Schema.optional(Schema.String), exclude: Schema.optional(Schema.Array(Schema.String)) }, Schema.Array(Schema.String))
export const FsLink = fs("FsLink", { from: Schema.String, to: Schema.String })
export const FsMakeDirectory = fs("FsMakeDirectory", { path: Schema.String, recursive: Schema.optional(Schema.Boolean), mode: Schema.optional(Schema.Number) })
export const FsMakeTempDirectory = fs("FsMakeTempDirectory", { directory: Schema.optional(Schema.String), prefix: Schema.optional(Schema.String) }, Schema.String)
export const FsMakeTempFile = fs("FsMakeTempFile", { directory: Schema.optional(Schema.String), prefix: Schema.optional(Schema.String), suffix: Schema.optional(Schema.String) }, Schema.String)
export const FsReadDirectory = fs("FsReadDirectory", { path: Schema.String, recursive: Schema.optional(Schema.Boolean) }, Schema.Array(Schema.String))
export const FsReadFile = fs("FsReadFile", { path: Schema.String }, Schema.Uint8Array)
export const FsReadLink = fs("FsReadLink", { path: Schema.String }, Schema.String)
export const FsRealPath = fs("FsRealPath", { path: Schema.String }, Schema.String)
export const FsRemove = fs("FsRemove", { path: Schema.String, recursive: Schema.optional(Schema.Boolean), force: Schema.optional(Schema.Boolean) })
export const FsRename = fs("FsRename", { from: Schema.String, to: Schema.String })
export const FsStat = fs("FsStat", { path: Schema.String }, WireFileInfo)
export const FsSymlink = fs("FsSymlink", { from: Schema.String, to: Schema.String })
export const FsTruncate = fs("FsTruncate", { path: Schema.String, length: Schema.optional(Schema.Number) })
export const FsUtimes = fs("FsUtimes", { path: Schema.String, atimeMs: Schema.Number, mtimeMs: Schema.Number })
export const FsWriteFile = fs("FsWriteFile", { path: Schema.String, data: Schema.Uint8Array, flag: Schema.optional(OpenFlag), mode: Schema.optional(Schema.Number) })

/** A file's bytes as they are read: `FileSystem.stream`, and `FileSystemExtras.fileBody`. */
export const FsStream = Rpc.make("FsStream", {
  payload: { path: Schema.String, offset: Schema.optional(Schema.String), bytesToRead: Schema.optional(Schema.String), chunkSize: Schema.optional(Schema.Number) },
  success: Schema.Uint8Array,
  error: WirePlatformError,
  stream: true,
})

/** A change, or `Ready` (first, once the runner's watcher is installed: `WatchOptions.onReady`). */
export const WireWatchEvent = Schema.Struct({ _tag: Schema.Literals(["Ready", "Create", "Update", "Remove"]), path: Schema.String })
export const FsWatch = Rpc.make("FsWatch", {
  payload: { path: Schema.String, recursive: Schema.optional(Schema.Boolean) },
  success: WireWatchEvent,
  error: WirePlatformError,
  stream: true,
})

/**
 * An open file handle on the machine. The handle lives as long as this
 * stream: the first element names it, and interrupting the stream (the
 * host's scope closing) closes the file.
 */
export const FsOpen = Rpc.make("FsOpen", {
  payload: { path: Schema.String, flag: Schema.optional(OpenFlag), mode: Schema.optional(Schema.Number), noFollow: Schema.optional(Schema.Boolean) },
  success: Schema.Struct({ handle: Schema.Number }),
  error: WirePlatformError,
  stream: true,
})
/** Up to `length` bytes; fewer (or none) at the end of the file. */
export const FileRead = fs("FileRead", { handle: Schema.Number, length: Schema.Number, position: Schema.optional(Schema.String) }, Schema.Uint8Array)
export const FileWrite = fs("FileWrite", { handle: Schema.Number, data: Schema.Uint8Array }, Schema.Number)
export const FileSeek = fs("FileSeek", { handle: Schema.Number, offset: Schema.String, from: Schema.Literals(["start", "current"]) }, Schema.String)
export const FileStat = fs("FileStat", { handle: Schema.Number }, WireFileInfo)
export const FileTruncate = fs("FileTruncate", { handle: Schema.Number, length: Schema.optional(Schema.Number) })
export const FileSync = fs("FileSync", { handle: Schema.Number })

// ── FileSystemExtras ────────────────────────────────────────────────────

export const FsxStat = fs("FsxStat", { path: Schema.String }, WirePreciseFileInfo)
export const FsxLstat = fs("FsxLstat", { path: Schema.String }, WirePreciseFileInfo)
export const FsxReadDirectoryEntries = fs("FsxReadDirectoryEntries", { path: Schema.String }, Schema.Array(Schema.Struct({
  name: Schema.String,
  kind: Schema.Literals(["File", "Directory", "SymbolicLink", "Other"]),
})))
export const FsxIsExecutableFile = Rpc.make("FsxIsExecutableFile", { payload: { path: Schema.String }, success: Schema.Boolean })
export const FsxReadFileHead = fs("FsxReadFileHead", { path: Schema.String, maxBytes: Schema.Number }, Schema.Struct({ info: WirePreciseFileInfo, head: Schema.Uint8Array }))

// ── Machine ─────────────────────────────────────────────────────────────

const Env = Schema.Record(Schema.String, Schema.String)

/** The machine's fixed facts and its environment, read once when the host connects. */
export const MachineInfo = Rpc.make("MachineInfo", {
  success: Schema.Struct({
    platform: Schema.String,
    arch: Schema.String,
    homeDirectory: Schema.String,
    tmpDirectory: Schema.String,
    workingDirectory: Schema.String,
    hostname: Schema.String,
    env: Env,
  }),
})
/** Mirrors a `MachineEnv.set`/`unset` the host made on its copy. `value` absent unsets. */
export const MachineSetEnv = Rpc.make("MachineSetEnv", { payload: { name: Schema.String, value: Schema.optional(Schema.String) } })
export const MachineWhich = Rpc.make("MachineWhich", {
  payload: { command: Schema.String, PATH: Schema.optional(Schema.String) },
  success: Schema.NullOr(Schema.String),
})
export const MachineUserShell = Rpc.make("MachineUserShell", { success: Schema.NullOr(Schema.String) })
export const MachineSignalProcess = Rpc.make("MachineSignalProcess", {
  payload: { pid: Schema.Number, signal: Schema.Union([Schema.String, Schema.Literal(0)]) },
  error: ProcessSignalError,
})

// ── Processes ───────────────────────────────────────────────────────────

const Output = Schema.Literals(["ignore", "inherit", "pipe"])
export const WireStdin = Schema.Union([
  Schema.Literals(["ignore", "inherit", "pipe"]),
  Schema.Struct({ text: Schema.String }),
  Schema.Struct({ bytes: Schema.Uint8Array }),
])
export type WireStdin = typeof WireStdin.Type

export const WireExit = Schema.Struct({
  code: Schema.Number,
  exitCode: Schema.NullOr(Schema.Number),
  signalCode: Schema.NullOr(Schema.String),
})

export const ProcessEvent = Schema.Union([
  Schema.TaggedStruct("Started", { handle: Schema.Number, pid: Schema.NullOr(Schema.Number) }),
  Schema.TaggedStruct("Stdout", { data: Schema.Uint8Array }),
  Schema.TaggedStruct("Stderr", { data: Schema.Uint8Array }),
  Schema.TaggedStruct("Exit", { exit: WireExit }),
])
export type ProcessEvent = typeof ProcessEvent.Type

/**
 * A process on the machine, for as long as this stream runs: `Started`
 * first, then its output as it arrives, then `Exit` once both pipes have
 * drained. Interrupting the stream kills the process, as closing the
 * spawning scope does locally.
 */
export const ProcessSpawn = Rpc.make("ProcessSpawn", {
  payload: {
    argv: Schema.Array(Schema.String),
    cwd: Schema.optional(Schema.String),
    env: Schema.optional(Env),
    stdin: WireStdin,
    stdout: Output,
    stderr: Output,
  },
  success: ProcessEvent,
  error: CommandSpawnError,
  stream: true,
})
export const ProcessWriteStdin = Rpc.make("ProcessWriteStdin", {
  payload: { handle: Schema.Number, data: Schema.Uint8Array },
  error: CommandSpawnError,
})
export const ProcessEndStdin = Rpc.make("ProcessEndStdin", { payload: { handle: Schema.Number } })
export const ProcessKill = Rpc.make("ProcessKill", { payload: { handle: Schema.Number, signal: Schema.optional(Schema.String) } })
export const ProcessSpawnDetached = Rpc.make("ProcessSpawnDetached", {
  payload: { command: Schema.String, args: Schema.Array(Schema.String) },
  error: CommandSpawnError,
})

// ── PTYs ────────────────────────────────────────────────────────────────

export const PtyEvent = Schema.Union([
  Schema.TaggedStruct("Started", { handle: Schema.Number, pid: Schema.NullOr(Schema.Number) }),
  Schema.TaggedStruct("Data", { data: Schema.Uint8Array }),
  Schema.TaggedStruct("Exit", { exit: WireExit }),
  /** Waiting for the process failed (`PtyProcess.exited`'s error). */
  Schema.TaggedStruct("ExitFailed", { message: Schema.String }),
])
export type PtyEvent = typeof PtyEvent.Type

/** A process on a PTY, for as long as this stream runs; like `ProcessSpawn`. */
export const PtySpawn = Rpc.make("PtySpawn", {
  payload: {
    argv: Schema.Array(Schema.String),
    cwd: Schema.optional(Schema.String),
    env: Schema.optional(Env),
    cols: Schema.Number,
    rows: Schema.Number,
    name: Schema.optional(Schema.String),
  },
  success: PtyEvent,
  error: CommandSpawnError,
  stream: true,
})
export const PtyAvailable = Rpc.make("PtyAvailable", { success: Schema.Boolean })
export const PtyWrite = Rpc.make("PtyWrite", { payload: { handle: Schema.Number, data: Schema.String }, error: CommandSpawnError })
export const PtyResize = Rpc.make("PtyResize", { payload: { handle: Schema.Number, cols: Schema.Number, rows: Schema.Number } })
export const PtySignalGroup = Rpc.make("PtySignalGroup", { payload: { handle: Schema.Number, signal: Schema.String }, success: Schema.Boolean })
export const PtyKill = Rpc.make("PtyKill", { payload: { handle: Schema.Number, signal: Schema.optional(Schema.String) } })
export const PtyClose = Rpc.make("PtyClose", { payload: { handle: Schema.Number } })

export class MachineRpcs extends RpcGroup.make(
  FsAccess, FsCopy, FsCopyFile, FsChmod, FsChown, FsGlob, FsLink, FsMakeDirectory, FsMakeTempDirectory, FsMakeTempFile,
  FsReadDirectory, FsReadFile, FsReadLink, FsRealPath, FsRemove, FsRename, FsStat, FsSymlink, FsTruncate, FsUtimes,
  FsWriteFile, FsStream, FsWatch, FsOpen, FileRead, FileWrite, FileSeek, FileStat, FileTruncate, FileSync,
  FsxStat, FsxLstat, FsxReadDirectoryEntries, FsxIsExecutableFile, FsxReadFileHead,
  MachineInfo, MachineSetEnv, MachineWhich, MachineUserShell, MachineSignalProcess,
  ProcessSpawn, ProcessWriteStdin, ProcessEndStdin, ProcessKill, ProcessSpawnDetached,
  PtySpawn, PtyAvailable, PtyWrite, PtyResize, PtySignalGroup, PtyKill, PtyClose,
) {}

/** The runner's WebSocket endpoint. The token rides in the query: a Worker's `WebSocket` can't set headers. */
export const RUNNER_RPC_PATH = "/machine"
export const RUNNER_TOKEN_PARAM = "token"
