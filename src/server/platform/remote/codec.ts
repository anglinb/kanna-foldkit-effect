import { ByteSize, Effect, FileSystem, Option, PlatformError } from "effect"
import type { ProcessExit, ProcessSignal } from "../process-spawner"
import { WirePlatformError, type WireFileInfo } from "./protocol"

// The runner sends these; the host rebuilds them. Each pair round-trips.

export function toWirePlatformError(error: PlatformError.PlatformError): WirePlatformError {
  const reason = error.reason
  if (reason._tag === "BadArgument") {
    return new WirePlatformError({
      kind: "BadArgument",
      reason: "BadArgument",
      module: reason.module,
      method: reason.method,
      ...(reason.description !== undefined ? { description: reason.description } : {}),
    })
  }
  return new WirePlatformError({
    kind: "SystemError",
    reason: reason._tag,
    module: reason.module,
    method: reason.method,
    ...(reason.description !== undefined ? { description: reason.description } : {}),
    ...(reason.syscall !== undefined ? { syscall: reason.syscall } : {}),
    ...(reason.pathOrDescriptor !== undefined ? { pathOrDescriptor: reason.pathOrDescriptor } : {}),
  })
}

export function fromWirePlatformError(error: WirePlatformError): PlatformError.PlatformError {
  if (error.kind === "BadArgument") {
    return PlatformError.badArgument({ module: error.module, method: error.method, description: error.description })
  }
  return PlatformError.systemError({
    _tag: error.reason as PlatformError.SystemErrorTag,
    module: error.module,
    method: error.method,
    description: error.description,
    syscall: error.syscall,
    pathOrDescriptor: error.pathOrDescriptor,
  })
}

/** Runner side: an effect over the local file system, failing with the wire error. */
export const wireFs = <A, R>(effect: Effect.Effect<A, PlatformError.PlatformError, R>) =>
  Effect.mapError(effect, toWirePlatformError)

/**
 * Host side: a remote call's failure as a `PlatformError`. A transport
 * failure (the runner went away mid-call) becomes the `Unknown` one a disk
 * failure would be, so callers' existing handling covers it.
 */
export function platformErrorOf(error: unknown): PlatformError.PlatformError {
  if (error instanceof WirePlatformError) return fromWirePlatformError(error)
  return PlatformError.systemError({
    _tag: "Unknown",
    module: "RemoteMachine",
    method: "rpc",
    description: error instanceof Error ? error.message : String(error),
    cause: error,
  })
}

/** Host side: a remote call, failing with the rebuilt `PlatformError`. */
export const unwireFs = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.mapError(effect, platformErrorOf)

const millis = (date: Option.Option<Date>) => Option.match(date, { onNone: () => null, onSome: (value) => value.getTime() })
const bigintText = (value: Option.Option<bigint>) => Option.match(value, { onNone: () => null, onSome: String })
const date = (ms: number | null) => ms === null ? Option.none() : Option.some(new Date(ms))
const bigint = (text: string | null) => text === null ? Option.none() : Option.some(BigInt(text))

export function toWireFileInfo(info: FileSystem.File.Info): WireFileInfo {
  return {
    type: info.type,
    mtimeMs: millis(info.mtime),
    atimeMs: millis(info.atime),
    birthtimeMs: millis(info.birthtime),
    dev: String(info.dev),
    ino: bigintText(info.ino),
    mode: info.mode,
    nlink: bigintText(info.nlink),
    uid: Option.getOrNull(info.uid),
    gid: Option.getOrNull(info.gid),
    rdev: bigintText(info.rdev),
    size: String(ByteSize.toBigInt(info.size)),
    blksize: Option.match(info.blksize, { onNone: () => null, onSome: (size) => String(ByteSize.toBigInt(size)) }),
    blocks: bigintText(info.blocks),
  }
}

export function fromWireFileInfo(info: WireFileInfo): FileSystem.File.Info {
  return {
    type: info.type,
    mtime: date(info.mtimeMs),
    atime: date(info.atimeMs),
    birthtime: date(info.birthtimeMs),
    dev: BigInt(info.dev),
    ino: bigint(info.ino),
    mode: info.mode,
    nlink: bigint(info.nlink),
    uid: Option.fromNullishOr(info.uid),
    gid: Option.fromNullishOr(info.gid),
    rdev: bigint(info.rdev),
    size: ByteSize.bytes(BigInt(info.size)),
    blksize: info.blksize === null ? Option.none() : Option.some(ByteSize.bytes(BigInt(info.blksize))),
    blocks: bigint(info.blocks),
  }
}

export const toWireExit = (exit: ProcessExit) => ({ code: exit.code, exitCode: exit.exitCode, signalCode: exit.signalCode })
export const fromWireExit = (exit: { readonly code: number; readonly exitCode: number | null; readonly signalCode: string | null }): ProcessExit => ({
  code: exit.code,
  exitCode: exit.exitCode,
  signalCode: exit.signalCode as ProcessSignal | null,
})

/** Drops undefined values: the wire's env is `Record<string, string>`. */
export function definedEnv(env: Record<string, string | undefined>): Record<string, string> {
  const result: Record<string, string> = {}
  for (const [name, value] of Object.entries(env)) if (value !== undefined) result[name] = value
  return result
}
