import path from "node:path"
import { Effect, Fiber, FileSystem, Schema, Stream } from "effect"
import { Machine } from "./platform/machine"
import { ProcessSpawner } from "./platform/process-spawner"

/**
 * The slice of an R2 bucket a home snapshot needs (declared here: the server
 * typechecks without Workers' types). Multipart, because a home is too big
 * to hold in a Durable Object's memory and a stream of unknown length can't
 * be put in one go.
 */
export interface SnapshotBucket {
  get(key: string): Promise<{ readonly body: ReadableStream<Uint8Array>; text(): Promise<string> } | null>
  put(key: string, value: string): Promise<unknown>
  delete(keys: string | Array<string>): Promise<void>
  list(options: { readonly prefix: string }): Promise<{ readonly objects: ReadonlyArray<{ readonly key: string }> }>
  createMultipartUpload(key: string): Promise<{
    uploadPart(partNumber: number, value: Uint8Array): Promise<{ readonly partNumber: number; readonly etag: string }>
    complete(parts: Array<{ readonly partNumber: number; readonly etag: string }>): Promise<unknown>
    abort(): Promise<void>
  }>
}

export class HomeSnapshotError extends Schema.TaggedError<HomeSnapshotError>()("@kanna/server/HomeSnapshotError", {
  operation: Schema.Literals(["snapshot", "restore"]),
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

/** R2 parts must be at least 5 MiB, except the last. */
const PART_BYTES = 8 * 1024 * 1024
/** What a restore writes, so a container that already has its home isn't restored over. */
const RESTORED_MARKER = ".kanna/restored-snapshot"
/** Caches that rebuild themselves: not worth carrying. */
const EXCLUDED = ["./.cache", "./.npm", "./.bun/install/cache"]

const latestKey = (prefix: string) => `${prefix}/latest`

const failure = (operation: HomeSnapshotError["operation"], message: string) => (cause: unknown) =>
  new HomeSnapshotError({ operation, message, cause })

/**
 * Saves the machine's home directory to `bucket` under `prefix`, as a
 * gzipped tar streamed straight from the machine into a multipart upload,
 * then points `<prefix>/latest` at it and drops the one before the previous.
 *
 * For a machine whose disk doesn't survive it sleeping (a Cloudflare
 * Container): its repos and CLI logins come back from here.
 */
export const snapshotHome = Effect.fn("HomeSnapshot.snapshot")(function* (bucket: SnapshotBucket, prefix: string) {
  const machine = yield* Machine
  const spawner = yield* ProcessSpawner
  const fs = yield* FileSystem.FileSystem
  // Only a home that was restored (or found to have nothing to restore) is
  // the real one: saving an empty disk would bury the last good snapshot.
  if (!(yield* fs.exists(path.join(machine.homeDirectory, RESTORED_MARKER)).pipe(Effect.orElseSucceed(() => false)))) {
    return yield* new HomeSnapshotError({ operation: "snapshot", message: "this home hasn't been restored yet", cause: null })
  }
  const key = `${prefix}/home-${Date.now()}.tar.gz`
  const startedAt = Date.now()
  const upload = yield* Effect.tryPromise({ try: () => bucket.createMultipartUpload(key), catch: failure("snapshot", "couldn't start the upload") })
  const parts: Array<{ readonly partNumber: number; readonly etag: string }> = []
  let size = 0

  const result = yield* Effect.gen(function* () {
    const tar = yield* spawner.spawn(
      ["tar", "-czf", "-", ...EXCLUDED.map((entry) => `--exclude=${entry}`), "-C", machine.homeDirectory, "."],
      { stdout: "pipe", stderr: "pipe" },
    )
    let pending: Array<Uint8Array> = []
    let pendingBytes = 0
    const flush = Effect.suspend(() => {
      if (pendingBytes === 0) return Effect.void
      const chunk = new Uint8Array(pendingBytes)
      let offset = 0
      for (const piece of pending) {
        chunk.set(piece, offset)
        offset += piece.length
      }
      pending = []
      pendingBytes = 0
      const partNumber = parts.length + 1
      return Effect.tryPromise({ try: () => upload.uploadPart(partNumber, chunk), catch: failure("snapshot", "a part failed to upload") }).pipe(
        Effect.map((part) => {
          parts.push(part)
          size += chunk.length
        }),
      )
    })
    const stderr = yield* Stream.runCollect(tar.stderr).pipe(Effect.map((chunks) => Buffer.concat([...chunks]).toString("utf8")), Effect.forkScoped)
    yield* Stream.runForEach(tar.stdout, (chunk) => {
      pending.push(chunk)
      pendingBytes += chunk.length
      return pendingBytes >= PART_BYTES ? flush : Effect.void
    }).pipe(Effect.mapError(failure("snapshot", "reading tar's output failed")))
    yield* flush
    const exit = yield* tar.exited
    // tar exits 1 when a file changed while it read it: the archive is still good.
    if (exit.code !== 0 && exit.code !== 1) {
      return yield* new HomeSnapshotError({ operation: "snapshot", message: `tar exited ${exit.code}: ${(yield* Fiber.join(stderr).pipe(Effect.orElseSucceed(() => ""))).trim().slice(0, 300)}`, cause: null })
    }
  }).pipe(Effect.scoped, Effect.exit)

  if (result._tag === "Failure" || parts.length === 0) {
    yield* Effect.promise(() => upload.abort().catch(() => {}))
    if (result._tag === "Failure") return yield* Effect.failCause(result.cause)
    return yield* new HomeSnapshotError({ operation: "snapshot", message: "tar produced nothing", cause: null })
  }
  yield* Effect.tryPromise({ try: () => upload.complete(parts), catch: failure("snapshot", "couldn't finish the upload") })
  yield* Effect.tryPromise({ try: () => bucket.put(latestKey(prefix), key), catch: failure("snapshot", "couldn't record the snapshot") })
  // Keep this one and the one before it.
  const older = yield* Effect.tryPromise({ try: () => bucket.list({ prefix: `${prefix}/home-` }), catch: failure("snapshot", "couldn't list snapshots") })
  const stale = older.objects.map((object) => object.key).sort().slice(0, -2)
  if (stale.length > 0) yield* Effect.promise(() => bucket.delete(stale).catch(() => {}))
  yield* Effect.logInfo(`saved the machine's home (${Math.round(size / 1024)} KiB in ${Date.now() - startedAt} ms)`)
  return { key, size }
})

/**
 * Puts the latest snapshot back into a home that doesn't have one yet (a
 * container that just started). A home that was restored before, or that
 * there's no snapshot for, is left as it is.
 */
export const restoreHome = Effect.fn("HomeSnapshot.restore")(function* (bucket: SnapshotBucket, prefix: string) {
  const machine = yield* Machine
  const fs = yield* FileSystem.FileSystem
  const spawner = yield* ProcessSpawner
  const marker = path.join(machine.homeDirectory, RESTORED_MARKER)
  if (yield* fs.exists(marker).pipe(Effect.orElseSucceed(() => false))) return "already" as const
  const writeMarker = (value: string) => fs.makeDirectory(path.dirname(marker), { recursive: true }).pipe(
    Effect.andThen(fs.writeFileString(marker, `${value}\n`)),
    Effect.mapError(failure("restore", "couldn't mark the home restored")),
  )

  const pointer = yield* Effect.tryPromise({ try: () => bucket.get(latestKey(prefix)), catch: failure("restore", "couldn't read the latest snapshot") })
  if (!pointer) {
    yield* writeMarker("none")
    return "none" as const
  }
  const key = (yield* Effect.promise(() => pointer.text())).trim()
  const object = yield* Effect.tryPromise({ try: () => bucket.get(key), catch: failure("restore", "couldn't read the snapshot") })
  if (!object) return yield* new HomeSnapshotError({ operation: "restore", message: `snapshot ${key} is missing`, cause: null })

  const startedAt = Date.now()
  yield* Effect.gen(function* () {
    const tar = yield* spawner.spawn(["tar", "-xzf", "-", "-C", machine.homeDirectory], { stdin: "pipe", stderr: "pipe" })
    const stderr = yield* Stream.runCollect(tar.stderr).pipe(Effect.map((chunks) => Buffer.concat([...chunks]).toString("utf8")), Effect.forkScoped)
    // Batched to ~1 MiB a write: each write is a round trip to the machine.
    let pending: Array<Uint8Array> = []
    let pendingBytes = 0
    const flush = Effect.suspend(() => {
      if (pendingBytes === 0) return Effect.void
      const chunk = Buffer.concat(pending)
      pending = []
      pendingBytes = 0
      return tar.writeStdin(chunk)
    })
    yield* Stream.fromReadableStream({ evaluate: () => object.body, onError: failure("restore", "reading the snapshot failed") }).pipe(
      Stream.runForEach((chunk) => {
        pending.push(chunk)
        pendingBytes += chunk.length
        return pendingBytes >= 1024 * 1024 ? flush : Effect.void
      }),
    )
    yield* flush
    yield* tar.endStdin
    const exit = yield* tar.exited
    if (exit.code !== 0) {
      return yield* new HomeSnapshotError({ operation: "restore", message: `tar exited ${exit.code}: ${(yield* Fiber.join(stderr).pipe(Effect.orElseSucceed(() => ""))).trim().slice(0, 300)}`, cause: null })
    }
  }).pipe(Effect.scoped, Effect.mapError((error) => error instanceof HomeSnapshotError ? error : failure("restore", "untar failed")(error)))
  yield* writeMarker(key)
  yield* Effect.logInfo(`restored the machine's home from ${key} in ${Date.now() - startedAt} ms`)
  return "restored" as const
})
