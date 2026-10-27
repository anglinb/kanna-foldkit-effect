import { createHash } from "node:crypto"
import { Context, Effect, Exit, Layer, Scope } from "effect"
import type { AuthSessionStore } from "./auth"
import { makeR2UploadBlobs, type R2BucketLike } from "./upload-blobs-r2"
import { restoreHome, snapshotHome, type SnapshotBucket } from "./home-snapshot"
import { runKanna } from "./effect/bridge"
import { DURABLE_OBJECT_ROOT, makeDurableObjectPlatform, PRIMARY_PORT, type HibernationContext, type WorkersWebSocket } from "./platform/cloudflare"
import { Machine } from "./platform/machine"
import type { RunnerEndpoint } from "./platform/remote/connection"
import type { SqlRunner } from "./platform/sqlite-file-system"
import { startKannaServerEffect } from "./server"

export interface DurableObjectKannaOptions {
  /** The object's `ctx.storage.sql`: Kanna's state lives here. */
  readonly sql: SqlRunner
  /** The object's state (`ctx`), whose hibernation API holds the browser sockets. */
  readonly hibernation: HibernationContext
  /** The machine: a runner holding the repos and agent CLIs. */
  readonly runner: RunnerEndpoint
  /** Required: a Durable Object is on the public internet. */
  readonly password: string
  /** The runner's home before it first connects (default `/home/kanna`, the runner image's). */
  readonly defaultHome?: string
  /** Where uploads are kept, so attachments show while the runner is offline. */
  readonly uploads?: R2BucketLike
  /**
   * For a runner whose disk is wiped when it sleeps (a Cloudflare Container):
   * its home is saved here and put back on each fresh start.
   */
  readonly homeSnapshots?: { readonly bucket: SnapshotBucket; readonly prefix: string }
}

export interface DurableObjectKanna {
  /** Answers one request the object's `fetch` received. */
  readonly serve: (request: Request) => Promise<Response>
  /** A browser socket's frame (`webSocketMessage`). */
  readonly message: (ws: WorkersWebSocket, data: string | ArrayBuffer) => Promise<void>
  /** A browser socket closed (`webSocketClose`, `webSocketError`). */
  readonly close: (ws: WorkersWebSocket, code: number, reason: string) => void
  /** Whether sleeping now would cut something off, and when the next schedule is due. */
  readonly activity: () => Promise<{ readonly busy: boolean; readonly nextScheduledAt: number | null }>
  /**
   * Stops the server so the object can sleep: its timers, its runner
   * connection, everything in memory. Browser sockets stay open; the next
   * server binds them again.
   */
  readonly stop: () => Promise<void>
}

/**
 * Starts Kanna's server inside a Durable Object: the composition root for
 * the hosted build, as `server-bun.ts` is for Bun. The same
 * `startKannaServerEffect`, on the Durable Object platform.
 *
 * The built client is served by the Worker's static assets, not by the
 * server, so no `assetDirs`. Cloud features (kanna.sh pairing, public
 * share tunnels) are off: the object is already hosted.
 */
export async function startKannaInDurableObject(options: DurableObjectKannaOptions): Promise<DurableObjectKanna> {
  const snapshots = options.homeSnapshots
  const { http, layer } = makeDurableObjectPlatform({
    sql: options.sql,
    hibernation: options.hibernation,
    runner: options.runner,
    ...(options.defaultHome ? { defaultHome: options.defaultHome } : {}),
    ...(snapshots ? { onConnected: (machine) => restoreHome(snapshots.bucket, snapshots.prefix).pipe(Effect.provideContext(machine)) } : {}),
  })
  const scope = await runKanna(Scope.make())
  const close = () => runKanna(Scope.close(scope, Exit.void))
  const startedAt = Date.now()
  try {
    const platform = await runKanna(Layer.buildWithScope(layer, scope))
    const machineHome = Context.get(platform, Machine).homeDirectory
    const started = await runKanna(startKannaServerEffect({
      port: PRIMARY_PORT,
      host: "0.0.0.0",
      strictPort: true,
      dataDir: `${DURABLE_OBJECT_ROOT}/data`,
      keybindingsPath: `${DURABLE_OBJECT_ROOT}/keybindings.json`,
      // On the machine: the CLIs there read them.
      accountsDir: `${machineHome}/.kanna/accounts`,
      password: options.password,
      // Sessions outlive the server: it stops every time the object sleeps.
      authSessions: sqliteAuthSessions(options.sql),
      // The password is the only gate in front of the object.
      loginRateLimit: { windowMs: 15 * 60_000, maxFailuresPerClient: 10, maxFailuresTotal: 100 },
      // The runner took its login shell's PATH when it started.
      inheritShellPath: false,
      discoverInBackground: true,
      ...(options.uploads ? { uploadBlobs: makeR2UploadBlobs(options.uploads) } : {}),
      // The Worker in front terminates TLS and forwards the request as-is.
      trustProxy: true,
      cloudEnabled: false,
      disabledProviders: ["pi"],
    }).pipe(Effect.provideContext(platform)))
    const saveHome = () => snapshots
      ? runKanna(snapshotHome(snapshots.bucket, snapshots.prefix).pipe(
        Effect.provideContext(platform),
        Effect.catchCause((cause) => Effect.logWarning("couldn't save the machine's home", cause)),
        Effect.asVoid,
      ))
      : Promise.resolve()
    // While awake, the home is saved every so often: the container can be
    // lost without warning, and this bounds what goes with it.
    if (snapshots) {
      await runKanna(Effect.promise(saveHome).pipe(
        Effect.delay(HOME_SNAPSHOT_INTERVAL),
        Effect.forever,
        Effect.forkIn(scope),
      ))
    }
    // Sockets that stayed open while the object slept get this server now,
    // so its pushes reach them without waiting for them to speak first.
    await http.restoreSockets()
    console.log(`[kanna] hosted server started in ${Date.now() - startedAt}ms`)
    return {
      serve: (request) => http.serve(request),
      message: (ws, data) => http.message(ws, data),
      close: (ws, code, reason) => http.close(ws, code, reason),
      activity: () => started.activity(),
      stop: async () => {
        try {
          // Before the runner connection goes: this is the last chance to
          // save the home before the container sleeps and loses its disk.
          await saveHome()
          await started.stop()
        } finally {
          await close()
        }
      },
    }
  } catch (error) {
    await close()
    throw error
  }
}


const HOME_SNAPSHOT_INTERVAL = "15 minutes"

/** Signed-in sessions in the object's SQLite, by the token's hash (never the token itself). */
function sqliteAuthSessions(sql: SqlRunner): AuthSessionStore {
  sql.exec("CREATE TABLE IF NOT EXISTS kanna_auth_sessions (token_hash TEXT PRIMARY KEY, created_at INTEGER NOT NULL)")
  const hash = (token: string) => createHash("sha256").update(token).digest("hex")
  return {
    has: (token) => [...sql.exec("SELECT 1 FROM kanna_auth_sessions WHERE token_hash = ?", hash(token))].length > 0,
    add: (token) => {
      sql.exec("INSERT OR IGNORE INTO kanna_auth_sessions VALUES (?, ?)", hash(token), Date.now())
    },
    delete: (token) => {
      sql.exec("DELETE FROM kanna_auth_sessions WHERE token_hash = ?", hash(token))
    },
  }
}
