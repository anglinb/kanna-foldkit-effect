import { DurableObject } from "cloudflare:workers"
import { TEXT_PING, TEXT_PONG } from "../src/shared/protocol"
import type { DurableObjectKanna } from "../src/server/server-durable-object"

/**
 * Hosted Kanna on Cloudflare: this Worker serves the built client as static
 * assets and hands everything the server answers (the WebSocket, /api, auth)
 * to one Durable Object, where Kanna's server runs with its state in the
 * object's SQLite. The machine — repos, agent CLIs, terminals — is a runner
 * in a container, reached over `KANNA_RUNNER_URL`.
 */
export interface Env {
  readonly KANNA: DurableObjectNamespace<KannaHost>
  readonly ASSETS: Fetcher
  readonly KANNA_RUNNER_URL: string
  readonly KANNA_RUNNER_TOKEN: string
  readonly KANNA_PASSWORD: string
  /** The runner's home directory, used until it first connects. Default /home/kanna (the runner image). */
  readonly KANNA_RUNNER_HOME?: string
  /** Seconds without a request or a message before an idle object sleeps. Default 300. */
  readonly KANNA_IDLE_SECONDS?: string
}

/** Paths Kanna's server answers; everything else is the client bundle. */
const SERVER_PATHS = /^\/(?:ws|health|auth\/|api\/|__cloud\/)/

const IDLE_CHECK_MS = 30_000

/**
 * The object runs Kanna while someone is using it and sleeps otherwise, so
 * it isn't billed for an open tab nobody is looking at.
 *
 * - Awake: a request, a socket message or an alarm starts the server (about
 *   100 ms; the runner connects in the background).
 * - Idle: no request or message for `KANNA_IDLE_SECONDS`, and no turn or
 *   terminal running. The object stops the server, which closes its runner
 *   connection and every timer, and sets an alarm for the next schedule.
 *   Browser sockets stay open: the runtime holds them (hibernation API) and
 *   answers their heartbeat itself (`setWebSocketAutoResponse`), so an open
 *   tab doesn't wake the object.
 * - The next message wakes it: a new server binds the sockets again and
 *   replays their subscriptions (see `restoreSockets`), then handles it.
 */
export class KannaHost extends DurableObject<Env> {
  private kanna: Promise<DurableObjectKanna> | null = null
  private parking: Promise<void> | null = null
  private lastActivity = Date.now()
  private idleTimer: ReturnType<typeof setTimeout> | null = null

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair(TEXT_PING, TEXT_PONG))
  }

  private async start() {
    // A park in progress finishes first: two servers must never share the
    // object's state or the runner.
    if (this.parking) await this.parking
    // Loaded here, not at the top: the server's modules generate ids and arm
    // timers as they load, which workerd allows only inside a handler.
    this.kanna ??= import("../src/server/server-durable-object").then(({ startKannaInDurableObject }) => {
      for (const name of ["KANNA_RUNNER_URL", "KANNA_RUNNER_TOKEN", "KANNA_PASSWORD"] as const) {
        if (!this.env[name]) throw new Error(`${name} is not set`)
      }
      return startKannaInDurableObject({
        sql: this.ctx.storage.sql,
        hibernation: this.ctx,
        runner: { url: this.env.KANNA_RUNNER_URL, token: this.env.KANNA_RUNNER_TOKEN },
        password: this.env.KANNA_PASSWORD,
        ...(this.env.KANNA_RUNNER_HOME ? { defaultHome: this.env.KANNA_RUNNER_HOME } : {}),
      })
    }).then((kanna) => {
      this.armIdleCheck()
      return kanna
    }).catch((error: unknown) => {
      // The next request tries again.
      this.kanna = null
      throw error
    })
    return this.kanna
  }

  private touch() {
    this.lastActivity = Date.now()
  }

  private idleMs() {
    const seconds = Number(this.env.KANNA_IDLE_SECONDS ?? 300)
    return (Number.isFinite(seconds) && seconds > 0 ? seconds : 300) * 1000
  }

  private armIdleCheck() {
    if (this.idleTimer) clearTimeout(this.idleTimer)
    this.idleTimer = setTimeout(() => void this.checkIdle(), Math.min(IDLE_CHECK_MS, this.idleMs()))
  }

  private async checkIdle() {
    this.idleTimer = null
    const kanna = this.kanna
    if (!kanna) return
    if (Date.now() - this.lastActivity < this.idleMs()) return this.armIdleCheck()
    const activity = await (await kanna).activity()
    if (activity.busy) return this.armIdleCheck()
    await this.park(activity.nextScheduledAt)
  }

  private park(nextScheduledAt: number | null) {
    const kanna = this.kanna
    this.kanna = null
    this.parking = (async () => {
      try {
        await (await kanna)?.stop()
        if (nextScheduledAt !== null) await this.ctx.storage.setAlarm(Math.max(nextScheduledAt, Date.now()))
        console.log(`[kanna] idle: stopped, sleeping${nextScheduledAt !== null ? ` until ${new Date(nextScheduledAt).toISOString()}` : ""}`)
      } catch (error) {
        console.error("[kanna] stopping for sleep failed:", error)
      } finally {
        this.parking = null
      }
    })()
    return this.parking
  }

  private unavailable(error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    console.error("[kanna] hosted server failed to start:", error)
    return new Response(`Kanna couldn't start: ${message}`, { status: 503, headers: { "Retry-After": "5" } })
  }

  override async fetch(request: Request): Promise<Response> {
    this.touch()
    try {
      return await (await this.start()).serve(request)
    } catch (error) {
      return this.unavailable(error)
    }
  }

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    this.touch()
    try {
      await (await this.start()).message(ws, message)
    } catch (error) {
      this.unavailable(error)
      ws.close(1011, "Kanna couldn't start")
    }
  }

  override async webSocketClose(ws: WebSocket, code: number, reason: string) {
    // A tab closing while the object sleeps needn't wake Kanna; its record
    // goes when the next server restores the sockets that are left.
    if (this.kanna) (await this.kanna).close(ws, code, reason)
  }

  override async webSocketError(ws: WebSocket) {
    if (this.kanna) (await this.kanna).close(ws, 1011, "socket error")
  }

  /** A schedule is due: wake, and the server's schedule runner fires it on start. */
  override async alarm() {
    this.touch()
    await this.start()
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    if (SERVER_PATHS.test(url.pathname)) {
      // One Kanna per deployment for now: one user, one machine.
      return env.KANNA.get(env.KANNA.idFromName("default")).fetch(request)
    }
    return env.ASSETS.fetch(request)
  },
} satisfies ExportedHandler<Env>
