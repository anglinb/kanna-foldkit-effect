import { DurableObject } from "cloudflare:workers"
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
}

/** Paths Kanna's server answers; everything else is the client bundle. */
const SERVER_PATHS = /^\/(?:ws|health|auth\/|api\/|__cloud\/)/

export class KannaHost extends DurableObject<Env> {
  private kanna: Promise<DurableObjectKanna> | null = null

  private start() {
    // Loaded here, not at the top: the server's modules generate ids and arm
    // timers as they load, which workerd allows only inside a handler.
    this.kanna ??= import("../src/server/server-durable-object").then(({ startKannaInDurableObject }) => {
      for (const name of ["KANNA_RUNNER_URL", "KANNA_RUNNER_TOKEN", "KANNA_PASSWORD"] as const) {
        if (!this.env[name]) throw new Error(`${name} is not set`)
      }
      return startKannaInDurableObject({
        sql: this.ctx.storage.sql,
        runner: { url: this.env.KANNA_RUNNER_URL, token: this.env.KANNA_RUNNER_TOKEN },
        password: this.env.KANNA_PASSWORD,
      })
    }).catch((error: unknown) => {
      // The next request tries again (the runner may have been down).
      this.kanna = null
      throw error
    })
    return this.kanna
  }

  override async fetch(request: Request): Promise<Response> {
    try {
      const kanna = await this.start()
      return await kanna.serve(request)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      console.error("[kanna] hosted server failed to start:", error)
      return new Response(`Kanna couldn't start: ${message}`, { status: 503, headers: { "Retry-After": "5" } })
    }
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
