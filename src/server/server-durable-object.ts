import { Effect, Exit, Layer, Scope } from "effect"
import { runKanna } from "./effect/bridge"
import { makeDurableObjectPlatform, PRIMARY_PORT } from "./platform/cloudflare"
import type { RunnerEndpoint } from "./platform/remote/connection"
import type { SqlRunner } from "./platform/sqlite-file-system"
import { startKannaServerEffect } from "./server"

export interface DurableObjectKannaOptions {
  /** The object's `ctx.storage.sql`: Kanna's state lives here. */
  readonly sql: SqlRunner
  /** The machine: a runner holding the repos and agent CLIs. */
  readonly runner: RunnerEndpoint
  /** Required: a Durable Object is on the public internet. */
  readonly password: string
}

export interface DurableObjectKanna {
  /** Answers one request the object's `fetch` received. */
  readonly serve: (request: Request) => Promise<Response>
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
  const { http, layer } = makeDurableObjectPlatform({ sql: options.sql, runner: options.runner })
  const scope = await runKanna(Scope.make())
  const close = () => runKanna(Scope.close(scope, Exit.void))
  const startedAt = Date.now()
  try {
    const platform = await runKanna(Layer.buildWithScope(layer, scope))
    const started = await runKanna(startKannaServerEffect({
      port: PRIMARY_PORT,
      host: "0.0.0.0",
      strictPort: true,
      password: options.password,
      // The Worker in front terminates TLS and forwards the request as-is.
      trustProxy: true,
      cloudEnabled: false,
    }).pipe(Effect.provideContext(platform)))
    console.log(`[kanna] hosted server started in ${Date.now() - startedAt}ms`)
    return {
      serve: (request) => http.serve(request),
      stop: async () => {
        try {
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

