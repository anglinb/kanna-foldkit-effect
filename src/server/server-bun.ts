import path from "node:path"
import { Effect, Exit, Layer, Scope } from "effect"
import { runKanna } from "./effect/bridge"
import { ServerPlatform } from "./effect/runtime"
import type { RunnerEndpoint } from "./platform/remote/connection"
import { RemoteMachinePlatform } from "./platform/remote/host-platform"
import { startKannaServerEffect, type ServerAssetDirs, type StartKannaServerOptions } from "./server"

/**
 * The package's built assets, found from this file. The server bundle
 * (scripts/build-server.ts) sits two levels under the package root, like
 * src/server, so these resolve the same from source or bundle.
 */
export const BUN_ASSET_DIRS: ServerAssetDirs = {
  client: path.join(import.meta.dir, "..", "..", "dist", "client"),
  exportViewer: path.join(import.meta.dir, "..", "..", "dist", "export-viewer"),
}

/**
 * A runner to use as the machine, from `KANNA_RUNNER_URL` and
 * `KANNA_RUNNER_TOKEN`. Unset, the machine is this computer.
 */
function runnerFromEnv(): RunnerEndpoint | null {
  const url = process.env.KANNA_RUNNER_URL?.trim()
  if (!url) return null
  const token = process.env.KANNA_RUNNER_TOKEN?.trim()
  if (!token) throw new Error("KANNA_RUNNER_URL is set but KANNA_RUNNER_TOKEN is not")
  return { url, token }
}

/**
 * Starts the server on this computer: `startKannaServerEffect` on the Bun
 * platform bindings, serving the package's own dist/. Another host composes
 * `startKannaServerEffect` with its own platform layer instead.
 *
 * With a runner configured, the server still runs here and keeps its data
 * here, but the repos, processes and agent CLIs are the runner's.
 */
export function startKannaServer(options: StartKannaServerOptions = {}) {
  const resolved = { ...options, assetDirs: options.assetDirs ?? BUN_ASSET_DIRS }
  const runner = runnerFromEnv()
  if (!runner) return runKanna(startKannaServerEffect(resolved).pipe(Effect.provide(ServerPlatform)))
  return startOnRunner(resolved, runner)
}

async function startOnRunner(options: StartKannaServerOptions, runner: RunnerEndpoint) {
  // The runner connection outlives startup: it is built in its own scope and
  // closed after the server stops.
  const scope = await runKanna(Scope.make())
  const close = () => runKanna(Scope.close(scope, Exit.void))
  try {
    const hostRoots = [options.assetDirs?.client, options.assetDirs?.exportViewer].filter((dir): dir is string => Boolean(dir))
    const platform = await runKanna(Layer.buildWithScope(
      RemoteMachinePlatform({ runner, hostRoots }).pipe(Layer.provideMerge(ServerPlatform)),
      scope,
    ))
    const started = await runKanna(startKannaServerEffect(options).pipe(Effect.provideContext(platform)))
    return {
      ...started,
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
