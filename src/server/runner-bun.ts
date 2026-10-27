import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { Effect, Layer } from "effect"
import { KannaLoggerLayer } from "./effect/logger"
import { BunPlatform } from "./platform/bun"
import { serveRunner } from "./platform/remote/runner"

/**
 * `kanna-runner`: the machine half of a hosted Kanna. Run it where the repos
 * and agent CLIs are (a container, or a laptop); the host connects to it.
 *
 *   KANNA_RUNNER_TOKEN=… bun src/server/runner-bun.ts [--port 7420] [--host 0.0.0.0]
 */
function argValue(name: string) {
  const index = process.argv.indexOf(name)
  return index === -1 ? undefined : process.argv[index + 1]
}

const token = process.env.KANNA_RUNNER_TOKEN
if (!token) {
  console.error("[kanna-runner] set KANNA_RUNNER_TOKEN to the secret hosts will present")
  process.exit(1)
}
const port = Number(argValue("--port") ?? process.env.PORT ?? 7420)
const hostname = argValue("--host") ?? "127.0.0.1"

const program = serveRunner({ token }).pipe(
  Effect.andThen(Effect.log(`[kanna-runner] serving ${hostname}:${port}`)),
  Effect.andThen(Effect.never),
)

BunRuntime.runMain(program.pipe(
  Effect.scoped,
  Effect.provide(Layer.mergeAll(BunHttpServer.layer({ port, hostname }), BunPlatform, KannaLoggerLayer)),
))
