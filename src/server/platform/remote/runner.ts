import { Effect } from "effect"
import { HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { MachineRpcs, RUNNER_RPC_PATH, RUNNER_TOKEN_PARAM } from "./protocol"
import { MachineRpcHandlers } from "./server"

export interface RunnerOptions {
  /** The shared secret a host must present. */
  readonly token: string
}

const encoder = new TextEncoder()

/** Compares in time independent of where the strings differ. */
function sameSecret(given: string | null, expected: string) {
  if (given === null) return false
  const a = encoder.encode(given)
  const b = encoder.encode(expected)
  let diff = a.length ^ b.length
  for (let index = 0; index < b.length; index += 1) diff |= (a[index] ?? 0) ^ b[index]!
  return diff === 0
}

/**
 * The runner: serves `MachineRpcs` on `RUNNER_RPC_PATH` with the machine's
 * own platform services, to a host that presents `token`. Needs an
 * `HttpServer` and the platform services; `runner-bun.ts` provides Bun's.
 *
 * One host at a time is the expected use, but nothing here enforces it:
 * every connection sees the same machine.
 */
export const serveRunner = Effect.fn("Runner.serve")(function* (options: RunnerOptions) {
  const rpc = yield* RpcServer.toHttpEffectWebsocket(MachineRpcs)
  const app = Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const url = new URL(request.url, "http://runner")
    if (url.pathname === "/health") return HttpServerResponse.text("ok")
    if (url.pathname !== RUNNER_RPC_PATH) return HttpServerResponse.empty({ status: 404 })
    if (!sameSecret(url.searchParams.get(RUNNER_TOKEN_PARAM), options.token)) return HttpServerResponse.empty({ status: 401 })
    return yield* rpc
  })
  yield* HttpServer.serveEffect(app)
}, (effect) => effect.pipe(Effect.provide(MachineRpcHandlers), Effect.provide(RpcSerialization.layerJson)))
