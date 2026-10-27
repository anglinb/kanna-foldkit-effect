import { Duration, Effect, Layer } from "effect"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { Socket } from "effect/socket"
import { makeRemoteMachineLayer, type RemoteMachineServices } from "./client"
import { MachineRpcs, RUNNER_RPC_PATH, RUNNER_TOKEN_PARAM } from "./protocol"

export interface RunnerEndpoint {
  /** The runner's base URL, `ws://` or `wss://` (an `http(s)://` one is rewritten). */
  readonly url: string
  /** The runner's shared secret. */
  readonly token: string
}

export function runnerSocketUrl(endpoint: RunnerEndpoint) {
  const url = new URL(endpoint.url)
  if (url.protocol === "http:") url.protocol = "ws:"
  if (url.protocol === "https:") url.protocol = "wss:"
  url.pathname = url.pathname.replace(/\/$/, "") + RUNNER_RPC_PATH
  url.searchParams.set(RUNNER_TOKEN_PARAM, endpoint.token)
  return url.toString()
}

/**
 * The machine services, served by the runner at `endpoint` over one
 * WebSocket. Uses the global `WebSocket`, which Bun and Workers both have.
 *
 * Building the layer connects and reads the machine's facts, so it fails
 * if the runner can't be reached. After that a dropped socket is retried;
 * calls in flight when it drops fail (processes die with the connection on
 * the runner's side).
 */
export const RemoteMachineLayer = (endpoint: RunnerEndpoint): Layer.Layer<RemoteMachineServices, unknown> =>
  Layer.unwrap(Effect.gen(function* () {
    const client = yield* RpcClient.make(MachineRpcs)
    return makeRemoteMachineLayer(client)
  })).pipe(
    Layer.provide(RpcClient.layerProtocolSocket({
      retryTransientErrors: true,
      pingInterval: Duration.seconds(10),
      pingTimeout: Duration.seconds(30),
    })),
    Layer.provide(Socket.layerWebSocket(runnerSocketUrl(endpoint), { openTimeout: Duration.seconds(10) })),
    Layer.provide(Socket.layerWebSocketConstructorGlobal),
    Layer.provide(RpcSerialization.layerJson),
  )
