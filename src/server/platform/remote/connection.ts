import { type Context, Deferred, Duration, Effect, Layer, Schedule, Stream } from "effect"
import { RpcClient, RpcSchema, RpcSerialization } from "effect/rpc"
import { Socket } from "effect/socket"
import type { MachineEnv } from "../machine"
import { MachineOfflineError, makeRemoteMachineContext, type MachineClient, type MachineFacts, type RemoteMachineServices, type RunnerLink } from "./client"
import { MachineRpcs, RUNNER_RPC_PATH, RUNNER_TOKEN_PARAM } from "./protocol"

export type RunnerEndpoint =
  | {
    /** The runner's base URL, `ws://` or `wss://` (an `http(s)://` one is rewritten). */
    readonly url: string
    /** The runner's shared secret. */
    readonly token: string
  }
  | {
    /**
     * Opens an already-accepted WebSocket to the runner's RPC endpoint: how a
     * host that reaches it through a binding, not a URL, connects (a Durable
     * Object and its Container).
     */
    readonly connect: () => Promise<Socket.WebSocketLike>
    /** What to call it in logs. */
    readonly label: string
  }

export interface RemoteMachineOptions {
  readonly runner: RunnerEndpoint
  /**
   * The machine's facts to start from without the runner: the last ones it
   * reported, kept by the host. Without them, building the layer waits for
   * the first connection.
   */
  readonly lastKnown?: MachineFacts | null
  /** Called with the facts each time the runner (re)connects, for the host to keep. */
  readonly onFacts?: (facts: MachineFacts) => void
  /**
   * Runs on each new connection before anything else may use it, with the
   * machine as that connection sees it: a host whose machine starts with an
   * empty disk restores it here. A failure drops the connection, and the
   * next attempt runs it again.
   */
  readonly onConnected?: (machine: Context.Context<RemoteMachineServices>) => Effect.Effect<void, unknown>
}

export function runnerSocketUrl(endpoint: { readonly url: string; readonly token: string }) {
  const url = new URL(endpoint.url)
  if (url.protocol === "http:") url.protocol = "ws:"
  if (url.protocol === "https:") url.protocol = "wss:"
  url.pathname = url.pathname.replace(/\/$/, "") + RUNNER_RPC_PATH
  url.searchParams.set(RUNNER_TOKEN_PARAM, endpoint.token)
  return url.toString()
}

const HEARTBEAT = Duration.seconds(15)
const HEARTBEAT_TIMEOUT = Duration.seconds(10)
const RECONNECT = Schedule.spaced(Duration.seconds(3))
/** How long a call waits on a connection attempt already under way (a host that just woke). */
const CONNECT_WAIT = Duration.seconds(5)

const isStreamRpc = (tag: string) => {
  const rpc = MachineRpcs.requests.get(tag)
  return rpc !== undefined && RpcSchema.isStreamSchema(rpc.successSchema)
}

/** The socket to the runner, by URL or through the host's own `connect`. */
function socketLayer(endpoint: RunnerEndpoint): Layer.Layer<Socket.Socket> {
  if ("url" in endpoint) {
    return Socket.layerWebSocket(runnerSocketUrl(endpoint), { openTimeout: Duration.seconds(10) }).pipe(
      Layer.provide(Socket.layerWebSocketConstructorGlobal),
    )
  }
  return Layer.effect(Socket.Socket, Socket.fromWebSocket(Effect.acquireRelease(
    Effect.tryPromise({
      try: endpoint.connect,
      catch: (cause) => new Socket.SocketError({ reason: new Socket.SocketOpenError({ kind: "Unknown", cause }) }),
    }),
    (ws) => Effect.sync(() => ws.close(1000)),
  )))
}

/**
 * Stands in for the client while there is no connection: every call fails
 * at once, so nothing waits on a runner that isn't there.
 */
const offlineClient: MachineClient = new Proxy({} as MachineClient, {
  get: (_target, tag) => {
    const isStream = isStreamRpc(String(tag))
    return (_payload: unknown, options?: { readonly asQueue?: boolean }) => {
      const error = new MachineOfflineError(String(tag))
      if (isStream && !options?.asQueue) return Stream.fail(error)
      return Effect.fail(error)
    }
  },
})

/**
 * The machine services, served by a runner over one WebSocket that is kept
 * open in the background: connect, read the machine's facts, heartbeat, and
 * on failure reconnect with backoff. Calls made while it is down fail at once
 * (`MachineOfflineError`); calls in flight when it drops fail, and the
 * runner kills the processes that connection started.
 *
 * Uses the global `WebSocket`, which Bun and Workers both have.
 */
export const RemoteMachineLayer = (options: RemoteMachineOptions): Layer.Layer<RemoteMachineServices> =>
  Layer.effectContext(Effect.gen(function* () {
    let current: MachineClient = offlineClient
    let facts: MachineFacts | null = options.lastKnown ?? null
    const firstConnected = yield* Deferred.make<void>()
    /** Set while an attempt to connect is under way; done when it connects or fails. */
    let attempt: Deferred.Deferred<void> | null = null

    // The host's copy of the environment, and what the host changed in it:
    // a runner that restarts comes back with its own, and the changes (the
    // login shell's PATH, say) are applied again.
    let env: Record<string, string | undefined> = { ...facts?.env }
    const overrides = new Map<string, string | undefined>()
    const mirror = (name: string, value: string | undefined) =>
      Effect.runFork(current.MachineSetEnv(value === undefined ? { name } : { name, value }).pipe(Effect.ignore))
    const machineEnv: MachineEnv = {
      get: (name) => env[name],
      snapshot: () => ({ ...env }),
      set: (name, value) => {
        env[name] = value
        overrides.set(name, value)
        mirror(name, value)
      },
      unset: (name) => {
        delete env[name]
        overrides.set(name, undefined)
        mirror(name, undefined)
      },
    }

    const session = Effect.gen(function* () {
      const thisAttempt = Deferred.makeUnsafe<void>()
      attempt = thisAttempt
      yield* Effect.addFinalizer(() => Effect.sync(() => {
        Deferred.doneUnsafe(thisAttempt, Effect.void)
        if (attempt === thisAttempt) attempt = null
      }))
      const client = yield* RpcClient.make(MachineRpcs)
      const info = yield* client.MachineInfo().pipe(Effect.timeout(HEARTBEAT_TIMEOUT))
      const ptyAvailable = yield* client.PtyAvailable().pipe(Effect.timeout(HEARTBEAT_TIMEOUT))
      facts = { ...info, ptyAvailable }
      env = { ...info.env }
      for (const [name, value] of overrides) {
        if (value === undefined) delete env[name]
        else env[name] = value
        yield* (value === undefined ? client.MachineSetEnv({ name }) : client.MachineSetEnv({ name, value }))
      }
      if (options.onConnected) {
        yield* options.onConnected(makeRemoteMachineContext({ client, facts: () => facts!, env: machineEnv }))
      }
      current = client
      Deferred.doneUnsafe(thisAttempt, Effect.void)
      options.onFacts?.(facts)
      yield* Deferred.succeed(firstConnected, undefined)
      yield* Effect.logInfo(`connected to the machine at ${"url" in options.runner ? new URL(options.runner.url).host : options.runner.label}`)
      // Until a heartbeat fails: then the session ends and the loop reconnects.
      yield* client.PtyAvailable().pipe(
        Effect.timeout(HEARTBEAT_TIMEOUT),
        Effect.delay(HEARTBEAT),
        Effect.forever,
      )
    }).pipe(
      Effect.ensuring(Effect.sync(() => {
        current = offlineClient
      })),
      Effect.scoped,
      Effect.provide(RpcClient.layerProtocolSocket({ retryTransientErrors: false }).pipe(
        Layer.provide(socketLayer(options.runner)),
        Layer.provide(RpcSerialization.layerJson),
      )),
    )

    yield* session.pipe(
      Effect.catchCause((cause) => Effect.logWarning("the machine is offline; reconnecting", cause)),
      Effect.repeat(RECONNECT),
      Effect.forkScoped,
    )

    if (facts === null) yield* Deferred.await(firstConnected)
    // Calls go to the current connection. With none, a call waits briefly for
    // an attempt already under way (a host that just woke is connecting) and
    // otherwise fails at once.
    const client = new Proxy({} as MachineClient, {
      get: (_target, tag) => (payload: unknown, callOptions?: { readonly asQueue?: boolean }) => {
        const call = () => (current as any)[tag](payload, callOptions)
        const pending = attempt
        if (current !== offlineClient || pending === null) return call()
        const wait = Deferred.await(pending).pipe(Effect.timeout(CONNECT_WAIT), Effect.ignore)
        if (isStreamRpc(String(tag)) && !callOptions?.asQueue) return Stream.unwrap(Effect.map(wait, call))
        return Effect.andThen(wait, Effect.suspend(call))
      },
    })
    const link: RunnerLink = {
      client,
      facts: () => facts!,
      env: machineEnv,
    }
    return makeRemoteMachineContext(link)
  }))
