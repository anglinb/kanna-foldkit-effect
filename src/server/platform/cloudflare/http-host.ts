import { Effect } from "effect"
import { HttpHost, type ClientSocket, type HttpListener, type HttpListenOptions } from "../http-host"
import type { SqlRunner } from "../sqlite-file-system"

/**
 * The slice of Workers' WebSocket API this needs, declared here: the server
 * typechecks against Bun's globals, which don't have `WebSocketPair`.
 */
export interface WorkersWebSocket {
  send(data: string): void
  close(code?: number, reason?: string): void
}
const workers = globalThis as unknown as { readonly WebSocketPair: new () => { readonly 0: WorkersWebSocket; readonly 1: WorkersWebSocket } }

/** The parts of a Durable Object's state the hibernation API needs. */
export interface HibernationContext {
  acceptWebSocket(ws: WorkersWebSocket, tags?: Array<string>): void
  getWebSockets(): Array<WorkersWebSocket>
  getTags(ws: WorkersWebSocket): Array<string>
}

/** The listener a Durable Object's `fetch` is handed to. Others are virtual (see below). */
export const PRIMARY_PORT = 3210

/** Request headers kept for a socket's replayed upgrade: what auth and the router read. */
const KEPT_HEADERS = ["cookie", "origin", "host", "user-agent", "upgrade", "x-forwarded-proto"]

/**
 * What a socket needs to be put back after the object slept: the request that
 * opened it (replayed through the server's own upgrade, so auth runs again)
 * and the subscriptions it has open (replayed as the client sent them). Kept
 * in SQLite under the id the socket is tagged with.
 */
function socketStore(sql: SqlRunner) {
  sql.exec("CREATE TABLE IF NOT EXISTS kanna_sockets (id TEXT PRIMARY KEY, url TEXT NOT NULL, headers TEXT NOT NULL, subscriptions TEXT NOT NULL)")
  const read = (id: string) => {
    const row = [...sql.exec("SELECT url, headers, subscriptions FROM kanna_sockets WHERE id = ?", id)][0]
    if (!row) return null
    return {
      url: String(row.url),
      headers: JSON.parse(String(row.headers)) as Array<[string, string]>,
      subscriptions: JSON.parse(String(row.subscriptions)) as Record<string, unknown>,
    }
  }
  const writeSubscriptions = (id: string, subscriptions: Record<string, unknown>) =>
    sql.exec("UPDATE kanna_sockets SET subscriptions = ? WHERE id = ?", JSON.stringify(subscriptions), id)
  return {
    add: (id: string, request: Request) => {
      const headers = KEPT_HEADERS.flatMap((name) => {
        const value = request.headers.get(name)
        return value === null ? [] : [[name, value] as [string, string]]
      })
      sql.exec("INSERT OR REPLACE INTO kanna_sockets VALUES (?, ?, ?, '{}')", id, request.url, JSON.stringify(headers))
    },
    read,
    subscribe: (id: string, subscriptionId: string, topic: unknown) => {
      const state = read(id)
      if (state) writeSubscriptions(id, { ...state.subscriptions, [subscriptionId]: topic })
    },
    unsubscribe: (id: string, subscriptionId: string) => {
      const state = read(id)
      if (!state) return
      const { [subscriptionId]: _, ...rest } = state.subscriptions
      writeSubscriptions(id, rest)
    },
    remove: (id: string) => sql.exec("DELETE FROM kanna_sockets WHERE id = ?", id),
    /** Drops the records of sockets that closed while no server was there to hear it. */
    keepOnly: (ids: ReadonlySet<string>) => {
      for (const row of [...sql.exec("SELECT id FROM kanna_sockets")]) {
        if (!ids.has(String(row.id))) sql.exec("DELETE FROM kanna_sockets WHERE id = ?", row.id)
      }
    },
  }
}

/** Subscriptions as the client sends them, read off the frames on their way to the router. */
function subscriptionChange(raw: string | Uint8Array) {
  if (typeof raw !== "string" || !raw.includes("subscribe")) return null
  try {
    const envelope = JSON.parse(raw) as { readonly type?: string; readonly id?: string; readonly topic?: unknown }
    if (typeof envelope.id !== "string") return null
    if (envelope.type === "subscribe") return { kind: "subscribe" as const, id: envelope.id, topic: envelope.topic }
    if (envelope.type === "unsubscribe") return { kind: "unsubscribe" as const, id: envelope.id }
  } catch {
    // Not JSON: the router answers it.
  }
  return null
}

/**
 * A Durable Object can't listen on a port: requests arrive at its `fetch`.
 * This `HttpHost` keeps each listener's handlers under a virtual port and
 * `serve` hands a request to one of them, upgrading WebSockets with a
 * `WebSocketPair` the way `Bun.serve` upgrades its own.
 *
 * Browser sockets use the hibernation API: the runtime holds them while the
 * object sleeps, and their events come in through the object's
 * `webSocketMessage` / `webSocketClose`, which call `message` / `close` here.
 * After a sleep the server is new and knows none of them; `restoreSockets`
 * replays each one's upgrade and subscriptions so it carries on.
 *
 * The server's own listener is `PRIMARY_PORT`; others (the Kanna MCP
 * endpoint asks for port 0) get ports from 40000 up, reachable only through
 * `serve(request, port)`.
 */
export function makeDurableObjectHttpHost(options: { readonly hibernation: HibernationContext; readonly sql: SqlRunner }) {
  const { hibernation } = options
  const store = socketStore(options.sql)
  const listeners = new Map<number, HttpListenOptions<any>>()
  /** The sockets the current server knows, by their Workers socket. */
  const bound = new Map<WorkersWebSocket, { readonly id: string; readonly socket: ClientSocket<any>; readonly listener: HttpListenOptions<any> }>()
  let nextVirtualPort = 40000

  const listen = <Data>(listenOptions: HttpListenOptions<Data>): Effect.Effect<HttpListener> => Effect.sync(() => {
    const port = listenOptions.port === 0 ? nextVirtualPort++ : listenOptions.port
    listeners.set(port, listenOptions)
    return {
      port,
      // Stopping the server leaves browser sockets open: they outlive it
      // across a sleep and are bound to the next server when it starts.
      stop: () => Effect.sync(() => {
        listeners.delete(port)
        for (const [ws, entry] of bound) if (entry.listener === listenOptions) bound.delete(ws)
      }),
    }
  })

  const bind = <Data>(listener: HttpListenOptions<Data>, ws: WorkersWebSocket, id: string, data: Data) => {
    const socket: ClientSocket<Data> = {
      data,
      // Workers negotiate their own per-message deflate; `compress` is a hint.
      send: (payload) => ws.send(payload),
      close: (code, reason) => ws.close(code, reason),
    }
    bound.set(ws, { id, socket, listener })
    listener.websocket!.open(socket)
    return socket
  }

  const upgrade = <Data>(listener: HttpListenOptions<Data>, request: Request, data: Data): Response | null => {
    if (!listener.websocket || request.headers.get("Upgrade")?.toLowerCase() !== "websocket") return null
    const pair = new workers.WebSocketPair()
    const id = crypto.randomUUID()
    hibernation.acceptWebSocket(pair[1], [id])
    store.add(id, request)
    bind(listener, pair[1], id, data)
    return new Response(null, { status: 101, webSocket: pair[0] } as ResponseInit)
  }

  /** Answers `request` with the listener on `port` (default: the server's own). */
  const serve = async (request: Request, port = PRIMARY_PORT): Promise<Response> => {
    const listener = listeners.get(port)
    if (!listener) return new Response("Kanna is starting", { status: 503, headers: { "Retry-After": "1" } })
    let upgraded: Response | null = null
    try {
      const response = await listener.fetch(request, {
        upgrade: (req, data) => {
          upgraded = upgrade(listener, req, data)
          return upgraded !== null
        },
      })
      return upgraded ?? response ?? new Response(null, { status: 404 })
    } catch (error) {
      if (listener.error) return listener.error(error)
      return new Response("Internal error", { status: 500 })
    }
  }

  const idOf = (ws: WorkersWebSocket) => hibernation.getTags(ws)[0] ?? null

  /**
   * Binds a socket the current server doesn't know (it opened before a sleep)
   * by replaying its upgrade, then its subscriptions. Closes it if the
   * replay is refused (its session ended) or its record is gone.
   */
  const restore = async (ws: WorkersWebSocket) => {
    const id = idOf(ws)
    const listener = listeners.get(PRIMARY_PORT)
    const state = id ? store.read(id) : null
    if (!id || !listener?.websocket || !state) {
      ws.close(1012, "Kanna restarted")
      return null
    }
    let socket: ClientSocket<any> | null = null
    await listener.fetch(new Request(state.url, { headers: state.headers }), {
      upgrade: (_request, data) => {
        socket = bind(listener, ws, id, data)
        return true
      },
    })
    if (!socket) {
      store.remove(id)
      ws.close(4401, "Sign in again")
      return null
    }
    for (const [subscriptionId, topic] of Object.entries(state.subscriptions)) {
      listener.websocket.message(socket, JSON.stringify({ v: 1, type: "subscribe", id: subscriptionId, topic }))
    }
    return socket
  }

  /** A frame from a browser socket (the object's `webSocketMessage`). */
  const message = async (ws: WorkersWebSocket, raw: string | ArrayBuffer) => {
    const data = typeof raw === "string" ? raw : new Uint8Array(raw)
    const entry = bound.get(ws)
    const socket = entry?.socket ?? await restore(ws)
    const listener = bound.get(ws)?.listener
    if (!socket || !listener?.websocket) return
    const change = subscriptionChange(data)
    const id = bound.get(ws)!.id
    if (change?.kind === "subscribe") store.subscribe(id, change.id, change.topic)
    if (change?.kind === "unsubscribe") store.unsubscribe(id, change.id)
    listener.websocket.message(socket, data)
  }

  /** A browser socket closed (the object's `webSocketClose` / `webSocketError`). */
  const close = (ws: WorkersWebSocket, code: number, reason: string) => {
    const entry = bound.get(ws)
    const id = entry?.id ?? idOf(ws)
    if (id) store.remove(id)
    bound.delete(ws)
    entry?.listener.websocket?.close(entry.socket, code, reason)
  }

  /** Binds every socket that outlived the last server, so pushes reach them. */
  const restoreSockets = async () => {
    const open = hibernation.getWebSockets()
    store.keepOnly(new Set(open.flatMap((ws) => hibernation.getTags(ws))))
    for (const ws of open) if (!bound.has(ws)) await restore(ws)
  }

  return {
    service: HttpHost.of({ listen }),
    serve,
    message,
    close,
    restoreSockets,
    listening: (port = PRIMARY_PORT) => listeners.has(port),
  }
}
