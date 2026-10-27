import { Effect } from "effect"
import { HttpHost, type ClientSocket, type HttpListener, type HttpListenOptions } from "../http-host"

/**
 * The slice of Workers' WebSocket API this needs, declared here: the server
 * typechecks against Bun's globals, which don't have `WebSocketPair`.
 */
interface WorkersWebSocket {
  accept(): void
  send(data: string): void
  close(code?: number, reason?: string): void
  addEventListener(type: "message", listener: (event: { readonly data: string | ArrayBuffer }) => void): void
  addEventListener(type: "close", listener: (event: { readonly code: number; readonly reason: string }) => void): void
  addEventListener(type: "error", listener: () => void): void
}
const workers = globalThis as unknown as { readonly WebSocketPair: new () => { readonly 0: WorkersWebSocket; readonly 1: WorkersWebSocket } }

/** The listener a Durable Object's `fetch` is handed to. Others are virtual (see below). */
export const PRIMARY_PORT = 3210

/**
 * A Durable Object can't listen on a port: requests arrive at its `fetch`.
 * This `HttpHost` keeps each listener's handlers under a virtual port and
 * `serve` hands a request to one of them, upgrading WebSockets with a
 * `WebSocketPair` the way `Bun.serve` upgrades its own.
 *
 * The server's own listener is `PRIMARY_PORT`; others (the Kanna MCP
 * endpoint asks for port 0) get ports from 40000 up, reachable only through
 * `serve(request, port)`.
 */
export function makeDurableObjectHttpHost() {
  const listeners = new Map<number, HttpListenOptions<any>>()
  let nextVirtualPort = 40000

  const listen = <Data>(options: HttpListenOptions<Data>): Effect.Effect<HttpListener> => Effect.sync(() => {
    const port = options.port === 0 ? nextVirtualPort++ : options.port
    listeners.set(port, options)
    return {
      port,
      stop: () => Effect.sync(() => {
        listeners.delete(port)
      }),
    }
  })

  const upgrade = <Data>(options: HttpListenOptions<Data>, request: Request, data: Data): Response | null => {
    const handlers = options.websocket
    if (!handlers || request.headers.get("Upgrade")?.toLowerCase() !== "websocket") return null
    const pair = new workers.WebSocketPair()
    const client = pair[0]
    const server = pair[1]
    server.accept()
    const socket: ClientSocket<Data> = {
      data,
      // Workers negotiate their own per-message deflate; `compress` is a hint.
      send: (payload) => server.send(payload),
      close: (code, reason) => server.close(code, reason),
    }
    server.addEventListener("message", (event) => {
      handlers.message(socket, typeof event.data === "string" ? event.data : new Uint8Array(event.data))
    })
    server.addEventListener("close", (event) => handlers.close(socket, event.code, event.reason))
    server.addEventListener("error", () => handlers.close(socket, 1011, "socket error"))
    handlers.open(socket)
    return new Response(null, { status: 101, webSocket: client } as ResponseInit)
  }

  /** Answers `request` with the listener on `port` (default: the server's own). */
  const serve = async (request: Request, port = PRIMARY_PORT): Promise<Response> => {
    const options = listeners.get(port)
    if (!options) return new Response("Kanna is starting", { status: 503, headers: { "Retry-After": "1" } })
    let upgraded: Response | null = null
    try {
      const response = await options.fetch(request, {
        upgrade: (req, data) => {
          upgraded = upgrade(options, req, data)
          return upgraded !== null
        },
      })
      return upgraded ?? response ?? new Response(null, { status: 404 })
    } catch (error) {
      if (options.error) return options.error(error)
      return new Response("Internal error", { status: 500 })
    }
  }

  return {
    service: HttpHost.of({ listen }),
    serve,
    listening: (port = PRIMARY_PORT) => listeners.has(port),
  }
}
