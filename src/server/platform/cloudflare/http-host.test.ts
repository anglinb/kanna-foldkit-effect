import { Database } from "bun:sqlite"
import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import type { ClientSocket, HttpListenOptions } from "../http-host"
import type { SqlRunner } from "../sqlite-file-system"
import { makeDurableObjectHttpHost, type HibernationContext, type WorkersWebSocket } from "./http-host"

class FakeWorkersSocket implements WorkersWebSocket {
  readonly sent: Array<string> = []
  closed: { code?: number; reason?: string } | null = null
  send(data: string) {
    this.sent.push(data)
  }
  close(code?: number, reason?: string) {
    this.closed = { code, reason }
  }
}

/** What the runtime keeps across a sleep: the accepted sockets and their tags. */
function fakeHibernation(): HibernationContext & { readonly sockets: Map<WorkersWebSocket, Array<string>> } {
  const sockets = new Map<WorkersWebSocket, Array<string>>()
  return {
    sockets,
    acceptWebSocket: (ws, tags = []) => void sockets.set(ws, tags),
    getWebSockets: () => [...sockets.keys()],
    getTags: (ws) => sockets.get(ws) ?? [],
  }
}

/** A server like Kanna's: upgrades /ws with a session cookie, records what each socket says. */
function fakeServer(options: { readonly accept: (request: Request) => boolean }) {
  const received: Array<{ readonly socket: ClientSocket<{ readonly n: number }>; readonly message: string }> = []
  let opened = 0
  const listener: HttpListenOptions<{ readonly n: number }> = {
    port: 3210,
    fetch: (request, context) => {
      if (!options.accept(request)) return new Response(null, { status: 401 })
      return context.upgrade(request, { n: ++opened }) ? undefined : new Response("not an upgrade", { status: 400 })
    },
    websocket: {
      open: () => {},
      message: (socket, message) => void received.push({ socket, message: String(message) }),
      close: () => {},
    },
  }
  return { listener, received }
}

let pairs: Array<FakeWorkersSocket> = []
beforeAll(() => {
  ;(globalThis as any).WebSocketPair = class {
    0 = new FakeWorkersSocket()
    1 = new FakeWorkersSocket()
    constructor() {
      pairs.push(this[1])
    }
  }
})
afterAll(() => {
  delete (globalThis as any).WebSocketPair
})

const bunSql = (db: Database): SqlRunner => ({ exec: (query, ...bindings) => db.query(query).all(...bindings) as never })
const upgradeRequest = (cookie: string) =>
  new Request("https://kanna.example/ws", { headers: { Upgrade: "websocket", Cookie: cookie, Origin: "https://kanna.example" } })

describe("Durable Object HttpHost", () => {
  test("a socket keeps its subscriptions across a server restart", async () => {
    pairs = []
    const db = new Database(":memory:")
    const hibernation = fakeHibernation()
    const sessions = new Set(["kanna_session=good"])
    const accept = (request: Request) => sessions.has(request.headers.get("cookie") ?? "")

    // The first server: a browser connects and subscribes.
    const first = makeDurableObjectHttpHost({ hibernation, sql: bunSql(db) })
    const one = fakeServer({ accept })
    await Effect.runPromise(first.service.listen(one.listener))
    const response = await first.serve(upgradeRequest("kanna_session=good"))
    expect(response.status).toBe(101)
    const ws = pairs[0]!
    await first.message(ws, JSON.stringify({ v: 1, type: "subscribe", id: "s1", topic: { type: "sidebar" } }))
    await first.message(ws, JSON.stringify({ v: 1, type: "subscribe", id: "s2", topic: { type: "chat", chatId: "c1" } }))
    await first.message(ws, JSON.stringify({ v: 1, type: "unsubscribe", id: "s2" }))
    expect(one.received).toHaveLength(3)

    // The object sleeps: the server stops; the runtime keeps the socket.
    // The next server, built from the same storage, gets it back.
    const second = makeDurableObjectHttpHost({ hibernation, sql: bunSql(db) })
    const two = fakeServer({ accept })
    await Effect.runPromise(second.service.listen(two.listener))
    await second.restoreSockets()
    expect(two.received.map((entry) => JSON.parse(entry.message))).toEqual([{ v: 1, type: "subscribe", id: "s1", topic: { type: "sidebar" } }])
    expect(two.received[0]!.socket.data.n).toBe(1)
    expect(ws.closed).toBeNull()

    // And what the browser says next goes to it.
    await second.message(ws, "{\"v\":1,\"type\":\"command\",\"id\":\"x\",\"command\":{\"type\":\"system.ping\"}}")
    expect(two.received).toHaveLength(2)
    // Sends go out on the same browser socket.
    two.received[0]!.socket.send("hello")
    expect(ws.sent).toEqual(["hello"])
  })

  test("a socket whose session ended is closed on restore, and a closed one forgotten", async () => {
    pairs = []
    const db = new Database(":memory:")
    const hibernation = fakeHibernation()
    const sessions = new Set(["kanna_session=a", "kanna_session=b"])
    const accept = (request: Request) => sessions.has(request.headers.get("cookie") ?? "")

    const first = makeDurableObjectHttpHost({ hibernation, sql: bunSql(db) })
    await Effect.runPromise(first.service.listen(fakeServer({ accept }).listener))
    await first.serve(upgradeRequest("kanna_session=a"))
    await first.serve(upgradeRequest("kanna_session=b"))
    const [a, b] = pairs as [FakeWorkersSocket, FakeWorkersSocket]

    // While asleep: a's session is signed out, and b's tab closes.
    sessions.delete("kanna_session=a")
    hibernation.sockets.delete(b)

    const second = makeDurableObjectHttpHost({ hibernation, sql: bunSql(db) })
    await Effect.runPromise(second.service.listen(fakeServer({ accept }).listener))
    await second.restoreSockets()
    expect(a.closed?.code).toBe(4401)
    const rows = db.query("SELECT id FROM kanna_sockets").all()
    expect(rows).toHaveLength(0)
  })
})
