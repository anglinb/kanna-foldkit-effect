import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { TEXT_PING, TEXT_PONG } from "../../shared/protocol"
import { KannaSocket } from "./socket"

type EventHandler = (event?: unknown) => void

class FakeEventTarget {
  private readonly listeners = new Map<string, Set<EventHandler>>()

  addEventListener(type: string, listener: EventHandler) {
    let handlers = this.listeners.get(type)
    if (!handlers) {
      handlers = new Set()
      this.listeners.set(type, handlers)
    }
    handlers.add(listener)
  }

  removeEventListener(type: string, listener: EventHandler) {
    this.listeners.get(type)?.delete(listener)
  }

  dispatchEvent(type: string, event?: unknown) {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event)
    }
  }
}

class FakeTimers {
  private nextId = 1
  readonly timeouts = new Map<number, () => void>()
  readonly intervals = new Map<number, () => void>()

  setTimeout = (callback: () => void) => {
    const id = this.nextId++
    this.timeouts.set(id, callback)
    return id
  }

  clearTimeout = (id: number) => {
    this.timeouts.delete(id)
  }

  setInterval = (callback: () => void) => {
    const id = this.nextId++
    this.intervals.set(id, callback)
    return id
  }

  clearInterval = (id: number) => {
    this.intervals.delete(id)
  }

  runTimeout(id: number) {
    const callback = this.timeouts.get(id)
    if (!callback) return
    this.timeouts.delete(id)
    callback()
  }

  runInterval(id: number) {
    this.intervals.get(id)?.()
  }
}

class FakeWebSocket {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static instances: FakeWebSocket[] = []

  readonly sent: Array<Record<string, unknown>> = []
  private readonly listeners = new Map<string, Set<EventHandler>>()
  readyState = FakeWebSocket.CONNECTING

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this)
  }

  addEventListener(type: string, listener: EventHandler) {
    let handlers = this.listeners.get(type)
    if (!handlers) {
      handlers = new Set()
      this.listeners.set(type, handlers)
    }
    handlers.add(listener)
  }

  send(message: string) {
    // Text heartbeat frames aren't JSON.
    this.sent.push(message.startsWith("{") ? JSON.parse(message) : { text: message })
  }

  open() {
    this.readyState = FakeWebSocket.OPEN
    this.emit("open")
  }

  receive(message: Record<string, unknown>) {
    this.emit("message", { data: JSON.stringify(message) })
  }

  receiveText(text: string) {
    this.emit("message", { data: text })
  }

  close() {
    if (this.readyState === FakeWebSocket.CLOSED) return
    this.readyState = FakeWebSocket.CLOSED
    this.emit("close")
  }

  private emit(type: string, event?: unknown) {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event)
    }
  }
}

describe("KannaSocket", () => {
  const originalWindow = globalThis.window
  const originalDocument = globalThis.document
  const originalWebSocket = globalThis.WebSocket

  let windowTarget: FakeEventTarget
  let documentTarget: FakeEventTarget & { visibilityState: "visible" | "hidden" }
  let timers: FakeTimers

  beforeEach(() => {
    FakeWebSocket.instances = []
    timers = new FakeTimers()
    windowTarget = new FakeEventTarget()
    documentTarget = Object.assign(new FakeEventTarget(), { visibilityState: "visible" as const })

    ;(globalThis as any).window = Object.assign(windowTarget, {
      setTimeout: timers.setTimeout,
      clearTimeout: timers.clearTimeout,
      setInterval: timers.setInterval,
      clearInterval: timers.clearInterval,
      location: { protocol: "http:", host: "localhost:3211" },
    })
    ;(globalThis as any).document = documentTarget
    ;(globalThis as any).WebSocket = FakeWebSocket
  })

  afterEach(() => {
    ;(globalThis as any).window = originalWindow
    ;(globalThis as any).document = originalDocument
    ;(globalThis as any).WebSocket = originalWebSocket
  })

  test("does not ping when the connection is already fresh", async () => {
    const socket = new KannaSocket("ws://localhost/ws")
    socket.start()
    const ws = FakeWebSocket.instances[0]!
    ws.open()

    await socket.ensureHealthyConnection()

    expect(ws.sent).toHaveLength(0)
    socket.dispose()
  })

  test("pings a stale open connection and resolves when acked", async () => {
    const socket = new KannaSocket("ws://localhost/ws")
    socket.start()
    const ws = FakeWebSocket.instances[0]!
    ws.open()
    ;(socket as any).lastOpenAt = Date.now() - 30_000
    ;(socket as any).lastMessageAt = Date.now() - 30_000

    const healthCheck = socket.ensureHealthyConnection()
    expect(ws.sent[0]).toEqual({ text: TEXT_PING })

    ws.receiveText(TEXT_PONG)
    await healthCheck

    expect(FakeWebSocket.instances).toHaveLength(1)
    socket.dispose()
  })

  test("pings a server too old for text pings with system.ping instead", async () => {
    const socket = new KannaSocket("ws://localhost/ws")
    socket.start()
    const ws = FakeWebSocket.instances[0]!
    ws.open()
    ;(socket as any).lastOpenAt = Date.now() - 30_000
    ;(socket as any).lastMessageAt = Date.now() - 30_000

    const healthCheck = socket.ensureHealthyConnection()
    expect(ws.sent[0]).toEqual({ text: TEXT_PING })
    // What such a server says to a frame that isn't JSON.
    ws.receive({ v: 1, type: "error", message: "Invalid JSON" })
    const ping = ws.sent[1]
    expect(ping?.command).toEqual({ type: "system.ping" })
    ws.receive({ v: 1, type: "ack", id: ping?.id })
    await healthCheck

    // And keeps to it on that connection.
    ;(socket as any).lastMessageAt = Date.now() - 30_000
    const second = socket.ensureHealthyConnection().catch(() => {})
    expect(ws.sent[2]?.command).toEqual({ type: "system.ping" })
    socket.dispose()
    await second
  })

  test("reconnects immediately when a stale ping times out", async () => {
    const socket = new KannaSocket("ws://localhost/ws")
    socket.start()
    const firstWs = FakeWebSocket.instances[0]!
    firstWs.open()
    ;(socket as any).lastOpenAt = Date.now() - 30_000
    ;(socket as any).lastMessageAt = Date.now() - 30_000

    const healthCheck = socket.ensureHealthyConnection()
    timers.runTimeout((socket as any).pingTimeoutTimer)

    await expect(healthCheck).rejects.toThrow("Disconnected")
    expect(FakeWebSocket.instances).toHaveLength(2)
    expect(FakeWebSocket.instances[1]?.readyState).toBe(FakeWebSocket.CONNECTING)
    socket.dispose()
  })

  test("runs health checks on focus, visibility restore, and online", async () => {
    const socket = new KannaSocket("ws://localhost/ws")
    socket.start()
    const ws = FakeWebSocket.instances[0]!
    ws.open()

    ;(socket as any).lastOpenAt = Date.now() - 30_000
    ;(socket as any).lastMessageAt = Date.now() - 30_000
    windowTarget.dispatchEvent("focus")
    expect(ws.sent.pop()).toEqual({ text: TEXT_PING })
    ws.receiveText(TEXT_PONG)
    await Promise.resolve()

    documentTarget.visibilityState = "hidden"
    documentTarget.dispatchEvent("visibilitychange")
    ;(socket as any).lastOpenAt = Date.now() - 30_000
    ;(socket as any).lastMessageAt = Date.now() - 30_000
    documentTarget.visibilityState = "visible"
    documentTarget.dispatchEvent("visibilitychange")
    expect(ws.sent.pop()).toEqual({ text: TEXT_PING })
    ws.receiveText(TEXT_PONG)
    await Promise.resolve()

    ;(socket as any).lastOpenAt = Date.now() - 30_000
    ;(socket as any).lastMessageAt = Date.now() - 30_000
    windowTarget.dispatchEvent("online")
    expect(ws.sent.pop()).toEqual({ text: TEXT_PING })
    ws.receiveText(TEXT_PONG)
    await Promise.resolve()
    socket.dispose()
  })

  test("keeps queued commands and flushes them once the socket opens", async () => {
    const socket = new KannaSocket("ws://localhost/ws")
    socket.start()
    const ws = FakeWebSocket.instances[0]!
    const pingPromise = socket.command({ type: "system.ping" })

    expect(ws.sent).toHaveLength(0)

    ws.open()
    const ping = ws.sent[0]
    ws.receive({ v: 1, type: "ack", id: ping?.id })

    await expect(pingPromise).resolves.toBeUndefined()
    expect(ws.sent).toHaveLength(1)
    socket.dispose()
  })

  test("sends heartbeat checks while visible", async () => {
    const socket = new KannaSocket("ws://localhost/ws")
    socket.start()
    const ws = FakeWebSocket.instances[0]!
    ws.open()
    ;(socket as any).lastOpenAt = Date.now() - 30_000
    ;(socket as any).lastMessageAt = Date.now() - 30_000

    timers.runInterval((socket as any).heartbeatTimer)

    expect(ws.sent[0]).toEqual({ text: TEXT_PING })
    ws.receiveText(TEXT_PONG)
    await Promise.resolve()
    socket.dispose()
  })

  test("async url provider resolves before each connection (cloud URL rotation)", async () => {
    const urls = ["ws://tunnel-one/ws?token=a", "ws://tunnel-two/ws?token=b"]
    let resolveCount = 0
    const socket = new KannaSocket(async () => urls[Math.min(resolveCount++, urls.length - 1)])

    socket.start()
    expect(FakeWebSocket.instances).toHaveLength(0)
    await Promise.resolve()
    await Promise.resolve()
    expect(FakeWebSocket.instances).toHaveLength(1)
    expect(FakeWebSocket.instances[0]!.url).toBe("ws://tunnel-one/ws?token=a")
    FakeWebSocket.instances[0]!.open()

    // Drop the connection → reconnect timer → provider re-resolves → new URL.
    FakeWebSocket.instances[0]!.close()
    const reconnectTimer = (socket as any).reconnectTimer as number
    timers.runTimeout(reconnectTimer)
    await Promise.resolve()
    await Promise.resolve()

    expect(FakeWebSocket.instances).toHaveLength(2)
    expect(FakeWebSocket.instances[1]!.url).toBe("ws://tunnel-two/ws?token=b")
    socket.dispose()
  })

  test("provider failure schedules a reconnect with backoff", async () => {
    const statuses: string[] = []
    let failuresLeft = 1
    const socket = new KannaSocket(async () => {
      if (failuresLeft > 0) {
        failuresLeft -= 1
        throw new Error("ws-endpoint unreachable")
      }
      return "ws://tunnel/ws?token=x"
    })
    socket.onStatus((status) => statuses.push(status))

    socket.start()
    await Promise.resolve()
    await Promise.resolve()

    expect(FakeWebSocket.instances).toHaveLength(0)
    expect(statuses).toContain("disconnected")
    const reconnectTimer = (socket as any).reconnectTimer as number
    expect(timers.timeouts.has(reconnectTimer)).toBe(true)

    timers.runTimeout(reconnectTimer)
    await Promise.resolve()
    await Promise.resolve()
    expect(FakeWebSocket.instances).toHaveLength(1)
    expect(FakeWebSocket.instances[0]!.url).toBe("ws://tunnel/ws?token=x")
    socket.dispose()
  })

  test("dispose during provider resolution never opens a socket", async () => {
    const resolver: { resolve: ((url: string) => void) | null } = { resolve: null }
    const socket = new KannaSocket(
      () => new Promise<string>((resolve) => {
        resolver.resolve = resolve
      }),
    )

    socket.start()
    socket.dispose()
    resolver.resolve?.("ws://tunnel/ws?token=late")
    await Promise.resolve()
    await Promise.resolve()

    expect(FakeWebSocket.instances).toHaveLength(0)
  })
})
