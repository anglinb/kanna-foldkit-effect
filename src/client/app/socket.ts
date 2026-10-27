import { recordClientPerformance } from "./clientPerformance"
import type {
  ClientCommand,
  ClientEnvelope,
  ServerEnvelope,
  SubscriptionTopic,
  TerminalEvent,
  TerminalSnapshot,
} from "../../shared/protocol"
import { LOG_PREFIX } from "../../shared/branding"
import { TEXT_PING, TEXT_PONG } from "../../shared/protocol"
import { generateUUID } from "../lib/utils"

type SnapshotListener<T> = (value: T) => void
type EventListener<T> = (value: T) => void
export type SocketStatus = "connecting" | "connected" | "disconnected"
type StatusListener = (status: SocketStatus) => void

const STALE_CONNECTION_MS = 25_000
const HEARTBEAT_INTERVAL_MS = 15_000
const PING_TIMEOUT_MS = 4_000

interface SubscriptionEntry<TSnapshot, TEvent = never> {
  topic: SubscriptionTopic
  paused?: boolean
  listener: SnapshotListener<TSnapshot>
  eventListener?: EventListener<TEvent>
  /**
   * The topic to resend after a reconnect, in place of the original. A chat
   * subscription uses it to name the span it holds by then, so the server
   * resumes with a tail instead of the full window it sent the first time.
   */
  topicOnReconnect?: () => SubscriptionTopic
}

/**
 * Where to connect: a fixed URL, or an async provider resolved before every
 * connection attempt. Cloud mode uses a provider that fetches
 * /api/cloud/ws-endpoint so each (re)connect picks up the current tunnel URL
 * and a fresh connect token.
 */
export type WsUrlSource = string | (() => Promise<string>)

export class KannaSocket {
  private readonly urlSource: WsUrlSource
  private connectSeq = 0
  private ws: WebSocket | null = null
  private started = false
  private reconnectTimer: number | null = null
  private reconnectDelayMs = 750
  private readonly subscriptions = new Map<string, SubscriptionEntry<unknown, unknown>>()
  private readonly pending = new Map<string, { resolve: (value: unknown) => void; reject: (reason?: unknown) => void }>()
  private readonly outboundQueue: ClientEnvelope[] = []
  private readonly statusListeners = new Set<StatusListener>()
  private heartbeatTimer: number | null = null
  private pingTimeoutTimer: number | null = null
  private pingPromise: Promise<void> | null = null
  private lastOpenAt = 0
  private lastMessageAt = 0
  private reconnectImmediatelyOnClose = false
  /** Until this connection's server shows it can't (see TEXT_PING). */
  private textPingSupported = true
  private pongWaiter: { readonly resolve: () => void; readonly reject: (error: Error) => void; readonly fallBack: () => void } | null = null
  private readonly handleWindowFocus = () => {
    void this.ensureHealthyConnection()
  }
  private readonly handleVisibilityChange = () => {
    this.syncVisibility()
    if (document.visibilityState === "visible") {
      this.startHeartbeat()
      void this.ensureHealthyConnection()
      return
    }
    this.stopHeartbeat()
  }
  private readonly handleOnline = () => {
    void this.ensureHealthyConnection()
  }

  constructor(url: WsUrlSource) {
    this.urlSource = url
  }

  start() {
    if (this.started) {
      return
    }
    this.started = true
    window.addEventListener("focus", this.handleWindowFocus)
    window.addEventListener("online", this.handleOnline)
    document.addEventListener("visibilitychange", this.handleVisibilityChange)
    this.connect()
  }

  dispose() {
    this.started = false
    if (this.reconnectTimer) {
      window.clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.stopHeartbeat()
    this.clearPingState()
    window.removeEventListener("focus", this.handleWindowFocus)
    window.removeEventListener("online", this.handleOnline)
    document.removeEventListener("visibilitychange", this.handleVisibilityChange)
    this.ws?.close()
    this.ws = null
    for (const pending of this.pending.values()) {
      pending.reject(new Error("Socket disposed"))
    }
    this.pending.clear()
    this.outboundQueue.length = 0
  }

  onStatus(listener: StatusListener) {
    this.statusListeners.add(listener)
    listener(this.getStatus())
    return () => {
      this.statusListeners.delete(listener)
    }
  }

  subscribe<TSnapshot, TEvent = never>(
    topic: SubscriptionTopic,
    listener: SnapshotListener<TSnapshot>,
    eventListener?: EventListener<TEvent>,
    options?: { topicOnReconnect?: () => SubscriptionTopic }
  ) {
    const id = generateUUID()
    this.subscriptions.set(id, {
      topic,
      listener: listener as SnapshotListener<unknown>,
      eventListener: eventListener as EventListener<unknown> | undefined,
      topicOnReconnect: options?.topicOnReconnect,
    })
    this.enqueue({ v: 1, type: "subscribe", id, topic })
    return () => {
      this.subscriptions.delete(id)
      this.enqueue({ v: 1, type: "unsubscribe", id })
    }
  }

  subscribeTerminal(
    terminalId: string,
    handlers: {
      onSnapshot: SnapshotListener<TerminalSnapshot | null>
      onEvent?: EventListener<TerminalEvent>
    }
  ) {
    const id = generateUUID()
    const topic: SubscriptionTopic = { type: "terminal", terminalId }
    this.subscriptions.set(id, {
      topic,
      listener: handlers.onSnapshot as SnapshotListener<unknown>,
      eventListener: handlers.onEvent as EventListener<unknown> | undefined,
    })
    this.enqueue({ v: 1, type: "subscribe", id, topic })
    return () => {
      this.subscriptions.delete(id)
      this.enqueue({ v: 1, type: "unsubscribe", id })
    }
  }

  command<TResult = unknown>(command: ClientCommand) {
    const id = generateUUID()
    const envelope: ClientEnvelope = { v: 1, type: "command", id, command }
    return new Promise<TResult>((resolve, reject) => {
      this.pending.set(id, { resolve: resolve as (value: unknown) => void, reject })
      this.enqueue(envelope)
    })
  }

  ensureHealthyConnection() {
    if (!this.ws || this.ws.readyState === WebSocket.CLOSED || this.ws.readyState === WebSocket.CLOSING) {
      this.reconnectNow()
      return Promise.resolve()
    }

    if (this.ws.readyState === WebSocket.CONNECTING) {
      return Promise.resolve()
    }

    if (!this.isConnectionStale()) {
      return Promise.resolve()
    }

    return this.sendPing()
  }

  private connect() {
    if (!this.started) {
      return
    }
    this.emitStatus("connecting")

    // Fixed URL: connect synchronously (unchanged local behavior).
    if (typeof this.urlSource === "string") {
      this.openSocket(this.urlSource)
      return
    }

    // Async provider (cloud mode): resolve the URL before each attempt so
    // reconnects pick up rotated tunnel URLs and fresh connect tokens. The
    // sequence counter drops stale resolves when a newer connect attempt (or
    // dispose) started meanwhile.
    const seq = ++this.connectSeq
    this.urlSource()
      .then((url) => {
        if (!this.started || seq !== this.connectSeq) {
          return
        }
        this.openSocket(url)
      })
      .catch(() => {
        if (!this.started || seq !== this.connectSeq) {
          return
        }
        this.emitStatus("disconnected")
        this.scheduleReconnect()
      })
  }

  private openSocket(url: string) {
    this.ws = new WebSocket(url)

    this.ws.addEventListener("open", () => {
      this.reconnectDelayMs = 750
      this.reconnectImmediatelyOnClose = false
      this.lastOpenAt = Date.now()
      this.lastMessageAt = this.lastOpenAt
      this.textPingSupported = true
      this.emitStatus("connected")
      this.startHeartbeat()
      for (const [id, subscription] of this.subscriptions.entries()) {
        subscription.paused = this.shouldPause(subscription.topic)
        if (subscription.paused) continue
        const topic = subscription.topicOnReconnect?.() ?? subscription.topic
        this.sendNow({ v: 1, type: "subscribe", id, topic })
      }
      while (this.outboundQueue.length > 0) {
        const envelope = this.outboundQueue.shift()
        if (envelope) {
          this.sendNow(envelope)
        }
      }
    })

    this.ws.addEventListener("message", (event) => {
      this.lastMessageAt = Date.now()
      if (event.data === TEXT_PONG) {
        this.pongWaiter?.resolve()
        this.pongWaiter = null
        return
      }
      let payload: ServerEnvelope
      try {
        const started = performance.now()
        const text = String(event.data)
        payload = JSON.parse(text) as ServerEnvelope
        recordClientPerformance("socket_parse_ms", performance.now() - started)
        recordClientPerformance("socket_message_bytes", text.length)
        recordClientPerformance("socket_messages", 1)
      } catch {
        return
      }

      if (payload.type === "snapshot") {
        const subscription = this.subscriptions.get(payload.id)
        if (!subscription?.paused) subscription?.listener(payload.snapshot.data)
        return
      }

      if (payload.type === "event") {
        const subscription = this.subscriptions.get(payload.id)
        if (!subscription?.paused) subscription?.eventListener?.(payload.event)
        return
      }

      if (payload.type === "ack") {
        const pending = this.pending.get(payload.id)
        if (!pending) return
        this.pending.delete(payload.id)
        pending.resolve(payload.result)
        return
      }

      if (payload.type === "error") {
        if (!payload.id && this.pongWaiter && payload.message === "Invalid JSON") {
          // A server from before TEXT_PING: ping it the old way from now on.
          this.textPingSupported = false
          this.pongWaiter.fallBack()
          this.pongWaiter = null
          return
        }
        if (!payload.id) {
          console.error(LOG_PREFIX, payload.message)
          return
        }
        const pending = this.pending.get(payload.id)
        if (!pending) return
        this.pending.delete(payload.id)
        pending.reject(new Error(payload.message))
      }
    })

    this.ws.addEventListener("close", () => {
      if (!this.started) {
        return
      }
      const reconnectImmediately = this.reconnectImmediatelyOnClose
      this.reconnectImmediatelyOnClose = false
      this.stopHeartbeat()
      this.clearPingState()
      this.emitStatus("disconnected")
      for (const pending of this.pending.values()) {
        pending.reject(new Error("Disconnected"))
      }
      this.pongWaiter?.reject(new Error("Disconnected"))
      this.pongWaiter = null
      this.pending.clear()
      if (reconnectImmediately) {
        this.connect()
        return
      }
      this.scheduleReconnect()
    })
  }

  private scheduleReconnect() {
    if (this.reconnectTimer !== null) return
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
      this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, 5_000)
    }, this.reconnectDelayMs)
  }

  private getStatus(): SocketStatus {
    if (this.ws?.readyState === WebSocket.OPEN) {
      return "connected"
    }
    if (this.ws?.readyState === WebSocket.CONNECTING) {
      return "connecting"
    }
    return "disconnected"
  }

  private emitStatus(status: SocketStatus) {
    for (const listener of this.statusListeners) {
      listener(status)
    }
  }

  private isConnectionStale() {
    const baseline = Math.max(this.lastMessageAt, this.lastOpenAt)
    return baseline > 0 && Date.now() - baseline >= STALE_CONNECTION_MS
  }

  private sendPing() {
    if (this.pingPromise) {
      return this.pingPromise
    }

    // A text ping when the server takes one: a sleeping hosted server answers
    // it without waking (see TEXT_PING).
    const ping = this.textPingSupported && this.ws?.readyState === WebSocket.OPEN
      ? new Promise<void>((resolve, reject) => {
        this.pongWaiter = {
          resolve,
          reject,
          fallBack: () => this.command({ type: "system.ping" }).then(() => resolve(), reject),
        }
        this.ws!.send(TEXT_PING)
      })
      : this.command({ type: "system.ping" })
    const pingPromise = ping
      .then(() => {
        this.clearPingState()
      })
      .catch((error) => {
        this.clearPingState()
        this.reconnectNow()
        throw error
      })

    this.pingTimeoutTimer = window.setTimeout(() => {
      this.clearPingState()
      this.reconnectNow()
    }, PING_TIMEOUT_MS)

    this.pingPromise = pingPromise
    return pingPromise
  }

  private reconnectNow() {
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }

    if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
      this.connect()
      return
    }

    if (this.ws.readyState === WebSocket.CONNECTING) {
      return
    }

    this.reconnectImmediatelyOnClose = true
    this.ws.close()
  }

  private startHeartbeat() {
    if (document.visibilityState !== "visible") {
      return
    }

    if (this.heartbeatTimer !== null) {
      return
    }

    this.heartbeatTimer = window.setInterval(() => {
      if (document.visibilityState !== "visible") {
        this.stopHeartbeat()
        return
      }
      if (this.ws?.readyState !== WebSocket.OPEN) {
        return
      }
      void this.ensureHealthyConnection()
    }, HEARTBEAT_INTERVAL_MS)
  }

  private stopHeartbeat() {
    if (this.heartbeatTimer !== null) {
      window.clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  private clearPingState() {
    if (this.pingTimeoutTimer !== null) {
      window.clearTimeout(this.pingTimeoutTimer)
      this.pingTimeoutTimer = null
    }
    this.pingPromise = null
  }

  private shouldPause(topic: SubscriptionTopic) {
    return document.visibilityState === "hidden" && ["chat", "project-git", "terminal"].includes(topic.type)
  }

  private syncVisibility() {
    for (const [id, subscription] of this.subscriptions) {
      const paused = this.shouldPause(subscription.topic)
      if (paused === Boolean(subscription.paused)) continue
      subscription.paused = paused
      if (this.ws?.readyState !== WebSocket.OPEN) continue
      if (paused) this.sendNow({ v: 1, type: "unsubscribe", id })
      else this.sendNow({ v: 1, type: "subscribe", id, topic: subscription.topicOnReconnect?.() ?? subscription.topic })
    }
  }

  getResourceCounts() {
    return {
      socket_pending_commands: this.pending.size,
      socket_subscriptions: this.subscriptions.size,
      socket_queued_commands: this.outboundQueue.length,
    }
  }

  private enqueue(envelope: ClientEnvelope) {
    if (envelope.type === "subscribe") {
      const subscription = this.subscriptions.get(envelope.id)
      if (subscription) subscription.paused = this.shouldPause(subscription.topic)
      if (subscription?.paused || this.ws?.readyState !== WebSocket.OPEN) return
    }
    // Reconnect replays the current subscriptions, not stale queued changes.
    if (envelope.type === "unsubscribe" && this.ws?.readyState !== WebSocket.OPEN) return
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.sendNow(envelope)
      return
    }
    this.outboundQueue.push(envelope)
  }

  private sendNow(envelope: ClientEnvelope) {
    this.ws?.send(JSON.stringify(envelope))
  }
}
