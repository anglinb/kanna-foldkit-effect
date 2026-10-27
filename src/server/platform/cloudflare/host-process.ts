import { Layer } from "effect"
import { HostProcess } from "../host-process"

/**
 * A Durable Object has no OS process of its own to report. Diagnostics get
 * a stand-in: a fixed pid, the object's age as uptime, and no memory or CPU
 * figures (Workers don't expose them).
 */
export const DurableObjectHostProcessLayer = Layer.sync(HostProcess, () => {
  const startedAt = Date.now()
  return {
    pid: 1,
    parentPid: 0,
    runtimeVersion: undefined,
    uptimeSeconds: () => (Date.now() - startedAt) / 1000,
    memoryUsage: () => ({ rss: 0, heapTotal: 0, heapUsed: 0, external: 0, arrayBuffers: 0 }),
    cpuUsage: () => ({ user: 0, system: 0 }),
  }
})
