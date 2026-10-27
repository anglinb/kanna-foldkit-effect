// Mistral's SDK (through Pi) imports @opentelemetry/api, an optional peer
// Kanna doesn't install. The Bun build leaves it external; the Worker bundle
// has no node_modules at run time, so it gets this no-op instead.
const noopSpan = { end() {}, setAttribute() {}, setAttributes() {}, setStatus() {}, recordException() {}, addEvent() {}, isRecording: () => false }
const noopTracer = { startSpan: () => noopSpan, startActiveSpan: (_name: string, ...args: any[]) => args[args.length - 1](noopSpan) }
export const trace = { getTracer: () => noopTracer, getActiveSpan: () => undefined, setSpan: (ctx: unknown) => ctx, getTracerProvider: () => ({ getTracer: () => noopTracer }) }
export const context = { active: () => ({}), with: (_ctx: unknown, fn: () => unknown) => fn() }
export const propagation = { inject() {}, extract: (ctx: unknown) => ctx }
export const SpanStatusCode = { UNSET: 0, OK: 1, ERROR: 2 }
export const SpanKind = { INTERNAL: 0, SERVER: 1, CLIENT: 2, PRODUCER: 3, CONSUMER: 4 }
