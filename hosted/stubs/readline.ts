// workerd's node:readline loads but never yields lines, and the Claude Agent
// SDK reads `claude`'s stream-json output with `createInterface({ input })`
// and `for await`. The SDK imports bare "readline"; this alias swaps in a
// line splitter for that one function and passes the rest through.
import { EventEmitter } from "node:events"
import type { Readable } from "node:stream"

export * from "node:readline"

class LineReader extends EventEmitter implements AsyncIterable<string> {
  private buffered = ""
  private readonly lines: Array<string> = []
  private readonly waiting: Array<(result: IteratorResult<string>) => void> = []
  private closed = false
  private readonly decoder = new TextDecoder()

  constructor(private readonly input: Readable) {
    super()
    input.on("data", this.onData)
    input.on("end", this.onEnd)
    input.on("close", this.onEnd)
    input.on("error", this.onEnd)
  }

  private readonly onData = (chunk: Uint8Array | string) => {
    this.buffered += typeof chunk === "string" ? chunk : this.decoder.decode(chunk, { stream: true })
    let newline = this.buffered.indexOf("\n")
    while (newline !== -1) {
      const line = this.buffered.slice(0, newline).replace(/\r$/, "")
      this.buffered = this.buffered.slice(newline + 1)
      this.push(line)
      newline = this.buffered.indexOf("\n")
    }
  }

  private readonly onEnd = () => {
    if (this.buffered) this.push(this.buffered)
    this.buffered = ""
    this.close()
  }

  private push(line: string) {
    this.emit("line", line)
    const next = this.waiting.shift()
    if (next) next({ value: line, done: false })
    else this.lines.push(line)
  }

  close() {
    if (this.closed) return
    this.closed = true
    this.input.off("data", this.onData)
    this.input.off("end", this.onEnd)
    this.input.off("close", this.onEnd)
    this.input.off("error", this.onEnd)
    for (const next of this.waiting.splice(0)) next({ value: undefined, done: true })
    this.emit("close")
  }

  [Symbol.asyncIterator](): AsyncIterator<string> {
    return {
      next: () => {
        const line = this.lines.shift()
        if (line !== undefined) return Promise.resolve({ value: line, done: false })
        if (this.closed) return Promise.resolve({ value: undefined, done: true })
        return new Promise((resolve) => this.waiting.push(resolve))
      },
      return: () => {
        this.close()
        return Promise.resolve({ value: undefined, done: true })
      },
    }
  }
}

export function createInterface(options: { readonly input: Readable }) {
  return new LineReader(options.input)
}

export default { createInterface }
