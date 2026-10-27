import { describe, expect, test } from "bun:test"
import { claudeCodeProcessSpawner } from "./claude-process"
import { testPlatformContext } from "./platform/testing"

const spawn = claudeCodeProcessSpawner(testPlatformContext)

describe("claudeCodeProcessSpawner", () => {
  test("pipes stdin to stdout and reports the exit after the output", async () => {
    const proc = spawn({ command: "cat", args: [], env: { PATH: process.env.PATH }, signal: new AbortController().signal })
    let output = ""
    proc.stdout.on("data", (chunk: Buffer) => {
      output += chunk.toString()
    })
    const exited = new Promise<[number | null, string | null]>((resolve) => proc.once("exit", (code, signal) => resolve([code, signal])))
    proc.stdin.write("{\"type\":\"user\"}\n")
    proc.stdin.end()
    expect(await exited).toEqual([0, null])
    expect(output).toBe("{\"type\":\"user\"}\n")
    expect(proc.exitCode).toBe(0)
  })

  test("kills on abort", async () => {
    const abort = new AbortController()
    const proc = spawn({ command: "sleep", args: ["30"], env: { PATH: process.env.PATH }, signal: abort.signal })
    const exited = new Promise<string | null>((resolve) => proc.once("exit", (_code, signal) => resolve(signal)))
    await new Promise((resolve) => setTimeout(resolve, 50))
    abort.abort()
    expect(await exited).toBe("SIGTERM")
    expect(proc.killed).toBe(true)
  })

  test("a missing executable is an error event", async () => {
    const proc = spawn({ command: "kanna-no-such-claude", args: [], env: { PATH: process.env.PATH }, signal: new AbortController().signal })
    const error = await new Promise<Error>((resolve) => proc.once("error", resolve))
    expect(error.message).toContain("kanna-no-such-claude")
  })
})
