import { EventEmitter } from "node:events"
import { Readable, Writable } from "node:stream"
import type { SpawnedProcess as ClaudeSpawnedProcess, SpawnOptions as ClaudeSpawnOptions } from "@anthropic-ai/claude-agent-sdk"
import { type Context, Effect, Exit, Scope, Stream } from "effect"
import { ProcessSpawner, type ProcessSignal, type SpawnedProcess } from "./platform/process-spawner"

/**
 * The Claude Agent SDK's `spawnClaudeCodeProcess`, on Kanna's
 * `ProcessSpawner`. The SDK spawns `claude` itself with node's
 * `child_process`, which only reaches this computer; when the machine is
 * elsewhere (`Machine.remote`), `claude` has to start there instead.
 *
 * The SDK wants the process back synchronously, with node streams, so the
 * spawn runs in the background: stdin writes wait for it, and a failed
 * spawn surfaces as the process's `error` event, as a missing executable
 * does with `child_process`. The process belongs to its own scope, closed
 * once it exits.
 */
export function claudeCodeProcessSpawner(platform: Context.Context<ProcessSpawner>) {
  return (options: ClaudeSpawnOptions): ClaudeSpawnedProcess => {
    const events = new EventEmitter()
    const scope = Effect.runSync(Scope.make())
    const run = <A, E>(effect: Effect.Effect<A, E, ProcessSpawner>) => Effect.runPromise(Effect.provide(effect, platform))
    let exitCode: number | null = null
    let signalCode: NodeJS.Signals | null = null
    let killed = false
    let pendingKill: ProcessSignal | null = null

    const started: Promise<SpawnedProcess> = run(Effect.gen(function* () {
      const spawner = yield* ProcessSpawner
      return yield* spawner.spawn([options.command, ...options.args], {
        ...(options.cwd !== undefined ? { cwd: options.cwd } : {}),
        env: options.env,
        stdin: "pipe",
        stdout: "pipe",
        // The SDK reads nothing from stderr when it doesn't own the spawn;
        // drop it rather than let the pipe fill.
        stderr: "ignore",
      }).pipe(Scope.provide(scope))
    }))

    const stdout = new Readable({ read() {} })
    const stdin = new Writable({
      write(chunk: Buffer | string, _encoding, callback) {
        started
          .then((proc) => run(proc.writeStdin(typeof chunk === "string" ? chunk : new Uint8Array(chunk))))
          .then(() => callback(), (error: Error) => callback(error))
      },
      final(callback) {
        started.then((proc) => run(proc.endStdin)).then(() => callback(), () => callback())
      },
    })

    started.then(
      (proc) => {
        if (pendingKill) void run(proc.kill(pendingKill))
        const drained = run(Stream.runForEach(proc.stdout, (chunk) => Effect.sync(() => {
          stdout.push(Buffer.from(chunk))
        }))).catch(() => {}).finally(() => stdout.push(null))
        void run(proc.exited).then(async (exit) => {
          // Every stdout chunk is pushed before the exit is reported, so the
          // SDK has the last of the output when it acts on the exit.
          await drained
          exitCode = exit.exitCode
          signalCode = exit.signalCode
          events.emit("exit", exit.exitCode, exit.signalCode)
          await Effect.runPromise(Scope.close(scope, Exit.void))
        })
      },
      (error: unknown) => {
        stdout.push(null)
        events.emit("error", error instanceof Error ? error : new Error(String(error)))
        void Effect.runPromise(Scope.close(scope, Exit.void))
      },
    )

    const kill = (signal: NodeJS.Signals = "SIGTERM") => {
      if (exitCode !== null || signalCode !== null) return false
      killed = true
      pendingKill = signal
      void started.then((proc) => run(proc.kill(signal)), () => {})
      return true
    }
    options.signal?.addEventListener("abort", () => kill("SIGTERM"), { once: true })

    return {
      stdin,
      stdout,
      get killed() {
        return killed
      },
      get exitCode() {
        return exitCode
      },
      get signalCode() {
        return signalCode
      },
      kill,
      on: (event: "exit" | "error", listener: (...args: any[]) => void) => {
        events.on(event, listener)
      },
      once: (event: "exit" | "error", listener: (...args: any[]) => void) => {
        events.once(event, listener)
      },
      off: (event: "exit" | "error", listener: (...args: any[]) => void) => {
        events.off(event, listener)
      },
    } as ClaudeSpawnedProcess
  }
}
