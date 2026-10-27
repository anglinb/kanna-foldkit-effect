import { Context, Effect, FileSystem, Layer, Path } from "effect"
import { KannaLoggerLayer } from "../../effect/logger"
import { FileSystemExtras } from "../file-system-extras"
import { HttpHost } from "../http-host"
import type { MachineFacts } from "../remote/client"
import type { RunnerEndpoint } from "../remote/connection"
import { RemoteMachinePlatform } from "../remote/host-platform"
import { makeSqliteFileSystem, type SqlRunner } from "../sqlite-file-system"
import { DurableObjectHostProcessLayer } from "./host-process"
import { makeDurableObjectHttpHost, type HibernationContext } from "./http-host"

export { PRIMARY_PORT, type HibernationContext, type WorkersWebSocket } from "./http-host"

/**
 * Kanna's own files in the object: a fixed path that is never the machine's,
 * so the data dir doesn't move with the runner's home directory.
 */
export const DURABLE_OBJECT_ROOT = "/kanna"

/**
 * Kanna's platform inside a Durable Object: its own state in the object's
 * SQLite, HTTP through the object's `fetch`, and the machine (repos,
 * processes, agent CLIs, terminals) at a runner.
 *
 * Returns the layer and the HTTP host whose `serve` the object's `fetch`
 * calls.
 */
export function makeDurableObjectPlatform(options: {
  readonly sql: SqlRunner
  /** The object's state, for its hibernatable browser sockets. */
  readonly hibernation: HibernationContext
  readonly runner: RunnerEndpoint
  /** The runner's home before it has ever connected: the container image's. */
  readonly defaultHome?: string
}) {
  const http = makeDurableObjectHttpHost({ hibernation: options.hibernation, sql: options.sql })
  const facts = machineFactsStore(options.sql)
  const home = options.defaultHome ?? "/home/kanna"
  const stored = Layer.effectContext(Effect.map(makeSqliteFileSystem(options.sql), ({ fs, extras }) =>
    Context.make(FileSystem.FileSystem, fs).pipe(Context.add(FileSystemExtras, extras))))
  const host = Layer.mergeAll(
    stored,
    Layer.succeed(HttpHost, http.service),
    DurableObjectHostProcessLayer,
    Path.layer,
    KannaLoggerLayer,
  )
  return {
    http,
    // The object starts from the runner's last-known facts, so Kanna (and
    // everything it shows from its own state) is up whether or not the runner
    // is.
    layer: RemoteMachinePlatform({
      runner: options.runner,
      lastKnown: facts.read() ?? {
        platform: "linux",
        arch: "x64",
        homeDirectory: home,
        tmpDirectory: "/tmp",
        workingDirectory: home,
        hostname: "runner",
        env: { HOME: home, PATH: "/usr/local/bin:/usr/bin:/bin" },
        ptyAvailable: true,
      },
      onFacts: facts.write,
      routes: { hostRoots: [DURABLE_OBJECT_ROOT] },
    }).pipe(Layer.provideMerge(host)),
  }
}

/** The runner's facts as it last reported them, kept in the object's SQLite. */
function machineFactsStore(sql: SqlRunner) {
  sql.exec("CREATE TABLE IF NOT EXISTS kanna_machine_facts (id INTEGER PRIMARY KEY CHECK (id = 1), json TEXT NOT NULL)")
  return {
    read: (): MachineFacts | null => {
      const row = [...sql.exec("SELECT json FROM kanna_machine_facts WHERE id = 1")][0]
      return row ? JSON.parse(String(row.json)) as MachineFacts : null
    },
    write: (facts: MachineFacts) => {
      sql.exec("INSERT OR REPLACE INTO kanna_machine_facts (id, json) VALUES (1, ?)", JSON.stringify(facts))
    },
  }
}
