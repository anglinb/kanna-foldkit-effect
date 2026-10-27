import { Context, Effect, FileSystem, Layer, Path } from "effect"
import { KannaLoggerLayer } from "../../effect/logger"
import { FileSystemExtras } from "../file-system-extras"
import { HttpHost } from "../http-host"
import type { RunnerEndpoint } from "../remote/connection"
import { RemoteMachinePlatform } from "../remote/host-platform"
import { makeSqliteFileSystem, type SqlRunner } from "../sqlite-file-system"
import { DurableObjectHostProcessLayer } from "./host-process"
import { makeDurableObjectHttpHost } from "./http-host"

export { PRIMARY_PORT } from "./http-host"

/**
 * Kanna's platform inside a Durable Object: its own state in the object's
 * SQLite, HTTP through the object's `fetch`, and the machine (repos,
 * processes, agent CLIs, terminals) at a runner.
 *
 * Returns the layer and the HTTP host whose `serve` the object's `fetch`
 * calls.
 */
export function makeDurableObjectPlatform(options: { readonly sql: SqlRunner; readonly runner: RunnerEndpoint }) {
  const http = makeDurableObjectHttpHost()
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
    layer: RemoteMachinePlatform({ runner: options.runner }).pipe(Layer.provideMerge(host)),
  }
}
