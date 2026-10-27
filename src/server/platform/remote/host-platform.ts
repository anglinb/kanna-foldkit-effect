import { Context, Effect, FileSystem, Layer } from "effect"
import { getDataDir, getDataRootDir } from "../../../shared/branding"
import { FileSystemExtras } from "../file-system-extras"
import { Machine } from "../machine"
import { makeRoutedFileSystem, type PathRoutes } from "../routed-file-system"
import { RemoteMachineLayer, type RemoteMachineOptions } from "./connection"

export interface RemoteMachinePlatformOptions extends RemoteMachineOptions {
  /**
   * Host paths besides the data root that stay with the host: the built
   * client and export viewer the server serves.
   */
  readonly hostRoots?: ReadonlyArray<string>
  /**
   * Replaces the default routes (the data root named from the machine's
   * home, minus its account dirs). A host whose data dir has a fixed path of
   * its own (a Durable Object) routes just that.
   */
  readonly routes?: PathRoutes
}

/**
 * A host's platform with the machine at a runner: `Machine`, processes and
 * PTYs from the runner, and one `FileSystem` routed between the two.
 *
 * Kanna's state (the data root, named from the machine's home as it always
 * is) stays with the host, in whatever `FileSystem` the host provides: its
 * disk on Bun, Durable Object storage hosted. Inside the data dir, the
 * account config dirs go to the machine, because the CLIs there read them.
 * Every other path is the machine's.
 *
 * Needs the host's own `FileSystem` and `FileSystemExtras`; the host's
 * `HttpHost`, `HostProcess` and `Path` pass through untouched.
 */
export const RemoteMachinePlatform = (options: RemoteMachinePlatformOptions) =>
  Layer.effectContext(Effect.gen(function* () {
    const hostFs = yield* FileSystem.FileSystem
    const hostExtras = yield* FileSystemExtras
    const remote = yield* Layer.build(RemoteMachineLayer(options))
    const machine = Context.get(remote, Machine)
    const routed = makeRoutedFileSystem(
      options.routes ?? {
        hostRoots: [getDataRootDir(machine.homeDirectory), ...(options.hostRoots ?? [])],
        machineRoots: [`${getDataDir(machine.homeDirectory)}/accounts`],
      },
      { fs: hostFs, extras: hostExtras },
      { fs: Context.get(remote, FileSystem.FileSystem), extras: Context.get(remote, FileSystemExtras) },
    )
    return remote.pipe(
      Context.add(FileSystem.FileSystem, routed.fs),
      Context.add(FileSystemExtras, routed.extras),
    )
  }))
