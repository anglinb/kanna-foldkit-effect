import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { runTest } from "./effect/testing"
import { restoreHome, snapshotHome, type SnapshotBucket } from "./home-snapshot"
import { Machine } from "./platform/machine"

/** An R2 bucket in memory, multipart included. */
function memoryBucket() {
  const objects = new Map<string, Uint8Array>()
  const bucket: SnapshotBucket = {
    get: async (key) => {
      const bytes = objects.get(key)
      return bytes ? { body: new Response(bytes as Uint8Array<ArrayBuffer>).body!, text: async () => new TextDecoder().decode(bytes) } : null
    },
    put: async (key, value) => void objects.set(key, new TextEncoder().encode(value)),
    delete: async (keys) => {
      for (const key of Array.isArray(keys) ? keys : [keys]) objects.delete(key)
    },
    list: async ({ prefix }) => ({ objects: [...objects.keys()].filter((key) => key.startsWith(prefix)).map((key) => ({ key })) }),
    createMultipartUpload: async (key) => {
      const parts = new Map<number, Uint8Array>()
      return {
        uploadPart: async (partNumber, value) => {
          parts.set(partNumber, value.slice())
          return { partNumber, etag: `e${partNumber}` }
        },
        complete: async (done) => void objects.set(key, Buffer.concat(done.map((part) => parts.get(part.partNumber)!))),
        abort: async () => {},
      }
    },
  }
  return { bucket, objects }
}

let home: string
beforeEach(() => {
  home = mkdtempSync(path.join(tmpdir(), "kanna-home-"))
})
afterEach(() => {
  rmSync(home, { recursive: true, force: true })
})

/**
 * Runs `effect` with the machine's home at the temp dir. Not through HOME:
 * Bun's os.homedir() doesn't see a changed HOME, and this must never touch
 * the real one.
 */
const inHome = <A, E>(effect: Effect.Effect<A, E, any>) => runTest(Effect.gen(function* () {
  const machine = yield* Machine
  if (!home.startsWith(tmpdir())) throw new Error("refusing a home outside the temp dir")
  return yield* effect.pipe(Effect.provideService(Machine, { ...machine, homeDirectory: home }))
}) as Effect.Effect<A, E, never>)

describe("home snapshots", () => {
  test("a home comes back after the disk is wiped, past one part, without its caches", async () => {
    const { bucket, objects } = memoryBucket()
    mkdirSync(path.join(home, "repo", ".git"), { recursive: true })
    writeFileSync(path.join(home, "repo", "README.md"), "hello")
    mkdirSync(path.join(home, ".claude"), { recursive: true })
    writeFileSync(path.join(home, ".claude", ".credentials.json"), "{\"token\":\"t\"}")
    // Random bytes don't compress: the archive spans more than one 8 MiB part.
    writeFileSync(path.join(home, "repo", "big.bin"), crypto.getRandomValues(new Uint8Array(65_536)).length ? Buffer.concat(Array.from({ length: 160 }, () => Buffer.from(crypto.getRandomValues(new Uint8Array(65_536))))) : "")
    mkdirSync(path.join(home, ".cache"), { recursive: true })
    writeFileSync(path.join(home, ".cache", "junk"), "skip me")

    // A first connection: nothing to restore, but the home is now the real one.
    expect(await inHome(restoreHome(bucket, "homes/test"))).toBe("none")
    const saved = await inHome(snapshotHome(bucket, "homes/test"))
    expect(saved.size).toBeGreaterThan(8 * 1024 * 1024)
    expect(new TextDecoder().decode(objects.get("homes/test/latest"))).toBe(saved.key)

    // The container sleeps: a fresh disk.
    rmSync(home, { recursive: true, force: true })
    mkdirSync(home)
    expect(await inHome(restoreHome(bucket, "homes/test"))).toBe("restored")
    expect(readFileSync(path.join(home, "repo", "README.md"), "utf8")).toBe("hello")
    expect(readFileSync(path.join(home, ".claude", ".credentials.json"), "utf8")).toBe("{\"token\":\"t\"}")
    expect(readFileSync(path.join(home, "repo", "big.bin")).length).toBe(160 * 65_536)
    expect(existsSync(path.join(home, ".cache", "junk"))).toBe(false)

    // Restored once: the next connection leaves it alone.
    writeFileSync(path.join(home, "repo", "README.md"), "edited since")
    expect(await inHome(restoreHome(bucket, "homes/test"))).toBe("already")
    expect(readFileSync(path.join(home, "repo", "README.md"), "utf8")).toBe("edited since")
  }, 60_000)

  test("an unrestored home isn't saved over the last snapshot", async () => {
    const { bucket, objects } = memoryBucket()
    writeFileSync(path.join(home, "a.txt"), "a")
    const error = await inHome(snapshotHome(bucket, "homes/x").pipe(Effect.flip))
    expect(error.message).toContain("hasn't been restored")
    expect(objects.size).toBe(0)
  })

  test("a home with no snapshot yet is marked and left alone; old snapshots are pruned to two", async () => {
    const { bucket, objects } = memoryBucket()
    expect(await inHome(restoreHome(bucket, "homes/none"))).toBe("none")
    expect(await inHome(restoreHome(bucket, "homes/none"))).toBe("already")

    writeFileSync(path.join(home, "a.txt"), "a")
    for (let index = 0; index < 3; index += 1) {
      await inHome(snapshotHome(bucket, "homes/none"))
      await Bun.sleep(5)
    }
    expect([...objects.keys()].filter((key) => key.startsWith("homes/none/home-"))).toHaveLength(2)
  })
})
