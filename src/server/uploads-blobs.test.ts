import { afterEach, describe, expect, test } from "bun:test"
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync, existsSync, chmodSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { runTest } from "./effect/testing"
import { handleAttachmentContent, handleProjectUploadDelete } from "./http-file-routes"
import { makeR2UploadBlobs, type R2BucketLike } from "./upload-blobs-r2"
import { ensureUploadsOnMachine, persistProjectUpload, uploadBlobKey } from "./uploads"

/** An R2 bucket in memory. */
function memoryBucket() {
  const objects = new Map<string, { readonly bytes: Uint8Array; readonly contentType?: string }>()
  const bucket: R2BucketLike = {
    head: async (key) => objects.get(key) ?? null,
    get: async (key) => {
      const object = objects.get(key)
      return object ? { body: new Response(object.bytes as Uint8Array<ArrayBuffer>).body!, httpMetadata: { contentType: object.contentType } } : null
    },
    put: async (key, value, options) => {
      objects.set(key, { bytes: value, contentType: options?.httpMetadata?.contentType })
    },
    delete: async (key) => {
      objects.delete(key)
    },
  }
  return { bucket, objects }
}

const dirs: string[] = []
afterEach(() => {
  for (const dir of dirs.splice(0)) {
    chmodSync(dir, 0o755)
    rmSync(dir, { recursive: true, force: true })
  }
})
const project = () => {
  const dir = mkdtempSync(path.join(tmpdir(), "kanna-uploads-blobs-"))
  dirs.push(dir)
  return dir
}
const bytes = (text: string) => new TextEncoder().encode(text)

describe("uploads with a blob store", () => {
  test("go to the store and the machine, and a name taken in either gets a counter", async () => {
    const { bucket, objects } = memoryBucket()
    const blobs = makeR2UploadBlobs(bucket)
    const localPath = project()
    mkdirSync(path.join(localPath, ".kanna", "uploads"), { recursive: true })
    writeFileSync(path.join(localPath, ".kanna", "uploads", "notes.txt"), "from before the store")
    objects.set(uploadBlobKey("p1", "notes-1.txt"), { bytes: bytes("only in the store") })

    const attachment = await runTest(persistProjectUpload({ projectId: "p1", localPath, fileName: "notes.txt", bytes: bytes("new"), blobs }))
    expect(attachment.relativePath).toBe("./.kanna/uploads/notes-2.txt")
    expect(new TextDecoder().decode(objects.get(uploadBlobKey("p1", "notes-2.txt"))!.bytes)).toBe("new")
    expect(readFileSync(path.join(localPath, ".kanna", "uploads", "notes-2.txt"), "utf8")).toBe("new")
  })

  test("still land while the machine can't be written, and reach it before a turn", async () => {
    const { bucket, objects } = memoryBucket()
    const blobs = makeR2UploadBlobs(bucket)
    const localPath = project()
    // The machine refuses writes (it's offline, as far as uploads can tell).
    chmodSync(localPath, 0o555)
    const attachment = await runTest(persistProjectUpload({ projectId: "p1", localPath, fileName: "shot.png", bytes: bytes("png bytes"), blobs }))
    expect(objects.has(uploadBlobKey("p1", "shot.png"))).toBe(true)
    expect(existsSync(path.join(localPath, ".kanna", "uploads", "shot.png"))).toBe(false)

    // Viewing it needs only the store.
    const route = `/api/projects/p1/uploads/shot.png/content`
    const url = new URL(`http://localhost${route}`)
    const store = { getProject: () => ({ id: "p1", localPath }) as never }
    const response = await runTest(handleAttachmentContent(new Request(url), url, store, blobs))
    expect(response?.status).toBe(200)
    expect(await response!.text()).toBe("png bytes")

    // The machine comes back; the next turn with the attachment copies it over.
    chmodSync(localPath, 0o755)
    await runTest(ensureUploadsOnMachine({ blobs, projectId: "p1", localPath, attachments: [attachment] }))
    expect(readFileSync(path.join(localPath, ".kanna", "uploads", "shot.png"), "utf8")).toBe("png bytes")
  })

  test("serve an upload from before the store off the machine, and delete from both", async () => {
    const { bucket, objects } = memoryBucket()
    const blobs = makeR2UploadBlobs(bucket)
    const localPath = project()
    mkdirSync(path.join(localPath, ".kanna", "uploads"), { recursive: true })
    writeFileSync(path.join(localPath, ".kanna", "uploads", "old.txt"), "machine only")
    const store = { getProject: () => ({ id: "p1", localPath }) as never }
    const content = new URL("http://localhost/api/projects/p1/uploads/old.txt/content")
    const response = await runTest(handleAttachmentContent(new Request(content), content, store, blobs))
    expect(await response!.text()).toBe("machine only")

    await runTest(persistProjectUpload({ projectId: "p1", localPath, fileName: "new.txt", bytes: bytes("both"), blobs }))
    const remove = new URL("http://localhost/api/projects/p1/uploads/new.txt")
    const deleted = await runTest(handleProjectUploadDelete(new Request(remove, { method: "DELETE" }), remove, store, blobs))
    expect(await deleted!.json()).toEqual({ ok: true })
    expect(objects.has(uploadBlobKey("p1", "new.txt"))).toBe(false)
    expect(existsSync(path.join(localPath, ".kanna", "uploads", "new.txt"))).toBe(false)
  })

  test("two uploads of one name at once get different names", async () => {
    const { bucket } = memoryBucket()
    const blobs = makeR2UploadBlobs(bucket)
    const localPath = project()
    const [a, b] = await runTest(Effect.all([
      persistProjectUpload({ projectId: "p1", localPath, fileName: "same.txt", bytes: bytes("a"), blobs }),
      persistProjectUpload({ projectId: "p1", localPath, fileName: "same.txt", bytes: bytes("b"), blobs }),
    ], { concurrency: 2 }))
    expect(new Set([a.relativePath, b.relativePath]).size).toBe(2)
  })
})
