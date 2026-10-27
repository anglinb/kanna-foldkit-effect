import { randomUUID } from "node:crypto"
import path from "node:path"
import { Effect, FileSystem, Schema } from "effect"
import { fileTypeFromBuffer } from "file-type"
import type { ChatAttachment } from "../shared/types"
import { getProjectUploadDir } from "./paths"
import { Machine } from "./platform/machine"

const DEFAULT_BINARY_MIME_TYPE = "application/octet-stream"
const IMAGE_MIME_PREFIX = "image/"
const TEXT_PLAIN_CONTENT_TYPE = "text/plain; charset=utf-8"

const TEXT_CONTENT_TYPE_BY_EXTENSION = new Map<string, string>([
  [".csv", "text/csv; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".jsonc", TEXT_PLAIN_CONTENT_TYPE],
  [".md", "text/markdown; charset=utf-8"],
  [".tsv", "text/tab-separated-values; charset=utf-8"],
])

const TEXT_LIKE_EXTENSIONS = new Set([
  ".c", ".cc", ".cfg", ".conf", ".cpp", ".cs", ".css", ".env", ".go", ".graphql", ".h", ".hpp", ".html",
  ".ini", ".java", ".js", ".jsx", ".kt", ".lua", ".mjs", ".php", ".pl", ".properties", ".py", ".rb", ".rs",
  ".scss", ".sh", ".sql", ".swift", ".toml", ".ts", ".tsx", ".txt", ".vue", ".xml", ".yaml", ".yml", ".zsh",
])

function sanitizeFileName(fileName: string) {
  const baseName = path.basename(fileName).trim()
  const cleaned = baseName.replace(/[^\w.-]+/g, "-").replace(/^-+|-+$/g, "")
  return cleaned || "upload"
}

function getUploadCandidateNames(originalName: string) {
  const sanitizedName = sanitizeFileName(originalName)
  const parsed = path.parse(sanitizedName)
  const extension = parsed.ext
  const name = parsed.name || "upload"

  return {
    first: sanitizedName,
    withCounter(counter: number) {
      return `${name}-${counter}${extension}`
    },
  }
}

export class UploadError extends Schema.TaggedError<UploadError>()(
  "@kanna/server/uploads/UploadError",
  {
    operation: Schema.Literals(["detect", "write"]),
    projectId: Schema.String,
    filePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return this.cause instanceof Error ? this.cause.message : String(this.cause)
  }
}

export interface PersistProjectUploadArgs {
  projectId: string
  localPath: string
  fileName: string
  bytes: Uint8Array
  fallbackMimeType?: string
  /** Where uploads are kept when the machine isn't always there (hosted: R2). */
  blobs?: UploadBlobStore | null
}

/**
 * Upload bodies kept apart from the machine, so attachments still show while
 * the machine is offline (hosted Kanna puts them in R2). The machine still
 * gets a copy: agents open attachments from disk. Locally there is none, and
 * uploads are only the files in the project's `.kanna/uploads`.
 */
export interface UploadBlobStore {
  /** Stores `bytes` under `key` unless something is there already; false if it is. */
  readonly putIfAbsent: (key: string, bytes: Uint8Array, contentType: string) => Effect.Effect<boolean, UploadBlobError>
  /** The stored upload, or null. */
  readonly get: (key: string) => Effect.Effect<{ readonly body: ReadableStream<Uint8Array>; readonly contentType: string } | null, UploadBlobError>
  readonly remove: (key: string) => Effect.Effect<void, UploadBlobError>
}

export class UploadBlobError extends Schema.TaggedError<UploadBlobError>()(
  "@kanna/server/uploads/UploadBlobError",
  { operation: Schema.Literals(["put", "get", "remove"]), key: Schema.String, cause: Schema.Defect() },
) {}

/** An upload's key in the blob store: the project, then the name it is stored under. */
export const uploadBlobKey = (projectId: string, storedName: string) => `uploads/${projectId}/${storedName}`

/** The stored name of an attachment that is a project upload, or null. */
export function uploadStoredName(attachment: Pick<ChatAttachment, "relativePath">) {
  const match = /^\.\/\.kanna\/uploads\/([^/\\]+)$/.exec(attachment.relativePath)
  return match ? match[1]! : null
}

/**
 * Writes an upload into the project's `.kanna/uploads`, never over another
 * file: a taken name gets a counter (`notes-1.txt`), decided by an exclusive
 * create, so concurrent uploads of one name all land.
 */
export const persistProjectUpload = Effect.fn("Uploads.persist")(function* (args: PersistProjectUploadArgs) {
  yield* Effect.annotateCurrentSpan("projectId", args.projectId)
  const fs = yield* FileSystem.FileSystem
  const uploadDir = getProjectUploadDir(args.localPath, (yield* Machine).homeDirectory)
  const writeError = (filePath: string) => (cause: unknown) =>
    Effect.fail(new UploadError({ operation: "write", projectId: args.projectId, filePath, cause }))
  yield* fs.makeDirectory(uploadDir, { recursive: true }).pipe(
    Effect.catchTag("PlatformError", (cause) => args.blobs ? Effect.void : writeError(uploadDir)(cause)),
  )

  const detectedType = yield* Effect.tryPromise({
    try: () => fileTypeFromBuffer(args.bytes),
    catch: (cause) => new UploadError({ operation: "detect", projectId: args.projectId, filePath: args.fileName, cause }),
  })
  const mimeType = detectedType?.mime ?? args.fallbackMimeType ?? DEFAULT_BINARY_MIME_TYPE
  const candidates = getUploadCandidateNames(args.fileName)

  const blobs = args.blobs ?? null
  const storeOnMachine = (storedName: string, counter: number): Effect.Effect<string, UploadError> => {
    const absolutePath = path.join(uploadDir, storedName)
    return fs.writeFile(absolutePath, args.bytes, { flag: "wx" }).pipe(
      Effect.as(storedName),
      Effect.catchReason("PlatformError", "AlreadyExists", () => storeOnMachine(candidates.withCounter(counter), counter + 1)),
      Effect.catchTag("PlatformError", writeError(absolutePath)),
    )
  }
  // With a blob store, it decides the name (taken there, or on the machine,
  // means try the next) and holds the upload; the machine's copy is for the
  // agents and may wait until a turn needs it (`ensureUploadsOnMachine`).
  const storeInBlobs = (storedName: string, counter: number): Effect.Effect<string, UploadError> => Effect.gen(function* () {
    const absolutePath = path.join(uploadDir, storedName)
    const next = () => storeInBlobs(candidates.withCounter(counter), counter + 1)
    if (yield* fs.exists(absolutePath).pipe(Effect.orElseSucceed(() => false))) return yield* next()
    const stored = yield* blobs!.putIfAbsent(uploadBlobKey(args.projectId, storedName), args.bytes, mimeType).pipe(
      Effect.mapError((cause) => new UploadError({ operation: "write", projectId: args.projectId, filePath: absolutePath, cause })),
    )
    if (!stored) return yield* next()
    yield* fs.writeFile(absolutePath, args.bytes, { flag: "wx" }).pipe(
      Effect.catchCause((cause) => Effect.logWarning(`upload ${storedName} not copied to the machine yet`, cause)),
    )
    return storedName
  })
  const storedName = yield* (blobs ? storeInBlobs(candidates.first, 1) : storeOnMachine(candidates.first, 1))
  const absolutePath = path.join(uploadDir, storedName)

  return {
    id: randomUUID(),
    kind: mimeType.startsWith(IMAGE_MIME_PREFIX) ? "image" : "file",
    displayName: args.fileName,
    absolutePath,
    relativePath: `./.kanna/uploads/${storedName}`,
    contentUrl: `/api/projects/${args.projectId}/uploads/${encodeURIComponent(storedName)}/content`,
    mimeType,
    size: args.bytes.byteLength,
  } satisfies ChatAttachment
})

export function inferAttachmentContentType(fileName: string, fallbackType?: string): string {
  const extension = path.extname(fileName).toLowerCase()
  const mappedType = TEXT_CONTENT_TYPE_BY_EXTENSION.get(extension)
  if (mappedType) {
    return mappedType
  }

  if (TEXT_LIKE_EXTENSIONS.has(extension)) {
    return TEXT_PLAIN_CONTENT_TYPE
  }

  return fallbackType || DEFAULT_BINARY_MIME_TYPE
}

export function inferProjectFileContentType(fileName: string, fallbackType?: string): string {
  return inferAttachmentContentType(fileName, fallbackType)
}

/** True when the upload is gone (or never was); false for a name outside the uploads dir or a failed delete. */
export const deleteProjectUpload = Effect.fn("Uploads.delete")(function* (args: {
  localPath: string
  storedName: string
  /** With a blob store, the stored copy goes too. */
  blobs?: { readonly store: UploadBlobStore; readonly projectId: string } | null
}) {
  const storedName = args.storedName
  if (!storedName || storedName.includes("/") || storedName.includes("\\") || storedName === "." || storedName === "..") {
    return false
  }

  const fs = yield* FileSystem.FileSystem
  const absolutePath = path.join(getProjectUploadDir(args.localPath, (yield* Machine).homeDirectory), storedName)
  if (args.blobs) {
    const removed = yield* args.blobs.store.remove(uploadBlobKey(args.blobs.projectId, storedName)).pipe(Effect.as(true), Effect.orElseSucceed(() => false))
    // The machine's copy may be out of reach; the stored one was the upload.
    yield* fs.remove(absolutePath, { force: true }).pipe(Effect.ignore)
    return removed
  }
  return yield* fs.remove(absolutePath, { force: true }).pipe(
    Effect.as(true),
    Effect.catchTag("PlatformError", () => Effect.succeed(false)),
  )
})

/**
 * Puts back on the machine any of a turn's attachments that are only in the
 * blob store (uploaded while it was offline, or the machine was replaced), so
 * the agent finds them where the prompt says. Best effort: one that can't be
 * copied is left for the agent to report missing.
 */
export const ensureUploadsOnMachine = Effect.fn("Uploads.ensureOnMachine")(function* (args: {
  blobs: UploadBlobStore
  projectId: string
  localPath: string
  attachments: ReadonlyArray<ChatAttachment>
}) {
  const fs = yield* FileSystem.FileSystem
  const uploadDir = getProjectUploadDir(args.localPath, (yield* Machine).homeDirectory)
  for (const attachment of args.attachments) {
    const storedName = uploadStoredName(attachment)
    if (!storedName) continue
    const absolutePath = path.join(uploadDir, storedName)
    if (yield* fs.exists(absolutePath).pipe(Effect.orElseSucceed(() => true))) continue
    yield* Effect.gen(function* () {
      const stored = yield* args.blobs.get(uploadBlobKey(args.projectId, storedName))
      if (!stored) return
      const bytes = new Uint8Array(yield* Effect.promise(() => new Response(stored.body).arrayBuffer()))
      yield* fs.makeDirectory(uploadDir, { recursive: true })
      yield* fs.writeFile(absolutePath, bytes)
    }).pipe(Effect.catchCause((cause) => Effect.logWarning(`could not copy upload ${storedName} to the machine`, cause)))
  }
})
