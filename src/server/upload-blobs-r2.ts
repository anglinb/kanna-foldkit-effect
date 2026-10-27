import { Effect } from "effect"
import { UploadBlobError, type UploadBlobStore } from "./uploads"

/** The slice of an R2 bucket binding this needs (declared here: the server typechecks without Workers' types). */
export interface R2BucketLike {
  head(key: string): Promise<unknown | null>
  get(key: string): Promise<{ readonly body: ReadableStream<Uint8Array>; readonly httpMetadata?: { readonly contentType?: string } } | null>
  put(key: string, value: Uint8Array, options?: { readonly httpMetadata?: { readonly contentType?: string } }): Promise<unknown>
  delete(key: string): Promise<void>
}

/**
 * Uploads in an R2 bucket, for hosted Kanna. One Durable Object is the only
 * writer, so `putIfAbsent` is exclusive by keeping the names it is writing:
 * two uploads of one name at once can't both see it free.
 */
export function makeR2UploadBlobs(bucket: R2BucketLike): UploadBlobStore {
  const writing = new Set<string>()
  const attempt = <A>(operation: UploadBlobError["operation"], key: string, run: () => Promise<A>) =>
    Effect.tryPromise({ try: run, catch: (cause) => new UploadBlobError({ operation, key, cause }) })

  return {
    putIfAbsent: (key, bytes, contentType) => Effect.suspend(() => {
      if (writing.has(key)) return Effect.succeed(false)
      writing.add(key)
      return attempt("put", key, async () => {
        if (await bucket.head(key)) return false
        await bucket.put(key, bytes, { httpMetadata: { contentType } })
        return true
      }).pipe(Effect.ensuring(Effect.sync(() => writing.delete(key))))
    }),
    get: (key) => attempt("get", key, async () => {
      const object = await bucket.get(key)
      return object ? { body: object.body, contentType: object.httpMetadata?.contentType ?? "" } : null
    }),
    remove: (key) => attempt("remove", key, () => bucket.delete(key)),
  }
}
