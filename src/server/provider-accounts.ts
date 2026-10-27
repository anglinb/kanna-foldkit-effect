import { Context, Effect, Exit, FileSystem, Layer, Path, Schema, Scope, Semaphore, Stream, SubscriptionRef } from "effect"
import { ACCOUNT_PROVIDERS, existingAccountId, type AccountInput, type AccountsSnapshot, type ProviderAccount } from "../shared/accounts"
import type { AgentProvider, AuthServiceId, ProviderAuthSnapshot } from "../shared/types"
import { Analytics } from "./analytics"
import { capturePlatform, Machine } from "./platform"
import { makeProviderAuth, ProviderAuth, type ProviderAuthOptions } from "./provider-auth"
import type { PiConnection } from "./pi-agent"

export class AccountError extends Schema.TaggedError<AccountError>()("@kanna/server/AccountError", { message: Schema.String }) {}

const SavedAccount = Schema.Struct({
  id: Schema.String, provider: Schema.Literals(ACCOUNT_PROVIDERS), label: Schema.String,
  mode: Schema.Literals(["native", "apiKey"]),
  apiKey: Schema.optionalKey(Schema.String), apiProvider: Schema.optionalKey(Schema.Literals(["openai", "openrouter", "custom"])),
  baseUrl: Schema.optionalKey(Schema.String),
})
const AccountsFile = Schema.Struct({ version: Schema.Literal(1), accounts: Schema.Array(SavedAccount), defaults: Schema.Record(Schema.String, Schema.String) })
type SavedAccount = typeof SavedAccount.Type

export interface AccountRuntime {
  accountId: string
  /** Private to the server. Never send the environment or connection to clients. */
  environment?: Record<string, string | undefined>
  connection?: PiConnection
}

/** Override inherited credentials explicitly, including variables that select a different billing account. */
export function accountEnvironment(provider: AgentProvider, directory: string, key?: string): Record<string, string | undefined> {
  if (provider === "claude") return {
    CLAUDE_CONFIG_DIR: directory, ANTHROPIC_BASE_URL: undefined, ANTHROPIC_API_KEY: undefined, ANTHROPIC_AUTH_TOKEN: undefined,
    CLAUDE_CODE_OAUTH_TOKEN: undefined, CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR: undefined,
    CLAUDE_CODE_USE_BEDROCK: undefined, CLAUDE_CODE_USE_VERTEX: undefined, CLAUDE_CODE_USE_FOUNDRY: undefined,
  }
  if (provider === "codex") return { CODEX_HOME: directory, OPENAI_API_KEY: undefined, CODEX_API_KEY: undefined, OPENAI_BASE_URL: undefined }
  if (provider === "cursor") return { CURSOR_API_KEY: key }
  return {}
}

export class ProviderAccounts extends Context.Service<ProviderAccounts, {
  readonly snapshot: Effect.Effect<AccountsSnapshot>
  readonly changes: Stream.Stream<number>
  readonly refresh: (options?: { force?: boolean }) => Effect.Effect<void>
  readonly create: (input: AccountInput) => Effect.Effect<string, AccountError>
  readonly setDefault: (provider: AgentProvider, id: string) => Effect.Effect<void, AccountError>
  readonly updateKey: (id: string, apiKey: string) => Effect.Effect<void, AccountError>
  readonly rename: (id: string, label: string) => Effect.Effect<void, AccountError>
  readonly remove: (id: string) => Effect.Effect<void, AccountError>
  readonly login: (id: string) => Effect.Effect<void, AccountError>
  readonly submitCode: (id: string, code: string) => Effect.Effect<void, AccountError>
  readonly cancelLogin: (id: string) => Effect.Effect<void, AccountError>
  readonly runtime: (provider: AgentProvider, id?: string) => Effect.Effect<AccountRuntime, AccountError>
}>()("@kanna/server/ProviderAccounts") {
  /** `accountsDir`: where each account's CLI config dir goes (default `<dataDir>/accounts`); it has to be on the machine. */
  static readonly layer = (dataDir: string, options: { readonly accountsDir?: string } = {}) =>
    Layer.effect(this, makeProviderAccounts(dataDir, {}, options.accountsDir))
}

export const makeProviderAccounts = Effect.fn("ProviderAccounts.make")(function* (dataDir: string, nativeAuthOptions: ProviderAuthOptions = {}, accountsDir?: string) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const machine = yield* Machine
  const platform = yield* capturePlatform()
  const analytics = yield* Analytics
  const legacy = yield* ProviderAuth
  const layerScope = yield* Effect.scope
  const lock = yield* Semaphore.make(1)
  const revision = yield* SubscriptionRef.make(0)
  const notify = SubscriptionRef.update(revision, (value) => value + 1)
  const file = path.join(dataDir, "accounts.json")
  const failure = () => new AccountError({ message: "Unable to read or save account storage." })
  const loaded = yield* fs.readFileString(file).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(AccountsFile))),
    Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed({ version: 1 as const, accounts: [], defaults: {} })),
    Effect.mapError(failure),
  )
  let saved = [...loaded.accounts]
  let defaults = { ...loaded.defaults }
  // IDs become directory names; refuse malformed or duplicate persisted records.
  if (saved.some((entry, index) => !/^[a-f0-9-]{36}$/.test(entry.id) || saved.findIndex((other) => other.id === entry.id) !== index
    || !entry.label.trim() || entry.label.length > 80
    || (entry.mode === "native" ? !["claude", "codex"].includes(entry.provider) : !["cursor", "pi"].includes(entry.provider) || !entry.apiKey?.trim()))) {
    return yield* new AccountError({ message: "Invalid account storage." })
  }
  if (Object.entries(defaults).some(([provider, id]) => !ACCOUNT_PROVIDERS.includes(provider as AgentProvider)
    || (id !== existingAccountId(provider as AgentProvider) && !saved.some((entry) => entry.id === id && entry.provider === provider)))) {
    return yield* new AccountError({ message: "Invalid default account storage." })
  }
  const auths = new Map<string, { auth: ProviderAuth["Service"]; scope: Scope.Closeable }>()
  const accountsRoot = accountsDir ?? path.join(dataDir, "accounts")
  const directory = (entry: SavedAccount) => path.join(accountsRoot, entry.id, entry.provider)
  const get = (id: string) => saved.find((entry) => entry.id === id)
  const requireSaved = (id: string) => Effect.suspend(() => { const entry = get(id); return entry ? Effect.succeed(entry) : Effect.fail(new AccountError({ message: "Account not found." })) })
  const persist = (accounts: SavedAccount[], nextDefaults = defaults) => Effect.gen(function* () {
    const temporary = `${file}.${crypto.randomUUID()}.tmp`
    yield* fs.makeDirectory(dataDir, { recursive: true })
    yield* fs.writeFileString(temporary, JSON.stringify({ version: 1, accounts, defaults: nextDefaults }, null, 2) + "\n", { mode: 0o600 })
    yield* fs.rename(temporary, file).pipe(Effect.onError(() => fs.remove(temporary).pipe(Effect.ignore)))
    saved = accounts
    defaults = nextDefaults
    yield* notify
  }).pipe(Effect.mapError(failure))

  const nativeAuth = (entry: SavedAccount) => Effect.gen(function* () {
    const existing = auths.get(entry.id)
    if (existing) return existing.auth
    const scope = yield* Scope.fork(layerScope)
    const auth = yield* makeProviderAuth({ ...nativeAuthOptions, services: [entry.provider as AuthServiceId], environment: accountEnvironment(entry.provider, directory(entry)) }).pipe(
      Effect.provide(platform), Effect.provideService(Analytics, analytics), Effect.provideService(Scope.Scope, scope),
    )
    auths.set(entry.id, { auth, scope })
    yield* auth.changes.pipe(Stream.runForEach(() => notify), Effect.forkIn(scope))
    return auth
  })

  const snapshot = Effect.gen(function* () {
    const native = yield* legacy.snapshot
    const accounts: ProviderAccount[] = ACCOUNT_PROVIDERS.map((provider) => {
      const status = native.services.find((entry) => entry.service === provider)
      return {
        id: existingAccountId(provider), provider, label: "Existing CLI account", mode: "existing",
        identity: status?.account ?? null, authStatus: status?.authStatus ?? "unknown",
        login: status?.login ?? { phase: "idle" }, statusDetail: status?.statusDetail ?? null,
      }
    })
    for (const entry of saved) {
      const status = auths.has(entry.id) ? (yield* auths.get(entry.id)!.auth.snapshot).services.find((service) => service.service === entry.provider) : undefined
      accounts.push({
        id: entry.id, provider: entry.provider, label: entry.label, mode: entry.mode,
        identity: status?.account ?? null,
        authStatus: entry.mode === "apiKey" ? (entry.apiKey ? "signed_in" : "signed_out") : status?.authStatus ?? "unknown",
        login: status?.login ?? { phase: "idle" }, statusDetail: status?.statusDetail ?? (entry.mode === "apiKey" ? "API key configured" : null),
      })
    }
    return { accounts, defaultAccountIds: Object.fromEntries(ACCOUNT_PROVIDERS.map((provider) => [provider, defaults[provider] ?? existingAccountId(provider)])) } satisfies AccountsSnapshot
  })

  const runtime = (provider: AgentProvider, requested?: string) => Effect.gen(function* () {
    const id = requested ?? defaults[provider] ?? existingAccountId(provider)
    if (id === existingAccountId(provider)) return { accountId: id } satisfies AccountRuntime
    const entry = yield* requireSaved(id)
    if (entry.provider !== provider) return yield* new AccountError({ message: "This account belongs to a different provider." })
    const auth = auths.get(id)?.auth
    if (auth) {
      const phase = (yield* auth.snapshot).services.find((service) => service.service === provider)?.login.phase
      if (phase && phase !== "idle" && phase !== "error") return yield* new AccountError({ message: "Finish this account's sign-in before starting a chat." })
    }
    const connection: PiConnection | undefined = entry.provider === "pi" ? {
      provider: entry.apiProvider ?? "openai", apiKey: entry.apiKey!, baseUrl: entry.baseUrl!,
    } : undefined
    return { accountId: id, environment: { ...machine.env.snapshot(), ...accountEnvironment(provider, directory(entry), entry.apiKey) }, ...(connection ? { connection } : {}) } satisfies AccountRuntime
  })

  const create = (input: AccountInput) => lock.withPermits(1)(Effect.gen(function* () {
    if (!ACCOUNT_PROVIDERS.includes(input.provider) || input.provider === "grok") return yield* new AccountError({ message: "Additional accounts are supported for Claude, Codex, Cursor, and Pi." })
    const label = input.label.trim()
    if (!label || label.length > 80) return yield* new AccountError({ message: "Enter an account name of up to 80 characters." })
    const mode = input.provider === "claude" || input.provider === "codex" ? "native" : "apiKey"
    if (mode === "native" && input.apiKey !== undefined) return yield* new AccountError({ message: "Use native sign-in for Claude and Codex accounts." })
    if (mode === "apiKey" && !input.apiKey?.trim()) return yield* new AccountError({ message: "Enter an API key." })
    if (input.provider === "pi" && input.apiProvider === "custom" && !input.baseUrl?.trim()) return yield* new AccountError({ message: "Enter the custom API URL." })
    const baseUrl = input.provider === "pi" ? (input.baseUrl?.trim() || (input.apiProvider === "openrouter" ? "https://openrouter.ai/api/v1" : "https://api.openai.com/v1")) : undefined
    if (baseUrl) {
      const url = yield* Effect.try({ try: () => new URL(baseUrl), catch: () => new AccountError({ message: "Enter an HTTP or HTTPS API URL." }) })
      if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return yield* new AccountError({ message: "Enter an HTTP or HTTPS API URL without embedded credentials." })
    }
    const entry: SavedAccount = { id: crypto.randomUUID(), provider: input.provider, label, mode, ...(mode === "apiKey" ? { apiKey: input.apiKey!.trim() } : {}), ...(baseUrl ? { baseUrl, apiProvider: input.apiProvider ?? "openai" } : {}) }
    if (mode === "native") {
      yield* fs.makeDirectory(directory(entry), { recursive: true, mode: 0o700 }).pipe(Effect.mapError(failure))
      if (entry.provider === "claude") yield* fs.writeFileString(path.join(directory(entry), "settings.json"), JSON.stringify({ forceLoginMethod: "claudeai" }) + "\n", { mode: 0o600 }).pipe(Effect.mapError(failure))
      if (entry.provider === "codex") yield* fs.writeFileString(path.join(directory(entry), "config.toml"), 'cli_auth_credentials_store = "file"\n', { mode: 0o600 }).pipe(Effect.mapError(failure))
    }
    yield* persist([...saved, entry])
    if (mode === "native") {
      const auth = yield* nativeAuth(entry)
      yield* auth.refresh().pipe(Effect.forkIn(layerScope))
    }
    return entry.id
  }))
  const login = (id: string) => Effect.gen(function* () {
    const entry = yield* requireSaved(id)
    if (entry.mode !== "native") return yield* new AccountError({ message: "This account uses an API key." })
    const auth = yield* nativeAuth(entry)
    yield* auth.probeService(entry.provider as AuthServiceId)
    yield* auth.startLogin(entry.provider as AuthServiceId).pipe(Effect.mapError(() => new AccountError({ message: "Unable to start account sign-in. Check the account status." })))
  })
  const refresh = (options?: { force?: boolean }) => Effect.gen(function* () {
    yield* legacy.refresh(options)
    for (const entry of saved.filter((entry) => entry.mode === "native")) {
      const auth = yield* nativeAuth(entry)
      yield* auth.refresh(options)
    }
    yield* notify
  })
  // Restore account state without blocking startup on installed CLI probes.
  for (const entry of saved.filter((entry) => entry.mode === "native")) yield* nativeAuth(entry)
  for (const { auth, scope } of auths.values()) yield* auth.refresh().pipe(Effect.forkIn(scope))

  return ProviderAccounts.of({
    snapshot, changes: SubscriptionRef.changes(revision), refresh, create, runtime, login,
    setDefault: (provider, id) => lock.withPermits(1)(Effect.gen(function* () { yield* runtime(provider, id); yield* persist(saved, { ...defaults, [provider]: id }) })),
    updateKey: (id, apiKey) => lock.withPermits(1)(Effect.gen(function* () {
      const entry = yield* requireSaved(id)
      if (entry.mode !== "apiKey") return yield* new AccountError({ message: "This account uses native sign-in." })
      if (!apiKey.trim()) return yield* new AccountError({ message: "Enter an API key." })
      yield* persist(saved.map((value) => value.id === id ? { ...value, apiKey: apiKey.trim() } : value))
    })),
    rename: (id, label) => lock.withPermits(1)(Effect.gen(function* () {
      yield* requireSaved(id)
      if (!label.trim() || label.trim().length > 80) return yield* new AccountError({ message: "Enter an account name of up to 80 characters." })
      yield* persist(saved.map((entry) => entry.id === id ? { ...entry, label: label.trim() } : entry))
    })),
    remove: (id) => lock.withPermits(1)(Effect.gen(function* () {
      const entry = yield* requireSaved(id)
      if (entry.mode === "native") {
        const auth = yield* nativeAuth(entry)
        yield* auth.signOut(entry.provider as "claude" | "codex").pipe(Effect.mapError(() => new AccountError({ message: "Unable to sign out this account. Reconnect the CLI and try again." })))
      }
      const child = auths.get(id)
      if (child) { yield* Scope.close(child.scope, Exit.void); auths.delete(id) }
      yield* persist(saved.filter((value) => value.id !== id), Object.fromEntries(Object.entries(defaults).filter(([, value]) => value !== id)))
      yield* fs.remove(path.join(accountsRoot, id), { recursive: true, force: true }).pipe(Effect.ignore)
    })),
    submitCode: (id, code) => Effect.gen(function* () { const entry = yield* requireSaved(id); const auth = yield* nativeAuth(entry); yield* auth.submitLoginCode(entry.provider as AuthServiceId, code).pipe(Effect.mapError(() => new AccountError({ message: "No account sign-in is waiting for a code." }))) }),
    cancelLogin: (id) => Effect.gen(function* () { const entry = yield* requireSaved(id); const auth = yield* nativeAuth(entry); yield* auth.cancelLogin(entry.provider as AuthServiceId) }),
  })
})

/** Keep onboarding/provider gating aligned with the selected default, without exposing credentials. */
export function withAccounts(auth: ProviderAuthSnapshot, accounts: AccountsSnapshot): ProviderAuthSnapshot {
  return { ...auth, ...accounts, services: auth.services.map((service) => {
    const account = accounts.accounts.find((entry) => entry.id === accounts.defaultAccountIds[service.service as AgentProvider])
    return account ? { ...service, ...(account.mode === "existing" ? {} : { accountId: account.id }), authStatus: account.authStatus, account: account.identity, statusDetail: account.statusDetail, login: account.login } : service
  }) }
}
