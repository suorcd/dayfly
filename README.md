# dayfly

Internal pastebin / file-share for LLM agents (and one human), backed by
a private Garage bucket. One small Rust binary, two modes sharing one
S3 core:

- **MCP server over stdio** (default, no subcommand) — the primary
  interface for agents.
- **HTTP mode** (`serve`) — a plain-curl API in the style of 0x0.st,
  a small server-rendered human frontend (`/`, `/pastes`,
  `/view/<id>`), and the same MCP server over streamable HTTP at
  `/mcp`.

There is **no database in v1 and no SQLite stub**. Listing and
info come from scanning the bucket directly. Listing uses ListObjectsV2
for each UTC date in the window, then HeadObject per key where metadata
is needed. See "No database in v1" below.

## What it is

- One base S3 object per paste/file. Key layout:
  `pastes/<yyyy-mm-dd>/<random-id>`, where the random ID has at least
  128 bits of entropy (base62). A user-supplied filename is never put
  in the key. An append adds one segment object under
  `pastes/<yyyy-mm-dd>/<random-id>/<sequence>-<suffix>`; see "Append and
  segmented storage". An alias adds one small object at
  `words/<phrase>`; see "Word aliases".
- Each new paste also gets a word alias, three words by default, for
  example `abacus-abdomen-zoology`. The alias is a second name for the
  same paste, not a second paste.
- Filename, title, tags, content-type, created date, TTL, and the
  resolved creator address are stored in object metadata. The S3
  `Content-Length` gives the size. A `sha256` value is
  stored when the hash is known before the object is created. A
  multipart stream cannot know the hash in advance, so it omits the
  value. The TTL is stored as exact seconds and as whole days when the
  duration has no smaller part. The base object stores the client
  address as `created-by` when the transport has one. The address is
  hidden unless `DAYFLY_EXPOSE_CREATOR_ADDR` is set. No separate
  metadata sidecar in v1.
- Expiry uses a read-time TTL. The TTL accepts seconds or a duration
  such as `90m` or `1h30m`. A bare integer is always seconds: `7` is
  seven seconds, and `7d` is seven days. `ttl_days` remains as a
  positive whole-day form. The default is 7 days, configurable via
  `DAYFLY_DEFAULT_TTL_DAYS`. `DAYFLY_MIN_TTL_DAYS` and
  `DAYFLY_MAX_TTL_DAYS` bound an explicit TTL.
  `DAYFLY_RETENTION_BY_SIZE` can shorten that TTL for a large paste. The API and MCP layer compare stored
  `expires-at` metadata with now and enforce expiry on read. S3
  lifecycle deletion is a separate, asynchronous sweeper (see
  `docs/lifecycle-example.json`): it is day-rounded and may run after
  the TTL. **Exact-time deletion from the bucket is not promised.**
- Streaming upload and download. A returned URL can itself be streamed
  back without downloading the whole object first.

## Garage first, expansion later

**v1 targets Garage as the S3 backend.** The code stays S3-shaped
(custom endpoint, `force_path_style = true`, region from env), but no
v1 effort is spent on generic AWS/MinIO-specific tuning or on a
multi-backend abstraction. Expansion to other S3-compatible stores is
a later, non-v1 item, to be done with real testing against that store
— not by guessing at feature differences.

Garage-specific warnings, encoded in code and docs, not worked around
by guessing:

- **Do not rely on `If-None-Match` / conditional writes** for
  create-only or burn-after-read on Garage: some S3-compatible stores,
  including Garage, accept the header but ignore it and overwrite.
  Mitigation in v1: a new random ID per store, never overwrite an
  existing ID by design, and no burn-after-read.
- **No presigned S3 URLs in v1.** Agents never talk to S3 directly,
  only through this binary.
- **Agents hold no S3 credentials.** The binary is the only S3
  credential holder.

### Garage permissions

The binary needs only **read** and **write** permission on the bucket.
It does not need **owner**. In Garage, owner is for `DeleteBucket`, the
website configuration, and the CORS configuration. The lifecycle
endpoints use the ordinary permissions:

| Operation | Permission |
| --- | --- |
| `GetObject`, `HeadObject`, `ListObjectsV2`, `ListMultipartUploads` | read |
| `PutObject`, `DeleteObject`, `CreateMultipartUpload`, `CompleteMultipartUpload`, `AbortMultipartUpload` | write |
| `GetBucketLifecycleConfiguration` | read |
| `PutBucketLifecycleConfiguration`, `DeleteBucketLifecycle` | write |

The Garage **admin API** is a separate service with separate bearer
tokens. Its scopes name endpoints, not buckets. An S3 access key does
not authenticate to it, and the owner flag on that key does not grant
access. For example, an admin token with the `GetBucketInfo` scope can
read bucket metadata, but no S3 key can. Create such a token with
`garage admin-token create --scope GetBucketInfo`.

Garage does not expose bucket quotas through the S3 API. The admin API
`GetBucketInfo` response carries `quotas`, `bytes`, `objects`, and
`lifecycleRules`. Dayfly does not call the admin API. It relies on the
write path: when a bucket quota is reached, Garage rejects the write
with `403 Forbidden` and a quota message, and dayfly returns 507.

### Bucket lifecycle status

At startup and after a configuration reload, dayfly reads
`GetBucketLifecycleConfiguration` and logs each rule at `info` with its
ID, status, prefix, expiration days, and abort-incomplete-upload days.
It logs one warning when the bucket has no rule, or when the call
fails. The check is best effort and does not delay the listener.

Dayfly also aborts stale multipart uploads in code. It lists the
`pastes/` prefix at startup and each hour. It aborts an upload older
than `DAYFLY_MULTIPART_ABORT_AFTER` seconds and skips an upload that
this process still drives. A failed sweep logs and does not stop the
listener. A lifecycle rule is a backstop, not a requirement. Run
`dayfly cleanup` for one manual pass. The command uses the same
threshold and needs no lifecycle rule.

The active-upload set is per process. A sweep cannot see an upload
that a second process drives. Keep `DAYFLY_MULTIPART_ABORT_AFTER`
above the longest upload time when two writers share the bucket. Add a
lifecycle rule for the day-rounded second guard.

## Access model — read this before deploying

The bucket is **private, NOT open, NOT anonymous**. No public bucket
policy and no anonymous access path are created or documented here.
The binary alone holds the Garage access key and secret, from
environment variables or credential files. Agents/clients never get S3
credentials and never talk to S3 directly; they only talk to the
binary's MCP/HTTP interface.

Bucket privacy is **not** the same as securing the binary's own HTTP
endpoint. The intended deployment is **private-network / localhost**.
As a simple, optional extra for the binary's own endpoint (not a
replacement for the S3 credentials), set
`DAYFLY_HTTP_BEARER_TOKEN`: HTTP mode then requires
`Authorization: Bearer <token>` on every HTTP route, including `/mcp`
and the human pages, unless `DAYFLY_WRITE_REQUIRES_TOKEN` is on (see
below). The exceptions are `DELETE` and `PATCH`, which
also accept a valid paste token when `DAYFLY_PASTE_TOKENS` is on. The
token must not contain ASCII whitespace;
startup fails if it does. Unset the variable to disable the token. A
variable that is set to an empty or whitespace-only value stops
startup, because it would look protected and accept every request. An
env file line such as `DAYFLY_HTTP_BEARER_TOKEN=""` is a set-but-empty
value and follows the same rule. Note the limitation this creates: a plain
browser form cannot send an `Authorization` header, so for browser use
of the frontend, run on a private network / localhost without the token
set, and use the token for API clients. Do not put the token in a URL
query
parameter — it would leak into logs and browser history.

`DAYFLY_WRITE_REQUIRES_TOKEN` adds a write-only auth mode. It needs
`DAYFLY_HTTP_BEARER_TOKEN`. Reads stay anonymous: downloads, metadata,
search, `/raw`, the human index and view pages, and listing. Writes
require the bearer token: uploads, appends, deletes, expiry changes,
the human write routes, and `/mcp`. A paste token still authorizes its
own `DELETE`, `PATCH`, and `append` when `DAYFLY_PASTE_TOKENS` is on.
`DAYFLY_LIST_REQUIRES_TOKEN` still gates the list separately. A browser
form cannot send the bearer token, so browser writes need an
authenticating proxy or a paste token. The service document reports
`auth: "write-bearer"` and names the gated routes.

`serve` refuses a bind address that other hosts can reach when the
bearer token is unset. Startup stops with a configuration error, and
the message names the ways out. The rule has three states:

- Loopback bind, no token: start. This is the default.
- Non-loopback bind with a token: start. With
  `DAYFLY_WRITE_REQUIRES_TOKEN`, reads work in a browser and writes need
  the token or an authenticating proxy.
- Non-loopback bind, no token: refuse, unless
  `DAYFLY_ALLOW_UNAUTHENTICATED=1` is set. With that value the service
  starts and writes one warning at startup. Set it only on a network
  you trust, or behind a proxy that authenticates.

`DAYFLY_TRUSTED_PROXIES` names the reverse proxies whose
`X-Forwarded-For` header is believed. It is a comma-separated list of
CIDR blocks, and a bare address is a single host. The default is empty:
the socket peer is the client and every forwarding header is ignored, so
a request from an unlisted peer that sets `X-Forwarded-For` is
attributed to that peer. When the socket peer is listed, the chain is
walked from the right, skipping listed proxies, and the first address
that is not a listed proxy is the client. A forged hop prepended on the
left cannot win. A malformed or over-long chain, or one that names only
trusted proxies, falls back to the socket peer with one warning.

Only the rightmost unlisted address is used, so a proxy must append to
`X-Forwarded-For`, not replace it. List only the proxies that actually
front this service, and do not leave a path around them: a direct
connection from an unlisted address is attributed to that address. This
setting is what makes a per-client limit meaningful behind an ingress.

`DAYFLY_LIST_ENABLED` turns listing off for every transport. With the
default `true`, listing works. With `false`, the human `/pastes` page,
`GET /v1/pastes`, and the MCP `list_recent` tool all refuse with 403 and
the message "Listing is turned off on this instance." This is the
setting for a public instance: no paste is listable, even by its
creator. There is no owner or creator field, so the instance cannot
filter a list by author.

`DAYFLY_LIST_REQUIRES_TOKEN` separates "public downloads" from "private
listing" when listing is on. With the default `false`, listing follows
the read rule: gated when the bearer token gates reads, open when reads
are anonymous. When `true`, `GET /v1/pastes`, the human `/pastes`
page, and the MCP `list_recent` tool require the bearer token even if
reads are otherwise anonymous. If the setting is on and no token is
configured, all three refuse and the message names the missing variable:
there is nothing to check against, and an open list is what the setting
prevents. The human page renders a 401 page rather than an empty table. A
browser cannot send an `Authorization` header, so put an authenticating
proxy in front of `/pastes`, or use the API with the token.
`DAYFLY_LIST_ENABLED` wins over this setting.

The HTTP `/mcp` endpoint requires the bearer token when one is
configured. The stdio transport is the local trust boundary and accepts
`list_recent` when a token is configured. With no token configured, both
transports refuse `list_recent`.

`DAYFLY_RATE_LIMIT_PER_MINUTE` caps requests per client per minute. It
is a token bucket keyed on the resolved client address: the burst is the
configured number, and the bucket refills at that number per minute.
Zero, the default, disables the limiter and allocates no counters. Above
the limit the response is 429 with `Retry-After` and a JSON error naming
`retry_after_seconds`; the upload body is not read. The bucket map is
bounded and releases idle clients. Every route counts, including `/` and
`/mcp`. The counter is per process and resets on restart. It is a flood
guard, not an access control: clients behind one NAT share a bucket, and
a client that can change its address gets a fresh one. Set
`DAYFLY_TRUSTED_PROXIES` first, or every request is attributed to the
ingress and shares one bucket.

`DAYFLY_RETENTION_BY_SIZE` bounds storage cost over time without a
database. It shortens the expiry of a large paste. A public instance
should set it, because a request limit does not bound the bytes that one
client can store. See "Retention by size".

`DAYFLY_PASTE_TOKENS` gives each new paste a management token. Creation
returns the token once: `paste_token` in the JSON response,
`X-Paste-Token` in the plain response, and the token on the human
create result page. Send that header on `DELETE`, `PATCH`, and
`append` to act on the paste without the bearer token. The
bearer token remains a universal key. The
base object stores only the SHA-256 of the token. The meta route and the
MCP metadata tool do not return it. Treat the token as a credential and
do not put it in a URL. A lost token means lost control of that paste;
there is no recovery. A paste created before the setting has
no token; only a bearer token can change it. A token for one paste
cannot change another paste. The human create result page includes the
token in its delete form. The view page has a token field in its delete
form. The delete form on each list row has no token field, so it needs
the bearer token or a proxy that authenticates.

The MCP stdio transport does not use the bearer token: spawning the
binary locally is the trust boundary there. The MCP transport over
HTTP at `/mcp` is part of HTTP mode and uses the same bearer token as
the REST routes.

The bearer token does not make the local-path tools safe. `store_file`
reads a server path and `fetch_file` writes one, so a caller of either
tool can reach what the dayfly process can reach. The stdio transport
keeps both tools, because the caller already runs on the host that
spawned the binary. The HTTP transport refuses both tools, and the
client gets an invalid-parameter error. To allow them over HTTP, set
`DAYFLY_MCP_LOCAL_ROOT` to one directory. Both tools then resolve the
path and refuse anything outside that directory, including a symlink
that points away from it. A paste endpoint must not also act as a
shell.

## v1 scope

Operations, in both MCP tools and HTTP endpoints:

| Operation | MCP tool | HTTP |
| --- | --- | --- |
| Store text | `store_text` | `POST /upload` or `POST /v1/pastes` (raw body, no filename) |
| Store file (multipart for large files) | `store_file` (local path) | `POST /upload` (raw body with `X-Filename`, or `-F file=@...`) |
| Append text | `append_text` | `POST /v1/pastes/<id-or-alias>/append` (plain text body) |
| Fetch text (offset/limit, tail, or line range) | `fetch_text` | `GET /v1/pastes/<id-or-alias>` (raw; use `Range` for slices) |
| Search text (literal or regex) | `search_text` | `GET /v1/pastes/<id-or-alias>/search?q=...` |
| Fetch file (streaming, Range) | `fetch_file` (downloads to a local path) | `GET /v1/pastes/<id-or-alias>` with `Range` → 206 |
| Fetch metadata (HeadObject equivalent) | `fetch_metadata` | `GET /v1/pastes/<id-or-alias>/meta`, or `HEAD /v1/pastes/<id-or-alias>` |
| Change expiry | `set_ttl` | `PATCH /v1/pastes/<id-or-alias>` with `X-TTL` or `X-TTL-Days` |
| Delete | `delete_paste` | `DELETE /v1/pastes/<id-or-alias>` |
| List recent (date or inclusive date window) | `list_recent` | `GET /v1/pastes?date=YYYY-MM-DD&limit=N` or `GET /v1/pastes?start=YYYY-MM-DD&end=YYYY-MM-DD&limit=N` |

The two file tools work with server-local paths, so the transport
decides what they may touch. `store_file` and `fetch_file` are
available on the stdio transport. The HTTP transport refuses both tools
unless `DAYFLY_MCP_LOCAL_ROOT` names a directory, and then both tools
stay inside that directory. The Access model section states the rule.

IDs may be full (`2026-10-28/abc123…`) or bare random IDs. Bare IDs
are resolved by scanning the `pastes/` prefix (up to 10 pages of
1,000 keys) — clients should keep the full ID returned at store time.
An ID has one or two components after `pastes/`. A segment key has a
third component and is never a paste ID. Listing and bare-ID
resolution skip segment keys.

Every operation that takes an ID also accepts a word alias, such as
`abacus-abdomen-zoology`. The JSON and MCP store responses include
both forms; the plain-text HTTP response returns only the URL. See
"Word aliases".

### Service description

The running service publishes `GET /.well-known/dayfly.json` without
authentication. The document reports the crate version, the auth mode
(`none`, `bearer`, or `write-bearer`), the gated write routes when write
mode is on, the content policy (`inline_pdf`), the creator-address
exposure setting (`expose_creator_addr`), the limits in force, and every
API route. The frontend links to it. The MCP tool `get_info` returns the
same document, plus the local-path tool mode for the transport. Both
read one function, so they cannot drift.

The reference deployment serves it at
[`/.well-known/dayfly.json`](https://dayfly.k.ser.ink/.well-known/dayfly.json).

### Explicitly OUT of v1

Listing/search/sorting beyond a date window, view counters,
burn-after-read, per-paste permissions/ownership, audit history, and
deduplication. A `created-by` address is not an owner, an account, or an
audit trail. It is hidden by default, and no query lists pastes by
address. These are the future SQLite triggers: they are the
reasons a database would be added later. No database schema is
invented in v1. A paste token is a per-paste delete and expiry
credential, not an ownership model.

### No database in v1 — the performance tradeoff

Listing is O(n) in objects scanned, plus one HeadObject per paste
(needed to recover expiry, content-type, filename, and the total size,
which ListObjectsV2 does not return, and to filter expired objects
out). The same date-prefix scan returns the segment keys, so a paste
with no segments costs no extra prefix listing.
For a small internal bucket that is fine. As the bucket grows,
listing gets slower — that slowdown, not a guessed threshold, is
exactly the trigger for adding a database later. The date prefix
(`pastes/<yyyy-mm-dd>/`) is the optimization available in v1: a
listing scans the requested dates, not the whole bucket. Each date
prefix follows continuation tokens for up to 10 pages of 1,000 keys.
A window can span at most 366 days.

The listing window is inclusive. With no dates, it covers yesterday
and today in UTC. At 20:00 Pacific the UTC clock is already on the
next day, so the pastes from that Pacific evening are under two UTC
date prefixes. The default window includes both, so the evening
listing does not go empty.

The scan starts at the newest date and moves to older dates. The
`limit` applies to the newest dates. Within one date, the limit applies
in S3 key order. Paste IDs are random, so key order is not creation
order. An older date can be omitted when the limit is reached. Results
are sorted by date ascending, then by key. The response reports
`truncated` and `next`. `next` is `{date, start_after}` or null. Pass
`next.date` as `after_date` and `next.start_after` as `after_key` to
read the next page. The cursor is stateless and follows scan order, not
the sorted display order. A page that ends exactly at the end of the
bucket can still return `truncated: true`; the next page is then empty.

## Configuration

All configuration comes from environment variables. A process
environment variable wins over the same name in an optional environment
file. Use `--config PATH` or `DAYFLY_CONFIG` to name the file. With
neither, dayfly reads `$XDG_CONFIG_HOME/dayfly/dayfly.env`, or
`$HOME/.config/dayfly/dayfly.env` when `XDG_CONFIG_HOME` is unset. A
missing default file is ignored. A named file must exist.

The file holds one `NAME=VALUE` pair per line. Blank lines and lines
that start with `#` are ignored. A leading `export` and one pair of
matching single or double quotes are accepted. A relative secret-file
or asset path in the file resolves against the directory of the file.
An exported relative secret-file or asset path resolves against the
working directory. Never commit a real `.env`; copy `.env.example` to
a path you choose and pass it with `--config`. The `.env` name is in
`.gitignore`.

```sh
dayfly config init --out ~/.config/dayfly/dayfly.env
```

`config init` writes the file and prints the required names. It refuses
to replace an existing file unless you add `--force`. It creates a new
parent directory with mode `0700` and the file with mode `0600`.

```sh
dayfly --config ~/.config/dayfly/dayfly.env config check
```

`config check` prints one line per setting with `set`, `unset`, or
`default`. It prints all faults in one run and exits 1 when a fault is
present. It never prints a secret value. It also prints a warning when
`DAYFLY_MAX_PASTE_BYTES` is lower than `DAYFLY_MAX_TEXT_BYTES` or
`DAYFLY_MAX_FILE_BYTES`. The warning names both values and gives the
winning cap. `--strict` turns warnings into faults: credential file
permissions and the lower total cap.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `S3_ENDPOINT` | yes | — | Garage endpoint URL, `http://` or `https://` |
| `S3_BUCKET` | yes | — | Bucket name (private) |
| `S3_ACCESS_KEY_ID` | one form | — | Garage access key, held only by this binary; set this variable or `S3_ACCESS_KEY_ID_FILE` |
| `S3_ACCESS_KEY_ID_FILE` | one form | — | Path to a file that contains the Garage access key; set this variable or `S3_ACCESS_KEY_ID` |
| `S3_SECRET_ACCESS_KEY` | one form | — | Garage secret key, held only by this binary; set this variable or `S3_SECRET_ACCESS_KEY_FILE` |
| `S3_SECRET_ACCESS_KEY_FILE` | one form | — | Path to a file that contains the Garage secret key; set this variable or `S3_SECRET_ACCESS_KEY` |
| `S3_REGION` | no | `us-east-1` | Signing region for Garage; supply your own, do not rely on the default being "yours" |
| `DAYFLY_MAX_TEXT_BYTES` | no | 104857600 (100 MiB) | Max text paste size for MCP `store_text`, the human text form, and `POST /upload` without a filename; oversize → 413. A new paste must also fit `DAYFLY_MAX_PASTE_BYTES`, so the lower cap wins. A filename (including a multipart filename) uses `DAYFLY_MAX_FILE_BYTES` instead |
| `DAYFLY_MAX_FILE_BYTES` | no | 1073741824 (1 GiB) | Max file paste size; oversize → 413 with a machine-readable error. A new paste must also fit `DAYFLY_MAX_PASTE_BYTES`, so the lower cap wins. The maximum is 52428800000 bytes: 10,000 parts of 5 MiB. A TTL change on a paste above the 5 GB copy limit fails with `copy_limit`; no object is rewritten |
| `DAYFLY_MAX_PASTE_BYTES` | no | 1073741824 (1 GiB) | Max total size for one paste, including the base object and every appended segment. A new paste or an append that would exceed the cap fails and writes no object |
| `DAYFLY_MULTIPART_ABORT_AFTER` | no | 3600 | Age in seconds after which the startup and hourly sweep aborts an incomplete multipart upload under `pastes/`. The manual `cleanup` command uses the same value. Must be positive. The sweep skips an upload that this process still drives. A second process is not visible, so keep this value above the longest upload time when two writers share the bucket |
| `DAYFLY_SEARCH_MAX_BYTES` | no | 67108864 (64 MiB) | Max bytes that one text search reads. The search returns `truncated` at the cap and the caller resumes with `next_offset`. Must be a positive number of bytes |
| `DAYFLY_ALIAS_WORDS` | no | 3 | Number of words in a new alias. Allowed range 3 to 6. Resolution accepts 3 words up to this value |
| `DAYFLY_DEFAULT_TTL_DAYS` | no | 7 | Default TTL in positive whole days when a request does not set a TTL. Must be within `DAYFLY_MIN_TTL_DAYS` (at least 1) and `DAYFLY_MAX_TTL_DAYS` |
| `DAYFLY_MIN_TTL_DAYS` | no | 0 | Minimum TTL in whole days for an explicit TTL. `0` allows a TTL shorter than one day. A shorter value is rejected with 400 and `the minimum TTL is N days` |
| `DAYFLY_MAX_TTL_DAYS` | no | 3650 | Maximum TTL in whole days. A longer value is rejected with 400 and `the maximum TTL is N days`. Must be from 1 to 3650 |
| `DAYFLY_TTL_CHOICES` | no | `1,3,7,14,30,90` | Comma-separated whole-day choices for the browser TTL selector. Each value must be within the minimum and maximum. The default TTL is added when absent |
| `DAYFLY_LIST_DEFAULT_DAYS` | no | 2 | Number of days in the default paste list window, ending today in UTC. Must be 1 to 366. The default of 2 covers yesterday and today |
| `DAYFLY_RETENTION_BY_SIZE` | no | `false` | Clamp the expiry of a paste to a curve based on its total size. The clamp only shortens. Off keeps the exact requested TTL. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_RETENTION_MIN_DAYS` | no | 30 | Minimum retention in whole days at `DAYFLY_MAX_PASTE_BYTES`. Must be at least 1 and at most 3650, and must not exceed `DAYFLY_RETENTION_MAX_DAYS` |
| `DAYFLY_RETENTION_MAX_DAYS` | no | 365 | Maximum retention in whole days at zero bytes. Must be at least 1 and at most 3650, and must be at least `DAYFLY_RETENTION_MIN_DAYS` |
| `DAYFLY_HTTP_BIND` | no | `127.0.0.1:8080` | HTTP bind address |
| `DAYFLY_LOG_FORMAT` | no | `text` | Log format. `text` keeps the existing output. `json` writes one JSON object per line to stderr. A change needs a restart |
| `DAYFLY_METRICS_BIND` | no | (unset) | Private bind address for the Prometheus metrics listener. Unset starts no metrics listener. The public listener never serves `/metrics` |
| `DAYFLY_METRICS_ALLOWED_NETWORKS` | no | (empty) | Comma-separated CIDR blocks allowed to scrape the metrics listener. A loopback bind needs no list. A non-loopback bind is refused at startup without a list. The check uses the socket peer, not `X-Forwarded-For` |
| `DAYFLY_TRUSTED_PROXIES` | no | (empty) | Comma-separated CIDR blocks (bare addresses are single hosts) of reverse proxies whose `X-Forwarded-For` header is believed. Empty keeps the socket peer as the client and ignores forwarding headers. The chain is walked from the right; a malformed or over-long chain falls back to the peer with one warning. Set this before any per-client limit behind an ingress |
| `DAYFLY_RATE_LIMIT_PER_MINUTE` | no | 0 | Maximum requests per client per minute. 0 disables the limiter and allocates no counters. The burst is this value, and the bucket refills at this rate. Over the limit → 429 with `Retry-After`. Keyed on the resolved client address, so set `DAYFLY_TRUSTED_PROXIES` behind an ingress. Per process; resets on restart |
| `DAYFLY_ALLOW_UNAUTHENTICATED` | no | `false` | Accept a non-loopback bind with no bearer token. Startup refuses that bind without this value, and with it the service writes one warning. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_WRITE_REQUIRES_TOKEN` | no | `false` | Require the bearer token for writes while reads stay anonymous. It requires `DAYFLY_HTTP_BEARER_TOKEN`. Uploads, appends, deletes, expiry changes, the human write routes, and `/mcp` require the token. Downloads, metadata, search, `/raw`, the human index and view pages, and listing stay anonymous unless `DAYFLY_LIST_REQUIRES_TOKEN` gates the list. A paste token still authorizes its own delete, patch, and append. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_LIST_ENABLED` | no | `true` | Allow listing on every transport. `false` makes `/pastes`, `GET /v1/pastes`, and the MCP `list_recent` tool return 403 with "Listing is turned off on this instance." It wins over `DAYFLY_LIST_REQUIRES_TOKEN`. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_LIST_REQUIRES_TOKEN` | no | `false` | Require the bearer token for `GET /v1/pastes`, the human `/pastes` page, and the MCP `list_recent` tool even when reads are anonymous. With this on and no token configured, all three refuse: there is nothing to check. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_PASTE_TOKENS` | no | `false` | Return a 128-bit management token at creation. The JSON store response carries `paste_token`; the plain response carries `X-Paste-Token`; the human create result page shows the token once. The token authorizes `DELETE` and `PATCH` for that paste. Only its SHA-256 is stored. The bearer token remains universal. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_EXPOSE_CREATOR_ADDR` | no | `false` | Return the stored `created-by` address in the meta route, the `X-Created-By` header on `HEAD`, and the MCP `fetch_metadata` tool. The access log includes the client address only when this is on. An address is personal data, so the default hides it. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_SHOW_WORDS` | no | `true` | Show the generated word alias in the human UI: create responses, the view page, and the paste list. Display only; the JSON/MCP responses still include `words`. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_SHOW_INTERNAL_PATHS` | no | `false` | Show the canonical date and ID under the alias in the human paste list. Off labels the column "Link" and shows only the alias. Display only; view URLs and API responses still accept the canonical ID. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_INLINE_PDF` | no | `true` | Serve a PDF download inline. Set this to `false` on a public instance so a PDF is an attachment. HTML and SVG are always attachments. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_VIEW_LINE_NUMBERS` | no | `false` | Number the lines of a text paste on the view page. The numbers are presentation only: a CSS counter renders them, they are not selectable, and the Copy button does not include them. Pastes above 5,000 lines render unnumbered. Accepts `true`/`false` (also `1`/`0`, `yes`/`no`, `on`/`off`) |
| `DAYFLY_HTTP_BEARER_TOKEN` | one form | (unset) | Optional bearer token for the binary's own HTTP endpoint (see Access model); set this variable or `DAYFLY_HTTP_BEARER_TOKEN_FILE`. An unset value disables auth; a set empty or whitespace-only value is a startup error |
| `DAYFLY_HTTP_BEARER_TOKEN_FILE` | one form | (unset) | Path to a file that contains the bearer token; set this variable or `DAYFLY_HTTP_BEARER_TOKEN` |
| `DAYFLY_MCP_LOCAL_ROOT` | no | (unset) | Directory for the MCP local-path tools. It must exist, or startup fails. When set, `store_file` and `fetch_file` refuse any path outside it, after resolving symlinks. When unset, the stdio transport accepts any readable path and the HTTP transport refuses both tools |
| `DAYFLY_FAVICON_PATH` | no | (unset) | Path to a favicon served at `/favicon.ico`. The file is read at startup and must be 1 MiB or smaller. The content type follows the extension (`.ico`, `.png`, `.svg`, `.gif`, `.jpg`, `.webp`). Unset returns 404 |
| `DAYFLY_ROBOTS_TXT_PATH` | no | (unset) | Path to a `robots.txt` served at `/robots.txt`. The file is read at startup and must be UTF-8 and 1 MiB or smaller. Unset returns 404 |
| `DAYFLY_SECURITY_TXT_PATH` | no | (unset) | Path to a `security.txt` served at `/.well-known/security.txt`. The file is read at startup and must be UTF-8 and 1 MiB or smaller. Write the RFC 9116 fields, including `Contact` and `Expires`. Unset returns 404 |
| `DAYFLY_SITE_NAME` | no | (unset) | Site name for the human UI footer, at most 80 characters. Unset keeps `dayfly — internal use only`. Set it on a public instance to replace that text |
| `DAYFLY_CONTACT` | no | (unset) | Takedown contact for the human UI footer, at most 200 characters. An `http://` or `https://` value becomes a link and a value with `@` becomes a `mailto:` link. Other values are plain text |
| `DAYFLY_AUP_URL` | no | (unset) | Acceptable-use URL for the human UI footer, at most 500 characters. It must use `http://` or `https://`. The footer links the text "acceptable use" to it |

### Retention by size

`DAYFLY_RETENTION_BY_SIZE` bounds storage cost with time. It is off by
default. When it is on, the expiry of a new paste is clamped to the
size curve:

```text
retention_days = min + (max - min) * (1 - size / cap)^3
```

`min` is `DAYFLY_RETENTION_MIN_DAYS` (default 30). `max` is
`DAYFLY_RETENTION_MAX_DAYS` (default 365). `size` is the total paste
size: the base object plus every segment. `cap` is
`DAYFLY_MAX_PASTE_BYTES`.

The clamp only shortens. A client TTL below the curve is kept. A paste
at or above the cap gets `min` days. An append that crosses a curve
boundary shortens the expiry of the base and every segment. `PATCH`
cannot extend a paste past the curve value.

A streamed upload without `Content-Length` uses its request cap as
`size`, so it gets the shortest retention for that cap. Send a length
to get the curve value for the real size.

The alias keeps its original expiry. It is never earlier than the
paste.

Alias strength by word count (7,776 words, 12.925 bits per word):

| Words | Bits | Combinations |
| --- | --- | --- |
| 3 | 38.8 | 4.7e11 |
| 4 | 51.7 | 3.7e15 |
| 5 | 64.6 | 2.9e19 |
| 6 | 77.5 | 2.2e23 |

The default three words are a name, not a secret. Use four or more
when aliases become addresses that a stranger can try. Resolution
accepts 3 words up to the configured count. If you lower the count,
aliases longer than the new count stop resolving. Keep the count at or
above the longest alias in use.

For each S3 credential and for the bearer token, set the direct
variable or its `_FILE` alternative, not both. If both are set, startup
stops with a configuration error. The binary trims whitespace from file
values and rejects an empty file.

Credentials are never logged. `Config` has no `Debug` that prints
secrets; startup logs only bucket, endpoint scheme, region, limits, and
whether word aliases are shown. A credential file read error names the
path only, not the file contents. The permission warnings never include
the file contents.

`--verbose` sets the log filter to INFO. `RUST_LOG` overrides it.
Dayfly runs in the foreground. Use systemd or the container runtime to
supervise it. There is no daemon mode.

### Reload

`serve` watches the environment file, the credential files, and the
asset files that it names. It reloads when one of them changes. The
watch checks the modification time every 2 seconds. A container cannot
send a signal to itself, so the files are the trigger.

A reload reads the same sources as startup: the process environment
first, then the file. These settings take effect on the next request:

- `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, and both S3 credentials
  (`S3_ACCESS_KEY_ID`/`S3_SECRET_ACCESS_KEY` or their `_FILE` forms)
- `DAYFLY_MAX_TEXT_BYTES`, `DAYFLY_MAX_FILE_BYTES`,
  `DAYFLY_MAX_PASTE_BYTES`, and `DAYFLY_SEARCH_MAX_BYTES`
- `DAYFLY_ALIAS_WORDS`
- `DAYFLY_DEFAULT_TTL_DAYS`, `DAYFLY_MIN_TTL_DAYS`,
  `DAYFLY_MAX_TTL_DAYS`, and `DAYFLY_TTL_CHOICES`
- `DAYFLY_RETENTION_BY_SIZE`, `DAYFLY_RETENTION_MIN_DAYS`, and
  `DAYFLY_RETENTION_MAX_DAYS`
- `DAYFLY_SHOW_WORDS`
- `DAYFLY_INLINE_PDF`
- `DAYFLY_FAVICON_PATH`, `DAYFLY_ROBOTS_TXT_PATH`, and
  `DAYFLY_SECURITY_TXT_PATH`
- `DAYFLY_TRUSTED_PROXIES`
- `DAYFLY_METRICS_ALLOWED_NETWORKS`
- `DAYFLY_WRITE_REQUIRES_TOKEN`
- `DAYFLY_LIST_ENABLED`
- `DAYFLY_LIST_REQUIRES_TOKEN`
- `DAYFLY_PASTE_TOKENS`
- `DAYFLY_EXPOSE_CREATOR_ADDR`
- `DAYFLY_RATE_LIMIT_PER_MINUTE`
- `DAYFLY_HTTP_BEARER_TOKEN` or `DAYFLY_HTTP_BEARER_TOKEN_FILE`
- `DAYFLY_SITE_NAME`, `DAYFLY_CONTACT`, and `DAYFLY_AUP_URL`

These settings need a restart:

- `DAYFLY_HTTP_BIND`: the listener is already bound.
- `DAYFLY_LOG_FORMAT`: the subscriber is built at startup.
- `DAYFLY_METRICS_BIND`: the metrics listener is already bound.
- `DAYFLY_ALLOW_UNAUTHENTICATED`: startup guard only.
- `DAYFLY_MCP_LOCAL_ROOT`: an MCP handler resolves it at startup.

A reload that names a different bind or a startup-only setting writes
one warning and keeps the old value. A malformed file writes one error
and keeps the running configuration. A successful reload writes one
line with the changed field names and the safe summary. It never writes
a value.

Each operation uses one store snapshot. A reload affects the next
request, not one in progress. An existing HTTP MCP session takes the
current snapshot for each tool call.

`mcp` (stdio) reads the environment at startup and does not watch the
file. If the process has no environment file, `serve` writes one line
and starts no watch.

## Access logs and metrics

HTTP mode writes one access log line for each request at `INFO`. The line
carries `request_id`, `method`, `route`, `status`, `latency_ms`,
`response_bytes`, and `paste_id`. It carries `client` only when
`DAYFLY_EXPOSE_CREATOR_ADDR` is on; otherwise the field is empty. The
route is the template, for example `/v1/pastes/{random}`. The line never
carries a credential, a paste token, or a query string. The latency stops
when the response starts, so a streamed body can take longer. Set
`--verbose` or `RUST_LOG=dayfly=info` to see the lines.

Set `DAYFLY_LOG_FORMAT=json` to write one JSON object per line on stderr.
A collector such as Alloy or Promtail can read the container stderr and
send the lines to Loki.

The metrics listener is separate. Set `DAYFLY_METRICS_BIND` to a private
address, for example `127.0.0.1:9091` or `10.0.0.5:9091`. The listener
serves only `GET /metrics` in the Prometheus text format. The public
listener returns 404 for `/metrics` in every configuration. Set
`DAYFLY_METRICS_ALLOWED_NETWORKS` when the bind is not loopback. The
listener checks the socket peer and never reads `X-Forwarded-For`.

The registry has `dayfly_build_info`, `dayfly_http_requests_total`,
`dayfly_http_request_duration_seconds`, `dayfly_http_requests_in_flight`,
`dayfly_http_request_bytes_total`, `dayfly_http_response_bytes_total`,
`dayfly_paste_operations_total`, `dayfly_s3_requests_total`,
`dayfly_rate_limited_total`, `dayfly_multipart_orphans_aborted_total`,
and the process metrics. Labels use the route template, not a paste ID or a
client address. Metrics are per process and reset on restart.

`dayfly_s3_requests_total` covers each abort outcome. The operation
label is `abort_multipart_upload`; the outcome label is `ok` or
`error`. `dayfly_multipart_orphans_aborted_total` counts the stale
uploads that the sweep aborts.

```yaml
scrape_configs:
  - job_name: dayfly
    static_configs:
      - targets: ["10.0.0.5:9091"]
```

Do not give Prometheus the bearer token. The metrics listener does not
check it. Use the private bind, the allowlist, and the network policy. If
the metrics listener needs authentication, put a proxy in front of it.

## Running the MCP server

stdio is the default transport. For the HTTP transport, see
[MCP over HTTP](#mcp-over-http).

```sh
# from the project directory, with env set (see .env.example)
cargo run
# or, once built:
./target/release/dayfly
# explicit subcommand, same thing:
./target/release/dayfly mcp
```

Example MCP client config. Replace the binary path with the installed
path on the machine that runs it. The config contains paths only, not
key material:

```json
{
  "mcpServers": {
    "dayfly": {
      "command": "/path/to/dayfly",
      "args": [],
      "env": {
        "S3_ENDPOINT": "https://garage.example.invalid:3900",
        "S3_BUCKET": "pastebin",
        "S3_REGION": "us-east-1",
        "S3_ACCESS_KEY_ID_FILE": "/home/you/.config/dayfly/access-key-id",
        "S3_SECRET_ACCESS_KEY_FILE": "/home/you/.config/dayfly/secret-access-key"
      }
    }
  }
}
```

Create the files outside the repository. The directory must have mode
`0700`, and each file must have mode `0600`. The account that runs
`dayfly` must own the directory and the files. On Unix, the binary
warns on stderr when a credential file is group or world readable or
writable. It also warns when the parent directory is accessible to the
group or other users, or when another user owns the file or directory.
Startup continues. For example:

```sh
install -d -m 0700 ~/.config/dayfly
umask 077
$EDITOR ~/.config/dayfly/access-key-id
$EDITOR ~/.config/dayfly/secret-access-key
```

The direct form is also valid: set `S3_ACCESS_KEY_ID` and
`S3_SECRET_ACCESS_KEY` in the environment. Do not set a direct
variable and its `_FILE` alternative together. A client config that
every agent can read must not hold the key values.

Logging goes to stderr only, never stdout, so the JSON-RPC stream is
not corrupted.

MCP tools: `store_text`, `store_file`, `append_text`, `set_ttl`,
`fetch_text`, `search_text`, `fetch_file` (downloads to a local path and
returns metadata — binary content is never sent over JSON-RPC),
`fetch_metadata`, `delete_paste`, `list_recent`, `get_info`.

`store_text` and `store_file` accept `title` and `tags`. The title is
at most 200 bytes. Tags are at most 8 values and 512 bytes in total.
`fetch_metadata` and `list_recent` return both fields.

When `DAYFLY_PASTE_TOKENS` is on, `store_text` and `store_file` return
`paste_token`. `delete_paste`, `set_ttl`, and `append_text` accept a
`paste_token` argument. A configured bearer token is checked by the
HTTP transport before the MCP call, so the paste token is optional in
that case. With no bearer token, those three tools require the paste
token.

`fetch_text` returns at most 8 MiB per call. Use `offset` and `limit`
to page through a larger paste. The response reports `total_bytes` and
`truncated`.

`store_file` and `fetch_file` are subject to the transport rule in the
Access model section: the HTTP transport refuses them until
`DAYFLY_MCP_LOCAL_ROOT` sets a directory.

## Local development

`scripts/dev.sh` runs the HTTP server on localhost with the Garage settings
pulled from OpenBao. It is the fastest way to iterate on the web UI without
building, pushing, and rolling out an image.

```sh
nix develop
./scripts/dev.sh
# -> http://127.0.0.1:8080

# Restart on Rust source changes:
./scripts/dev.sh --watch

# Use another port:
DAYFLY_HTTP_BIND=127.0.0.1:8090 ./scripts/dev.sh
```

The script reads `secret/service/lan/dayfly` by default, exports every field
as an environment variable, and never prints secret values. It uses
`BAO_ADDR` (default `https://bao.k.ser.ink`) and `BAO_TOKEN`, falling back to
the workstation OpenBao agent token at `/run/openbao/token`.

By default local runs share the same `dayfly-files` bucket as the deployed
instance, so test pastes appear in the live listing until they expire or are
deleted. For isolation, create a separate bucket and key and point the script
at its OpenBao path:

```sh
DAYFLY_BAO_PATH=secret/service/lan/dayfly-local ./scripts/dev.sh
```

## Running HTTP mode

```sh
./target/release/dayfly serve
# or with an explicit bind address:
./target/release/dayfly serve --bind 127.0.0.1:8080
```

`serve` handles `SIGTERM` and `SIGINT`. On a signal, it stops the public
listener and the metrics listener, then drains in-flight requests. The
drain has a 30-second timeout. The process exits with status 0 after a
requested shutdown. A drain that reaches the timeout writes a warning
and exits with status 0. The log records the signal and the drain result.

An upload that reaches the timeout ends with the process. The existing
orphan sweep aborts its incomplete multipart upload after the configured
age.

With a bearer token set, add `-H "Authorization: Bearer $TOKEN"` to
every curl example below.

### Store text

```sh
printf 'hello, world\n' | curl --data-binary @- \
  -H 'Content-Type: text/plain' -H 'X-TTL-Days: 7' \
  http://127.0.0.1:8080/upload
# → http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...  (plain text)

curl --data-binary @- -H 'Accept: application/json' \
  http://127.0.0.1:8080/upload < note.txt
# → {"id": "...", "url": "...", "size": ..., "sha256": "...",
#    "words": "abacus-abdomen-zoology",
#    "expires_at": "...", "content_type": "...", "filename": ...,
#    "detection_source": "...", "detection_conflict": null}
```

When `DAYFLY_PASTE_TOKENS` is on, the JSON response adds `paste_token`
and the plain response adds an `X-Paste-Token` header. The plain
response also carries `X-Paste-Id` with the canonical paste ID. It
carries `X-Words` with the word alias when the store wrote one. The
body stays one URL and one newline, so `url=$(curl ...)` keeps working.

Send `X-Title` and `X-Tags` to name and group a paste. `X-Tags` is a
comma-separated list. The metadata headers accept UTF-8 values, so
`X-Title: 日本語` and an emoji filename work. A client that cannot send
non-ASCII header bytes can use the multipart form fields or the MCP
arguments instead. The store trims the title, lowercases and
deduplicates the tags, and accepts at most 8 tags and 512 bytes of tags.
A title is at most 200 bytes. The JSON response and the metadata route
return `title` and `tags`.

```sh
printf 'build failed\n' | curl --data-binary @- \
  -H 'X-Title: Build log' -H 'X-Tags: ops,build' \
  http://127.0.0.1:8080/upload
```

`POST /v1/pastes` is an alias for `POST /upload`.

### TTL forms

Send `X-TTL` (or `?ttl=`) with seconds or a duration. A bare integer
is seconds: `3600` is one hour, and `7` is seven seconds. Use `7d` for
seven days. The duration forms are number and unit pairs with `s`,
`m`, `h`, and `d` (`90m`, `1h30m`, `2d`, `1d12h`).

`X-TTL-Days` and `ttl_days` accept a positive whole number of days.
Zero is rejected. Do not send both forms: a duration form plus a day
form returns 400. Repeated values of one form keep the last value. For
one form, a header wins over the query parameter.

A malformed or zero TTL returns 400 with a JSON error. It does not
fall back to the default. `DAYFLY_DEFAULT_TTL_DAYS` sets the default
in positive whole days when no TTL is given. `DAYFLY_MIN_TTL_DAYS` and
`DAYFLY_MAX_TTL_DAYS` bound an explicit TTL; a value outside the range
returns 400 with the limit in the message. The hard maximum is 3650
days. Set the minimum to `1` to reject sub-day TTLs.

```sh
# Sub-day TTL: one hour and thirty minutes.
cat scratch.txt | curl --data-binary @- \
  -H 'X-TTL: 1h30m' \
  http://127.0.0.1:8080/upload

# The same TTL as a number of seconds.
cat scratch.txt | curl --data-binary @- \
  -H 'X-TTL: 5400' \
  http://127.0.0.1:8080/upload

# Seven days. A bare 7 would mean seven seconds.
cat notes.txt | curl --data-binary @- \
  -H 'X-TTL: 7d' \
  http://127.0.0.1:8080/upload

# The legacy day form still works.
cat notes.txt | curl --data-binary @- \
  -H 'X-TTL-Days: 7' \
  http://127.0.0.1:8080/upload
```

### Change expiry

`PATCH` the paste with a new TTL. The new expiry is measured from now,
so a shorter or longer window both work. With `DAYFLY_RETENTION_BY_SIZE`
on, the new expiry stops at the curve value for the current total size.
The call rewrites the base, every segment, and the alias, then returns
the metadata shape. The base object is the read-time authority. A
shortening writes the base first; a later failure keeps the shorter
expiry. An extension writes the base last; a failure before it keeps the
old expiry. A failed step returns an error that names the step and the
object key.

```sh
# Shorten to one hour.
curl -X PATCH -H 'X-TTL: 1h' \
  http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Extend to thirty days.
curl -X PATCH -H 'X-TTL-Days: 30' \
  http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Change the expiry with the paste token, when no bearer token is set.
curl -X PATCH -H 'X-TTL: 1h' -H 'X-Paste-Token: <token>' \
  http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...
```

An expired paste returns 410, and the call writes nothing.

### Append text

```sh
printf 'another line\n' | curl --data-binary @- \
  -H 'Content-Type: text/plain' \
  http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123.../append
# → {"id": "...", "url": "...", "appended_bytes": 13,
#    "segment_key": "pastes/2026-10-28/AbC123.../00000000000000000000-Xk9mP2qR7sT4",
#    "size": ..., "sha256": null, "expires_at": "..."}
```

An append writes only the new bytes. It does not rewrite the base
object, except when `DAYFLY_RETENTION_BY_SIZE` shortens the expiry. The
text cap applies to this append, not to the total size.
An expired or missing ID returns 410 or 404 and writes nothing. An
empty body writes nothing. Use the bare ID form
`/v1/pastes/<random>/append` if you do not have the date. A word alias
works in place of the ID.

### Streaming upload (curl piping, 0x0.st style)

```sh
# Text paste: no filename, capped by DAYFLY_MAX_TEXT_BYTES.
cat notes.txt | curl --data-binary @- http://127.0.0.1:8080/upload

# File upload: a filename selects the DAYFLY_MAX_FILE_BYTES cap.
cat large-file.bin | curl --data-binary @- \
  -H 'X-Filename: large-file.bin' \
  http://127.0.0.1:8080/upload
URL=$(cat large-file.bin | curl --data-binary @- \
  -H 'X-Filename: large-file.bin' \
  http://127.0.0.1:8080/upload)

# With a filename, TTL, and an explicit content type:
cat report.csv | curl --data-binary @- \
  -H 'X-Filename: report.csv' -H 'X-TTL-Days: 14' \
  -H 'Content-Type: text/csv' \
  http://127.0.0.1:8080/upload

# Multipart form upload (also streamed, not buffered whole):
curl -F file=@photo.png -F ttl=90m http://127.0.0.1:8080/upload
curl -F file=@photo.png -F ttl_days=30 http://127.0.0.1:8080/upload
```

A request with no filename is a text paste and is capped at the lower
of `DAYFLY_MAX_TEXT_BYTES` and `DAYFLY_MAX_PASTE_BYTES`; supply
`X-Filename` (or a multipart filename) for larger files. A file upload
is capped at the lower of `DAYFLY_MAX_FILE_BYTES` and
`DAYFLY_MAX_PASTE_BYTES`. The text/file split is a client-declared
convention, not a property of the bytes: adding a filename opts into
the file cap.
The text cap is therefore a guardrail for text-paste traffic, not an
authorization boundary or a hard bound on stored inline text. Enforcing
it from the sniffed type would make an ASCII file upload with a
filename subject to the text cap, which contradicts the file-upload
contract above. Both `Content-Length` and chunked transfer encoding
(unknown length) are accepted. For S3: a single streaming PutObject is
used when the length is known and at most one 5 MiB part; otherwise S3
multipart upload is used — parts are buffered to the 5 MiB minimum
(except the last part) and uploaded as they arrive, `CompleteMultipartUpload` at
EOF, `AbortMultipartUpload` on client disconnect or any error.
**A URL is returned only on successful completion. On disconnect or
any error, no URL is returned and any multipart upload is aborted.**
An abort is retried up to three times with a short backoff. If every
attempt fails, dayfly logs the key and the upload ID and keeps the
original upload error. The orphan sweep, or a lifecycle rule as a
backstop, removes a remaining upload. An empty upload becomes a single
empty PutObject.

The local-file path (`store_file`) reads one open handle for the size,
the hash, and the uploaded bytes, so the recorded metadata describes the
bytes that were uploaded. If a large file changes while it is uploaded,
the store deletes the object and returns an error, rather than keeping a
paste whose hash and content disagree. A file that grows past the
effective file cap (the lower of `DAYFLY_MAX_FILE_BYTES` and
`DAYFLY_MAX_PASTE_BYTES`) after the first size check is rejected, not
uploaded.

Note on multipart forms: text fields (`filename`, `ttl`, `ttl_days`)
must come before the `file` field if you want them honored; fields
after the file field are not read. Use headers (`X-Filename`,
`X-TTL`, `X-TTL-Days`) or query parameters (`?filename=…&ttl=…` or
`?filename=…&ttl_days=…`) if in doubt.

### Fetch, range, metadata, delete, list

```sh
# Fetch text (plain curl GET returns the paste):
curl http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Fetch by word alias (the same route):
curl http://127.0.0.1:8080/v1/pastes/abacus-abdomen-zoology

# Fetch a byte range (206, streaming, never the whole object):
curl -H 'Range: bytes=0-999' http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...
curl -H 'Range: bytes=-1024' http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Metadata:
curl http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123.../meta
curl -I http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Delete (204 on success, and also 204 when the object does not exist):
curl -X DELETE http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Delete with the paste token, when no bearer token is set:
curl -X DELETE -H 'X-Paste-Token: <token>' \
  http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# List recent (expired filtered out). No dates means the default window:
# the last DAYFLY_LIST_DEFAULT_DAYS days, ending today in UTC (2 by default).
curl 'http://127.0.0.1:8080/v1/pastes?limit=100'

# List one UTC date:
curl 'http://127.0.0.1:8080/v1/pastes?date=2026-10-28&limit=100'

# List an inclusive range across a date boundary:
curl 'http://127.0.0.1:8080/v1/pastes?start=2026-10-28&end=2026-10-29&limit=100'

# Filter the returned page by text or tag:
curl 'http://127.0.0.1:8080/v1/pastes?q=build'
curl 'http://127.0.0.1:8080/v1/pastes?tag=ops'

# Read the next page with the cursor from the previous response:
curl 'http://127.0.0.1:8080/v1/pastes?date=2026-10-28&limit=100&after_date=2026-10-28&after_key=pastes%2F2026-10-28%2FAbC123...'
```

The base object records the resolved creator address as `created-by` when
the transport has a client address. `GET .../meta` returns it as
`created_by`, and `HEAD` returns it as `X-Created-By`, only when
`DAYFLY_EXPOSE_CREATOR_ADDR` is on. The MCP `fetch_metadata` tool uses the
same gate. The stdio MCP transport has no client address, so it stores no
key.

`date` takes precedence over `start` and `end`. Supply both `start` and
`end` for a range; a one-sided range returns 400. Results are in date
order, then in S3 key order within each date. The limit applies to the
newest dates first, then to key order within a date. Key order is not
creation order because IDs are random. An older date can be omitted.

The JSON response carries `truncated` and `next`. `next` is
`{date, start_after}` or null. Supply both `after_date` and `after_key`
to read the next page; one part alone returns 400 with code
`invalid_cursor`. The cursor follows scan order (newest date first,
then S3 key order), not the sorted order of the returned items.

`q` matches the alias, title, filename, or ID. `tag` matches one tag.
The filter applies to the items in the returned page. Use the cursor to
read further pages.

### Search text

```sh
# The query is literal unless regex=true.
curl 'http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123.../search?q=error&context=1'
curl 'http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123.../search?q=error%5B0-9%5D%2B&regex=true&ignore_case=true'

# Resume after the previous next_offset.
curl 'http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123.../search?q=error&after_offset=12345'
```

The search response carries `hits`, `scanned_bytes`, `truncated`, and
`next_offset`. A hit carries `line` (1-based), `byte_offset` (the start
of the matching line), `preview` (the line with optional context, at
most 4 KiB), and `ranges` (match byte ranges within the preview). The
search is line-oriented: a match cannot span two lines. It reads at
most `DAYFLY_SEARCH_MAX_BYTES` per call. When `truncated` is true, send
`next_offset` as `after_offset` to read the next page. The search uses
no index and is O(bytes read). A binary paste returns 415 with code
`not_text`. An invalid regular expression returns 400 with code
`invalid_search`.

Downloads return an `ETag`. Send it as `If-None-Match` to get
`304 Not Modified` when the paste has not changed. The validator covers
the joined paste, so an append changes it. `HEAD` returns the same tag.

Error responses are machine-readable JSON, e.g.
`{"error": "payload too large: limit is 1073741824 bytes", "code":
"too_large"}` with HTTP 413 for oversize, 400 for invalid input, 404
for not found, 410 for expired, 416 for an unsatisfiable range, 507
for a bucket quota limit, and 502 for other S3 errors. An upload body
shorter than its `Content-Length` returns 400 and stores nothing.

Every HTTP error carries a stable `code`. The current values are:
`unauthorized`, `forbidden`, `listing_disabled`, `not_found`,
`expired`, `too_large`, `length_mismatch`, `too_many_segments`,
`range_not_satisfiable`, `invalid_request`, `invalid_id`,
`invalid_date`, `invalid_range`, `invalid_cursor`, `invalid_title`,
`invalid_tags`, `invalid_search`, `not_text`, `invalid_ttl`,
`invalid_lifecycle`,
`invalid_path`, `path_outside_root`, `local_files_disabled`,
`alias_unavailable`, `io_error`, `s3_error`, `quota_exceeded`,
`copy_limit`, `bucket_unavailable`, `partial_update`, `rate_limited`,
and `method_not_allowed`. Do not parse the `error` text; branch on the
`code`.

The MCP tools use the same codes. A domain error returns a tool result
with `isError` set and a JSON text block:
`{"code": "not_found", "message": "paste not found: ..."}`. A bad
argument stays a JSON-RPC `-32602` error, and an internal fault stays a
JSON-RPC `-32603` error; both carry the code in the error `data`.

One status code needs reading carefully, because the current
behaviour is narrower than the list above suggests. `DELETE` is
idempotent, so deleting a full paste ID that does not exist returns
204 rather than 404; a 404 comes back only when a bare random ID
cannot be resolved against the bucket at all.

A wrong `S3_BUCKET` name is not one of those cases. `NoSuchBucket`
carries its own error and maps to 502, so a misconfigured bucket says
so rather than reporting every paste as missing.

## MCP over HTTP

`serve` exposes the MCP server over streamable HTTP at `/mcp`. The
endpoint is on by default; no flag is needed. It shares the one
`Store` and the one configuration path with the REST routes. A second
store does not exist.

stdio remains the default. The `mcp` subcommand and the start with no
subcommand keep the stdio transport. Run `serve` when several agents
must share one process and one binary.

The endpoint uses the same auth rule as the REST routes:

- When `DAYFLY_HTTP_BEARER_TOKEN` is set, every request needs
  `Authorization: Bearer <token>`. A missing or wrong token gets HTTP
  401 and `{"error":"unauthorized"}`. `DAYFLY_WRITE_REQUIRES_TOKEN`
  makes reads anonymous, but `/mcp` stays gated because it carries
  writes.
- When the token is unset, the endpoint stays open. In that case,
  serve it on a private network or localhost only.

Each `initialize` request creates an in-memory MCP session. The
process accepts several sessions at the same time. Sessions do not
survive a restart; the client then initializes again. A reload does not
end an existing session; each tool call uses the current configuration.

Minimal client configuration. Field names depend on the client; this
example uses the common `mcpServers` shape:

```json
{
  "mcpServers": {
    "dayfly": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8080/mcp",
      "headers": {
        "Authorization": "Bearer YOUR-BEARER-TOKEN"
      }
    }
  }
}
```

Remove the `headers` block when the token is unset. Start the server
with `dayfly serve` before the client connects.

## Append and segmented storage

S3 has no append primitive. dayfly stores each append as a new segment
object and leaves the base object unchanged.

- The base object stays at `pastes/<date>/<id>`. It is segment 0.
- An append writes `pastes/<date>/<id>/<sequence>-<suffix>`. The
  sequence is a 20-digit zero-padded number. The suffix is 12 random
  base62 characters. The first append has sequence 0. A segment stays
  under the base date prefix, so the paste belongs to its creation
  date, not to the append date.
- An append lists the segment prefix and takes max + 1. It writes only
  the new bytes. The cost is the new bytes plus metadata calls that
  scale with the segment count, not with the total size. It does not
  copy or rewrite the base object, except when
  `DAYFLY_RETENTION_BY_SIZE` shortens the expiry.
- A shortening append writes the base first, then the earlier segments,
  and the new segment last. A failure before the last write leaves no
  new content, so a retry does not duplicate the text. A later `PATCH`
  rewrites every segment if an earlier segment update failed.
- Two appends can choose the same sequence. The random suffix keeps the
  object names distinct, so both appends survive.
- An ID has at most two components after `pastes/`. A segment key has
  a third component, so listing and bare-ID resolution skip it.
- Reads join the base object and the segments in sequence order. Two
  segments with the same sequence use suffix order.
- A range read maps the global byte range to the base object and to
  the segments that hold it. It sends one range request for the
  overlap in each object. It does not read the whole paste for a small
  range.
- `fetch_metadata` reports the total size: base plus segments.
- `delete` removes the base object, every object under its segment
  prefix, and the alias object. It lists the segment prefix again after
  the base delete and removes anything that arrived. An append checks
  the base after it writes the segment; if the base is gone, it removes
  the segment.
- An append keeps the `expires-at` value of the base object. It does
  not extend the TTL. With `DAYFLY_RETENTION_BY_SIZE` on, an append
  that crosses a curve boundary shortens the expiry and rewrites the
  base and the earlier segments.
- An append to an expired or missing ID returns an error and writes no
  object.
- A stored `sha256` covers the base object only. When segments exist,
  the API returns `sha256: null`. No stored value covers the joined
  paste.
- `list_recent` reports the total size and does not show a segment as
  a paste.

Limits:

- **Concurrent appends are lossless but unordered.** Two appends to the
  same ID can choose the same sequence. Both writes survive, because
  the segment names have random suffixes. A read returns both chunks.
  The order of the chunks for one sequence follows the suffix, so the
  order is arbitrary but deterministic. The store does not use a
  conditional write, because Garage accepts `If-None-Match` and ignores
  it.
- **A delete that races an append does not leave an orphan segment.**
  The delete removes the base, then lists the segment prefix again and
  removes anything left. An append checks the base after it writes its
  segment; if the base is gone, it removes the segment. A later delete
  of the full ID retries the cleanup. A bare ID or a word alias may not
  resolve after the base is gone, so keep the full ID.
- **A segment key is not a paste ID.** The API rejects an ID with three
  components after `pastes/`.
- **An empty append is a no-op.** It writes no object and keeps the
  base `sha256`. It succeeds even when the paste is at the total cap.
- **A paste holds at most `DAYFLY_MAX_PASTE_BYTES` total bytes.** The
  default is 1 GiB. A new paste or an append that would exceed the cap
  fails with `TooLarge` and writes no object. A new paste uses the lower
  of its type cap and the total cap. The base object and every segment
  count toward the cap.
- **A paste holds at most 4,096 appended segments.** An append at the
  limit fails and writes no object. A metadata read, download, list, or
  delete fails when the bucket holds more segments than the limit.
- **A transient alias check keeps the new object.** The store deletes
  the new object only when the alias points at another paste or the
  alias is gone. An S3 error after the upload returns an error and
  keeps the object.
- **Segments count toward scan limits.** A listing scans one page of up
  to 1,000 keys. Bare-ID resolution scans up to 10 pages. Segment keys
  consume that budget, so keep the full ID for a paste with many
  segments.

## Word aliases

Every new paste gets an alias of `DAYFLY_ALIAS_WORDS` words. The
default is three, for example `abacus-abdomen-zoology`. The alias is a
second name for the paste, not a second object. Use the canonical ID or
the alias in every operation that takes an ID: fetch, metadata, append,
and delete.

- The words come from the bundled EFF large wordlist in
  `src/wordlist.txt` (7,776 words). The source and the CC BY 3.0
  attribution are in `src/wordlist.LICENSE`. No external service is
  called. Four wordlist entries contain a hyphen; the parser matches
  each one as a single word.
- An alias is 3 to 6 words joined by hyphens. `DAYFLY_ALIAS_WORDS`
  sets the count for new aliases. The store returns the hyphen form.
  Input is case-insensitive; spaces and hyphens are accepted as
  separators. A phrase outside 3 words up to the configured maximum, or
  with an unknown word, is rejected.
- The alias object is `words/<phrase>`. Its body is the canonical key
  `pastes/<date>/<id>`. Its metadata holds `target`, `expires-at`, and
  `kind=alias`. Lookup is O(1): one GetObject, then a HeadObject of
  the target.
- Uniqueness: one Store instance serializes the alias check and write.
  It retries up to 8 random phrases. If every attempt
  collides, the store returns an error and writes no paste. Two dayfly
  processes that share one bucket can still race, because Garage
  ignores conditional writes; run one writer per bucket for a strict
  guarantee.
- Expiry: the alias inherits the paste `expires-at`. It does not
  extend the TTL. An expired or missing paste does not resolve through
  its alias. An expired alias object is free to reuse, and a new paste
  writes over it. An alias object without a valid `expires-at` value
  stays taken.
- Delete: delete by ID or by alias. Either form removes the base
  object, every segment, and the alias object when the alias still
  points to that paste. A phrase that another store has rebound is left
  in place.
- Legacy objects have no alias. They keep working by ID, and `words`
  is absent or null.
- Display: the human UI uses the alias in create responses, the view
  page, and the paste list by default. Set `DAYFLY_SHOW_WORDS=false`
  to use and show the canonical ID instead. The setting is display-only:
  the alias is still generated and the JSON/MCP responses still return
  it.
- Entropy: see the table in "Configuration". The default three words
  give about 38.8 bits. The uniqueness check prevents collisions, not
  guessing. Treat the alias as a name, not a secret. The bearer token
  and the private bucket remain the access control.
- The alias object is small. It has the same expiry as the paste.
  `docs/lifecycle-example.json` includes a `words/` rule. Add one if you
  write your own lifecycle document. Lookup still rejects an expired
  target.

## Content-Type detection

The server sets the S3 object's Content-Type from detection and
serves that same type back on download, so a web frontend can render
an image inline, show text, or download a binary correctly.
Detection precedence, in order:

1. **Specific client-supplied Content-Type** — except curl's
   `--data-binary` default, `application/x-www-form-urlencoded`,
   and the generic `application/octet-stream`, which are treated as
   unspecified.
2. **Magic-byte sniffing** of the first bytes (`infer` crate) for
   images (PNG, JPEG, GIF, WebP, …), PDF, ZIP, video/audio
   containers, and other formats with signatures. A ZIP signature
   alone cannot name an Office, OpenDocument, or EPUB document. A
   known zip-based extension (`.docx`, `.xlsx`, `.pptx`, `.odt`,
   `.ods`, `.odp`, `.odg`, `.epub`, `.docm`, `.xlsm`, `.pptm`)
   supplies the specific type. The extension comes from the
   client-supplied filename; the container contents are not inspected.
3. **Filename extension** (`mime_guess`), when an `X-Filename` /
   multipart filename is supplied.
4. **Plain-text detection**: valid UTF-8 with no NUL bytes in the
   whole payload → `text/plain; charset=utf-8`. A text paste with no
   filename is buffered up to the text cap, so it checks every byte.
   A file upload streams and checks only the 8 KiB sample, so a
   truncated sample cannot prove that the whole body is text; see the
   streaming constraint below.
5. **Fallback**: `application/octet-stream`.

If sniffing and an explicit type disagree on a well-known signature
(e.g. the client says `text/plain` but the bytes are a PNG), the
sniffed type wins and the conflict is recorded in object metadata as
`detection-conflict` (e.g. `text/plain -> image/png`).

**Buffering and streaming:** a text paste with no filename is
buffered up to the lower of `DAYFLY_MAX_TEXT_BYTES` and
`DAYFLY_MAX_PASTE_BYTES` before detection and upload.
The text cap bounds this buffer, so text detection is exact and an
oversize body is rejected before any S3 write. A file upload streams.
Sniffing peeks at only the first chunk (up to 8 KiB) and then
forwards those same bytes into the S3 upload stream. It does not
buffer the whole file to detect its type, and the peeked bytes are
neither lost nor duplicated. If the peek is a truncated sample, a
plain-text candidate becomes `application/octet-stream`, because the
sample cannot prove that the rest of the body is text. A specific
client type or filename extension still wins. If a known length is at
or below 8 KiB, the whole body was seen, so text detection still
applies.

Stored in S3 object metadata: the final `content-type`, the
detection source (`client` | `sniffed` | `extension` | `text` |
`fallback`), and the original filename if supplied. The base object also
records `created-by` when the transport has a client address.
`DAYFLY_EXPOSE_CREATOR_ADDR` controls whether a response returns it.
`fetch_metadata` and the upload JSON response return `content_type`,
`detection_source`, and `detection_conflict`.

### Serving behaviour and the HTML/SVG safety note

Download responses set `Content-Disposition: inline` for safe types
(`text/plain`, `text/csv`, `text/markdown`, images except SVG, and PDF
when `DAYFLY_INLINE_PDF` is true) and `attachment` for everything else
— **specifically including `text/html` and `image/svg+xml`, which are
always served as attachments, never rendered inline**. A text response
adds `charset=utf-8` when the stored type has no charset. A filename
with non-ASCII characters is sent with the RFC 5987 `filename*=UTF-8''`
form and an ASCII fallback in `filename`. Every download carries
`X-Content-Type-Options: nosniff`. An inline response also
carries `Content-Security-Policy: sandbox`, so a served document cannot
run script in the service's origin even if a type slips through
detection. The human view page shows text in `<pre>`, including the
escaped source of HTML, SVG, and XML, and safe images inline. It never
renders HTML/SVG as markup. The raw link drops markup types to
`text/plain` so the source opens as text. Every UI HTML response
carries `X-Content-Type-Options: nosniff` and the framing headers.

Why: for internal use this is fine, but serving a sniffed
`text/html` or `image/svg+xml` upload as inline HTML on a trusted
origin would let an uploaded file run script in that origin's
context. v1 forces attachment for those types and adds the sandbox
policy to every inline response. Set `DAYFLY_INLINE_PDF=false` on a
public instance so a PDF is an attachment too. If the frontend ever
moves to a separate, untrusted origin, revisit this; do not silently
flip HTML/SVG to inline.

## Human frontend

Server-rendered HTML with a small vanilla-JS drag-and-drop
enhancement and copy buttons for the body and the view link; no
JavaScript framework, no build step. Plain forms and links otherwise.
Internal use by default — same private-network / binary-auth model as
the API, no accounts or comments.

The footer shows `dayfly — internal use only · © 2026 suorcd` by
default. `DAYFLY_SITE_NAME` replaces the internal-use text with the site
name. `DAYFLY_CONTACT` adds a takedown contact and `DAYFLY_AUP_URL`
adds an acceptable-use link. The values are HTML-escaped. A public
instance sets the site name and leaves the default for a private
instance.

- `GET /` — combined new-paste form: paste text or drop and choose a
  file, with a TTL selector in whole days (1, 3, 7, 14, 30, 90 days;
  the API also accepts sub-day TTLs). Optional title and tags fields
  name and group the paste. One "Create paste" button posts
  multipart data to `POST /ui/create`; a selected file takes
  precedence over the text fields. Without JavaScript, the native file
  picker and textarea still work. The page shows the configured file
  size limit; with JavaScript, an oversized file is rejected before the
  upload starts, and the multipart handlers reject an obviously
  oversized `Content-Length` before reading the body. Under the form,
  the page shows curl examples for a text paste, a file paste, and a
  JSON response. The examples use the request scheme and host. When a
  token is configured, the examples add a bearer header placeholder. On
  success, with `DAYFLY_PASTE_TOKENS` off, the browser is redirected to
  the new paste: the word alias when `DAYFLY_SHOW_WORDS` is true (the
  default), otherwise the canonical ID. With `DAYFLY_PASTE_TOKENS` on,
  the browser gets a one-time result page. The page shows the paste
  token, the view URL, and a delete form. It states that the token is
  shown once and cannot be recovered. The response carries
  `Cache-Control: no-store`, and the token never appears in a URL or a
  log line.
- `GET /pastes` — current pastes, built from the bucket scan (no
  database): alias when enabled (with the canonical ID as secondary
  text when `DAYFLY_SHOW_INTERNAL_PATHS` is true), title, filename if
  any, content type, size, created date, and TTL in days
  remaining, and a delete action per row. Expired items are
  filtered out, consistent with the API's read-time expiry check. The
  Start and End inputs start with the default window:
  `DAYFLY_LIST_DEFAULT_DAYS` days, ending today in UTC (2 by default).
  Add `?date=YYYY-MM-DD` for another day. Add `?sort=size|created|expires`
  and `?order=asc|desc` to sort. The default is `created` descending.
  The Size and Created headers are links. A click toggles the
  direction and keeps the date filters. A missing created or expiry
  value sorts last in both directions. The Search and Tag inputs filter
  the returned rows with `q` and `tag`.
- `GET /view/<id>` — one paste: the title as the page heading when it
  exists, otherwise the alias, then Download, Raw, and Copy link
  actions, with a right-aligned Metadata & delete toggle on the same
  row. The toggle opens a full-width box with the title, tags,
  metadata, and the delete form. Then the content: text in `<pre>`
  (capped at 1 MiB for the page; the raw route and the API return the
  full paste) with a Copy button inside the box, including the escaped
  source of HTML and SVG, safe images inline, other types via the raw
  link. When `DAYFLY_VIEW_LINE_NUMBERS` is true, text up to 5,000 lines
  gets presentation-only line numbers that the Copy button does not
  include. The Copy and Copy link buttons appear only when JavaScript
  runs.
- `GET /raw/<id>` — the unformatted bytes. Markup types (`text/html`,
  `application/xhtml+xml`, `image/svg+xml`, and XML) and unknown
  binary types are served as `text/plain` and open inline. Other types
  keep the stored Content-Type and the API disposition rules.
- `GET /favicon.ico` — the file named by `DAYFLY_FAVICON_PATH`, or 404
  when unset. The file is read at startup and cached for one day.
- `GET /robots.txt` — the file named by `DAYFLY_ROBOTS_TXT_PATH`, or 404
  when unset. Served as `text/plain; charset=utf-8`.
- `GET /.well-known/security.txt` — the file named by
  `DAYFLY_SECURITY_TXT_PATH`, or 404 when unset. Served as
  `text/plain; charset=utf-8`.

Theme: the pages adapt the base look of the user's own site
stylesheet, https://suorcd.us/style.css (verified live as the site's
single stylesheet; `/main.css` and `/css/style.css` return 404, no
`@import`), palette `cyberpunkNeon` — the rendered homepage look:
background `#091833` (very dark blue), text `#e3e8fc`, accent
`#ea00d9` (neon pink, links/headings), code background `#133e7c`,
border `#0abdc6` (bright cyan). The file's other two palettes
(`green-dark`, `neon-dark01`) are **not** used. Layout mirrors the
site: monospace-first typography, line-height 1.6, 20px body padding,
1200px centred container (1600px on the paste list), a header with a
gradient
accent-to-transparent bottom border, nav links with a dimmed terminal
prompt prefix and a dashed cyan border + glow on hover, pink links
with underline + glow on hover and no visited styling, lowercase
accent headings (glow for hero-style headings, dotted cyan underlines
for section headings), translucent code-blue panels with dashed cyan
borders and rounded corners, dashed-cyan tables with accent headers,
a centred small footer with a dashed cyan top border, one mobile
breakpoint at 768px, and dark-only (no light-mode switch in v1).
Blog-specific rules (post lists, hero scanline animation, code tabs,
tag pills) are not imported. The whole stylesheet is **not** copied —
only the relevant base look is adapted. **Font flag:** the site's
font stack (JetBrainsMono Nerd Font / JetBrains Mono / Hack / Fira
Code for body, Source Code Pro / Hack for code) is referenced in the
CSS stack but the fonts are **not bundled** here, since licensing
for this use was not established; on machines without those fonts
the stack falls back to system monospace. No suorcd.us content is
copied.

### Write-route origin check

The browser write routes (`POST /ui/create`, the legacy
`POST /ui/text` and `POST /ui/upload`, and `POST /ui/delete`) require
an `Origin` header that matches the `Host` header. If `Origin` is
absent, the server uses `Referer`.

The server returns 403 when both headers are absent. It also returns
403 when a value is `null` or malformed, or when the host or port does
not match `Host`. A scripted client must send `Origin` or `Referer`.
A reverse proxy must preserve the public `Host` header.

The API write routes (`POST /upload`, `POST /v1/pastes`, and the append
routes) reject a request when an `Origin` or `Referer` header is present
and does not match `Host`. They accept a request with neither header,
so curl and other non-browser clients still work. A browser write
normally sends at least one of these headers, so a cross-site form
normally cannot create a paste or append bytes.

`GET` routes and `DELETE` do not use this check. A browser `DELETE`
needs a CORS preflight, which the server does not answer.

## Testing

```sh
cargo fmt
cargo clippy --all-targets
cargo test
cargo build --release
```

`cargo test` runs the unit tests in the modules and the integration suite in
`tests/`. The integration suite is hermetic. All S3-backed tests use the one
in-process fake S3 service in `tests/common/mod.rs`. It listens on an
ephemeral localhost port, and a real `Store` points at it with dummy
credentials. The suite needs no network and no Garage.

| File | Covers |
| --- | --- |
| `tests/common/mod.rs` | The single fake S3 service, request helpers, multipart bodies, temporary files |
| `tests/store_s3.rs` | `store_text`, `store_file_path`, `store_stream`, multipart abort paths, caps, `head_metadata`, `fetch_text`, `fetch_range_by_key`, `download_file`, `delete`, `list_recent`, error mapping, append and segment reads |
| `tests/http_routes.rs` | Raw and multipart uploads, bearer and write-only auth, download, `HEAD`, `Range`, metadata, delete, list, append, MCP shared store, failed uploads |
| `tests/web_handlers.rs` | Index, text submit, upload submit, list, view, delete, auth, error pages |
| `tests/cleanup_cli.rs` | `dayfly cleanup` dry-run and live passes against the fake S3 service, no lifecycle rule, per-item failure and exit code |
| `tests/mcp_stdio.rs` | The real binary over MCP stdio: all tools and their error paths |
| `tests/config_env.rs` | `Config::from_env` defaults, overrides, and error text |
| `tests/real_garage.rs` | Ignored tests for a disposable real Garage/S3 bucket |

The unit tests in the modules are retained. They cover: key generation, ID
validation (including traversal rejection), range parsing, offset/limit
windows, UTF-8 prefix truncation, metadata/expiry round-trip, content-type
detection (PNG, JPEG, GIF, WebP, PDF, ZIP, UTF-8 text, binary fallback, curl's
`x-www-form-urlencoded` default ignored, explicit type overriding a generic
one, sniffed-signature conflict, extension fallback), peeked-byte forwarding
(neither lost nor duplicated), streaming part buffering, chunked and
multi-part-sized uploads, disconnect/part-failure/oversize abort (no URL
material), empty uploads, HTML escaping, human sizes, and days-remaining math.

`.github/workflows/ci.yml` runs the fast job (`cargo fmt --check`,
clippy with warnings denied, `cargo test`, and `cargo build`) on every
pull request and on every push. The coverage job runs `cargo llvm-cov`
on pushes to `main` and on manual dispatch. The flake job (`nix flake
check` and `nix build -L`) and the container image job run on pushes
to `main`, on `v*` tags, on manual dispatch, and on a pull request with
the `ci:full` label. A documentation-only pull request skips CI. A push
to `main` builds and smoke-tests the image but publishes nothing. A
`v*` tag publishes the image, and attaches the release binary and the
image tar to the GitHub release. The image tags are `sha-<short>`, the
tag name, and `latest` for a stable tag. The `edge` tag is gone. The
flake job restores and saves a Nix store cache, so a main or tag build
normally reuses it.

### Nix build path (additional, not a replacement for Cargo)

The project is also flake-based. The flake uses nixpkgs'
`rustPlatform` (`buildRustPackage`, reading the repo's `Cargo.lock`)
rather than crane or naersk: it needs no third-party flake input to
track, and this is a single-binary crate with no workspace or
cross-compilation complexity that would justify crane's incremental
caching machinery. If build times become a problem, crane is the
first alternative to revisit, not a custom vendored toolchain.
nixpkgs tracks the `nixos-unstable` input in `flake.nix`, because
the AWS SDK crates in `Cargo.lock` require rustc 1.94.1 while
`nixos-25.11` ships 1.91.1, which fails before compilation. After
the first `nix flake update`, commit the generated `flake.lock` so
the exact nixpkgs commit is pinned too. The flake
provides `packages.default` (the binary), `devShells.default`
(cargo, rustc, rust-analyzer, clippy, rustfmt), and `apps.default`
(`nix run` starts the binary), usable on `x86_64-linux` and
`aarch64-linux`.

```sh
nix develop   # shell with cargo, rustc, rust-analyzer, clippy, rustfmt
nix build     # builds packages.default; ./result/bin/dayfly
nix run       # runs the binary (default: MCP over stdio)
# with env set, e.g.:
S3_ENDPOINT=... S3_BUCKET=... S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=... \
  nix run -- serve --bind 127.0.0.1:8080
nix flake check
```

### Cleaning local build outputs

Remove the local build outputs of the current flake with:

```sh
nix run .#clean
# or directly:
./scripts/clean.sh
```

The command removes the `result` and `result-*` symlinks, `target/`,
and `.direnv/`. It finds `flake.nix` in the current directory or a
parent directory. Use `--root PATH` to name another flake. Use
`--dry-run` to print the paths without removing them.

Run `./scripts/clean.sh --help` or `nix run .#clean -- --help` to show
all options.

Add `--rust-cache` to remove the Cargo registry cache, the Cargo git
cache, and the sccache cache:

```sh
nix run .#clean -- --rust-cache
./scripts/clean.sh --rust-cache
```

The command does not delete Nix store paths. It removes the result
symlinks, which are GC roots. Run `nix store gc` to delete the
unlinked build outputs.

The flake also builds the OCI image:

```sh
nix build .#container   # OCI image tar in ./result
```

The image runs `dayfly serve` on port 8080 as uid 65534. The container
workflow publishes only `x86_64-linux` images. GitHub runners build that
platform. A multi-platform image needs an emulated or native
`aarch64-linux` builder; this workflow does not add one. The flake still
lists `aarch64-linux` for local builds.

What a pass looks like, on a machine with Nix: `nix flake check`
exits 0 with no errors; `nix build` exits 0 and creates a
`./result` symlink pointing into the Nix store, and
`./result/bin/dayfly --help` prints the CLI usage
(subcommands `mcp`, `serve`, `config`, `lifecycle`, and `cleanup`);
`nix develop` drops into a shell
where `cargo --version`, `rustc --version`, and
`rust-analyzer --version` all succeed.

**Nix validation result:** `nix build` and the test suite were run on
`x86_64-linux` against `nixos-unstable`: 423 passed and 2 ignored at
commit `95af178`. On the stable `nixos-25.11` branch the build fails
before compilation with a rustc-too-old error naming 23 AWS crates,
which is why the flake tracks unstable.

Unit tests cover: key generation, ID validation (including
traversal and segment-key rejection), range parsing, offset/limit
windows, UTF-8 prefix truncation, metadata/expiry round-trip,
content-type detection (PNG, JPEG, GIF, WebP, PDF, ZIP, whole-payload
UTF-8 text for buffered pastes, truncated-sample fallback, binary
fallback, curl's `x-www-form-urlencoded` default ignored, explicit
type overriding a generic one, sniffed-signature conflict, extension
fallback), peeked-byte forwarding (neither lost nor duplicated),
streaming part buffering, chunked and multi-part-sized uploads,
disconnect/part-failure/oversize abort (no URL material), empty
uploads, the write-route origin guard, HTML escaping, human sizes,
days-remaining math, MCP HTTP bearer auth, two independent concurrent
MCP sessions, and the unchanged stdio initialize path.

The append tests in `tests/store_s3.rs` cover suffixed segment key
format, suffix shape, segment order, segment range mapping, total size,
listing and bare-ID skipping of segment keys, delete of segments,
append cost (only the new bytes), same-sequence appends that both
survive a read, and append errors before a write. The append tests in
`tests/http_routes.rs` cover the append endpoint and the HTTP MCP
append tool. All S3-backed tests use the single fake S3 harness in
`tests/common/mod.rs`. It implements Put, Get (with Range), Head, List
and Delete.

### Optional tests against a real Garage or S3-compatible endpoint

The default suite never contacts a network service. `tests/real_garage.rs`
holds ignored tests for a real endpoint. Use a disposable test bucket. The
tests create new random IDs and delete only the objects they create.

```sh
export DAYFLY_TEST_S3_ENDPOINT=https://garage.example.invalid:3900
export DAYFLY_TEST_S3_BUCKET=dayfly-test-disposable
export DAYFLY_TEST_S3_REGION=us-east-1
export DAYFLY_TEST_S3_ACCESS_KEY_ID=TEST-KEY
export DAYFLY_TEST_S3_SECRET_ACCESS_KEY=TEST-SECRET
cargo test --test real_garage -- --ignored --test-threads=1
```

`DAYFLY_TEST_S3_REGION` is optional and defaults to `us-east-1`.

Run these checks against a real Garage instance:

- Create a test bucket (or test prefix) separate from any
  production data.
- Store a text paste, fetch it back, fetch a byte range, fetch
  metadata, append text, fetch the joined text and a range across
  the base/segment boundary, confirm the total size and a null
  `sha256`, delete it, and confirm that the base and segments are
  gone.
- Run two concurrent appends to one ID. Confirm that two segment
  objects exist and that a read returns both chunks.
- Verify path-style requests work (Garage uses path-style URLs,
  not virtual-hosted style; the binary sets
  `force_path_style = true`).
- Verify that `If-None-Match` behaviour is **not** relied upon:
  the binary never sends a conditional write for create-only
  semantics; a new random ID is created per store.
- Verify day TTL: store with `ttl_days=1`, confirm the paste is
  readable before expiry and returns 410 / expired after the
  stored `expires-at` passes.
- Verify sub-day TTL: store with `X-TTL: 1h`, confirm the stored
  `expires-at` is one hour after `created-at` and that the paste
  returns 410 / expired after that time.
- Verify a buffered text paste: pipe 20 KiB of text with
  `cat text | curl --data-binary @- <base>/upload`, confirm
  `text/plain; charset=utf-8` and an inline download.
- Verify streaming file upload: pipe a file larger than 5 MiB with
  `cat file | curl -H 'X-Filename: large.bin' --data-binary @- <base>/upload`,
  confirm the returned URL streams back (Range download), and
  confirm that killing the client mid-upload yields no URL and
  leaves no completed object (an aborted multipart upload may
  leave an incomplete upload; the startup and hourly sweep abort
  it, and a lifecycle rule is a backstop — see below).
- Verify content-type tagging: upload a PNG without an explicit
  Content-Type (curl's default), confirm metadata shows
  `image/png` with source `sniffed`; upload text with
  `-H 'Content-Type: text/csv'`, confirm `text/csv` with source
  `client`; upload a PNG claiming `text/plain`, confirm
  `image/png` with a recorded conflict.
- Verify the human frontend: `GET /` creates a paste, `GET
  /pastes` lists it with expiry/days remaining, `GET /view/<id>`
  renders text / an image inline, and delete removes it.
- Apply `docs/lifecycle-example.json` manually if desired (it is
  **not** applied automatically), and confirm it only ever
  deletes objects past their TTL, plus aborts incomplete
  multipart uploads.

## Manual cleanup

`dayfly cleanup` runs one cleanup pass and exits. The pass does this
work in order:

1. It aborts incomplete multipart uploads under `pastes/` that are older
   than `DAYFLY_MULTIPART_ABORT_AFTER` seconds.
2. It lists the `pastes/` prefix and reads the `expires-at` value of each
   base object. It deletes each expired paste through the normal delete
   path. That path removes the base object, its segments, and a matching
   alias.
3. It lists the `words/` prefix. It deletes each alias object with a past
   `expires-at` value.

The command does not read or need a bucket lifecycle rule. The command
scans the bucket directly, so its cost is O(n) in stored objects.

```sh
dayfly cleanup --dry-run
dayfly cleanup
```

`--dry-run` prints the same item lines and counts, but changes nothing.
Live output prints one line per item and a final `cleanup:` line. The
final line holds these counts: `aborted`, `would_abort`,
`pastes_deleted`, `pastes_would_delete`, `aliases_deleted`,
`aliases_would_delete`, and `failed`. The command continues after a
failed item. It names the key or upload ID of the failed item. It exits
1 when any item failed.

**Concurrency risk.** The active-upload set is per process. A cleanup
process cannot see an upload that a server process still drives. A short
threshold can abort a live upload. Stop the service during cleanup, or
keep `DAYFLY_MULTIPART_ABORT_AFTER` above the longest upload time. A
concurrent writer can also add an object during a scan. The next pass
removes a skipped object.

## Lifecycle configuration

`docs/lifecycle-example.json` is an **example only**. It is not
applied automatically. Apply the file with:

```sh
dayfly lifecycle apply --file docs/lifecycle-example.json
```

Dayfly cleans orphan multipart uploads in code. It lists the `pastes/`
prefix at startup and each hour. It aborts an incomplete upload older
than `DAYFLY_MULTIPART_ABORT_AFTER` seconds. The lifecycle rule in the
example is a backstop, not a requirement. Keep it when a second process
writes to the bucket, or when you want a day-rounded second guard.

The sweep cannot see an upload that a second process drives. A short
threshold can abort a live upload from that process. Keep
`DAYFLY_MULTIPART_ABORT_AFTER` above the longest expected upload time
when two writers share the bucket.

The command reads the bucket configuration, replaces a rule with the
same `ID`, and keeps every other rule. It then writes the merged set.
`--dry-run` prints the plan without a write.

The call needs read and write permission on the bucket. Garage does not
require owner permission for a lifecycle write. Garage lifecycle support
depends on the Garage version. Verify `Expiration` and
`AbortIncompleteMultipartUpload` on the exact Garage build before
production use.

An `Expiration.Days` value at or below `DAYFLY_MAX_TTL_DAYS` is
rejected, because the sweeper could delete an object before its
read-time expiry. The example rules use 3651 days, one day above the
default maximum, so they apply without the override. Use
`--allow-short-expiration` only when you accept that risk. Without the
override, keep the rule longer than the longest TTL in use. Deletion is
day-rounded and asynchronous, and it can run after the TTL. Objects are
already unreadable through the API when `expires-at` passes, regardless
of when the sweeper runs.

The example has a rule for `pastes/` and a rule for `words/`. Keep
both when you write your own document. The `words/` rule lets the
sweeper remove expired alias objects. Lookup rejects an expired target
even when the alias object remains.

## Known limitations (v1)

These are known limitations of the current implementation. They are
recorded here so readers can plan around them.

- **A streaming multipart upload has no stored `sha256`.** S3 takes
  object metadata only at `CreateMultipartUpload`, before the hash is
  known. `HeadObject` and the `fetch_metadata` MCP tool report no hash
  for these objects. The store response still includes it, and the
  size still comes from `Content-Length`.
- **Text detection inspects the first 8 KiB only.** A file that is
  ASCII at the start and binary later is stored as `text/plain` and
  served inline.
- **Some zip-based documents still report as `application/zip`.** The
  mapping covers the common Office, OpenDocument, and EPUB extensions
  only. Other zip-based formats keep the plain ZIP type.
- **An append removes the total-file `sha256`.** The stored hash covers
  the base object only, so `fetch_metadata` returns `sha256: null`
  after the first append. See "Append and segmented storage".
- **Concurrent appends to one paste are lossless but unordered.** Two
  appends can choose the same sequence. Both survive, because the
  segment names have random suffixes. A read orders one sequence by
  suffix. See "Append and segmented storage".
- **A word alias is guessable at the default count.** The default three
  words from the 7,776-word list give about 38.8 bits. Use
  `DAYFLY_ALIAS_WORDS=4` or more when aliases are public. The alias is a
  second name, not a bearer token. Use the bearer token and the private
  network.
- **Search is line-oriented and has no index.** A match cannot span two
  lines. One call reads at most `DAYFLY_SEARCH_MAX_BYTES`; a larger paste
  needs `next_offset` pages. A long line is cut at the 4 KiB preview
  limit, and its match range can be omitted.
- **A title and tags use S3 user metadata.** Keep them small: 200 bytes
  for the title and 512 bytes for tags. AWS-compatible stores cap user
  metadata at 2 KB per object; verify the limit on your Garage build.
- **A TTL change on a paste above 5 GB is rejected.** `PATCH` rewrites
  metadata with one `CopyObject`, and S3 caps that call at 5 GB. The
  error code is `copy_limit` and no object is rewritten. Read the paste
  and store it again to change its expiry.
- **The creator address is metadata, not ownership.** The base object
  stores `created-by` when the transport has a client address. The
  address is hidden unless `DAYFLY_EXPOSE_CREATOR_ADDR` is on. It is not
  an account, an audit trail, or a query key.
- **The HTTP endpoint is intended for a private network.** The
  bearer token comparison is constant time for equal-length tokens.
  A length difference still leaks through timing. The write-route origin
  check trusts the `Host` header. Run this on a private network rather
  than exposing it.

The MCP transport over HTTP adds these limitations:

- **MCP sessions are held in memory.** A restart ends every session.
  The client must initialize again. The process accepts several
  sessions at the same time.
- **The MCP endpoint accepts any `Host` header.** This matches the
  REST routes, and a LAN name must work in a private-network
  deployment. Use the bearer token or a private network as the guard.

## Future work (not in v1)

- A database (SQLite or similar) for listing/search/sorting,
  view counters, burn-after-read, per-paste ownership, audit
  history, and deduplication — triggered by listing slowdown, not
  added speculatively.
- Expansion to S3 stores other than Garage, with real testing
  against each.
- Any frontend theme refinement against suorcd.us's actual
  stylesheet (see Theme flag above).
