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

There is **no database in v1, and no SQLite placeholder**. Listing and
info come from scanning the bucket directly. Listing uses ListObjectsV2
for each UTC date in the window, then HeadObject per key where metadata
is needed. See "No database in v1" below.

> **Copyright holder is still open.** The LICENSE file says
> `Copyright (c) 2026 suorcd` and `Cargo.toml` points at
> `https://github.com/suorcd/dayfly`. The repository URL is now real.
> The copyright line uses the GitHub handle rather than a full legal
> name, which is a deliberate placeholder pending the holder's
> decision. Do not add a legal name without the owner's say.

## What it is

- One base S3 object per paste/file. Key layout:
  `pastes/<yyyy-mm-dd>/<random-id>`, where the random ID has at least
  128 bits of entropy (base62). A user-supplied filename is never put
  in the key. An append adds one segment object under
  `pastes/<yyyy-mm-dd>/<random-id>/<sequence>-<suffix>`; see "Append and
  segmented storage". An alias adds one small object at
  `words/<phrase>`; see "Three-word alias".
- Each new paste also gets a three-word alias, for example
  `abacus-abdomen-zoology`. The alias is a second name for the same
  paste, not a second paste.
- Filename, content-type, created date, and TTL are stored in object
  metadata. The S3 `Content-Length` gives the size. A `sha256` value is
  stored when the hash is known before the object is created. A
  multipart stream cannot know the hash in advance, so it omits the
  value. The TTL is stored as exact seconds and as whole days when the
  duration has no smaller part. No separate metadata sidecar in v1.
- Expiry uses a read-time TTL. The TTL accepts seconds or a duration
  such as `90m` or `1h30m`. A bare integer is always seconds: `7` is
  seven seconds, and `7d` is seven days. `ttl_days` remains as a
  positive whole-day form. The default is 7 days, configurable via
  `DAYFLY_DEFAULT_TTL_DAYS`. The API and MCP layer compare stored
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
and the human pages. The token must not contain ASCII whitespace;
startup fails if it does. Note the limitation this creates: a plain
browser form cannot send an `Authorization` header, so for browser use
of the frontend, run on a private network / localhost without the token
set, and use the token for API clients. Do not put the token in a URL
query
parameter — it would leak into logs and browser history.

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
| Fetch text (offset/limit) | `fetch_text` | `GET /v1/pastes/<id-or-alias>` (raw; use `Range` for slices) |
| Fetch file (streaming, Range) | `fetch_file` (downloads to a local path) | `GET /v1/pastes/<id-or-alias>` with `Range` → 206 |
| Fetch metadata (HeadObject equivalent) | `fetch_metadata` | `GET /v1/pastes/<id-or-alias>/meta`, or `HEAD /v1/pastes/<id-or-alias>` |
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

Every operation that takes an ID also accepts a three-word alias, such
as `abacus-abdomen-zoology`. The JSON and MCP store responses include
both forms; the plain-text HTTP response returns only the URL. See
"Three-word alias".

### Explicitly OUT of v1

Listing/search/sorting beyond a date window, view counters,
burn-after-read, per-paste permissions/ownership, audit history, and
deduplication. These are the future SQLite triggers: they are the
reasons a database would be added later. No database schema is
invented in v1.

### No database in v1 — the honest tradeoff

Listing is O(n) in objects scanned, plus one HeadObject and one
segment-prefix listing per paste (needed to recover expiry,
content-type, filename, and the total size, which ListObjectsV2 does
not return, and to filter expired objects out).
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
are sorted by date ascending, then by key. The response has no
truncation field in v1; use a narrower date range to see older objects.

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
path in the file resolves against the directory of the file. An
exported relative secret-file path resolves against the working
directory. Never commit a real `.env`; copy `.env.example` to a path
you choose and pass it with `--config`. The `.env` name is in
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
present. It never prints a secret value. `--strict` turns credential
file permission warnings into faults.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `S3_ENDPOINT` | yes | — | Garage endpoint URL, `http://` or `https://` |
| `S3_BUCKET` | yes | — | Bucket name (private) |
| `S3_ACCESS_KEY_ID` | one form | — | Garage access key, held only by this binary; set this variable or `S3_ACCESS_KEY_ID_FILE` |
| `S3_ACCESS_KEY_ID_FILE` | one form | — | Path to a file that contains the Garage access key; set this variable or `S3_ACCESS_KEY_ID` |
| `S3_SECRET_ACCESS_KEY` | one form | — | Garage secret key, held only by this binary; set this variable or `S3_SECRET_ACCESS_KEY_FILE` |
| `S3_SECRET_ACCESS_KEY_FILE` | one form | — | Path to a file that contains the Garage secret key; set this variable or `S3_SECRET_ACCESS_KEY` |
| `S3_REGION` | no | `us-east-1` | Signing placeholder for Garage; supply your own, do not rely on the default being "yours" |
| `DAYFLY_MAX_TEXT_BYTES` | no | 104857600 (100 MiB) | Max text paste size for MCP `store_text`, the human text form, and `POST /upload` without a filename; oversize → 413. A filename (including a multipart filename) uses `DAYFLY_MAX_FILE_BYTES` instead |
| `DAYFLY_MAX_FILE_BYTES` | no | 1073741824 (1 GiB) | Max file paste size; oversize → 413 with a machine-readable error. The maximum is 52428800000 bytes: 10,000 parts of 5 MiB |
| `DAYFLY_DEFAULT_TTL_DAYS` | no | 7 | Default TTL in positive whole days when a request does not set a TTL. Must be at least 1. The name and meaning are unchanged |
| `DAYFLY_HTTP_BIND` | no | `127.0.0.1:8080` | HTTP bind address |
| `DAYFLY_HTTP_BEARER_TOKEN` | one form | (unset) | Optional bearer token for the binary's own HTTP endpoint (see Access model); set this variable or `DAYFLY_HTTP_BEARER_TOKEN_FILE` |
| `DAYFLY_HTTP_BEARER_TOKEN_FILE` | one form | (unset) | Path to a file that contains the bearer token; set this variable or `DAYFLY_HTTP_BEARER_TOKEN` |
| `DAYFLY_MCP_LOCAL_ROOT` | no | (unset) | Directory for the MCP local-path tools. It must exist, or startup fails. When set, `store_file` and `fetch_file` refuse any path outside it, after resolving symlinks. When unset, the stdio transport accepts any readable path and the HTTP transport refuses both tools |

For each S3 credential and for the bearer token, set the direct
variable or its `_FILE` alternative, not both. If both are set, startup
stops with a configuration error. The binary trims whitespace from file
values and rejects an empty file.

Credentials are never logged. `Config` has no `Debug` that prints
secrets; startup logs only bucket, endpoint scheme, region, and limits.
A credential file read error names the path only, not the file
contents. The permission warnings never include the file contents.

`--verbose` sets the log filter to INFO. `RUST_LOG` overrides it.
Dayfly runs in the foreground. Use systemd or the container runtime to
supervise it. There is no daemon mode.

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

Example MCP client config (placeholder binary path — replace with the
real path on the machine where the binary is installed; no real paths
to the user's machines are in this repo). The config contains paths
only, not key material:

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

MCP tools: `store_text`, `store_file`, `append_text`, `fetch_text`,
`fetch_file` (downloads to a local path and returns metadata — binary
content is never sent over JSON-RPC), `fetch_metadata`, `delete_paste`,
`list_recent`.

`store_file` and `fetch_file` are subject to the transport rule in the
Access model section: the HTTP transport refuses them until
`DAYFLY_MCP_LOCAL_ROOT` sets a directory.

## Running HTTP mode

```sh
./target/release/dayfly serve
# or with an explicit bind address:
./target/release/dayfly serve --bind 127.0.0.1:8080
```

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
in positive whole days when no TTL is given; its name and meaning are
unchanged.

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
object. The text cap applies to this append, not to the total size.
An expired or missing ID returns 410 or 404 and writes nothing. An
empty body writes nothing. Use the bare ID form
`/v1/pastes/<random>/append` if you do not have the date. A three-word
alias works in place of the ID.

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

A request with no filename is a text paste and is capped at
`DAYFLY_MAX_TEXT_BYTES`; supply `X-Filename` (or a multipart filename)
for larger files. The text/file split is a client-declared convention,
not a property of the bytes: adding a filename opts into the file cap.
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
An empty upload becomes a single empty PutObject.

The local-file path (`store_file`) reads one open handle for the size,
the hash, and the uploaded bytes, so the recorded metadata describes the
bytes that were uploaded. If a large file changes while it is uploaded,
the store deletes the object and returns an error, rather than keeping a
paste whose hash and content disagree. A file that grows past
`DAYFLY_MAX_FILE_BYTES` after the first size check is rejected, not
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

# Fetch by three-word alias (the same route):
curl http://127.0.0.1:8080/v1/pastes/abacus-abdomen-zoology

# Fetch a byte range (206, streaming, never the whole object):
curl -H 'Range: bytes=0-999' http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...
curl -H 'Range: bytes=-1024' http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Metadata:
curl http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123.../meta
curl -I http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Delete (204 on success, and also 204 when the object does not exist):
curl -X DELETE http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# List recent (expired filtered out). No dates means yesterday and today UTC:
curl 'http://127.0.0.1:8080/v1/pastes?limit=100'

# List one UTC date:
curl 'http://127.0.0.1:8080/v1/pastes?date=2026-10-28&limit=100'

# List an inclusive range across a date boundary:
curl 'http://127.0.0.1:8080/v1/pastes?start=2026-10-28&end=2026-10-29&limit=100'
```

`date` takes precedence over `start` and `end`. Supply both `start` and
`end` for a range; a one-sided range returns 400. Results are in date
order, then in S3 key order within each date. The limit applies to the
newest dates first, then to key order within a date. Key order is not
creation order because IDs are random. An older date can be omitted.

Error responses are machine-readable JSON, e.g.
`{"error": "payload too large: limit is 1073741824 bytes"}` with HTTP
413 for oversize, 400 for invalid input, 404 for not found, 410 for
expired, 416 for an unsatisfiable range, and 502 for S3 errors. An
upload body shorter than its `Content-Length` returns 400 and stores
nothing.

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
  401 and `{"error":"unauthorized"}`.
- When the token is unset, the endpoint stays open. In that case,
  serve it on a private network or localhost only.

Each `initialize` request creates an in-memory MCP session. The
process accepts several sessions at the same time. Sessions do not
survive a restart; the client then initializes again.

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
  copy or rewrite the base object.
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
  prefix, and the alias object.
- An append keeps the `expires-at` value of the base object. It does
  not extend the TTL.
- An append to an expired or missing ID returns an error and writes no
  object.
- A stored `sha256` covers the base object only. When segments exist,
  the API returns `sha256: null`. No stored value covers the joined
  paste.
- `list_recent` reports the total size and does not show a segment as
  a paste.

An earlier version wrote `pastes/<date>/<id>/<sequence>` with no
suffix. No old segment data exists. This version does not read the
unsuffixed name.

Limits:

- **Concurrent appends are lossless but unordered.** Two appends to the
  same ID can choose the same sequence. Both writes survive, because
  the segment names have random suffixes. A read returns both chunks.
  The order of the chunks for one sequence follows the suffix, so the
  order is arbitrary but deterministic. The store does not use a
  conditional write, because Garage accepts `If-None-Match` and ignores
  it. A delete that races an append can leave an orphan segment; a
  later delete of the full ID removes it.
- **A segment key is not a paste ID.** The API rejects an ID with three
  components after `pastes/`.
- **An empty append is a no-op.** It writes no object and keeps the
  base `sha256`.
- **Segments count toward scan limits.** A listing scans one page of up
  to 1,000 keys. Bare-ID resolution scans up to 10 pages. Segment keys
  consume that budget, so keep the full ID for a paste with many
  segments.

## Three-word alias

Every new paste gets a three-word alias, for example
`abacus-abdomen-zoology`. The alias is a second name for the paste,
not a second object. Use the canonical ID or the alias in every
operation that takes an ID: fetch, metadata, append, and delete.

- The words come from the bundled EFF large wordlist in
  `src/wordlist.txt` (7,776 words). The source and the CC BY 3.0
  attribution are in `src/wordlist.LICENSE`. No external service is
  called. Four wordlist entries contain a hyphen; the parser matches
  each one as a single word.
- An alias is three words joined by hyphens. The store returns the
  hyphen form. Input is case-insensitive; spaces and hyphens are
  accepted as separators. A phrase with the wrong word count or an
  unknown word is rejected.
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
  its alias.
- Delete: delete by ID or by alias. Either form removes the base
  object, every segment, and the alias object when the alias still
  points to that paste. A phrase that another store has rebound is left
  in place.
- Legacy objects have no alias. They keep working by ID, and `words`
  is absent or null.
- Entropy: three words give about 38.8 bits. The uniqueness check
  prevents collisions, not guessing. Treat the alias as a name, not a
  secret. The bearer token and the private bucket remain the access
  control.
- The alias object is small. It has the same expiry as the paste.
  A lifecycle rule for `pastes/` does not cover `words/`. Add a
  `words/` rule if you want the storage sweeper to remove expired
  alias objects. Lookup still rejects an expired target.

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
buffered up to `DAYFLY_MAX_TEXT_BYTES` before detection and upload.
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
`fallback`), and the original filename if supplied. `fetch_metadata`
and the upload JSON response return `content_type`,
`detection_source`, and `detection_conflict`.

### Serving behaviour and the HTML/SVG safety note

Download responses set `Content-Disposition: inline` for safe types
(`text/plain`, `text/csv`, `text/markdown`, images except SVG, PDF)
and `attachment` for everything else — **specifically including
`text/html` and `image/svg+xml`, which are always served as
attachments, never rendered inline**, plus
`X-Content-Type-Options: nosniff`. The human view page follows the
same rule: text in `<pre>`, safe images inline, HTML/SVG as a
download link only.

Why: for internal use this is fine, but serving a sniffed
`text/html` or `image/svg+xml` upload as inline HTML on a trusted
origin would let an uploaded file run script in that origin's
context. v1 deliberately does **not** build a sandbox for this —
instead it documents the choice and forces attachment for those
types. If the frontend ever moves to a separate, untrusted origin,
revisit this; do not silently flip HTML/SVG to inline.

## Human frontend

Server-rendered HTML, no JavaScript framework, no build step, plain
forms and links. Internal use only — same private-network /
binary-auth model as the API, no public sharing, accounts, or
comments.

- `GET /` — new paste/upload: paste text directly, or upload a file,
  with a TTL selector in whole days (1, 3, 7, 14, 30, 90 days; the API
  also accepts sub-day TTLs). On success it shows the resulting URL as
  a clickable link, plus the paste/file ID.
- `GET /pastes` — current pastes, built from the bucket scan (no
  database): link/ID, filename if any, content type, size, created
  date, expiry date plus days remaining, and a delete action per
  row. Expired items are filtered out, consistent with the API's
  read-time expiry check. Add `?date=YYYY-MM-DD` for another day.
- `GET /view/<id>` — one paste: text in `<pre>` (capped at 1 MiB for
  the page; the API remains the route for full/sliced reads), safe
  images inline, other types as a download link, plus a delete form.

Theme: the pages adapt the base look of the user's own site
stylesheet, https://suorcd.us/style.css (verified live as the site's
single stylesheet; `/main.css` and `/css/style.css` return 404, no
`@import`), palette `cyberpunkNeon` — the rendered homepage look:
background `#091833` (very dark blue), text `#e3e8fc`, accent
`#ea00d9` (neon pink, links/headings), code background `#133e7c`,
border `#0abdc6` (bright cyan). The file's other two palettes
(`green-dark`, `neon-dark01`) are **not** used. Layout mirrors the
site: monospace-first typography, line-height 1.6, 20px body padding,
1200px centred container, a header with a gradient
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

The three browser write routes (`POST /ui/text`, `POST /ui/upload`,
and `POST /ui/delete`) require an `Origin` header that matches the
`Host` header. If `Origin` is absent, the server uses `Referer`.

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
| `tests/http_routes.rs` | Raw and multipart uploads, bearer auth, download, `HEAD`, `Range`, metadata, delete, list, append, MCP shared store, failed uploads |
| `tests/web_handlers.rs` | Index, text submit, upload submit, list, view, delete, auth, error pages |
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

`.github/workflows/ci.yml` runs `cargo fmt --check`, clippy with warnings
denied, `cargo test`, and `cargo build` on every push to `main` and on every
pull request. A coverage job runs `cargo llvm-cov` on pushes to `main` and on
manual dispatch. A separate flake job runs `nix flake check` and
`nix build -L`.

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

The flake also builds the OCI image:

```sh
nix build .#container   # OCI image tar in ./result
```

The image runs `dayfly serve` on port 8080 as uid 65534. The container
workflow publishes only `x86_64-linux` images. GitHub runners build that
platform, and this pass does not add an emulated or native `aarch64-linux`
builder. The flake still lists `aarch64-linux` for local builds.

What a pass looks like, on a machine with Nix: `nix flake check`
exits 0 with no errors; `nix build` exits 0 and creates a
`./result` symlink pointing into the Nix store, and
`./result/bin/dayfly --help` prints the CLI usage
(subcommands `mcp` and `serve`); `nix develop` drops into a shell
where `cargo --version`, `rustc --version`, and
`rust-analyzer --version` all succeed.

**Nix validation result:** `nix build` and the test suite were run on
`x86_64-linux` against `nixos-unstable`: 354 passed and 2 ignored. On the stable
`nixos-25.11` branch the build fails before compilation with a
rustc-too-old error naming 23 AWS crates, which is why the flake
tracks unstable. Before this was built on a machine with Nix, an
earlier version of this note said the flake was unvalidated. That is
no longer true and is corrected here rather than left standing.

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

Use the same checklist for a real Garage instance:

- [ ] Create a test bucket (or test prefix) separate from any
      production data.
- [ ] Store a text paste, fetch it back, fetch a byte range, fetch
      metadata, append text, fetch the joined text and a range across
      the base/segment boundary, confirm the total size and a null
      `sha256`, delete it, and confirm that the base and segments are
      gone.
- [ ] Run two concurrent appends to one ID. Confirm that two segment
      objects exist and that a read returns both chunks.
- [ ] Verify path-style requests work (Garage uses path-style URLs,
      not virtual-hosted style; the binary sets
      `force_path_style = true`).
- [ ] Verify that `If-None-Match` behaviour is **not** relied upon:
      the binary never sends a conditional write for create-only
      semantics; a new random ID is created per store.
- [ ] Verify day TTL: store with `ttl_days=1`, confirm the paste is
      readable before expiry and returns 410 / expired after the
      stored `expires-at` passes.
- [ ] Verify sub-day TTL: store with `X-TTL: 1h`, confirm the stored
      `expires-at` is one hour after `created-at` and that the paste
      returns 410 / expired after that time.
- [ ] Verify a buffered text paste: pipe 20 KiB of text with
      `cat text | curl --data-binary @- <base>/upload`, confirm
      `text/plain; charset=utf-8` and an inline download.
- [ ] Verify streaming file upload: pipe a file larger than 5 MiB with
      `cat file | curl -H 'X-Filename: large.bin' --data-binary @- <base>/upload`,
      confirm the returned URL streams back (Range download), and
      confirm that killing the client mid-upload yields no URL and
      leaves no completed object (an aborted multipart upload may
      leave an incomplete upload for the lifecycle sweeper — see
      below).
- [ ] Verify content-type tagging: upload a PNG without an explicit
      Content-Type (curl's default), confirm metadata shows
      `image/png` with source `sniffed`; upload text with
      `-H 'Content-Type: text/csv'`, confirm `text/csv` with source
      `client`; upload a PNG claiming `text/plain`, confirm
      `image/png` with a recorded conflict.
- [ ] Verify the human frontend: `GET /` creates a paste, `GET
      /pastes` lists it with expiry/days remaining, `GET /view/<id>`
      renders text / an image inline, and delete removes it.
- [ ] Apply `docs/lifecycle-example.json` manually if desired (it is
      **not** applied automatically), and confirm it only ever
      deletes objects past their TTL, plus aborts incomplete
      multipart uploads.

## Lifecycle example

`docs/lifecycle-example.json` is an **example only**. It is not
applied automatically. Apply the file manually to a test bucket first.
Garage lifecycle support is partial and version-dependent. Verify
`Expiration` and `AbortIncompleteMultipartUpload` on the exact Garage
build before production use.

The rule expires objects after 90 days. 90 days is the longest TTL
the human frontend offers. API clients can set a longer TTL, so do not
set the rule below the longest TTL you use. Otherwise, the sweeper can
delete an object that the API still serves. Deletion is day-rounded
and asynchronous, and it can run after the TTL. Objects are already
unreadable through the API when `expires-at` passes, regardless of
when the sweeper runs.

The example rule covers `pastes/` only. An alias object lives under
`words/`. Add a second rule with the `words/` prefix to sweep expired
alias objects. Lookup rejects an expired target even when the alias
object remains.

## Known limitations (v1)

These were found by running the binary against a Garage 1.3.1 bucket,
not by reading the code. They are recorded here so a reader does not
have to discover them, and so the claims elsewhere in this file stay
honest.

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
- **A three-word alias is guessable.** Three words from the 7,776-word
  list give about 38.8 bits. The alias is a second name, not a bearer
  token. Use the bearer token and the private network.
- **The HTTP endpoint is internal-only in the weakest sense.** The
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
