# dayfly

Internal pastebin / file-share for LLM agents (and one human), backed by
a private Garage bucket. One small Rust binary, two transports sharing
one S3 core:

- **MCP server over stdio** (default, no subcommand) — the primary
  interface for agents.
- **HTTP mode** (`serve`) — a plain-curl API in the style of 0x0.st,
  plus a small server-rendered human frontend (`/`, `/pastes`,
  `/view/<id>`).

There is **no database in v1, and no SQLite placeholder**. Listing and
info come from scanning the bucket directly: ListObjectsV2 (paginated,
1,000 keys per page) for a date prefix, plus HeadObject per key where
metadata is needed. See "No database in v1" below.

> **Copyright holder is still open.** The LICENSE file says
> `Copyright (c) 2026 suorcd` and `Cargo.toml` points at
> `https://github.com/suorcd/dayfly`. The repository URL is now real.
> The copyright line uses the GitHub handle rather than a full legal
> name, which is a deliberate placeholder pending the holder's
> decision. Do not add a legal name without the owner's say.

## What it is

- One S3 object per paste/file. Key layout:
  `pastes/<yyyy-mm-dd>/<random-id>`, where the random ID has at least
  128 bits of entropy (base62). A user-supplied filename is never put
  in the key.
- Filename, content-type, size, sha256, created date, and TTL in days
  are stored in object metadata/headers. No separate metadata sidecar
  in v1.
- Expiry is **day-granular only**. `ttl_days` on store (default 7,
  configurable via `DAYFLY_DEFAULT_TTL_DAYS`). Expiry is enforced on
  read in the API/MCP layer by comparing stored `expires-at` metadata
  with now. S3 lifecycle deletion is a separate, asynchronous sweeper
  (see `docs/lifecycle-example.json`): it is day-rounded and may run
  after the TTL day. **Exact-day deletion is not promised.**
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
The binary alone holds the Garage access key and secret, from the
environment only. Agents/clients never get S3 credentials and never
talk to S3 directly; they only talk to the binary's MCP/HTTP interface.

Bucket privacy is **not** the same as securing the binary's own HTTP
endpoint. The intended deployment is **private-network / localhost**.
As a simple, optional extra for the binary's own endpoint (not a
replacement for the S3 credentials), set
`DAYFLY_HTTP_BEARER_TOKEN`: HTTP mode then requires
`Authorization: Bearer <token>` on every route, including the human
pages. Note the limitation this creates: a plain browser form cannot
send an `Authorization` header, so for browser use of the frontend,
run on a private network / localhost without the token set, and use
the token for API clients. Do not put the token in a URL query
parameter — it would leak into logs and browser history.

The MCP stdio transport does not use the bearer token: spawning the
binary locally is the trust boundary there.

## v1 scope

Operations, in both MCP tools and HTTP endpoints:

| Operation | MCP tool | HTTP |
| --- | --- | --- |
| Store text | `store_text` | `POST /upload` or `POST /v1/pastes` (raw body, no filename) |
| Store file (multipart for large files) | `store_file` (local path) | `POST /upload` (raw body with `X-Filename`, or `-F file=@...`) |
| Fetch text (offset/limit) | `fetch_text` | `GET /v1/pastes/<id>` (raw; use `Range` for slices) |
| Fetch file (streaming, Range) | `fetch_file` (downloads to a local path) | `GET /v1/pastes/<id>` with `Range` → 206 |
| Fetch metadata (HeadObject equivalent) | `fetch_metadata` | `GET /v1/pastes/<id>/meta`, or `HEAD /v1/pastes/<id>` |
| Delete | `delete_paste` | `DELETE /v1/pastes/<id>` |
| List recent (date prefix only) | `list_recent` | `GET /v1/pastes?date=YYYY-MM-DD&limit=N` |

IDs may be full (`2026-10-28/abc123…`) or bare random IDs. Bare IDs
are resolved by scanning the `pastes/` prefix (up to 10 pages of
1,000 keys) — clients should keep the full ID returned at store time.

### Explicitly OUT of v1

Listing/search/sorting beyond a date prefix, view counters,
burn-after-read, per-paste permissions/ownership, audit history, and
deduplication. These are the future SQLite triggers: they are the
reasons a database would be added later. No database schema is
invented in v1.

### No database in v1 — the honest tradeoff

Listing is O(n) in objects scanned, plus one HeadObject per listed
key (needed to recover expiry, content-type, and filename, which
ListObjectsV2 does not return, and to filter expired objects out).
For a small internal bucket that is fine. As the bucket grows,
listing gets slower — that slowdown, not a guessed threshold, is
exactly the trigger for adding a database later. The date prefix
(`pastes/<yyyy-mm-dd>/`) is the optimisation available in v1: recent
listings scan one day, not the whole bucket.

## Configuration

All configuration is environment variables. Never commit a real
`.env`; copy `.env.example` to `.env` (or export directly) with
placeholder values replaced by your own Garage values.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `S3_ENDPOINT` | yes | — | Garage endpoint URL, `http://` or `https://` |
| `S3_BUCKET` | yes | — | Bucket name (private) |
| `S3_ACCESS_KEY_ID` | yes | — | Garage access key, held only by this binary |
| `S3_SECRET_ACCESS_KEY` | yes | — | Garage secret key, held only by this binary |
| `S3_REGION` | no | `us-east-1` | Signing placeholder for Garage; supply your own, do not rely on the default being "yours" |
| `DAYFLY_MAX_TEXT_BYTES` | no | 10485760 (10 MiB) | Max text paste size for MCP `store_text`, the human text form, and `POST /upload` without a filename; oversize → 413. A filename (including a multipart filename) uses `DAYFLY_MAX_FILE_BYTES` instead |
| `DAYFLY_MAX_FILE_BYTES` | no | 1073741824 (1 GiB) | Max file paste size; oversize → 413 with a machine-readable error |
| `DAYFLY_DEFAULT_TTL_DAYS` | no | 7 | Default TTL in whole days |
| `DAYFLY_HTTP_BIND` | no | `127.0.0.1:8080` | HTTP bind address |
| `DAYFLY_HTTP_BEARER_TOKEN` | no | (unset) | Optional bearer token for the binary's own HTTP endpoint (see Access model) |

Credentials are never logged. `Config` has no `Debug` that prints
secrets; startup logs only bucket, endpoint scheme, region, and limits.

## Running the MCP server

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
to the user's machines are in this repo):

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
        "S3_ACCESS_KEY_ID": "YOUR-GARAGE-ACCESS-KEY",
        "S3_SECRET_ACCESS_KEY": "YOUR-GARAGE-SECRET-KEY"
      }
    }
  }
}
```

Logging goes to stderr only, never stdout, so the JSON-RPC stream is
not corrupted.

MCP tools: `store_text`, `store_file`, `fetch_text`, `fetch_file`
(downloads to a local path and returns metadata — binary content is
never sent over JSON-RPC), `fetch_metadata`, `delete_paste`,
`list_recent`.

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
#    "expires_at": "...", "content_type": "...", "filename": ...,
#    "detection_source": "...", "detection_conflict": null}
```

`POST /v1/pastes` is an alias for `POST /upload`.

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

Note on multipart forms: text fields (`filename`, `ttl_days`) must
come before the `file` field if you want them honoured; fields after
the file field are not read. Use headers (`X-Filename`,
`X-TTL-Days`) or query parameters (`?filename=…&ttl_days=…`) if in
doubt.

### Fetch, range, metadata, delete, list

```sh
# Fetch text (plain curl GET returns the paste):
curl http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Fetch a byte range (206, streaming, never the whole object):
curl -H 'Range: bytes=0-999' http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...
curl -H 'Range: bytes=-1024' http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Metadata:
curl http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123.../meta
curl -I http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# Delete (204 on success, and also 204 when the object does not exist):
curl -X DELETE http://127.0.0.1:8080/v1/pastes/2026-10-28/AbC123...

# List recent (date prefix only, expired filtered out):
curl 'http://127.0.0.1:8080/v1/pastes?date=2026-10-28&limit=100'
```

Error responses are machine-readable JSON, e.g.
`{"error": "payload too large: limit is 1073741824 bytes"}` with HTTP
413 for oversize, 400 for invalid input, 404 for not found, 410 for
expired, 416 for an unsatisfiable range, and 502 for S3 errors.

One status code needs reading carefully, because the current
behaviour is narrower than the list above suggests. `DELETE` is
idempotent, so deleting a full paste ID that does not exist returns
204 rather than 404; a 404 comes back only when a bare random ID
cannot be resolved against the bucket at all.

A wrong `S3_BUCKET` name is not one of those cases. `NoSuchBucket`
carries its own error and maps to 502, so a misconfigured bucket says
so rather than reporting every paste as missing.

## Content-Type detection (streamed uploads)

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
   containers, and other formats with signatures.
3. **Filename extension** (`mime_guess`), when an `X-Filename` /
   multipart filename is supplied.
4. **Plain-text detection**: valid UTF-8 with no NUL bytes in the
   sniffed prefix → `text/plain; charset=utf-8`. This inspects the
   first 8 KiB only, so a file that is ASCII at the start and binary
   later is classified as text and served inline.
5. **Fallback**: `application/octet-stream`.

If sniffing and an explicit type disagree on a well-known signature
(e.g. the client says `text/plain` but the bytes are a PNG), the
sniffed type wins and the conflict is recorded in object metadata as
`detection-conflict` (e.g. `text/plain -> image/png`).

**Streaming constraint:** sniffing peeks at only the first chunk (up
to 8 KiB) and then forwards those same bytes into the S3 upload
stream. It does not buffer the whole file to detect its type, and the
peeked bytes are neither lost nor duplicated.

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
  with a TTL-in-days selector (1, 3, 7, 14, 30, 90 days; day
  granularity, same as the API). On success it shows the resulting
  URL as a clickable link, plus the paste/file ID.
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

## Testing

```sh
cargo fmt
cargo clippy --all-targets
cargo test
cargo build --release
```

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

What a pass looks like, on a machine with Nix: `nix flake check`
exits 0 with no errors; `nix build` exits 0 and creates a
`./result` symlink pointing into the Nix store, and
`./result/bin/dayfly --help` prints the CLI usage
(subcommands `mcp` and `serve`); `nix develop` drops into a shell
where `cargo --version`, `rustc --version`, and
`rust-analyzer --version` all succeed.

**Nix validation result:** `nix build` and the unit test suite were
run on `x86_64-linux` against `nixos-unstable` and both pass, 36 of
36 tests. On the stable `nixos-25.11` branch the build fails before
compilation with a rustc-too-old error naming 23 AWS crates, which
is why the flake tracks unstable. Before this was built on a machine
with Nix, an earlier version of this note said the flake was
unvalidated. That is no longer true and is corrected here rather
than left standing.

Unit tests cover: key generation, ID validation (including
traversal rejection), range parsing, offset/limit windows, UTF-8
prefix truncation, metadata/expiry round-trip, content-type
detection (PNG, JPEG, GIF, WebP, PDF, ZIP, UTF-8 text, binary
fallback, curl's `x-www-form-urlencoded` default ignored, explicit
type overriding a generic one, sniffed-signature conflict, extension
fallback), peeked-byte forwarding (neither lost nor duplicated),
streaming part buffering, chunked and multi-part-sized uploads,
disconnect/part-failure/oversize abort (no URL material), empty
uploads, HTML escaping, human sizes, and days-remaining math.

### Against a local S3-compatible emulator or a separate test bucket

Garage is on the user's LAN and is out of reach from this VM, so no
Garage testing was done here. To test where Garage (or an emulator)
is reachable, use a **separate test bucket/prefix** first — do not
point the binary at the production bucket until the checklist below
passes:

```sh
export S3_ENDPOINT=https://garage.example.invalid:3900
export S3_BUCKET=pastebin-test
export S3_REGION=us-east-1
export S3_ACCESS_KEY_ID=TEST-KEY
export S3_SECRET_ACCESS_KEY=TEST-SECRET
./target/release/dayfly serve &
curl --data-binary @- http://127.0.0.1:8080/upload <<< 'hello'
# then fetch, range-fetch, metadata, delete, and list with the returned URL
```

### Garage test checklist (run on a machine where Garage is reachable)

- [ ] Create a test bucket (or test prefix) separate from any
      production data.
- [ ] Store a text paste, fetch it back, fetch a byte range, fetch
      metadata, delete it, and confirm the delete.
- [ ] Verify path-style requests work (Garage uses path-style URLs,
      not virtual-hosted style; the binary sets
      `force_path_style = true`).
- [ ] Verify that `If-None-Match` behaviour is **not** relied upon:
      the binary never sends a conditional write for create-only
      semantics; a new random ID is created per store.
- [ ] Verify day TTL: store with `ttl_days=1`, confirm the paste is
      readable before expiry and returns 410 / expired after the
      stored `expires-at` passes.
- [ ] Verify streaming upload: pipe a file larger than 5 MiB with
      `cat file | curl --data-binary @- <base>/upload`, confirm the
      returned URL streams back (Range download), and confirm that
      killing the client mid-upload yields no URL and leaves no
      completed object (an aborted multipart upload may leave an
      incomplete upload for the lifecycle sweeper — see below).
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
      deletes objects past their TTL day, plus aborts incomplete
      multipart uploads.

## Lifecycle example

`docs/lifecycle-example.json` is an **example only**. It is not
applied automatically. Apply it manually to the test bucket, verify
the behaviour above, and only then consider the production bucket.
Deletion may run after the TTL day; objects are already unreadable
via the API once `expires-at` passes, regardless of when the sweeper
runs.

## Known limitations (v1)

These were found by running the binary against a Garage 1.3.1 bucket,
not by reading the code. They are recorded here so a reader does not
have to discover them, and so the claims elsewhere in this file stay
honest.

- **A local-file upload over 8 MiB depends on a copy for metadata.**
  `store_file` creates the multipart upload with no metadata and adds
  it afterwards with a same-key `CopyObject` whose error is discarded.
  If that copy fails, the object has no `expires-at` and never expires
  on read.
- **Text detection inspects the first 8 KiB only.** A file that is
  ASCII at the start and binary later is stored as `text/plain` and
  served inline.
- **Office documents report as `application/zip`.** Sniffing outranks
  the filename extension and the signature is only the PK header.
- **The HTTP endpoint is internal-only in the weakest sense.** The
  bearer token comparison is not constant time, and the HTML form
  routes have no CSRF protection, so run this on a private network
  rather than exposing it.

## Future work (not in v1)

- A database (SQLite or similar) for listing/search/sorting,
  view counters, burn-after-read, per-paste ownership, audit
  history, and deduplication — triggered by listing slowdown, not
  added speculatively.
- Expansion to S3 stores other than Garage, with real testing
  against each.
- Any frontend theme refinement against suorcd.us's actual
  stylesheet (see Theme flag above).
