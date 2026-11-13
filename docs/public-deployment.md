# Public deployment profile

This page lists the settings for a public dayfly instance. It also states
the costs and the limits that remain. The default deployment is a private
network or localhost. A public instance needs the changes below.

## Security model

- The binary holds the only S3 credentials. The bucket stays private.
- One shared bearer token protects writes. The instance has no accounts.
- A paste token is a delete, expiry, and append credential for one paste.
  It is not an ownership model.
- Expiry is enforced on read. The lifecycle sweep is day-rounded.
- The code does not scan files for malware. HTML and SVG are served as
  attachments.
- The instance has no per-client byte quota. The per-paste caps, the
  retention curve, and the proxy body limit are the byte limits.

## Required settings

| Setting | Public value | Reason |
| --- | --- | --- |
| `DAYFLY_HTTP_BEARER_TOKEN` | A long random value, from a file with mode `0600` | Protects writes and `/mcp` |
| `DAYFLY_WRITE_REQUIRES_TOKEN` | `true` | Reads stay anonymous; writes need the token |
| `DAYFLY_ALLOW_UNAUTHENTICATED` | `false` | Refuses an exposed bind without a token |
| `DAYFLY_LIST_ENABLED` | `false` | Turns off every listing route |
| `DAYFLY_LIST_REQUIRES_TOKEN` | `true` if listing stays on | Gates the list when it is on |
| `DAYFLY_TRUSTED_PROXIES` | The proxy CIDR, for example `10.0.0.0/8` | Makes the per-client limits use the real client |
| `DAYFLY_RATE_LIMIT_PER_MINUTE` | `60` or your value | Bounds request floods |
| `DAYFLY_MAX_TEXT_BYTES` | A small value, for example `1048576` | Bounds one text paste |
| `DAYFLY_MAX_FILE_BYTES` | A small value, for example `104857600` | Bounds one file paste |
| `DAYFLY_MAX_PASTE_BYTES` | A small value, for example `104857600` | Bounds the total paste, including appends |
| `DAYFLY_SEARCH_MAX_BYTES` | A small value, for example `8388608` | Bounds one search |
| `DAYFLY_DEFAULT_TTL_DAYS` | A short value, for example `7` | Bounds the default retention |
| `DAYFLY_MAX_TTL_DAYS` | The longest TTL you allow, for example `30` | Bounds an explicit TTL |
| `DAYFLY_MIN_TTL_DAYS` | `1` | Rejects a sub-day TTL |
| `DAYFLY_RETENTION_BY_SIZE` | `true` | Shortens the TTL of a large paste |
| `DAYFLY_RETENTION_MIN_DAYS` | A short value, for example `1` | Minimum retention at the size cap |
| `DAYFLY_RETENTION_MAX_DAYS` | At least `DAYFLY_MAX_TTL_DAYS`, for example `30` | Maximum retention at zero bytes |
| `DAYFLY_PASTE_TOKENS` | `true` | Lets an uploader delete its own paste |
| `DAYFLY_INLINE_PDF` | `false` | Serves a PDF as an attachment |
| `DAYFLY_ALIAS_WORDS` | `4` or more | Raises alias entropy |
| `DAYFLY_MULTIPART_ABORT_AFTER` | `3600` or your value | Aborts an incomplete multipart upload |
| `DAYFLY_MCP_LOCAL_ROOT` | Leave unset | HTTP MCP refuses the local-path tools |
| `DAYFLY_SITE_NAME` | Your site name | Replaces the internal-use footer |
| `DAYFLY_CONTACT` | A takedown address | Adds the contact to the footer |
| `DAYFLY_AUP_URL` | Your acceptable-use URL | Adds the link to the footer |
| `DAYFLY_ROBOTS_TXT_PATH` | Your `robots.txt` | Serves the crawler policy |
| `DAYFLY_SECURITY_TXT_PATH` | Your `security.txt` | Serves the RFC 9116 contact |
| `DAYFLY_METRICS_BIND` | A private address, for example `127.0.0.1:9091` | Keeps metrics off the public listener |
| `DAYFLY_METRICS_ALLOWED_NETWORKS` | The scraper CIDR, when the bind is not loopback | Restricts the metrics listener |
| `DAYFLY_LOG_FORMAT` | `json` | Sends structured lines to the collector |

Bind the public listener to the loopback address behind the proxy, or to
an address with a token. A non-loopback bind without a token is refused.

## Listing trade

`DAYFLY_LIST_ENABLED=false` turns off `GET /v1/pastes`, the human
`/pastes` page, and the MCP `list_recent` tool. This is the safest public
choice.

`DAYFLY_LIST_REQUIRES_TOKEN=true` keeps listing but requires the bearer
token. A browser cannot send the token, so `/pastes` then needs an
authenticating proxy or the API. Use one of these two choices on a
public instance. Do not leave the list open.

## Proxy requirements

- Terminate TLS at the proxy. The binary serves plain HTTP.
- Set `client_max_body_size` at or above `DAYFLY_MAX_FILE_BYTES`, plus
  the form overhead.
- Set `limit_req` and a connection limit. These act before the bytes
  arrive.
- Set body, header, and read timeouts for the largest upload you allow.
- Preserve the public `Host` header.
- Append to `X-Forwarded-For`. Do not replace it.
- Set `X-Forwarded-Proto` to `https`.
- Do not expose the metrics listener through the proxy.
- Do not cache a paste response.

`DAYFLY_TRUSTED_PROXIES` must name only the proxies that front this
service. A direct connection from an unlisted address is attributed to
that address. A forged forwarded header from an unlisted peer is ignored.

## Before you start

Run the configuration check with the real environment file:

```sh
dayfly --config /path/to/dayfly.env config check --strict
```

Apply a lifecycle rule. The example file uses an expiration above the
default maximum TTL. If you lower `DAYFLY_MAX_TTL_DAYS`, set the rule
above your new maximum and apply it again:

```sh
dayfly lifecycle apply --file docs/lifecycle-example.json
```

The example includes a `words/` rule and an
`AbortIncompleteMultipartUpload` rule. The code also runs its own
multipart sweep at startup and every hour.

Run a manual cleanup after an outage, a missed sweep, or a change to the
retention policy:

```sh
dayfly cleanup --dry-run
dayfly cleanup
```

The dry run reports the objects and uploads that the command would
remove. The live command aborts stale uploads and deletes expired paste
objects and aliases. Stop the service during the pass, or keep
`DAYFLY_MULTIPART_ABORT_AFTER` above the longest upload time.

## Limits that remain

- One shared token. A leak is a full write compromise. Rotate it.
- Browser creation needs an authenticating proxy. A paste token covers
  delete, expiry, and append for an existing paste only.
- No per-paste ownership. A paste token is the only per-paste control.
- No per-client stored-byte quota. A request limit does not bound the
  bytes one client can store over time. Use the per-paste caps, the
  retention curve, and the proxy.
- No malware scan. Serve active types as attachments.
- Expiry is read-time. The lifecycle sweep can run later than the TTL.
- Listing is O(n) in objects scanned. It gets slower as the bucket grows.
- The rate limiter is per process. A second replica has its own counters.
- MCP sessions are in memory. A restart ends them.
- The write gate is not a per-user identity. Use a proxy with identity if
  you need one.
