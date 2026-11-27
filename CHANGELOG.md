# Changelog

All notable changes to Dayfly are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Dayfly uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
This file starts at 0.6.0. Earlier releases are on the
[GitHub releases page](https://github.com/suorcd/dayfly/releases).

## [Unreleased]

## [0.6.0] - 2026-11-27

### Added

- Add an optional periodic cleanup loop for expired objects
  ([#251](https://github.com/suorcd/dayfly/pull/251)).
- Add Docker, Compose, and systemd deployment paths
  ([#253](https://github.com/suorcd/dayfly/pull/253)).
- Add virtual-hosted S3 endpoint support
  ([#254](https://github.com/suorcd/dayfly/pull/254)).
- Add request and S3 deadlines with in-flight request caps
  ([#268](https://github.com/suorcd/dayfly/pull/268)).
- Add `DAYFLY_LOG_RAW_PASTE_IDS` to opt in to raw paste IDs in logs for
  debugging ([#272](https://github.com/suorcd/dayfly/pull/272)).

### Changed

- Resolve bare IDs by the exact random component. Return an error for an
  ambiguous bare ID ([#265](https://github.com/suorcd/dayfly/pull/265)).
- Bound prefix scans for search and line ranges
  ([#267](https://github.com/suorcd/dayfly/pull/267)).
- Treat blank date filters as absent and allow one-sided date ranges
  ([#275](https://github.com/suorcd/dayfly/pull/275)).
- Fit the current-pastes table in the body width
  ([#281](https://github.com/suorcd/dayfly/pull/281)).
- Show a loading indicator during a file upload and block a second submit
  ([#283](https://github.com/suorcd/dayfly/pull/283)).

### Fixed

- Stop deriving public URLs from untrusted forwarded headers
  ([#266](https://github.com/suorcd/dayfly/pull/266)).
- Send `Cache-Control: no-store` on dynamic and token-bearing responses
  ([#269](https://github.com/suorcd/dayfly/pull/269)).
- Make rooted local-file access race-safe and downloads atomic
  ([#270](https://github.com/suorcd/dayfly/pull/270)).
- Check range response lengths against the planned length
  ([#271](https://github.com/suorcd/dayfly/pull/271)).
- Link the system AWS-LC library in the Nix build
  ([#278](https://github.com/suorcd/dayfly/pull/278)).

### Security

- Redact paste IDs and aliases from request logs by default
  ([#272](https://github.com/suorcd/dayfly/pull/272)).
- Redact paste IDs, aliases, and object keys from store logs by default
  ([#276](https://github.com/suorcd/dayfly/pull/276)).

### Documentation

- Add per-object lifecycle research
  ([#252](https://github.com/suorcd/dayfly/pull/252)).
- Record that `DAYFLY_PASTE_TOKENS` is not retroactive
  ([#279](https://github.com/suorcd/dayfly/pull/279)).

### Upgrade notes

- Logs no longer contain raw paste IDs, aliases, or object keys by
  default. Set `DAYFLY_LOG_RAW_PASTE_IDS=1` only for debugging.
- `DAYFLY_PASTE_TOKENS` does not protect a paste that existed before the
  setting was enabled. On an instance without a bearer token, anyone with
  the link can delete, append, or change the expiry of an old paste.
- The container image tags for this release are `v0.6.0`, `sha-<short>`,
  and `latest`.

[0.6.0]: https://github.com/suorcd/dayfly/compare/v0.5.0...v0.6.0
