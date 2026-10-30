# AGENTS.md — dayfly

How agents (including future LLM agents working on this repo) should
build, test, and change this project. This file is about the repo's
conventions, not about the user.

## What this project is

Internal pastebin / file-share for LLM agents and one human, backed
by a **private Garage bucket**. One small Rust binary, two transports
sharing one S3 core: MCP server over stdio (default) and HTTP mode
(`serve`, curl API plus a server-rendered human frontend). Full
specification is in `README.md`; do not re-litigate settled decisions
there (Garage first, no database in v1, read-time expiry, no
presigned URLs, no `If-None-Match` reliance) without new user input.

## Writing standard

All new and changed prose must follow ASD-STE100 Simplified Technical
English. The current issue is Issue 9, released 2025-01-15. The
standard is at https://www.asd-ste100.org/.

This standard covers this prose:

- `AGENTS.md`, `README.md`, and `docs/`
- commit messages and pull request descriptions
- Rust doc comments (`///` and `//!`) and code comments
- CLI help, error messages, and frontend copy

This standard does not cover names in code:

- identifiers, module paths, CLI flags, JSON keys, HTTP headers, and
  MCP tool names
- quoted third-party text, license text, and verbatim command output

Rules (a subset of the 53 writing rules):

- Write one instruction in one sentence. Use the active voice.
- Use 20 words or fewer in a procedural sentence. Use 25 words or
  fewer in a descriptive sentence.
- Use 6 sentences or fewer in a paragraph.
- Write the action as a verb. Write "delete the object", not "perform
  the deletion".
- Use one word for one thing. An S3 object is an "object" in all
  files.
- Do not use marketing adjectives, idioms, or phrasal verbs.
- Do not use "simply", "just", or "obviously".
- Use American English spelling.

Do not change code names to obey these rules. Keep the prose term and
the identifier equal in meaning, so the documents cannot drift from the
API.

Existing text does not obey these rules yet. Do not rewrite unrelated
text in a change that has another purpose.

## Build and test (Cargo — the primary path)

```sh
cargo fmt
cargo clippy --all-targets
cargo test
cargo build
cargo build --release
```

- Cargo must keep working standalone. The flake is an additional
  build path, not a replacement.
- Run `cargo fmt` before handing code over; fix clippy warnings
  rather than leaving them, or say plainly why one is left.
- Tests live in the modules they cover (`src/store.rs`,
  `src/stream.rs`, `src/content_type.rs`, `src/web.rs`) and in the
  hermetic integration suite in `tests/`. All S3-backed tests use the
  one in-process fake S3 harness in `tests/common/mod.rs`. Do not add a
  second fake S3 implementation. Do not make the default test run
  depend on Garage or a network service.
- Add a test for any new detection, range, expiry, or streaming
  behaviour — especially: a failed upload must return no URL material,
  and peeked bytes must be forwarded exactly once.
- Never fake a build/test result. Report the real command output,
  including failures.

## Build and test (Nix — additional path)

```sh
nix develop   # shell with cargo, rustc, rust-analyzer, clippy, rustfmt
nix build     # builds packages.default
nix flake check
```

What a pass looks like, on a machine with Nix:

- `nix flake check` exits 0 with no errors.
- `nix build` exits 0 and creates a `./result` symlink pointing into
  the Nix store; `./result/bin/dayfly --help` prints the CLI
  usage (subcommands `mcp` and `serve`).
- `nix develop` drops into a shell where `cargo --version`,
  `rustc --version`, and `rust-analyzer --version` all succeed.

## Nix build status (read this before claiming anything)

The flake builds and its test suite passes on `x86_64-linux` against
`nixos-unstable`: 276 passed and 2 ignored. That is a real result from
a real run, not an assumption.

It does **not** build on the `nixos-25.11` stable branch. The AWS SDK
crates pinned in `Cargo.lock` require rustc 1.94.1 and 25.11 ships
1.91.1, so the build fails before compilation with a rustc-too-old
error naming 23 AWS crates. That is why the flake tracks unstable.
Move back to a stable channel once one carries rustc 1.94.1 or newer.

Do not claim a nix build succeeded if it did not run. When working on
a machine with Nix, run the three commands above. The flake uses
nixpkgs' rustPlatform (`buildRustPackage`, `Cargo.lock` in the repo)
over crane / naersk, see `flake.nix` for the justification, and
pins nixpkgs via the `nixos-unstable` input. After the first
`nix flake update`, commit the generated `flake.lock`.

## Red-green test protocol (required for every change)

Every change must follow the red-green protocol, or state why a test
is not applicable. Write the test before you change the code. Name the
test for the behavior under test, not for the fix.

Run the single test against the unmodified tree. Capture the failure.
If the test passes, it does not reproduce the defect: the test is
wrong, or the diagnosis is wrong. Correct the test or the diagnosis
before you continue.

The change has four evidence checkpoints:

1. **Test**: the test name and the behavior that it asserts.
2. **Red run**: the command and the tail of the failing output from the
   unmodified tree.
3. **Green run**: the same command and the tail of the passing output
   after the change.
4. **Full suite**: the command and the totals.

The red run and the green run must use the same test invocation. Make
the change between the two runs. A failure from one command and a pass
from a different command are not evidence. The red run must fail for
the reason that the test targets.

Use these commands:

- Single test: `nix develop -c cargo test <test_name>`
- Full suite through the real build: `nix build -L`. The check phase
  runs the test suite.
- Flake level: `nix flake check`

Post the command and the tail of its real output for each checkpoint
in the pull request or issue comments. Do not paraphrase, summarize, or
describe the output. A checkpoint without captured output has not
happened.

When a test is not applicable, state the reason in one line in the pull
request or issue comments. Docs, prose, comments, version pins, and
dependency bumps may have nothing to assert. Use this line for a
documentation change:

    No test: documentation only

For another change, write `No test: <reason>`. Silence is not
acceptable: a reviewer cannot tell "no test needed" from "no test
written".

When the change is not directly testable but its effect is observable,
test the effect. Check a flake change by building it. Check a config
change by running the binary with the value and reading the error.

Test standards:

- Assert on behavior, not on rendered strings. A test on an error
  message can pass while the behavior is wrong.
- Exercise the boundary that the change touches, not a helper next to
  it.
- Do not write a test that can only fail when the compiler breaks.
- Prefer the smallest test that fails for the right reason over a broad
  test that fails for any reason.

## Key constraints (do not break these silently)

- **Local only until the user says otherwise.** Do not publish to
  GitHub, push, create a remote, or contact the user's real Garage
  instance from a VM where it is out of reach. Do not use or invent
  real S3 credentials. `.env.example` holds placeholders only; a
  real `.env` is gitignored and must never be committed.
- **The binary is the sole S3 credential holder.** Agents/clients
  never get S3 credentials and never talk to S3 directly. No
  presigned URLs in v1. The bucket stays private, not open, not
  anonymous. No public bucket policy.
- **No database in v1, no SQLite placeholder, no invented schema.**
  Listing/info scan the bucket directly (ListObjectsV2 by date
  prefix + HeadObject per key) and filter expired objects. Document
  the O(n) tradeoff honestly; its slowdown is the trigger for a
  database later.
- **A three-word alias is a second name, not a second object.** The
  canonical ID and the alias resolve to the same key. A change to one
  form must work for the other.
- **Garage first, expansion later.** Keep the code S3-shaped, but do
  not spend v1 effort on generic AWS/MinIO tuning or multi-backend
  abstraction.
- **Logging to stderr only** in both modes, so MCP JSON-RPC on
  stdout is never corrupted. Never log credentials.
- **No panics on bad input.** Validate IDs (reject traversal),
  enforce size limits with a clear 413 / machine-readable error,
  and never return a URL for a failed or incomplete upload
  (abort multipart on disconnect/error).
- **Content-Type detection** follows the precedence in README
  (specific client type, except curl's `--data-binary` default, then
  magic-byte sniffing, then extension, then text, then fallback).
  Sniffed HTML/SVG are always served as attachments, never inline.
- **Human frontend** is server-rendered HTML, no JavaScript
  framework, no build step. Its theme adapts the base look of
  suorcd.us/style.css, palette `cyberpunkNeon`; the site's fonts are
  referenced but not bundled (system monospace fallback). Do not
  copy the whole stylesheet, and do not use the file's other
  palettes (`green-dark`, `neon-dark01`).
- **License / placeholders.** MIT. `LICENSE` and `Cargo.toml` use
  `suorcd` and `github.com/suorcd/dayfly` as placeholders;
  the copyright holder line and final repository name/URL must be
  confirmed before any publish. Do not invent a full legal name.
