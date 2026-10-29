# dayfly — Nix flake (an additional build path, not a replacement for Cargo:
# `cargo build` / `cargo test` still work standalone).
#
# Approach: nixpkgs' own rustPlatform (buildRustPackage). It is chosen over
# crane / naersk deliberately: it needs no third-party flake input to track,
# it reads Cargo.lock directly, and this is a single-binary crate with no
# workspace or cross-compilation complexity that would justify crane's
# incremental-caching machinery. If build times become a problem, crane is the
# first alternative to revisit, not a custom vendored toolchain.
#
# nixpkgs tracks nixos-unstable, NOT a stable branch. The AWS SDK crates pinned
# in Cargo.lock require rustc 1.94.1 while nixos-25.11 ships 1.91.1, so the
# build fails outright on the stable branch with a rustc-too-old error naming
# 23 AWS crates. Move back to a stable channel once one carries rustc 1.94.1.
#
# Commit the generated flake.lock so the exact nixpkgs commit is pinned too.

{
  description = "dayfly: ephemeral paste and file store for LLM agents, backed by a private Garage bucket";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forEachSystem =
        f: nixpkgs.lib.genAttrs systems (system: f system (import nixpkgs { inherit system; }));
    in
    {
      packages = forEachSystem (system: pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = "dayfly";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          # No system libraries are needed: TLS goes through Rust crates
          # (rustls), not OpenSSL, and no C build steps exist.
        };
      });

      devShells = forEachSystem (system: pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.cargo
            pkgs.rustc
            pkgs.rust-analyzer
            pkgs.clippy
            pkgs.rustfmt
          ];
        };
      });

      apps = forEachSystem (system: pkgs: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/dayfly";
        };
      });
    };
}
