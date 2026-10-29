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
      packages = forEachSystem (
        system: pkgs:
        let
          dayfly = pkgs.rustPlatform.buildRustPackage {
            pname = "dayfly";
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            # No system libraries are needed: TLS goes through Rust crates
            # (rustls), not OpenSSL, and no C build steps exist.
          };
          # OCI image for the HTTP server. Layered rather than one blob so a
          # registry stores the dependency layers once across versions instead
          # of re-uploading the whole binary every release. The name and tag
          # stay generic here; the workflow retags for ghcr.io, so the flake
          # does not hard-code a registry.
          container = pkgs.dockerTools.buildLayeredImage {
            name = "dayfly";
            tag = "0.1.0";
            contents = [
              pkgs.dockerTools.binSh
              pkgs.dockerTools.fakeNss
              pkgs.dockerTools.caCertificates
              dayfly
            ];
            config = {
              Entrypoint = [ "${dayfly}/bin/dayfly" ];
              Cmd = [ "serve" ];
              Env = [
                "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
                # The binary defaults to 127.0.0.1, which is unreachable
                # through a published port, so the image binds all interfaces.
                # Set DAYFLY_HTTP_BIND at runtime to change it.
                "DAYFLY_HTTP_BIND=0.0.0.0:8080"
              ];
              ExposedPorts = {
                "8080/tcp" = { };
              };
              # Unprivileged: the server binds 8080 and reads all configuration
              # from the environment, so nothing needs root. fakeNss supplies a
              # passwd entry for this uid.
              User = "65534:65534";
              WorkingDir = "/";
            };
          };
        in
        {
          default = dayfly;
          inherit container;
        }
      );

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
