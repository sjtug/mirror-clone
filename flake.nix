{
  description = "A Nix-flake-based Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane"; # Check https://crane.dev for detailed guide
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
    pre-commit-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      flake-parts,
      advisory-db,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.treefmt-nix.flakeModule
        inputs.pre-commit-hooks.flakeModule
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      perSystem =
        {
          config,
          pkgs,
          lib,
          system,
          ...
        }:
        let
          inherit (pkgs)
            stdenv
            pkgsStatic
            cacert
            rust-jemalloc-sys
            ;
          craneLib = (inputs.crane.mkLib pkgs).overrideToolchain (p: p.rustToolchain);

          src =
            let
              root = ./.;
            in
            lib.fileset.toSource {
              inherit root;
              fileset = lib.fileset.unions [
                (craneLib.fileset.commonCargoSources root)
                (lib.fileset.maybeMissing (root + "/tests"))
                (lib.fileset.maybeMissing (root + "/src/snapshots"))
              ];
            };

          # NOTE: `buildInputs` and sometimes `nativeBuildInputs`
          # should be explicitly overridden for cross compilation
          commonArgs = {
            inherit src;
            strictDeps = true;
            nativeBuildInputs = [ cacert ];
            buildInputs = [ rust-jemalloc-sys ];
            VERGEN_GIT_SHA = self.rev or "dirty";
          };
          # Build *just* the cargo dependencies, so we can reuse
          # all of that work (e.g. via cachix) when running in CI
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          defaultTarget = stdenv.hostPlatform.config;
          muslTarget =
            {
              "x86_64-linux" = "x86_64-unknown-linux-musl";
              "aarch64-linux" = "aarch64-unknown-linux-musl";
              "aarch64-darwin" = throw "${system} does not support cross compilation to musl";
            }
            .${system};

          # Build the actual crate itself, reusing the dependency
          # artifacts from above.
          my-crate = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              CARGO_PROFILE = "dev";
              CARGO_BUILD_TARGET = defaultTarget;
            }
          );

          my-crate-musl = craneLib.buildPackage (
            commonArgs
            // {
              nativeBuildInputs = [
                # Required by aws-lc-sys
                cacert
                stdenv.cc
                pkgsStatic.stdenv.cc
              ];
              buildInputs = [ pkgsStatic.rust-jemalloc-sys ];
              doCheck = true; # always run checkPhase for release artifact
              CARGO_PROFILE = "release";
              CARGO_BUILD_TARGET = muslTarget;
              CARGO_BUILD_FLAGS = "-C target-feature=+crt-static";
            }
          );
        in
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [
              inputs.rust-overlay.overlays.default
              (_final: prev: {
                rustToolchain = prev.rust-bin.stable.latest.default.override {
                  extensions = [
                    "rust-src"
                    "rust-analyzer"
                  ];
                  targets = [ muslTarget ];
                };
              })
            ];
          };

          # https://flake.parts/options/treefmt-nix.html
          # Example: https://github.com/nix-community/buildbot-nix/blob/main/nix/treefmt/flake-module.nix
          treefmt = {
            projectRootFile = "flake.nix";
            flakeCheck = false;
            settings.global.excludes = [ ];

            programs = {
              autocorrect.enable = true;
              nixfmt.enable = true;
              taplo.enable = true;
              zizmor.enable = true;
            };
          };

          # https://flake.parts/options/git-hooks-nix.html
          # Example: https://github.com/cachix/git-hooks.nix/blob/master/template/flake.nix
          pre-commit.settings.package = pkgs.prek;
          pre-commit.settings.configPath = ".pre-commit-config.flake.yaml";
          pre-commit.settings.hooks = {
            commitizen.enable = true;
            treefmt.enable = true;
          };

          checks = {
            # Build the crate as part of `nix flake check` for convenience
            inherit my-crate;

            # Run clippy (and deny all warnings) on the crate source,
            # again, reusing the dependency artifacts from above.
            #
            # Note that this is done as a separate derivation so that
            # we can block the CI if there are issues here, but not
            # prevent downstream consumers from building our crate by itself.
            my-crate-clippy = craneLib.cargoClippy (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoClippyExtraArgs = "--workspace --all-targets -- --deny warnings";
              }
            );

            my-crate-doc = craneLib.cargoDoc (
              commonArgs
              // {
                inherit cargoArtifacts;
                # This can be commented out or tweaked as necessary, e.g. set to
                # `--deny rustdoc::broken-intra-doc-links` to only enforce that lint
                env.RUSTDOCFLAGS = "--deny warnings";
              }
            );

            # Check Rust project formatting
            my-crate-fmt = craneLib.cargoFmt {
              inherit src;
            };

            # Check TOML formatting
            my-crate-toml-fmt = craneLib.taploFmt {
              src = lib.sources.sourceFilesBySuffices src [ ".toml" ];
              # taplo arguments can be further customized below as needed
              # taploExtraArgs = "--config ./taplo.toml";
            };

            # Audit dependencies
            my-crate-audit = craneLib.cargoAudit {
              inherit src advisory-db;
            };

            # Audit licenses
            my-crate-deny = craneLib.cargoDeny {
              inherit src;
            };

            # Run tests with cargo-nextest
            my-crate-nextest = craneLib.cargoNextest (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoNextestExtraArgs = "--workspace";
                doCheck = true;
              }
            );
          };

          devShells.default = craneLib.devShell {
            inputsFrom = [
              config.treefmt.build.devShell
              config.pre-commit.devShell
              my-crate
            ];

            # Required for tikv-jemallocator
            hardeningDisable = [ "fortify" ];

            packages =
              with pkgs;
              [
                # pkg-config
                # rustPlatform.bindgenHook
                cargo-insta

                ### Miscellaneous ###
                # cargo-audit
                cargo-bloat
                # cargo-license
                # cargo-nextest
                # cargo-outdated
                # cargo-show-asm
                # samply
                # watchexec
                # bacon
              ]
              ++ lib.optionals (!pkgs.stdenv.isDarwin) [
                cargo-llvm-cov
                # valgrind
              ];

            VERGEN_GIT_SHA = self.rev or "dirty";
          };

          packages.default = my-crate;
          packages.musl = my-crate-musl;
        };
    };
}
