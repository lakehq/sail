{
  description = "Sail – Spark-compatible compute engine (dev shell + package)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix.url = "github:nix-community/fenix";
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;

        isLinux = pkgs.stdenv.isLinux;
        isDarwin = pkgs.stdenv.isDarwin;

        rustStable = fenix.packages.${system}.stable.toolchain;
        rustNightly = fenix.packages.${system}.latest.toolchain;

        py = pkgs.python313;
        pyp = pkgs.python313Packages;
        py311 = pkgs.python311;

        version = "0.5.0";

      in {
        # ── Package ────────────────────────────────────────────────────
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "sail";
          inherit version;
          src = lib.cleanSource ./.;

          cargoLock.lockFile = ./Cargo.lock;

          # Only build the CLI binary
          cargoBuildFlags = [ "-p" "sail-cli" ];
          cargoTestFlags = [ "-p" "sail-cli" ];

          nativeBuildInputs = [
            pkgs.protobuf   # protoc – required by tonic-build / prost-build
            pkgs.pkg-config
          ];

          buildInputs = [
            py               # PyO3 links against libpython at build time
          ];

          # PyO3 configuration
          env = {
            PYO3_PYTHON = "${py}/bin/python";
          };

          # Some proto builds look for protoc via this env var
          PROTOC = "${pkgs.protobuf}/bin/protoc";

          meta = with lib; {
            description = "Spark-compatible compute engine built on Apache Arrow and DataFusion";
            homepage = "https://github.com/lakehq/sail";
            license = licenses.asl20;
            mainProgram = "sail";
          };
        };

        # ── Dev shell (unchanged) ─────────────────────────────────────
        devShells.default = pkgs.mkShell {
          buildInputs =
            (with pkgs; [
              # Shell utils commonly assumed by scripts
              fzf
              bashInteractive
              coreutils
              findutils
              gnugrep
              gnused
              gawk
              which
              procps
              ripgrep

              # Prereqs from Sail docs
              protobuf   # protoc
              nodejs_20
              pnpm
              zig
              maturin

              # Rust toolchains (stable available; nightly is default)
              rustStable
              rustNightly

              cargo-nextest
              pkg-config

              # Java for Spark
              jdk17

              # Tooling for hatch + uv
              hatch
              uv

              # Python base for venvs
              py
              pyp.virtualenv
              pyp.setuptools
              pyp.wheel

              # Native deps often needed by Arrow/Spark ecosystems
              arrow-cpp
              openssl
            ])
            ++ lib.optionals isLinux [
              pkgs.stdenv.cc.cc.lib
            ];

          shellHook =
            ''
              export RUST_BACKTRACE=1
              export JAVA_HOME=${pkgs.jdk17}

              export PYTHONPATH="$PWD/python:$PYTHONPATH"
              export PYO3_PYTHON="${py}/bin/python"

              # Accept `cargo +nightly ...` even without rustup (already on nightly via Nix)
              cargo() {
                if [ "$1" = "+nightly" ]; then
                  shift
                fi
                command cargo "$@"
              }

              # Pretty powerline prompt (Sail ⛵)
              if [ -z "$_NIX_OLD_PS1" ]; then
                export _NIX_OLD_PS1="$PS1"
              fi
              export PS1="\[\e[48;5;24m\]\[\e[38;5;231m\]  ⛵ sail  \[\e[0m\] \[\e[38;5;75m\]\w\[\e[0m\] \$ "

              # Enable fzf keybindings (Ctrl-R, Ctrl-T, Alt-C)
              if [ -e "${pkgs.fzf}/share/fzf/key-bindings.bash" ]; then
                source "${pkgs.fzf}/share/fzf/key-bindings.bash"
              fi
              if [ -e "${pkgs.fzf}/share/fzf/completion.bash" ]; then
                source "${pkgs.fzf}/share/fzf/completion.bash"
              fi
            ''
            + lib.optionalString isLinux ''
              export ARROW_LIB_DIR=${pkgs.arrow-cpp}/lib
              export LD_LIBRARY_PATH=${py311}/lib:${py}/lib:${pkgs.stdenv.cc.cc.lib}/lib:${pkgs.arrow-cpp}/lib:${pkgs.openssl}/lib:$LD_LIBRARY_PATH
            '';
        };
      }
    );
}
