{
  description = "Sail dev shell (Spark/Ibis tests) with reproducible Rust nightly";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    # Reproducible Rust toolchains (nightly pinned by date)
    fenix.url = "github:nix-community/fenix";
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;

        isLinux = pkgs.stdenv.isLinux;

        # Pin Rust nightly by date (reproducible; controlled by flake.lock + this date)
        rustToolchain = fenix.packages.${system}.latest.toolchain;

        # If you prefer "latest nightly pinned only by flake.lock", use:
        # rustToolchain = fenix.packages.${system}.latest.toolchain;

        py = pkgs.python313;
        pyp = pkgs.python313Packages;
      in {
        devShells.default = pkgs.mkShell {
          buildInputs =
            (with pkgs; [
              # Shell utils commonly assumed by scripts
              bashInteractive
              coreutils
              findutils
              gnugrep
              gnused
              gawk
              which
              procps
              ripgrep

              # Rust (nightly, reproducible)
              rustToolchain
	      cargo-nextest
              # Some Rust build helpers often needed by crates
              pkg-config

              # Java for Spark
              jdk17

              # Tooling for hatch + uv (avoid pipx uv issues)
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
              # Needed on Linux for many binary Python wheels (pandas/pyarrow/etc.)
              pkgs.stdenv.cc.cc.lib
            ];

          shellHook =
            ''
              export RUST_BACKTRACE=1
              export JAVA_HOME=${pkgs.jdk17}

              export PYTHONPATH="$PWD/python:$PYTHONPATH"
	      export PYO3_PYTHON="$(which python)"


              # Accept `cargo +nightly ...` even without rustup (we're already on nightly via Nix)
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
              ''
              + lib.optionalString isLinux ''
                export ARROW_LIB_DIR=${pkgs.arrow-cpp}/lib
                export LD_LIBRARY_PATH=${py}/lib:${pkgs.stdenv.cc.cc.lib}/lib:${pkgs.arrow-cpp}/lib:${pkgs.openssl}/lib:$LD_LIBRARY_PATH
              '';

        };
      }
    );
}
