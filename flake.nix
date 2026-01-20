{
  description = "Sail dev shell (Spark/Ibis tests)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;

        py = pkgs.python312;
        pyp = pkgs.python312Packages;

        isLinux = pkgs.stdenv.isLinux;
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

              # Use Nix-provided uv (avoids NixOS stub-ld issues)
              uv

              # Rust toolchain
              rustc
              cargo
              rustfmt
              clippy
              pkg-config

              # Java for Spark
              jdk17

              # Python + helpers (hatch via pipx)
              py
              pyp.virtualenv
              pyp.setuptools
              pyp.wheel
              pipx

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

              export PIPX_HOME="$HOME/.local/pipx"
              export PIPX_BIN_DIR="$HOME/.local/bin"

              # Make sure Nix-provided uv wins over any pipx-provided uv
              export PATH=${pkgs.uv}/bin:$PATH
              export PATH="$PIPX_BIN_DIR:$PATH"
            ''
            + lib.optionalString isLinux ''
              export ARROW_LIB_DIR=${pkgs.arrow-cpp}/lib
              export LD_LIBRARY_PATH=${pkgs.stdenv.cc.cc.lib}/lib:${pkgs.arrow-cpp}/lib:${pkgs.openssl}/lib:$LD_LIBRARY_PATH
            '';
        };
      }
    );
}
