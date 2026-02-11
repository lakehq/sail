{
  description = "Sail dev shell (Spark/Ibis tests) with reproducible Rust nightly";

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

        rustStable = fenix.packages.${system}.stable.toolchain;
        rustNightly = fenix.packages.${system}.latest.toolchain;

        py = pkgs.python313;
        pyp = pkgs.python313Packages;
        py311 = pkgs.python311;

      in {
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
              nodejs_22
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
              py311
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
              export PROTOC="${pkgs.protobuf}/bin/protoc"

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

              # Auto-download Ibis test data if not present
              if [ ! -d "opt/ibis-testing-data" ]; then
                echo ""
                echo -e "\033[1;36mDownloading Ibis test data...\033[0m"
                git clone --depth 1 https://github.com/ibis-project/testing-data opt/ibis-testing-data
                echo -e "\033[1;32m✓ Ibis test data downloaded\033[0m"
              fi

              # Print helpful commands
              echo ""
              echo -e "\033[1;36m=== Sail Development Commands ===\033[0m"
              echo ""
              echo "hatch run maturin develop"
              echo "hatch run test-spark.spark-4.1.1:pip install 'pyspark[connect]==4.1.1' 'pandas'"
	      echo ""
              echo -e "\033[1;32m✓ Ibis test data ready at opt/ibis-testing-data\033[0m"
              echo ""
              echo -e "\033[1;33m1. Run Sail server (port 50051):\033[0m"
              echo "   hatch run test-ibis:scripts/spark-tests/run-server.sh"
              echo ""
              echo -e "\033[1;33m2. Run Ibis tests (against Sail server on localhost:50051):\033[0m"
              echo "   hatch run test-ibis:env SPARK_REMOTE=\"sc://localhost:50051\" scripts/spark-tests/run-tests.sh"
              echo ""
              echo -e "\033[1;33m3. Run feature tests (BDD with pytest-bdd):\033[0m"
              echo "   hatch run pytest python/pysail/tests/spark/*/test_features.py"
              echo ""
              echo -e "\033[1;33m4. Run all tests (against Sail server on localhost:50051):\033[0m"
              echo "   env SPARK_REMOTE=\"sc://localhost:50051\" hatch run pytest --pyargs pysail"
              echo ""
              echo -e "\033[1;33m5. Run tests with Spark local (not Sail):\033[0m"
              echo "   env SPARK_REMOTE=\"local\" hatch run pytest --pyargs pysail"
              echo ""
              echo -e "\033[1;33m4. Run all tests features (against Sail server on localhost:50051):\033[0m"
              echo " export SPARK_REMOTE=\"sc://localhost:50051\" && hatch run pytest python/pysail/tests/spark/function/test_features.py"
              echo ""
            ''
            + lib.optionalString isLinux ''
              export ARROW_LIB_DIR=${pkgs.arrow-cpp}/lib
              export LD_LIBRARY_PATH=${py311}/lib:${py}/lib:${pkgs.stdenv.cc.cc.lib}/lib:${pkgs.arrow-cpp}/lib:${pkgs.openssl}/lib:$LD_LIBRARY_PATH
            '';
        };
      }
    );
}
