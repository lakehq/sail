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

        # Single Python version
        py = pkgs.python311;
        pyp = pkgs.python311Packages;

      in {
        devShells.default = pkgs.mkShell {

          buildInputs =
            (with pkgs; [
              # Shell utils
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

              # Sail prerequisites
              protobuf
              nodejs_22
              pnpm
              zig
              maturin

              # Rust
              rustStable
              rustNightly
              cargo-nextest
              pkg-config

              # Java (Spark)
              jdk17

              # Python
              py
              pyp.virtualenv
              pyp.setuptools
              pyp.wheel

              # Other tooling
              hatch
              uv

              # Native deps
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

              # Clean Python env
              unset PYTHONHOME
              unset PYTHONPATH

              # Force PyO3 to exact interpreter
              export PYO3_PYTHON="${py}/bin/python3.11"
              export PYTHON_SYS_EXECUTABLE="${py}/bin/python3.11"
              export PYO3_USE_ABI3=0
	      export PKG_CONFIG_PATH="${py}/lib/pkgconfig:$PKG_CONFIG_PATH"

              export PYTHONPATH="$PWD/python"

              cargo() {
                if [ "$1" = "+nightly" ]; then
                  shift
                fi
                command cargo "$@"
              }

              if [ -z "$_NIX_OLD_PS1" ]; then
                export _NIX_OLD_PS1="$PS1"
              fi
              export PS1="\[\e[48;5;24m\]\[\e[38;5;231m\]  ⛵ sail  \[\e[0m\] \[\e[38;5;75m\]\w\[\e[0m\] \$ "

              if [ -e "${pkgs.fzf}/share/fzf/key-bindings.bash" ]; then
                source "${pkgs.fzf}/share/fzf/key-bindings.bash"
              fi
              if [ -e "${pkgs.fzf}/share/fzf/completion.bash" ]; then
                source "${pkgs.fzf}/share/fzf/completion.bash"
              fi

	      echo ""
	      echo -e "\033[1;36m=== Sail Development Commands ===\033[0m"
	      echo ""
	      echo "hatch run maturin develop"
        echo "hatch run test-spark.spark-4.1.1:pip install 'pyspark[connect]==4.1.1' 'pandas'"
        echo "hatch run test-spark.spark-4.1.1:bash scripts/spark-tests/run-tests.sh"
        echo "export SPARK_REMOTE=\"sc://localhost:50051\" && hatch run pytest --pyargs pysail"
        echo ""
        echo "env RUST_LOG=\"DEBUG\" SAIL_EXECUTION__DEFAULT_PARALLELISM=4 cargo run -p sail-cli -- spark server"
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
        echo -e "\033[1;33m6. Run only function feature tests:\033[0m"
        echo "   export SPARK_REMOTE=\"sc://localhost:50051\" && hatch run pytest python/pysail/tests/spark/function/test_features.py"
        echo ""
            ''
            + lib.optionalString isLinux ''
              export ARROW_LIB_DIR=${pkgs.arrow-cpp}/lib
              export LD_LIBRARY_PATH=${py}/lib:${pkgs.stdenv.cc.cc.lib}/lib:${pkgs.arrow-cpp}/lib:${pkgs.openssl}/lib:$LD_LIBRARY_PATH
            '';
        };
      }
    );
}
