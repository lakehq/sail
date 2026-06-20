{
  description = "Sail – Spark-compatible compute engine (dev shell + package)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix.url = "github:nix-community/fenix";
    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, fenix, devshell }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ devshell.overlays.default ];
        };

        lib = pkgs.lib;
        isLinux = pkgs.stdenv.isLinux;

        fenixPkgs = fenix.packages.${system};
        rustToolchain = fenixPkgs.combine [
          fenixPkgs.stable.cargo
          fenixPkgs.stable.rustc
          fenixPkgs.stable.clippy
          fenixPkgs.stable.rust-src
          fenixPkgs.stable.rust-std
          fenixPkgs.stable.rust-analyzer
          fenixPkgs.latest.rustfmt
        ];

        py = pkgs.python313.withPackages (ps: with ps; [
          pip
          setuptools
          wheel
        ]);

        # Use the default (latest) protobuf to stay aligned with CI, which
        # installs the latest protoc via `arduino/setup-protoc`.
        protobuf3 = pkgs.protobuf;

        # Rust toolchain used both in the dev shell and for `nix build`.
        # We carve out a minimal toolchain (no rustfmt) for the package build so
        # nothing nightly leaks into the reproducible derivation.
        rustForBuild = fenixPkgs.combine [
          fenixPkgs.stable.cargo
          fenixPkgs.stable.rustc
          fenixPkgs.stable.rust-std
        ];

        # Read the project version from pyproject.toml so it tracks the source
        # of truth automatically. Suffix with `-dev` so Nix-built wheels are
        # never confused with official PyPI releases.
        pysailVersion = (builtins.fromTOML (builtins.readFile ./pyproject.toml)).project.version;

        pysail = pkgs.python313Packages.buildPythonPackage {
          pname = "pysail";
          version = "${pysailVersion}-dev";
          src = ./.;
          format = "pyproject";

          cargoDeps = pkgs.rustPlatform.importCargoLock {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = (with pkgs; [
            rustPlatform.cargoSetupHook
            rustPlatform.maturinBuildHook
            rustForBuild
            pkg-config
            protobuf3
          ]) ++ lib.optionals isLinux [ pkgs.gcc pkgs.mold ];

          buildInputs = lib.optionals isLinux [ pkgs.stdenv.cc.cc.lib ];

          # maturin needs to be pointed at the sail-python crate, since this is a
          # workspace and the project's pyproject.toml only mentions it via
          # `tool.maturin.manifest-path`.
          maturinBuildFlags = [ "--manifest-path" "crates/sail-python/Cargo.toml" ];

          PROTOC = "${protobuf3}/bin/protoc";
          PYO3_PYTHON = "${pkgs.python313}/bin/python";

          # Link with mold inside the Nix sandbox too (the dev shell already
          # does this). Only speeds up linking; the resulting binary is
          # unchanged. sccache is intentionally omitted — it can't work in the
          # network-less, isolated sandbox.
          RUSTFLAGS = lib.optionalString isLinux "-C link-arg=-fuse-ld=mold";

          # The wheel embeds CLI bins; tests live outside the package.
          doCheck = false;

          meta = with lib; {
            description = "Sail — Spark-compatible compute engine (Python bindings)";
            homepage = "https://github.com/lakehq/sail";
            license = licenses.asl20;
            platforms = platforms.unix;
          };
        };
      in {
        packages.default = pysail;
        packages.pysail = pysail;

        devShells.default = pkgs.devshell.mkShell {
          name = "sail";

          packages =
            (with pkgs; [
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
              curl
              unzip

              nodejs_22
              pnpm
              zig
              maturin

              rustToolchain
              cargo-nextest
              pkg-config

              jdk17
              maven

              hatch
              uv

              mold
              sccache
            ])
            ++ [ py protobuf3 ]
            ++ lib.optionals isLinux [ pkgs.stdenv.cc.cc.lib pkgs.gcc ];

          env = [
            { name = "RUST_BACKTRACE"; value = "1"; }
            { name = "PROTOC"; value = "${protobuf3}/bin/protoc"; }
            { name = "PYO3_PYTHON"; value = "${py}/bin/python"; }
            { name = "PYTHON_SYS_EXECUTABLE"; value = "${py}/bin/python"; }
            { name = "PYO3_USE_ABI3"; value = "0"; }
            { name = "PATH"; prefix = "${protobuf3}/bin"; }
            { name = "PKG_CONFIG_PATH"; prefix = "${py}/lib/pkgconfig"; }
            { name = "PYTHONPATH"; eval = "$PRJ_ROOT/python"; }
          ];

          commands = [
            {
              category = "build";
              name = "sail-build";
              help = "Build and install pysail in the venv as editable (maturin develop)";
              command = ''hatch run maturin develop "$@"'';
            }
            {
              category = "build";
              name = "sail-nix-build";
              help = "Reproducible build of pysail via Nix sandbox — capped at SAIL_BUILD_JOBS=4 cores";
              command = ''
                cd "$PRJ_ROOT"
                jobs="''${SAIL_BUILD_JOBS:-4}"
                echo "Building with --cores $jobs --max-jobs 1 (override with SAIL_BUILD_JOBS=N)"
                nix build .#pysail --cores "$jobs" --max-jobs 1 "$@"
                echo ""
                echo "Output: $PRJ_ROOT/result"
              '';
            }
            {
              category = "build";
              name = "sail-fmt";
              help = "Format Rust code (nightly rustfmt)";
              command = ''cargo fmt "$@"'';
            }
            {
              category = "build";
              name = "sail-clippy";
              help = "Run clippy --all-targets --all-features -- -D warnings";
              command = ''cargo clippy --all-targets --all-features -- -D warnings "$@"'';
            }
            {
              category = "build";
              name = "sail-precommit";
              help = "Run the full pre-commit pipeline (fmt + clippy + build + nextest)";
              command = ''
                set -e
                cargo fmt
                cargo clippy --all-targets --all-features -- -D warnings
                cargo build
                env SAIL_UPDATE_GOLD_DATA=1 cargo nextest run
                cargo fmt
              '';
            }

            {
              name = "sail-flake-update";
              help = "Update flake inputs (no args = all; or pass input names: sail-flake-update fenix nixpkgs)";
              command = ''cd "$PRJ_ROOT" && nix flake update "$@"'';
            }

            {
              category = "server";
              name = "sail-server";
              help = "Run Sail Spark Connect server on :50051";
              command = ''env RUST_LOG="''${RUST_LOG:-sail=debug}" SAIL_EXECUTION__DEFAULT_PARALLELISM=''${SAIL_EXECUTION__DEFAULT_PARALLELISM:-4} cargo run -p sail-cli -- spark server --port 50051 "$@"'';
            }
            {
              category = "server";
              name = "sail-server-spark-3.5.7";
              help = "Run the test server for the spark-3.5.7 suite (catalog + UDF env)";
              command = ''
                bash "$PRJ_ROOT/scripts/spark-tests/ensure-venv.sh" "$PRJ_ROOT/.venvs/test-spark.spark-3.5.7"
                bash "$PRJ_ROOT/scripts/spark-tests/fetch-pyspark.sh" 3.5.7
                hatch run test-spark.spark-3.5.7:python -c 'import pyspark' 2>/dev/null \
                  || hatch run test-spark.spark-3.5.7:install-pyspark
                hatch run test-spark.spark-3.5.7:bash scripts/spark-tests/run-server.sh
              '';
            }
            {
              category = "server";
              name = "sail-server-spark";
              help = "Run the test server for the spark-4.1.1 suite (catalog + UDF env)";
              command = ''
                bash "$PRJ_ROOT/scripts/spark-tests/ensure-venv.sh" "$PRJ_ROOT/.venvs/test-spark.spark-4.1.1"
                bash "$PRJ_ROOT/scripts/spark-tests/fetch-pyspark.sh" 4.1.1
                hatch run test-spark.spark-4.1.1:python -c 'import pyspark' 2>/dev/null \
                  || hatch run test-spark.spark-4.1.1:install-pyspark
                hatch run test-spark.spark-4.1.1:bash scripts/spark-tests/run-server.sh
              '';
            }
            {
              category = "server";
              name = "sail-server-ibis";
              help = "Run the test server for the Ibis suite (catalog + UDF env)";
              command = ''
                bash "$PRJ_ROOT/scripts/spark-tests/ensure-venv.sh" "$PRJ_ROOT/.venvs/test-ibis"
                hatch run test-ibis:bash scripts/spark-tests/run-server.sh
              '';
            }

            {
              category = "test";
              name = "sail-test";
              help = "Run all Rust tests (nextest)";
              command = ''cargo nextest run "$@"'';
            }
            {
              category = "test";
              name = "sail-test-feature";
              help = "Run pytest BDD feature tests against Sail server on :50051";
              command = ''export SPARK_REMOTE="''${SPARK_REMOTE:-sc://localhost:50051}" && hatch run pytest python/pysail/tests/spark/function/test_features.py "$@"'';
            }
            {
              category = "test";
              name = "sail-test-ibis";
              help = "Run Ibis tests against Sail server on :50051 (auto-clones ibis-testing-data)";
              command = ''
                _data_dir="$PRJ_ROOT/opt/ibis-testing-data"
                if [ ! -d "$_data_dir/.git" ]; then
                  echo "⛵ Cloning ibis-testing-data (shallow)..."
                  git clone --depth 1 https://github.com/ibis-project/testing-data.git "$_data_dir"
                fi
                bash "$PRJ_ROOT/scripts/spark-tests/ensure-venv.sh" "$PRJ_ROOT/.venvs/test-ibis"
                export SPARK_REMOTE="sc://localhost:50051"
                hatch run test-ibis:bash scripts/spark-tests/run-tests.sh "$@"
              '';
            }
            {
              category = "test";
              name = "sail-test-jvm";
              help = "Run feature tests against local Spark JVM (no Sail)";
              command = ''SPARK_REMOTE="local" hatch run pytest python/pysail/tests/spark/function/test_features.py -v "$@"'';
            }
            {
              category = "test";
              name = "sail-fetch-pyspark";
              help = "Download patched PySpark packages (3.5.7 + 4.1.1) if missing (anonymous, no gh)";
              command = ''bash "$PRJ_ROOT/scripts/spark-tests/fetch-pyspark.sh" "$@"'';
            }
            {
              category = "test";
              name = "sail-test-spark";
              help = "Run Spark patched tests (spark-4.1.1, auto-fetches + installs PySpark)";
              command = ''
                bash "$PRJ_ROOT/scripts/spark-tests/ensure-venv.sh" "$PRJ_ROOT/.venvs/test-spark.spark-4.1.1"
                bash "$PRJ_ROOT/scripts/spark-tests/fetch-pyspark.sh" 4.1.1
                hatch run test-spark.spark-4.1.1:python -c 'import pyspark' 2>/dev/null \
                  || hatch run test-spark.spark-4.1.1:install-pyspark
                hatch run test-spark.spark-4.1.1:bash scripts/spark-tests/run-tests.sh "$@"
              '';
            }
            {
              category = "test";
              name = "sail-test-spark-3.5.7";
              help = "Run Spark patched tests (spark-3.5.7, auto-fetches + installs PySpark)";
              command = ''
                bash "$PRJ_ROOT/scripts/spark-tests/ensure-venv.sh" "$PRJ_ROOT/.venvs/test-spark.spark-3.5.7"
                bash "$PRJ_ROOT/scripts/spark-tests/fetch-pyspark.sh" 3.5.7
                hatch run test-spark.spark-3.5.7:python -c 'import pyspark' 2>/dev/null \
                  || hatch run test-spark.spark-3.5.7:install-pyspark
                hatch run test-spark.spark-3.5.7:bash scripts/spark-tests/run-tests.sh "$@"
              '';
            }
            {
              category = "test";
              name = "sail-pytest";
              help = "Run pytest against Sail server (set SPARK_REMOTE for you)";
              command = ''export SPARK_REMOTE="sc://localhost:50051" && hatch run pytest "$@"'';
            }
          ];

          devshell.startup.fenix-update.text = ''
            _stamp_dir="''${XDG_CACHE_HOME:-$HOME/.cache}/sail"
            _repo_hash=$(printf '%s' "$PRJ_ROOT" | shasum 2>/dev/null | cut -c1-8)
            _stamp_file="$_stamp_dir/fenix-update-$_repo_hash"
            _today=$(date +%Y-%m-%d)
            _last_attempt=$(cat "$_stamp_file" 2>/dev/null || true)
            if [ "$_last_attempt" != "$_today" ]; then
              mkdir -p "$_stamp_dir"
              _lock_before=$(shasum "$PRJ_ROOT/flake.lock" 2>/dev/null | cut -d' ' -f1)
              echo -e "\033[1;33m⛵ Checking fenix toolchain update...\033[0m"
              if (cd "$PRJ_ROOT" && nix flake update fenix 2>&1); then
                echo "$_today" > "$_stamp_file"
                _lock_after=$(shasum "$PRJ_ROOT/flake.lock" 2>/dev/null | cut -d' ' -f1)
                if [ "$_lock_before" != "$_lock_after" ]; then
                  echo ""
                  echo -e "\033[1;32m✓ fenix updated. Please restart the shell:\033[0m"
                  echo -e "\033[1;36m  exit && nix develop\033[0m"
                  echo ""
                  return 0 2>/dev/null || exit 0
                fi
              else
                echo -e "\033[1;31m⚠ Could not update fenix (no network?). Continuing with current toolchain.\033[0m"
              fi
            fi
          '';

          devshell.startup.sail-shell.text = ''
            if command -v nixos-option >/dev/null 2>&1; then
              if [ "$(nixos-option programs.nix-ld.enable 2>/dev/null | awk '/Value:/ {getline; print $1}')" != "true" ]; then
                echo ""
                echo "⚠️  nix-ld is not enabled. Add to configuration.nix:"
                echo "     programs.nix-ld.enable = true;"
                echo "   then run: sudo nixos-rebuild switch"
                echo ""
              fi
            fi

            unset PYTHONHOME

            # JAVA_HOME after user's rc (SDKMAN etc) so it doesn't get clobbered.
            # pkgs.jdk17.home resolves to the correct path on both linux and darwin.
            export JAVA_HOME="${pkgs.jdk17.home}"
            export PATH="$JAVA_HOME/bin:$PATH"

            cargo() {
              if [[ "''${1:-}" == +* ]]; then
                shift
              fi
              command cargo "$@"
            }
            export -f cargo

            if [ -z "''${_NIX_OLD_PS1:-}" ]; then
              export _NIX_OLD_PS1="$PS1"
            fi
            export PS1="\[\e[48;5;24m\]\[\e[38;5;231m\]  ⛵ sail  \[\e[0m\] \[\e[38;5;75m\]\w\[\e[0m\] \$ "

            if [ -e "${pkgs.fzf}/share/fzf/key-bindings.bash" ]; then
              source "${pkgs.fzf}/share/fzf/key-bindings.bash"
            fi
            if [ -e "${pkgs.fzf}/share/fzf/completion.bash" ]; then
              source "${pkgs.fzf}/share/fzf/completion.bash"
            fi
          '' + lib.optionalString isLinux ''
            export LD_LIBRARY_PATH=${py}/lib:${pkgs.stdenv.cc.cc.lib}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
            export RUSTFLAGS="-C link-arg=-fuse-ld=mold"
            export RUSTC_WRAPPER="${pkgs.sccache}/bin/sccache"
          '';
        };
      }
    );
}
