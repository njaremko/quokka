# quokka — common dev tasks. Run `just -l` to list. All recipes assume the
# devenv shell (`devenv shell` / direnv) so buck2, reindeer, rustc are on PATH.

# Build the runner binary under Buck2 (nix toolchains).
build:
    buck2 build //:quokka

# Build everything (library, binary, tests).
build-all:
    buck2 build //...

# Run all tests under Buck2.
test:
    buck2 test //:quokka-lib-test //:scheduler_integration

# Build + place the runner where a host repo's `[test] v2_test_executor` can
# point at it (mirrors how nobie consumes the runner from `.tmp/`).
build-runner:
    buck2 build //:quokka --out .tmp/quokka

# Cargo build/test (the second, standalone build graph).
cargo-build:
    cargo build

cargo-test:
    cargo test

# Lint via the workspace clippy set (Buck2 toolchain) and Cargo.
clippy:
    buck2 build '//:quokka-lib[clippy.txt]'
    cargo clippy --all-targets

# Regenerate third-party/Cargo.lock + third-party/BUCK from third-party/Cargo.toml.
regen-third-party:
    reindeer --third-party-dir third-party update
    reindeer --third-party-dir third-party buckify

# Regenerate the checked-in tonic/prost bindings from proto/*.proto.
regen-protos:
    ./proto/regenerate.sh
