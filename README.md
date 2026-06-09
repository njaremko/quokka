# quokka

A better **external test runner for [Buck2](https://buck2.build)**.

Quokka is primarily optimized for rust projects at the moment, but I'll probably add support for other languages in the future.

If you use remote execution, quokka will give you the following "for free":
- Automatic distributed sharing of unit tests across your fleet of workers
- Caching of test results, so only changed crates have to run
- Test duration and flake tracking of your test suite

Point a host repo's `.buckconfig` at the built binary to use it:

```ini
[test]
  v2_test_executor = /path/to/quokka
```

## Caching & Flakiness

By integrating with Buck2's external test runner protocol, `quokka` controls when test execution actions are cached via the `disable_test_execution_caching` option.

### Controlling Caching with Labels
You can disable caching for a specific test target by adding non-hermetic or unstable labels to it in your `BUCK` file. Caching is disabled if a label matches or ends with any of the following (separated by `:`):
* `cache_disabled`
* `flaky` (e.g., `rust:flaky`)
* `uses_network`
* `uses_wall_clock`
* `uses_randomness_without_seed`
* `requires_external_service`
* `serial_global_state`
* `stress`
* `network-private`

### Flaky Test Retries
If a test is known to flake (meaning it has at least one failure recorded in the local history database), `quokka` will internally manage retries. The maximum retry attempts are configured via a TOML file located at `$HOME/.quokka/config.toml`:

```toml
[flaky_retry]
attempts = 3
```

During retry attempts, caching is bypassed to guarantee fresh execution results.

### Unseen Test Cache-Busting
When `quokka` encounters a test case not previously seen in its local database, it bypasses caching (`disable_test_execution_caching = true`) for the initial run. This ensures the runner gathers a baseline duration to populate the performance tracking database. Subsequent runs of the same binary are allowed to hit the cache (assuming they pass and have no flake or non-hermetic labels).

### Enabling Caching for Rust Tests

Because Buck2's standard prelude does not natively expose `supports_test_execution_caching` as an attribute or set it in the standard `rust_test` rule, you must wrap `rust_test` to enable caching.

You can define a wrapper rule in a Starlark extension file (e.g. `//rules:cached_rust_test.bzl`) that intercepts the providers from the prelude implementation and injects the capability:

```python
load("@prelude//rust:rust_binary.bzl", "rust_test_impl")
load("@prelude//decls:rust_rules.bzl", "rust_test")

def _cached_rust_test_impl(ctx: AnalysisContext) -> list[Provider]:
    providers = rust_test_impl(ctx)
    new_providers = []
    test_info = None
    
    for p in providers:
        if type(p) == "ExternalRunnerTestInfo":
            test_info = p
            break

    if test_info != None:
        new_test_info = ExternalRunnerTestInfo(
            type = test_info.type,
            command = test_info.command,
            env = test_info.env,
            labels = test_info.labels,
            contacts = test_info.contacts,
            default_executor = test_info.default_executor,
            executor_overrides = test_info.executor_overrides,
            run_from_project_root = test_info.run_from_project_root,
            use_project_relative_paths = test_info.use_project_relative_paths,
            supports_test_execution_caching = True,  # <-- Enable Caching
        )
        for p in providers:
            if type(p) == "ExternalRunnerTestInfo":
                new_providers.append(new_test_info)
            else:
                new_providers.append(p)
    else:
        new_providers = providers
        
    return new_providers

cached_rust_test = rule(
    impl = _cached_rust_test_impl,
    attrs = rust_test.attrs,
    uses_plugins = rust_test.uses_plugins,
    supports_incoming_transition = getattr(rust_test, "supports_incoming_transition", True),
)
```

Then, import and use this wrapper in your `BUCK` files:

```python
load("//rules:cached_rust_test.bzl", "cached_rust_test")

cached_rust_test(
    name = "my_cached_test",
    srcs = ["src/lib.rs"],
    edition = "2024",
)
```

## Layout

| Path | What |
| --- | --- |
| `src/` | The runner (scheduler, translator, transport, result/verdict, duration DB, …). |
| `proto/` | Vendored Buck2 test-protocol `.proto`s + `regenerate.sh`. Bindings are pre-generated into `src/proto/gen/` (no `protoc` at build time). |
| `tests/` | `scheduler_integration.rs` — end-to-end fan-out/verdict/retry tests against an in-process orchestrator. |
| `nix/` | The nix↔Buck2 toolchain bridge: `flake.nix` exposes rustc/clang/llvm, `remote_flake.bzl` wraps them as Buck2 `RunInfo` targets (`//nix:*`), `buck2.nix` pins the Buck2 binary. |
| `toolchains/` | Buck2 toolchains: nix-backed `rust` + `cxx`, plus the prelude's system genrule/python/test toolchains. |
| `third-party/` | reindeer-generated `BUCK` (`http_archive` per crate, fetched from crates.io at build time) + `Cargo.toml`/`Cargo.lock` + per-crate `fixups/`. |

## Two build graphs

quokka builds two ways. Both use the **same** pinned nightly rustc.

### Cargo

```sh
cargo build          # or: just cargo-build
cargo test           # or: just cargo-test
```

### Buck2 (nix toolchains)

The Buck2 graph compiles rustc/clang/llvm from `nix/flake.nix` (the same nix
buck2 support nobie uses — wrapper scripts that lazily `nix build` the toolchain
and exec the resolved `/nix/store` binary), and links against the nix-provided
Apple SDK / libiconv on macOS.

```sh
just build           # buck2 build //:quokka
just test            # buck2 test //:quokka-lib-test //:scheduler_integration
just build-runner    # build + drop the binary at .tmp/quokka for a host repo's v2_test_executor
```

## Development environment

The repo ships a [devenv](https://devenv.sh) shell that provides Buck2,
reindeer, the pinned nightly toolchain, clang/llvm, protobuf, and jj/just:

```sh
devenv shell         # or use direnv
```

## Regenerating generated inputs

```sh
just regen-third-party   # reindeer vendor + buckify  (after editing third-party/Cargo.toml)
just regen-protos        # re-run proto/regenerate.sh (after editing proto/*.proto)
```

`third-party/` build-script and `include_str!` decisions live in
`third-party/fixups/<crate>/fixups.toml`.
