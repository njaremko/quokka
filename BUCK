# quokka — a TPX-style external test runner for Buck2, specialized for Rust
# tests. Built here with the nix-backed toolchains in //toolchains and the
# reindeer-vendored deps in //third-party. See README.md and DESIGN.md.

# Direct third-party dependencies (reindeer aliases under //third-party).
THIRD_PARTY = [
    "//third-party:anyhow",
    "//third-party:async-trait",
    "//third-party:clap",
    "//third-party:futures",
    "//third-party:hyper-util",
    "//third-party:parking_lot",
    "//third-party:prost",
    "//third-party:prost-types",
    "//third-party:rustc-hash",
    "//third-party:serde",
    "//third-party:serde_json",
    "//third-party:thiserror",
    "//third-party:tokio",
    "//third-party:toml",
    "//third-party:tonic",
    "//third-party:tower",
    "//third-party:tracing",
]

_LIB_SRCS = glob(["src/**/*.rs"])
_RESOURCE_SRCS = ["src/resource_supervisor.sh"]

rust_library(
    name = "quokka-lib",
    srcs = _LIB_SRCS + _RESOURCE_SRCS,
    resources = ["src/resource_supervisor.sh"],
    crate = "quokka",
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = THIRD_PARTY,
    visibility = ["PUBLIC"],
)

rust_binary(
    name = "quokka",
    srcs = ["src/main.rs"],
    crate = "quokka",
    crate_root = "src/main.rs",
    edition = "2024",
    deps = [":quokka-lib", "//third-party:tokio", "//third-party:clap"],
    visibility = ["PUBLIC"],
)

load("//rules:cached_rust_test.bzl", "cached_rust_test")

# Unit tests live inside the library modules; compile the crate with `--test`.
cached_rust_test(
    name = "quokka-lib-test",
    srcs = _LIB_SRCS + _RESOURCE_SRCS,
    resources = ["src/resource_supervisor.sh"],
    crate = "quokka",
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = THIRD_PARTY,
    visibility = ["PUBLIC"],
)

cached_rust_test(
    name = "scheduler_integration",
    srcs = ["tests/scheduler_integration.rs"],
    crate_root = "tests/scheduler_integration.rs",
    edition = "2024",
    deps = [":quokka-lib"] + THIRD_PARTY,
    visibility = ["PUBLIC"],
)

cached_rust_test(
    name = "resource_supervisor_watchdog_test",
    srcs = ["tests/resource_supervisor_watchdog_test.rs"] + _RESOURCE_SRCS,
    resources = ["src/resource_supervisor.sh"],
    crate_root = "tests/resource_supervisor_watchdog_test.rs",
    edition = "2024",
    deps = [":quokka-lib"] + THIRD_PARTY,
    visibility = ["PUBLIC"],
)

