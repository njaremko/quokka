load("@prelude//rust:rust_toolchain.bzl", "PanicRuntime", "RustToolchainInfo")

_DEFAULT_TRIPLE = select({
    "prelude//os:linux": select({
        "prelude//cpu:arm64": "aarch64-unknown-linux-gnu",
        "prelude//cpu:riscv64": "riscv64gc-unknown-linux-gnu",
        "prelude//cpu:x86_64": "x86_64-unknown-linux-gnu",
    }),
    "prelude//os:macos": select({
        "prelude//cpu:arm64": "aarch64-apple-darwin",
        "prelude//cpu:x86_64": "x86_64-apple-darwin",
    }),
    "prelude//os:windows": select({
        "prelude//cpu:arm64": select({
            "DEFAULT": "aarch64-pc-windows-msvc",
            "prelude//abi:gnu": "aarch64-pc-windows-gnu",
            "prelude//abi:msvc": "aarch64-pc-windows-msvc",
        }),
        "prelude//cpu:x86_64": select({
            "DEFAULT": "x86_64-pc-windows-msvc",
            "prelude//abi:gnu": "x86_64-pc-windows-gnu",
            "prelude//abi:msvc": "x86_64-pc-windows-msvc",
        }),
    }),
})

def _quokka_nix_rust_toolchain_impl(ctx):
    return [
        DefaultInfo(),
        RustToolchainInfo(
            allow_lints = ctx.attrs.allow_lints,
            clippy_driver = ctx.attrs.clippy_driver[RunInfo],
            clippy_toml = ctx.attrs.clippy_toml[DefaultInfo].default_outputs[0] if ctx.attrs.clippy_toml else None,
            compiler = ctx.attrs.compiler[RunInfo],
            default_edition = ctx.attrs.default_edition,
            deny_lints = ctx.attrs.deny_lints,
            doctests = ctx.attrs.doctests,
            extra_rustc_flags = ctx.attrs.extra_rustc_flags,
            nightly_features = ctx.attrs.nightly_features,
            panic_runtime = PanicRuntime("unwind"),
            report_unused_deps = ctx.attrs.report_unused_deps,
            rustc_binary_flags = ctx.attrs.rustc_binary_flags,
            rustc_env = ctx.attrs.rustc_env,
            rustc_flags = ctx.attrs.rustc_flags,
            rustc_target_triple = ctx.attrs.rustc_target_triple,
            rustc_test_flags = ctx.attrs.rustc_test_flags,
            rustdoc = ctx.attrs.rustdoc[RunInfo],
            rustdoc_env = ctx.attrs.rustdoc_env,
            rustdoc_flags = ctx.attrs.rustdoc_flags,
            warn_lints = ctx.attrs.warn_lints,
        ),
    ]

# A Rust toolchain whose rustc/rustdoc/clippy-driver are the nix-provided
# binaries wrapped by `//nix:rustc` / `//nix:clippy` (see nix/remote_flake.bzl).
quokka_nix_rust_toolchain = rule(
    impl = _quokka_nix_rust_toolchain_impl,
    attrs = {
        "allow_lints": attrs.list(attrs.string(), default = []),
        "clippy_driver": attrs.exec_dep(providers = [RunInfo]),
        "clippy_toml": attrs.option(attrs.dep(providers = [DefaultInfo]), default = None),
        "compiler": attrs.exec_dep(providers = [RunInfo]),
        "default_edition": attrs.option(attrs.string(), default = None),
        "deny_lints": attrs.list(attrs.string(), default = []),
        "doctests": attrs.bool(default = False),
        "extra_rustc_flags": attrs.list(attrs.arg(), default = []),
        "nightly_features": attrs.bool(default = False),
        "report_unused_deps": attrs.bool(default = False),
        "rustc_binary_flags": attrs.list(attrs.arg(), default = []),
        "rustc_env": attrs.dict(key = attrs.string(), value = attrs.arg(), default = {}),
        "rustc_flags": attrs.list(attrs.arg(), default = []),
        "rustc_target_triple": attrs.string(default = _DEFAULT_TRIPLE),
        "rustc_test_flags": attrs.list(attrs.arg(), default = []),
        "rustdoc": attrs.exec_dep(providers = [RunInfo]),
        "rustdoc_env": attrs.dict(key = attrs.string(), value = attrs.arg(), default = {}),
        "rustdoc_flags": attrs.list(attrs.arg(), default = []),
        "restricted_rustc_flags": attrs.list(attrs.arg(), default = []),
        "warn_lints": attrs.list(attrs.string(), default = []),
    },
    is_toolchain_rule = True,
)
