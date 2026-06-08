load("@prelude//decls/common.bzl", "buck")
load("@prelude//os_lookup:defs.bzl", "Os", "OsLookup")

def _flake_system(target_os_info):
    if target_os_info.os == Os("linux"):
        os = "linux"
    elif target_os_info.os == Os("macos"):
        os = "darwin"
    else:
        fail("host os not supported: {}".format(target_os_info.os))

    cpu_value = target_os_info.cpu
    if not cpu_value:
        host_arch = host_info().arch
        if host_arch.is_aarch64:
            cpu_value = "arm64"
        elif host_arch.is_arm:
            cpu_value = "arm32"
        elif host_arch.is_i386:
            cpu_value = "x86_32"
        elif host_arch.is_riscv64:
            cpu_value = "riscv64"
        else:
            cpu_value = "x86_64"

    if cpu_value == "x86_64":
        cpu = "x86_64"
    elif cpu_value == "arm64":
        cpu = "aarch64"
    else:
        fail("host arch is not supported: {}".format(cpu_value))

    return "{}-{}".format(cpu, os)

def _flake_attribute(package_set, system, package, output):
    return "{}.{}.{}.{}".format(package_set, system, package, output)

def _flake_package_impl(ctx, path, package_set, package, output, binary, binaries, target_os_info):
    attribute = _flake_attribute(package_set, _flake_system(target_os_info), package, output)
    out = ctx.actions.declare_output("out" if output == "out" else "out-" + output, dir = True)

    wrapper_bins = []
    if binary:
        wrapper_bins.append(binary)
    for bin in binaries:
        if bin not in wrapper_bins:
            wrapper_bins.append(bin)

    # Force the wrapper's `nix build` to talk to the Nix daemon. The CI agent's
    # environment hook exports `NIX_REMOTE=local` (a leftover from when the
    # agent had a single-user, daemon-less Nix install), and Buck actions
    # inherit that. With NIX_REMOTE=local the client tries to grab
    # /nix/var/nix/db/big-lock directly; the buildkite-agent user does not own
    # it under the current multi-user Determinate install, so the wrapper
    # fails with `opening lock file '/nix/var/nix/db/big-lock': Permission
    # denied`. The daemon socket is already up by the time any action runs
    # (the pre-command hook owns that), so the wrapper just needs to override
    # the inherited NIX_REMOTE.
    wrapper_script = """
set -eu
out="$1"
attr="$2"
flake="$3"
rm -rf "$out"
mkdir -p "$out/bin" "$out/flake"
cp -R "$flake/." "$out/flake"
printf '%s\n' "$attr" > "$out/nix-attribute"
""" + "\n".join([
        "cat > \"$out/bin/{bin}\" <<'EOF'\n#!/bin/sh\nset -eu\nself=\"$0\"\ncase \"$self\" in\n  */*) ;;\n  *) self=\"$(command -v \"$self\")\" ;;\nesac\nbin_dir=\"$(CDPATH= cd -- \"$(dirname -- \"$self\")\" && pwd -P)\"\nroot=\"$(CDPATH= cd -- \"$bin_dir/..\" && pwd -P)\"\ntmp=\"${{TMPDIR:-/tmp}}/nobie-nix-tool-$$\"\nrm -rf \"$tmp\"\nmkdir -p \"$tmp\"\ntrap 'rm -rf \"$tmp\"' EXIT HUP INT TERM\nif [ -S /nix/var/nix/daemon-socket/socket ]; then NIX_REMOTE=daemon; export NIX_REMOTE; fi\nnix --extra-experimental-features 'nix-command flakes' build --out-link \"$tmp/result\" \"path:$root/flake#{attribute}\"\nstore=\"$(readlink \"$tmp/result\")\"\ncase \"$store\" in\n  /nix/store/*) ;;\n  *) echo \"nix output did not resolve into /nix/store: $store\" >&2; exit 1 ;;\nesac\n\"$store/bin/{bin}\" \"$@\"\nexit $?\nEOF\nchmod +x \"$out/bin/{bin}\"".format(attribute = attribute, bin = bin)
        for bin in wrapper_bins
    ]) + """
"""

    ctx.actions.run(
        cmd_args([
            "sh",
            "-c",
            wrapper_script,
            "--",
            out.as_output(),
            attribute,
            path,
        ]),
        category = "nix_flake",
    )

    run_info = []
    if binary:
        run_info.append(RunInfo(args = cmd_args(out, "bin", binary, delimiter = "/")))

    sub_targets = {
        bin: [DefaultInfo(default_output = out), RunInfo(args = cmd_args(out, "bin", bin, delimiter = "/"))]
        for bin in binaries
    }

    return [DefaultInfo(default_output = out, sub_targets = sub_targets)] + run_info

def _required_real_dirs(sub_targets):
    # Buck's `project()` refuses to descend through a symlink, so every
    # directory it must traverse to reach a sub_target has to be a real
    # directory in the output. That is the set of: every strict-ancestor
    # directory of each sub_target path, plus any sub_target path that is itself
    # an ancestor of a deeper sub_target. Everything outside this set stays a
    # symlink into the immutable /nix/store path, so the bulk content (e.g. the
    # whole MacOSX.sdk header tree) is referenced by a single link, never copied.
    real = {".": True}
    paths = list(sub_targets.values())
    for p in paths:
        components = p.split("/")
        for i in range(1, len(components)):
            real["/".join(components[:i])] = True
    for p in paths:
        for q in paths:
            if q != p and (q + "/").startswith(p + "/"):
                real[p] = True
    return sorted(real.keys())

def _flake_output_impl(ctx, path, package_set, package, output, sub_targets, target_os_info):
    attribute = _flake_attribute(package_set, _flake_system(target_os_info), package, output)
    out = ctx.actions.declare_output("out" if output == "out" else "out-" + output, dir = True)

    # See `wrapper_script` above for the rationale on forcing NIX_REMOTE=daemon
    # when the daemon socket is present.
    #
    # The output references the resolved /nix/store path by symlink instead of
    # materializing a copy. The store path is immutable and content-addressed,
    # so a symlink uniquely identifies its contents; deep-copying it duplicates
    # gigabytes (e.g. the Apple SDK) into buck-out on every build, leaves an
    # un-deletable read-only tree, and `cp -L` cannot even traverse the SDK's
    # cyclic header symlinks. Buck declares `$out` as `dir = True` and forbids
    # projecting a sub_target through a symlink, so we build a real directory
    # skeleton covering only the paths Buck must traverse (see
    # `_required_real_dirs`) and symlink every other entry straight into the
    # store. These `output` targets are consumed only as `$(location ...)`
    # sysroot/lib paths on macOS host builds, where /nix/store is always
    # present, so the links resolve.
    real_dirs = "\n".join(_required_real_dirs(sub_targets))

    script = """
set -eu
out="$1"
attr="$2"
flake="$3"
real_dirs="$4"
tmp="${TMPDIR:-/tmp}/nobie-nix-output-$$"
rm -rf "$tmp"
mkdir -p "$tmp/flake"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
cp -R "$flake/." "$tmp/flake"
if [ -S /nix/var/nix/daemon-socket/socket ]; then NIX_REMOTE=daemon; export NIX_REMOTE; fi
nix --extra-experimental-features 'nix-command flakes' build --out-link "$tmp/result" "path:$tmp/flake#$attr"
store="$(readlink "$tmp/result")"
case "$store" in
  /nix/store/*) ;;
  *) echo "nix output did not resolve into /nix/store: $store" >&2; exit 1 ;;
esac
rm -rf "$out"
mkdir -p "$out"
# Create the real-directory skeleton first so the symlink pass can recognise
# (and skip) those directories rather than clobbering them with a symlink.
printf '%s\\n' "$real_dirs" | while IFS= read -r d; do
  [ -n "$d" ] || continue
  if [ "$d" = "." ]; then mkdir -p "$out"; else mkdir -p "$out/$d"; fi
done
printf '%s\\n' "$real_dirs" | while IFS= read -r d; do
  [ -n "$d" ] || continue
  if [ "$d" = "." ]; then src="$store"; dst="$out"; else src="$store/$d"; dst="$out/$d"; fi
  for entry in "$src"/* "$src"/.[!.]* "$src"/..?*; do
    [ -e "$entry" ] || [ -L "$entry" ] || continue
    target="$dst/${entry##*/}"
    if [ -d "$target" ] && [ ! -L "$target" ]; then continue; fi
    ln -s "$entry" "$target"
  done
done
"""

    ctx.actions.run(
        cmd_args([
            "sh",
            "-c",
            script,
            "--",
            out.as_output(),
            attribute,
            path,
            real_dirs,
        ]),
        category = "nix_flake_output",
    )

    return [
        DefaultInfo(
            default_output = out,
            sub_targets = {
                name: [DefaultInfo(default_output = out.project(path))]
                for name, path in sub_targets.items()
            },
        ),
    ]

_common_attrs = {
    "binary": attrs.option(attrs.string(), default = None),
    "binaries": attrs.list(attrs.string(), default = []),
    "output": attrs.string(default = "out"),
    "package": attrs.option(attrs.string(), default = None),
    "path": attrs.source(allow_directory = True),
    "_target_os_type": buck.target_os_type_arg(),
}

_package = rule(
    impl = lambda ctx: _flake_package_impl(
        ctx,
        ctx.attrs.path,
        "packages",
        ctx.attrs.package or ctx.label.name,
        ctx.attrs.output,
        ctx.attrs.binary,
        ctx.attrs.binaries,
        ctx.attrs._target_os_type[OsLookup],
    ),
    attrs = _common_attrs,
)

_output = rule(
    impl = lambda ctx: _flake_output_impl(
        ctx,
        ctx.attrs.path,
        "packages",
        ctx.attrs.package or ctx.label.name,
        ctx.attrs.output,
        ctx.attrs.sub_targets,
        ctx.attrs._target_os_type[OsLookup],
    ),
    attrs = {
        "output": attrs.string(default = "out"),
        "package": attrs.option(attrs.string(), default = None),
        "path": attrs.source(allow_directory = True),
        "sub_targets": attrs.dict(key = attrs.string(), value = attrs.string(), default = {}),
        "_target_os_type": buck.target_os_type_arg(),
    },
)

remote_flake = struct(
    output = _output,
    package = _package,
)
