#!/usr/bin/env bash
# Regenerate the checked-in tonic/prost bindings in ../src/proto/gen/.
#
# We pre-generate (rather than running tonic-build from a build.rs) so the crate
# builds in the Buck2 graph with no protoc/codegen step. Run this only when a
# .proto in this directory changes. Requires `protoc` and `cargo` on PATH.
#
# The tonic/prost versions here MUST match the runtime versions pinned in the
# crate's Cargo.toml, or the generated code will not compile against them.
set -euo pipefail

proto_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
crate_dir="$(cd "${proto_dir}/.." && pwd)"
gen_dir="${crate_dir}/src/proto/gen"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

cat > "${work}/Cargo.toml" <<'EOF'
[package]
name = "quokka-protogen"
version = "0.0.0"
edition = "2021"

[dependencies]
tonic = "0.12"
prost = "0.13"
prost-types = "0.13"

[build-dependencies]
tonic-build = "0.12"
EOF

mkdir -p "${work}/src"
echo 'fn main() {}' > "${work}/src/main.rs"

cat > "${work}/build.rs" <<'EOF'
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::path::PathBuf::from(std::env::var("PROTOGEN_OUT").unwrap());
    let dir = std::env::var("PROTOGEN_PROTO_DIR").unwrap();
    std::fs::create_dir_all(&out)?;
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out)
        .compile_protos(
            &[
                format!("{dir}/test.proto"),
                format!("{dir}/data.proto"),
                format!("{dir}/host_sharing.proto"),
                format!("{dir}/downward_api.proto"),
            ],
            &[dir],
        )?;
    Ok(())
}
EOF

PROTOGEN_OUT="${work}/out" PROTOGEN_PROTO_DIR="${proto_dir}" \
    cargo build --manifest-path "${work}/Cargo.toml"

mkdir -p "${gen_dir}"
cp "${work}/out/buck.test.rs" "${gen_dir}/test.rs"
cp "${work}/out/buck.data.rs" "${gen_dir}/data.rs"
cp "${work}/out/buck.host_sharing.rs" "${gen_dir}/host_sharing.rs"
cp "${work}/out/buck.downward_api.rs" "${gen_dir}/downward_api.rs"

echo "Regenerated bindings in ${gen_dir}"
