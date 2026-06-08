{ pkgs, lib, ... }:

{
  # Buck2's local link/compile actions shell out to a C toolchain; expose the
  # nix library/header search paths the same way nobie's devenv does.
  env = {
    BAZEL_LINKOPTS = "-L${pkgs.libiconv}/lib";
  };

  # https://devenv.sh/packages/
  packages = [
    # The pinned Buck2 binary (2026-05-18, commit 3f054b09…) the vendored
    # test-runner protos are wire-pinned against. The prelude is bundled inside
    # the binary (`[external_cells] prelude = bundled` in .buckconfig).
    (pkgs.callPackage ./nix/buck2.nix { })

    # Toolchain + binutils. These also back the nix//:* Buck2 toolchain targets
    # via nix/flake.nix, so building under Buck2 and building under Cargo use the
    # same compiler.
    pkgs.llvmPackages.clang
    pkgs.llvm
    pkgs.lld
    pkgs.libiconv

    # Third-party buckification (reindeer) + protobuf for regenerating the
    # vendored test-runner protos (proto/regenerate.sh).
    pkgs.reindeer
    pkgs.protobuf

    # Dev workflow.
    pkgs.jujutsu
    pkgs.just
    pkgs.fd
    pkgs.ripgrep
  ];

  # https://devenv.sh/languages/
  # Nightly pinned to match nix/flake.nix's rust-bin channel, so the Cargo build
  # and the Buck2 build use the same rustc.
  languages.rust = {
    enable = true;
    channel = "nightly";
    version = "2026-02-03";
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-src"
      "rust-analyzer"
    ];
    targets = [ "wasm32-unknown-unknown" ];
  };

  dotenv.disableHint = true;

  enterShell =
    let
      libraryPath = lib.makeLibraryPath [
        pkgs.libiconv
        pkgs.zlib
      ];
    in
    ''
      if [ -z "''${QUOKKA_PROFILE_SOURCED:-}" ]; then
        export QUOKKA_PROFILE_SOURCED=1
        export LIBRARY_PATH=${libraryPath}:''${LIBRARY_PATH-}
      fi
    '';
}
