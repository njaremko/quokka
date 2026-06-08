{
  description = "Toolchain packages for quokka's Buck2 graph (consumed by nix/BUCK via remote_flake).";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils/main";
    rust-overlay.url = "github:oxalica/rust-overlay/master";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      rust-overlay,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
          config.allowUnfree = true;
        };

        # Pinned nightly, matching devenv.nix's `languages.rust.version`. The
        # `default` profile bundles rustc + cargo + clippy-driver + rustdoc, so a
        # single derivation backs both the `rustc` and `clippy` Buck2 targets.
        rustToolchain = pkgs.rust-bin.nightly."2026-02-03".default.override {
          extensions = [
            "rust-src"
            "rust-analyzer"
          ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        # Pinned Buck2 release (2026-05-18, commit 3f054b09…) the vendored
        # test-runner protos are wire-pinned against. See nix/buck2.nix.
        buck2 = pkgs.callPackage ./buck2.nix { };
      in
      {
        packages = {
          default = rustToolchain;
          rustc = rustToolchain;
          clippy = rustToolchain;
          clang = pkgs.llvmPackages.clang;
          llvm = pkgs.llvm;
          libiconv = pkgs.libiconv;
          buck2 = buck2;
        };

        devShells.default = pkgs.mkShell {
          packages = [
            rustToolchain
            buck2
            pkgs.llvmPackages.clang
            pkgs.llvm
            pkgs.protobuf
            pkgs.reindeer
            pkgs.jujutsu
            pkgs.just
          ];
        };
      }
    );
}
