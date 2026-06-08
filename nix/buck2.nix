{ buck2, fetchurl, lib, stdenv }:

let
  version = "2026-06-01";

  hashes = {
    aarch64-darwin = {
      buck2 = "sha256-e4Hvd5PAuzq8HhSusRHgbyn6Kb2HF6NnUrMXugVz1TM=";
      rust-project = "sha256-YKn7RwUR0maS1KmpDTbAHZmNMvRRXb1PPyWj7K3cZH8=";
    };
    x86_64-darwin = {
      buck2 = "sha256-QYwhtHHpuocQ5mW2I7kLoV3insKx/1h7LMYusU3UgEI=";
      rust-project = "sha256-GCtZ+57/i4+G1M5U4nDwNBQDp9sKcCexjooBLrqhfMk=";
    };
    aarch64-linux = {
      buck2 = "sha256-YjrzGKROO4Ghe/FmDCPdWnXKhoZyfD8PlMjoymlNrKE=";
      rust-project = "sha256-SZn/bXUsb4HSLNimWQSnvQ0IXTQJtvDLCUEx36dX8eI=";
    };
    x86_64-linux = {
      buck2 = "sha256-TdmuVMh/3PeVEBB0+HiCMq9VUjiFE11eM1jHc2WZNVU=";
      rust-project = "sha256-ERXOL1zOskOkys0XBpyfWntGtSfr6nCbvMDKl+ul9b0=";
    };
  };

  platformSuffix = {
    aarch64-darwin = "aarch64-apple-darwin";
    x86_64-darwin = "x86_64-apple-darwin";
    aarch64-linux = "aarch64-unknown-linux-gnu";
    x86_64-linux = "x86_64-unknown-linux-gnu";
  }.${stdenv.hostPlatform.system};

  archHashes = hashes.${stdenv.hostPlatform.system};
in
buck2.overrideAttrs (_old: {
  version = "unstable-${version}";

  srcs = [
    (fetchurl {
      url = "https://github.com/facebook/buck2/releases/download/${version}/buck2-${platformSuffix}.zst";
      hash = archHashes.buck2;
    })
    (fetchurl {
      url = "https://github.com/facebook/buck2/releases/download/${version}/rust-project-${platformSuffix}.zst";
      hash = archHashes.rust-project;
    })
  ];
})
