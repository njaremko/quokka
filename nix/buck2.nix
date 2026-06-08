{ buck2, fetchurl, lib, stdenv }:

let
  version = "2026-05-18";

  hashes = {
    aarch64-darwin = {
      buck2 = "sha256-vsKMz6KMT7QgouL5YM8GOQEpzv8F8UoEPTF2gnRg37I=";
      rust-project = "sha256-ebzPxNkfjxFL5esUMB0RRvaxLz0N92NOXlIOa8w+uy8=";
    };
    x86_64-darwin = {
      buck2 = "sha256-kPJxiZvDEdyrAJvx9/01Lj3uCwkZiVMtxwwBcsCBP7k=";
      rust-project = "sha256-ZjW7tGjVHY+yAYqrZ+0dhx7yHq611mDg+TWXsqe5fuk=";
    };
    aarch64-linux = {
      buck2 = "sha256-CEuX4ypWBtZVxc93zFYHWWYaDRxCttyVhpwfy3e4+z4=";
      rust-project = "sha256-SrT81tzO5BXa7pQVfrQbtQpyYG+HGZUJNnkOB1S97ZU=";
    };
    x86_64-linux = {
      buck2 = "sha256-p+ZahhpH9GLncgvYxZ7iYuIzTv1jWox8DzINDABl3VE=";
      rust-project = "sha256-/7CI14kysFtw9qd81vLjM71giEq7jExEqbQG308sbbo=";
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
