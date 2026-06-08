#!/usr/bin/env bash
set -eo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
FILE="$DIR/buck2.nix"

echo "Fetching latest buck2 release version..."
LATEST_VERSION=$(curl -s https://api.github.com/repos/facebook/buck2/releases | jq -r '.[].tag_name' | grep -v 'latest' | head -n 1)

if [[ -z "$LATEST_VERSION" ]]; then
  echo "Error: Could not determine latest version."
  exit 1
fi

echo "Latest version is $LATEST_VERSION"

# Update version in nix file
perl -pi -e "s/version = \"\d{4}-\d{2}-\d{2}\";/version = \"$LATEST_VERSION\";/" "$FILE"

get_hash() {
  local version=$1
  local artifact=$2
  local platform=$3
  local url="https://github.com/facebook/buck2/releases/download/${version}/${artifact}-${platform}.zst"
  
  # Fetch base32 hash using nix-prefetch-url
  local base32_hash
  base32_hash=$(nix-prefetch-url "$url" 2>/dev/null)
  
  if [[ -z "$base32_hash" ]]; then
    echo "Error: Failed to prefetch $url" >&2
    exit 1
  fi
  
  # Convert to SRI format
  nix hash convert --to sri --hash-algo sha256 "$base32_hash" 2>/dev/null
}

declare -A platforms=(
  ["aarch64-darwin"]="aarch64-apple-darwin"
  ["x86_64-darwin"]="x86_64-apple-darwin"
  ["aarch64-linux"]="aarch64-unknown-linux-gnu"
  ["x86_64-linux"]="x86_64-unknown-linux-gnu"
)

for arch in "${!platforms[@]}"; do
  platform="${platforms[$arch]}"
  echo "Processing $arch ($platform)..."
  
  buck2_hash=$(get_hash "$LATEST_VERSION" "buck2" "$platform")
  echo "  buck2 hash: $buck2_hash"
  perl -pi -e "s|buck2 = \".*\";|buck2 = \"$buck2_hash\";| if /^\s*$arch = \{/ ... /^\s*\};/" "$FILE"
  
  rust_project_hash=$(get_hash "$LATEST_VERSION" "rust-project" "$platform")
  echo "  rust-project hash: $rust_project_hash"
  perl -pi -e "s|rust-project = \".*\";|rust-project = \"$rust_project_hash\";| if /^\s*$arch = \{/ ... /^\s*\};/" "$FILE"
done

echo "Done updating buck2.nix to $LATEST_VERSION!"
