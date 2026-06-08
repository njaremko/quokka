#!/usr/bin/env python3
import urllib.request
import json
import subprocess
import re
import sys
from pathlib import Path

def get_latest_version():
    req = urllib.request.Request("https://api.github.com/repos/facebook/buck2/releases")
    with urllib.request.urlopen(req) as response:
        releases = json.loads(response.read())
        for r in releases:
            if r['tag_name'] != 'latest':
                return r['tag_name']
    raise Exception("Could not find latest version")

def get_hash(version, artifact, platform):
    url = f"https://github.com/facebook/buck2/releases/download/{version}/{artifact}-{platform}.zst"
    try:
        # Get base32 hash
        base32_hash = subprocess.check_output(
            ["nix-prefetch-url", url],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        
        # Convert to SRI
        sri_hash = subprocess.check_output(
            ["nix", "hash", "convert", "--to", "sri", "--hash-algo", "sha256", base32_hash],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        return sri_hash
    except subprocess.CalledProcessError as e:
        print(f"Error fetching hash for {url}", file=sys.stderr)
        raise e

platforms = {
    "aarch64-darwin": "aarch64-apple-darwin",
    "x86_64-darwin": "x86_64-apple-darwin",
    "aarch64-linux": "aarch64-unknown-linux-gnu",
    "x86_64-linux": "x86_64-unknown-linux-gnu",
}

def main():
    print("Fetching latest buck2 release version...")
    latest_version = get_latest_version()
    print(f"Latest version is {latest_version}")

    nix_file = Path(__file__).parent / "buck2.nix"
    content = nix_file.read_text()

    # Update version at the top
    # We want to replace `version = "YYYY-MM-DD";`
    content = re.sub(r'version = "\d{4}-\d{2}-\d{2}";', f'version = "{latest_version}";', content)

    for arch, platform in platforms.items():
        print(f"Processing {arch} ({platform})...")
        
        buck2_hash = get_hash(latest_version, "buck2", platform)
        print(f"  buck2 hash: {buck2_hash}")
        
        rust_project_hash = get_hash(latest_version, "rust-project", platform)
        print(f"  rust-project hash: {rust_project_hash}")
        
        # Update hashes in the block for this arch
        # Regex to match the block:
        # arch = {
        #   buck2 = "sha256-...";
        #   rust-project = "sha256-...";
        # };
        
        pattern = r'(' + re.escape(arch) + r'\s*=\s*\{.*?buck2\s*=\s*")[^"]+(".*?rust-project\s*=\s*")[^"]+(".*?\};)'
        def replacer(match):
            return f"{match.group(1)}{buck2_hash}{match.group(2)}{rust_project_hash}{match.group(3)}"
            
        content = re.sub(pattern, replacer, content, flags=re.DOTALL)

    nix_file.write_text(content)
    print(f"Done updating buck2.nix to {latest_version}!")

if __name__ == "__main__":
    main()
