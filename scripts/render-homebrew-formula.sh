#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <version-tag> <checksums.txt>" >&2
  exit 1
fi

version_tag="$1"
checksums_file="$2"

if [[ ! -f "$checksums_file" ]]; then
  echo "checksums file not found: $checksums_file" >&2
  exit 1
fi

version="${version_tag#v}"
repo="https://github.com/hollis-labs/hadron/releases/download/${version_tag}"

sha_for() {
  local artifact="$1"
  awk -v name="$artifact" '$2 == name { print $1 }' "$checksums_file"
}

darwin_amd64="hadron_${version_tag}_darwin_amd64.tar.gz"
darwin_arm64="hadron_${version_tag}_darwin_arm64.tar.gz"
linux_amd64="hadron_${version_tag}_linux_amd64.tar.gz"
linux_arm64="hadron_${version_tag}_linux_arm64.tar.gz"

darwin_amd64_sha="$(sha_for "$darwin_amd64")"
darwin_arm64_sha="$(sha_for "$darwin_arm64")"
linux_amd64_sha="$(sha_for "$linux_amd64")"
linux_arm64_sha="$(sha_for "$linux_arm64")"

for value in \
  "$darwin_amd64_sha" \
  "$darwin_arm64_sha" \
  "$linux_amd64_sha" \
  "$linux_arm64_sha"; do
  if [[ -z "$value" ]]; then
    echo "missing checksum entry in $checksums_file" >&2
    exit 1
  fi
done

cat <<EOF
# typed: false
# frozen_string_literal: true

class Hadron < Formula
  desc "Local-first, agent-first blueprint automation runner"
  homepage "https://github.com/hollis-labs/hadron"
  version "${version}"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "${repo}/${darwin_arm64}"
      sha256 "${darwin_arm64_sha}"
    else
      url "${repo}/${darwin_amd64}"
      sha256 "${darwin_amd64_sha}"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "${repo}/${linux_arm64}"
      sha256 "${linux_arm64_sha}"
    else
      url "${repo}/${linux_amd64}"
      sha256 "${linux_amd64_sha}"
    end
  end

  def install
    bin.install "hadron", "hadrond"
    doc.install "README.md", "LICENSE"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hadron version")
    assert_match version.to_s, shell_output("#{bin}/hadrond version")
  end

  livecheck do
    url :stable
    strategy :github_latest
  end
end
EOF
