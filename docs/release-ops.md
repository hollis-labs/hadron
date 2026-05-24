# Release Operations

This document covers the current public beta release flow for Hadron.

## Release Shape

Tagged releases produce:

- GitHub release tarballs for `hadron` and `hadrond`
- `checksums.txt`
- CI verification before publish

Current artifact targets:

- macOS `amd64`
- macOS `arm64`
- Linux `amd64`
- Linux `arm64`

## Cut A Release

1. Ensure `main` is green.
2. Create and push a tag:

```sh
git tag -a v0.4.2-beta.1 -m "Hadron v0.4.2-beta.1"
git push origin v0.4.2-beta.1
```

3. GitHub Actions runs `.github/workflows/release.yml`.
4. Verify the release page contains:
   - the four tarballs
   - `checksums.txt`

## Build Artifacts Locally

```sh
make package-release VERSION=v0.4.2-beta.1
```

This writes archives and checksums to `dist/`.

## Homebrew Tap Update

Hadron’s tap lives in `hollis-labs/homebrew-tap`.

Render the formula from release checksums:

```sh
scripts/render-homebrew-formula.sh v0.4.2-beta.1 dist/checksums.txt
```

Then update `Formula/hadron.rb` in the tap repo with the rendered content.

## Important Constraint

The Homebrew tap formula requires Hadron release assets to be publicly
downloadable.

If:

- the Hadron repo is private, or
- release assets are not public

then `brew install hollis-labs/tap/hadron` will fail for end users even if the
formula itself is valid.

That means the practical order is:

1. make the repo and release assets public
2. confirm release asset URLs work without authentication
3. publish or update the tap formula
