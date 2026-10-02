# Contributing

## Ground rules

- **TDD**: every production change starts as a failing test. Integration
  coverage for Kubernetes paths goes in `tests/mock_api.rs` — no real
  cluster needed.
- `cargo fmt`, `cargo clippy --locked --all-targets -- -D warnings` and
  `cargo test --locked` must pass before merge (CI enforces).
- Conventional Commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`) —
  release-please derives versions and the changelog from them.
- Markdown is linted with `markdownlint-cli2` (project config at the root).
- Never commit Secret values, private URLs, or deployment details — see
  `docs/deployment-profiles.md` for the sanitisation pattern.

## Behaviour contract

Compatibility target is upstream `kiwigrid/k8s-sidecar` **2.11.2**.
Intentional deviations (hardening, reliability fixes) are fine — document
them in README "Deliberate differences" and cover them with tests. Do not
replicate upstream bugs.

## Releases

Releases are automated: merge the release-please PR on `main` to cut a
versioned release; the publish workflow pushes multi-arch images to
`ghcr.io/damacus/k8s-sidecar-rs` with semver tags, `sha-*`, an SBOM and
build provenance attestation.
