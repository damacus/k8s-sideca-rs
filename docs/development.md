# Development

## Source layout

```text
src/main.rs    startup, signal handling, task supervision
src/config.rs  env + CLI parsing and validation
src/watch.rs   kube-rs watchers/listers -> bounded queue -> reconcile loop
src/files.rs   path safety, atomic writes, ownership manifest
src/reload.rs  callback generations and retries
src/health.rs  /healthz readiness + liveness
```

## Build

```shell
cargo build --release
docker buildx build --platform linux/amd64,linux/arm64 .
```

The image is `scratch` + a static musl binary (ring TLS, embedded
`webpki-roots` — outbound HTTPS works without a CA bundle in the image).

## Docs

This site is built with [Zensical](https://zensical.org). Source lives in
`docs/`; `zensical.toml` holds the site config.

```shell
uvx zensical serve   # local preview with live reload
uvx zensical build   # static output in site/
```

Pushes to `main` deploy to GitHub Pages via `.github/workflows/docs.yml`.
