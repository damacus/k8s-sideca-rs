# k8s-sidecar-rs

Rust reimplementation of [kiwigrid/k8s-sidecar](https://github.com/kiwigrid/k8s-sidecar)
(pinned compatibility targets: upstream `1.30.2` and `2.5.0`).

Watches ConfigMaps and Secrets matching a label, writes their data to a shared
folder atomically, and optionally calls an HTTP endpoint when files change.

## Status

Port in progress — not yet released or deployed.

## Supported configuration

| Variable | Notes |
|---|---|
| `LABEL`, `LABEL_VALUE` | required label selector |
| `FOLDER` | required destination root |
| `FOLDER_ANNOTATION` | per-resource folder override (default `k8s-sidecar-target-directory`) |
| `FOLDER_PER_NAMESPACE` | append namespace to destination (upstream ≥ 2.11.0 feature) |
| `NAMESPACE` | comma list, `ALL`, or pod namespace default |
| `RESOURCE` | `configmap`, `secret`, `both` |
| `RESOURCE_NAME` | `name`, `kind/name`, `ns/kind/name`; forces SLEEP-style polling per namespace |
| `METHOD` | `LIST` (once, exit), `SLEEP` (poll every `SLEEP_TIME`), default WATCH |
| `SLEEP_TIME`, `ERROR_THROTTLE_SLEEP` | poll / error-backoff seconds |
| `REQ_URL`, `REQ_METHOD`, `REQ_PAYLOAD` | reload callback |
| `REQ_USERNAME`, `REQ_PASSWORD`, `REQ_USERNAME_FILE`, `REQ_PASSWORD_FILE`, `--req-username-file`, `--req-password-file` | basic auth; files re-read per attempt |
| `REQ_BASIC_AUTH_ENCODING` | `latin1` (default) or `utf-8` |
| `REQ_RETRY_*`, `REQ_TIMEOUT`, `REQ_SKIP_INIT`, `REQ_SKIP_TLS_VERIFY` | shared HTTP budget (also used for `*.url` downloads) |
| `ENABLE_5XX`, `UNIQUE_FILENAMES`, `DEFAULT_FILE_MODE` | |
| `SKIP_TLS_VERIFY`, `KUBECONFIG` | Kubernetes client |
| `WATCH_SERVER_TIMEOUT`, `WATCH_CLIENT_TIMEOUT` | server `timeoutSeconds` / client read timeout |
| `IGNORE_ALREADY_PROCESSED` | dedupe on resourceVersion |
| `HEALTH_PORT` | `/healthz` (default 8080) |
| `LOG_LEVEL`, `LOG_FORMAT`, `LOG_TZ` | `JSON`/`LOGFMT` |

## Deliberate differences from upstream

- **Atomic writes** — files are written to a temp path then renamed; upstream
  writes in place.
- **Ownership manifest** — `.k8s-sidecar-rs.manifest.json` tracks which files
  this sidecar owns, so a restart cleans owned stale files without touching
  unrelated files. Upstream leaves stale files across restarts.
- **Path traversal** — a relative `FOLDER_ANNOTATION` that escapes `FOLDER` is
  rejected; upstream allows it.
- **Failed `.url` fetch keeps the previous file** — upstream writes an empty
  file.
- **Failed reload callbacks stay pending** and are retried; upstream drops them
  after the retry budget is exhausted.
- **`SCRIPT` is unsupported** — startup fails loudly (no shell in the scratch
  image anyway).
- **`DISABLE_X509_STRICT_VERIFICATION` is unsupported** — rustls has no
  non-strict mode.
- **Liveness tracks per-stream API contact** — upstream's supervisor loop
  unconditionally refreshes the shared contact timestamp every 5s, so the
  check is ineffective at 2.5.0 (fixed upstream in 2.11.2; we use the fixed
  semantics).

## Layout

```
src/main.rs    startup, signal handling, task supervision
src/config.rs  env + CLI parsing and validation
src/watch.rs   kube-rs watchers/listers -> bounded queue -> reconcile loop
src/files.rs   path safety, atomic writes, ownership manifest
src/reload.rs  callback generations and retries
src/health.rs  /healthz readiness + liveness
```

## Build

```
cargo build --release
docker buildx build --platform linux/amd64,linux/arm64 .
```

The image is `scratch` + a static musl binary (ring TLS, embedded
`webpki-roots` — outbound HTTPS works without a CA bundle in the image).
