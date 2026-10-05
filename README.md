# k8s-sideca-rs

Rust reimplementation of [kiwigrid/k8s-sidecar](https://github.com/kiwigrid/k8s-sidecar)
(pinned compatibility target: upstream `2.11.2`).

Watches ConfigMaps and Secrets matching a label, writes their data to a shared
folder atomically, and optionally calls an HTTP endpoint when files change.

## Status

Port in progress — not yet released or deployed.

## Supported configuration

| Variable                                                                                                               | Notes                                                                             |
|------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `LABEL`, `LABEL_VALUE`                                                                                                 | required label selector                                                           |
| `FOLDER`                                                                                                               | required destination root                                                         |
| `FOLDER_ANNOTATION`                                                                                                    | per-resource folder override (default `k8s-sidecar-target-directory`)             |
| `FOLDER_PER_NAMESPACE`                                                                                                 | `FOLDER/<ns>`; relative annotation → `FOLDER/<ns>/<ann>`; absolute stays verbatim |
| `NAMESPACE`                                                                                                            | comma list, `ALL`, or pod namespace default                                       |
| `RESOURCE`                                                                                                             | `configmap`, `secret`, `both`                                                     |
| `RESOURCE_NAME`                                                                                                        | `name`, `kind/name`, `ns/kind/name`; forces SLEEP-style polling per namespace     |
| `METHOD`                                                                                                               | `LIST` (once, exit), `SLEEP` (poll every `SLEEP_TIME`), default WATCH             |
| `SLEEP_TIME`, `ERROR_THROTTLE_SLEEP`                                                                                   | poll / error-backoff seconds                                                      |
| `REQ_URL`, `REQ_METHOD`, `REQ_PAYLOAD`                                                                                 | reload callback                                                                   |
| `REQ_USERNAME`, `REQ_PASSWORD`, `REQ_USERNAME_FILE`, `REQ_PASSWORD_FILE`, `--req-username-file`, `--req-password-file` | basic auth; files re-read per attempt                                             |
| `REQ_BASIC_AUTH_ENCODING`                                                                                              | `latin1` (default) or `utf-8`                                                     |
| `REQ_RETRY_*`, `REQ_TIMEOUT`, `REQ_SKIP_INIT`, `REQ_SKIP_TLS_VERIFY`                                                   | shared HTTP budget (also for `*.url` downloads)                                   |
| `ENABLE_5XX`, `UNIQUE_FILENAMES`, `DEFAULT_FILE_MODE`                                                                  |                                                                                   |
| `SKIP_TLS_VERIFY`, `KUBECONFIG`                                                                                        | Kubernetes client                                                                 |
| `WATCH_SERVER_TIMEOUT`, `WATCH_CLIENT_TIMEOUT`                                                                         | server `timeoutSeconds` / client read timeout                                     |
| `IGNORE_ALREADY_PROCESSED`                                                                                             | dedupe on resourceVersion                                                         |
| `HEALTH_PORT`                                                                                                          | `/healthz` (default 8080)                                                         |
| `K8S_CONTACT_THRESHOLD_SECONDS`                                                                                        | liveness staleness override; default 2× per-stream heartbeat                      |
| `LOG_LEVEL`, `LOG_FORMAT`, `LOG_TZ`                                                                                    | `JSON`/`LOGFMT`                                                                   |

## Deliberate differences from upstream

- **Atomic writes** — files are written to a temp path then renamed; upstream
  writes in place.
- **Ownership manifest** — `.k8s-sideca-rs.manifest.json` tracks which files
  this sidecar owns, so a restart cleans owned stale files without touching
  unrelated files. Upstream leaves stale files across restarts.
  The manifest lives inside `FOLDER` — the only guaranteed-writable path.
  Consumers scanning `FOLDER` must tolerate a dot-prefixed JSON file (Grafana's
  file provisioner skips dotfiles; Loki's `*.yaml` glob is unaffected).
- **Path traversal** — a relative `FOLDER_ANNOTATION` that escapes `FOLDER` is
  rejected; upstream allows it. An **absolute** annotation path is still used
  verbatim (upstream parity): anyone able to create a matching resource in a
  watched namespace can redirect writes anywhere the pod user can write.
  Treat RBAC on labeled ConfigMaps/Secrets as the control.
- **Failed apply preserves files** — if destination resolution or a write
  fails for a resource that was previously applied, its existing files are
  kept rather than deleted at the end of the relist.
- **Failed `.url` fetch keeps the previous file** — upstream writes an empty
  file.
- **Failed reload callbacks stay pending** and are retried; upstream drops them
  after the retry budget is exhausted.
- **Non-JSON `REQ_PAYLOAD` is sent as `text/plain`** — upstream posts it with
  `application/json` while quoting the string as JSON. If `REQ_PAYLOAD` parses
  as JSON it is still sent as `application/json`, matching upstream.
- **`SCRIPT` is unsupported** — startup fails loudly (no shell in the scratch
  image anyway).
- **`DISABLE_X509_STRICT_VERIFICATION` is unsupported** — rustls has no
  non-strict mode.
- **Liveness tracks per-stream API contact** — matches upstream 2.11.2's fix:
  each stream stamps contact on events and on watch-stream return; staleness
  threshold is 2× that stream's heartbeat interval (`SLEEP_TIME` for pollers,
  `WATCH_SERVER_TIMEOUT` for watchers) or `K8S_CONTACT_THRESHOLD_SECONDS` when
  set. (Upstream derives one threshold from `mode` for all streams — ours is
  per-stream, which handles mixed watch/sleep topologies exactly.)
- **A dead worker task is fatal** — like upstream's supervisor exit, the
  process exits nonzero so the container runtime restarts it. In-place stream
  restarts still happen on retryable watch errors.
- **A failed file write skips that file, not the batch** — matching upstream's
  per-key error isolation, while still preserving the previous content.

## Layout

```shell
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
