# Configuration

Everything is configured through environment variables (with a couple of
file-based variants). This mirrors upstream `kiwigrid/k8s-sidecar` `2.11.2`
so existing deployments drop in unchanged.

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

## Unsupported upstream variables

- `SCRIPT` — startup fails loudly (no shell in the scratch image anyway).
- `DISABLE_X509_STRICT_VERIFICATION` — rustls has no non-strict mode.
