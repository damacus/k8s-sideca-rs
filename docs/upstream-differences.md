# Differences from upstream

Deliberate behaviour changes relative to `kiwigrid/k8s-sidecar` `2.11.2`.

- **Atomic writes** — files are written to a temp path then renamed; upstream
  writes in place.
- **Ownership manifest** — `.k8s-sideca-rs.manifest.json` tracks which files
  this sidecar owns, so a restart cleans owned stale files without touching
  unrelated files. Upstream leaves stale files across restarts.
  The manifest lives inside `FOLDER` — the only guaranteed-writable path.
  Consumers scanning `FOLDER` must tolerate a dot-prefixed JSON file
  (Grafana's file provisioner skips dotfiles; Loki's `*.yaml` glob is
  unaffected).
- **Path traversal** — a relative `FOLDER_ANNOTATION` that escapes `FOLDER`
  is rejected; upstream allows it.

    !!! warning

        An **absolute** annotation path is still used verbatim (upstream
        parity): anyone able to create a matching resource in a watched
        namespace can redirect writes anywhere the pod user can write. Treat
        RBAC on labeled ConfigMaps/Secrets as the control.

- **Failed apply preserves files** — if destination resolution or a write
  fails for a resource that was previously applied, its existing files are
  kept rather than deleted at the end of the relist.
- **Failed `.url` fetch keeps the previous file** — upstream writes an empty
  file.
- **Failed reload callbacks stay pending** and are retried; upstream drops
  them after the retry budget is exhausted.
- **Non-JSON `REQ_PAYLOAD` is sent as `text/plain`** — upstream posts it with
  `application/json` while quoting the string as JSON. If `REQ_PAYLOAD`
  parses as JSON it is still sent as `application/json`, matching upstream.
- **`SCRIPT` is unsupported** — startup fails loudly (no shell in the
  scratch image anyway).
- **`DISABLE_X509_STRICT_VERIFICATION` is unsupported** — rustls has no
  non-strict mode.
- **Liveness tracks per-stream API contact** — matches upstream 2.11.2's
  fix: each stream stamps contact on events and on watch-stream return;
  staleness threshold is 2× that stream's heartbeat interval (`SLEEP_TIME`
  for pollers, `WATCH_SERVER_TIMEOUT` for watchers) or
  `K8S_CONTACT_THRESHOLD_SECONDS` when set. (Upstream derives one threshold
  from `mode` for all streams — ours is per-stream, which handles mixed
  watch/sleep topologies exactly.)
- **A dead worker task is fatal** — like upstream's supervisor exit, the
  process exits nonzero so the container runtime restarts it. In-place
  stream restarts still happen on retryable watch errors.
- **A failed file write skips that file, not the batch** — matching
  upstream's per-key error isolation, while still preserving the previous
  content.
