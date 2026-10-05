# k8s-sideca-rs

<!-- markdownlint-disable MD046 -->

Rust reimplementation of
[kiwigrid/k8s-sidecar](https://github.com/kiwigrid/k8s-sidecar)
(pinned compatibility target: upstream `2.11.2`).

Watches ConfigMaps and Secrets matching a label, writes their data to a shared
folder atomically, and optionally calls an HTTP endpoint when files change.

## Why

The upstream Python sidecar works, but a Rust rewrite buys you:

- **A scratch image** — single static musl binary, no shell, no CA bundle
  required (rustls + embedded `webpki-roots`).
- **Atomic writes** — files appear complete or not at all; consumers never
  read a half-written dashboard.
- **Owned-file cleanup on restart** — an ownership manifest lets the sidecar
  remove *its* stale files without touching anything else.

See [Differences from upstream](upstream-differences.md) for the full list of
deliberate behaviour changes.

!!! warning "Status"

    Port in progress — not yet released or deployed.

## Basic usage

Run it as a sidecar container sharing an `emptyDir` volume with your app. The
minimum configuration is a label selector and a destination folder:

```yaml
env:
  - name: LABEL
    value: grafana_dashboard
  - name: FOLDER
    value: /tmp/dashboards
```

With those two set, the defaults do the rest: `RESOURCE=both`,
`METHOD=WATCH`, pod namespace only. Every ConfigMap and Secret carrying the
label is synced into `/tmp/dashboards`, live, as the API server streams
changes.

To reload Grafana when files change, add a callback:

```yaml
  - name: REQ_URL
    value: http://localhost:3000/api/admin/provisioning/dashboards/reload
  - name: REQ_METHOD
    value: POST
  - name: REQ_USERNAME
    valueFrom: { secretKeyRef: { name: grafana, key: admin-user } }
  - name: REQ_PASSWORD
    valueFrom: { secretKeyRef: { name: grafana, key: admin-password } }
```

The full environment surface is documented under
[Configuration](configuration.md); a real-world rollout study lives in
[Deployment profiles](deployment-profiles.md).
