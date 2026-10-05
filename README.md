# k8s-sideca-rs

Rust reimplementation of [kiwigrid/k8s-sidecar](https://github.com/kiwigrid/k8s-sidecar)
(pinned compatibility target: upstream `2.11.2`).

Watches ConfigMaps and Secrets matching a label, writes their data to a
shared folder atomically, and optionally calls an HTTP endpoint when files
change — scratch image, single static binary, owned-file cleanup on restart.

## Status

Port in progress — not yet released or deployed.

## Basic usage

Run as a sidecar container sharing an `emptyDir` volume with your app. The
minimum configuration is a label selector and a destination folder:

```yaml
env:
  - name: LABEL
    value: grafana_dashboard
  - name: FOLDER
    value: /tmp/dashboards
```

Defaults: `RESOURCE=both`, `METHOD=WATCH`, pod namespace only.

## Documentation

Full docs — configuration reference, deliberate differences from upstream,
real-world deployment profiles, and development notes — live at
<https://damacus.github.io/k8s-sideca-rs/> (source in `docs/`).
