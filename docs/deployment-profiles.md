# Deployed sidecar profiles — captured from `ironstone` (live cluster audit)

Captured from running pod specs; no Secret values — all credentials are
`secretKeyRef` indirections.

## Instances (6 sidecar containers)

| Pod                                   | Container              | Image                                   | Selector                        | NAMESPACE                 | FOLDER                                          | Reload                                                                     |
|---------------------------------------|------------------------|-----------------------------------------|---------------------------------|---------------------------|-------------------------------------------------|----------------------------------------------------------------------------|
| `monitoring/grafana-*`                | `grafana-sc-alerts`    | `quay.io/kiwigrid/k8s-sidecar:2.5.0`    | `grafana_alert=1`               | `monitoring`              | `/etc/grafana/provisioning/alerting`            | POST `localhost:3000/api/admin/provisioning/alerting/reload`, basic auth   |
| `monitoring/grafana-*`                | `grafana-sc-dashboard` | `quay.io/kiwigrid/k8s-sidecar:2.5.0`    | `grafana_dashboard` (existence) | `ALL`                     | `/tmp/dashboards` + `grafana_folder` annotation | POST `localhost:3000/api/admin/provisioning/dashboards/reload`, basic auth |
| `monitoring/loki-0`                   | sidecar                | `docker.io/kiwigrid/k8s-sidecar:2.5.0`  | `loki_rule` (existence)         | pod ns (`monitoring`)     | `/rules`                                        | none                                                                       |
| `openebs-system/openebs-loki-{0,1,2}` | `loki-sc-rules`        | `docker.io/kiwigrid/k8s-sidecar:1.30.2` | `loki_rule`                     | pod ns (`openebs-system`) | `/rules`                                        | none                                                                       |

All six: `METHOD=WATCH`, `RESOURCE=both`, `WATCH_SERVER/CLIENT_TIMEOUT=60`
(loki only), no `RESOURCE_NAME`, no `UNIQUE_FILENAMES`, no `SCRIPT`, no
`DEFAULT_FILE_MODE`, no health probes (liveness/readiness both absent —
`/healthz` is served but unprobed).

## Matched resources (live counts)

- `grafana_dashboard` (ALL namespaces): **45 ConfigMaps**, one `*.json` key
  each. Three carry `grafana_folder` annotation (`Home`, `Cilium`,
  `Network`) — all relative paths. No Secrets, no `.url` keys, no
  `binaryData`.
- `grafana_alert=1` (monitoring): **1 ConfigMap**.
- `loki_rule` (monitoring): **0 resources**.
- `loki_rule` (openebs-system): **0 resources** — the three OpenEBS sidecars
  plus `monitoring/loki-0` are idle watchers today.

## Source-of-truth map

- `monitoring/loki`: Flux HelmRelease `monitoring/loki`, chart `loki@7.3.0`,
  `sidecar.image.repository=ghcr.io/kiwigrid/k8s-sidecar` (tag = chart
  default). Good canary candidate — single pod, no reload callback.
- `monitoring/grafana`: Flux HelmRelease `monitoring/grafana`, grafana chart;
  **no sidecar image override** — both sidecars inherit the chart's single
  `sidecar.image` setting, so dashboards+alerts cannot be canaried
  separately via values. Canary needs an isolated deployment or a chart
  change.
- `openebs-system/openebs-loki`: rendered by the Flux HelmRelease `openebs`
  (chart `openebs@4.6.1`) — it embeds `loki` as a subchart (`loki-6.29.0`),
  which is where the `1.30.2` sidecar default comes from. Override point is
  the subchart values key `loki.sidecar.image.*` (or a `postRenderers` image
  rewrite on the `openebs` HR). Oldest version skew (1.30.2).

## Drift to verify at rollout time

- `monitoring/loki` HelmRelease declares `sidecar.rules.searchNamespace: ALL`
  and `folder: /rules/fake`, but the live pod has `FOLDER=/rules` and no
  `NAMESPACE` env (pod namespace). Either the chart does not render those
  values into env, or live predates the values — reconcile before relying
  on HR values for the new image config.
- Grafana helmrelease enables `sidecar.datasources` (`labelValue: ""`), yet
  the live pod has only alerts + dashboards sidecars.

## Feature surface actually exercised

| Feature                                          | Used by          |
|--------------------------------------------------|------------------|
| `METHOD=WATCH`, `RESOURCE=both` (2 streams each) | all 6            |
| Label existence selector                         | dashboards, loki |
| Label equality selector                          | alerts           |
| `NAMESPACE=ALL`                                  | dashboards       |
| Explicit namespace                               | alerts           |
| Pod-namespace default                            | loki ×4          |
| `FOLDER_ANNOTATION` (relative)                   | dashboards       |
| `REQ_*` POST + env-var basic auth                | grafana ×2       |
| `WATCH_*_TIMEOUT` overrides                      | loki ×4          |

Not used anywhere: `RESOURCE_NAME`, `LIST`/`SLEEP`, `UNIQUE_FILENAMES`,
`FOLDER_PER_NAMESPACE`, `.url` keys, `binaryData`, `SCRIPT`,
`REQ_SKIP_INIT`, `ENABLE_5XX`, `DEFAULT_FILE_MODE`, health probes.
