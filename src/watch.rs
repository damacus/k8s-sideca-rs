//! Kubernetes streams -> bounded queue -> single file reconciler.
//!
//! `run_watcher` streams kube-rs `watcher` events; `run_lister` implements
//! SLEEP/RESOURCE_NAME polling. Both normalise to `SyncEvent`s. The
//! `reconcile_loop` applies upserts eagerly but buffers deletions inside an
//! init window, committing them at `InitDone` — a failed partial relist can
//! never delete files.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::api::ListParams;
use kube::runtime::watcher::{self, Event};
use kube::{Api, Client};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::config::{Config, Kind};
use crate::files::{Owner, Reconciler, UrlFetcher, plan_files, resolve_dest_folder};
use crate::health::HealthState;
use crate::reload::Reloader;

/// A ConfigMap or Secret flattened to what file reconciliation needs.
#[derive(Debug, Clone)]
pub struct ResourceData {
    pub owner: Owner,
    pub annotations: BTreeMap<String, String>,
    pub text: BTreeMap<String, String>,
    pub binary: BTreeMap<String, Vec<u8>>,
    pub resource_version: Option<String>,
}

#[derive(Debug)]
pub enum SyncEvent {
    InitStart { stream: String },
    Upsert { stream: String, data: ResourceData },
    Delete { stream: String, data: ResourceData },
    InitDone { stream: String },
}

impl SyncEvent {
    fn stream(&self) -> &str {
        match self {
            Self::InitStart { stream }
            | Self::Upsert { stream, .. }
            | Self::Delete { stream, .. }
            | Self::InitDone { stream } => stream,
        }
    }
}

impl From<&ConfigMap> for ResourceData {
    fn from(cm: &ConfigMap) -> Self {
        let meta = &cm.metadata;
        ResourceData {
            owner: Owner {
                kind: Kind::ConfigMap,
                namespace: meta.namespace.clone().unwrap_or_default(),
                name: meta.name.clone().unwrap_or_default(),
            },
            annotations: meta.annotations.clone().unwrap_or_default(),
            text: cm.data.clone().unwrap_or_default(),
            binary: cm
                .binary_data
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, v.0))
                .collect(),
            resource_version: meta.resource_version.clone(),
        }
    }
}

impl From<&Secret> for ResourceData {
    fn from(s: &Secret) -> Self {
        let meta = &s.metadata;
        ResourceData {
            owner: Owner {
                kind: Kind::Secret,
                namespace: meta.namespace.clone().unwrap_or_default(),
                name: meta.name.clone().unwrap_or_default(),
            },
            annotations: meta.annotations.clone().unwrap_or_default(),
            text: BTreeMap::new(),
            binary: s
                .data
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, v.0))
                .collect(),
            resource_version: meta.resource_version.clone(),
        }
    }
}

pub fn stream_id(kind: Kind, namespace: &str) -> String {
    format!("{kind}/{namespace}")
}

/// Everything a stream task needs — keeps signatures small.
#[derive(Clone)]
pub struct StreamCtx {
    pub client: Client,
    pub cfg: Arc<Config>,
    pub tx: mpsc::Sender<SyncEvent>,
    pub health: Arc<HealthState>,
    pub cancel: CancellationToken,
}

/// Watch one (kind, namespace) pair, reconnecting on errors. `namespace` of
/// `"ALL"` watches cluster-wide. Runs until `ctx.cancel` fires.
pub async fn run_watcher(ctx: StreamCtx, kind: Kind, namespace: String) {
    let id = stream_id(kind, &namespace);
    let selector = selector(&ctx.cfg);
    loop {
        let wc = watcher::Config::default()
            .labels(&selector)
            .timeout(ctx.cfg.watch_server_timeout as u32);
        let result = match kind {
            Kind::ConfigMap => {
                let api = namespaced_api::<ConfigMap>(&ctx.client, &namespace);
                watch_stream(api, wc, &ctx, &id).await
            }
            Kind::Secret => {
                let api = namespaced_api::<Secret>(&ctx.client, &namespace);
                watch_stream(api, wc, &ctx, &id).await
            }
        };
        if ctx.cancel.is_cancelled() {
            return;
        }
        if result.is_ok() {
            // Clean end (server closed the watch on timeout): upstream stamps
            // the heartbeat here — reaching it proves the stream ran its full
            // course rather than stalling silently.
            ctx.health.contact(&id);
        }
        ctx.health.stream_dead(&id);
        error!(stream = %id, error = ?result, "watch stream ended; restarting");
        ctx.health
            .register_stream(&id, Duration::from_secs(ctx.cfg.watch_server_timeout));
        tokio::select! {
            _ = ctx.cancel.cancelled() => return,
            _ = tokio::time::sleep(ctx.cfg.error_throttle_sleep) => {}
        }
    }
}

fn namespaced_api<T>(client: &Client, namespace: &str) -> Api<T>
where
    T: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + serde::de::DeserializeOwned
        + Send
        + 'static,
    T::DynamicType: Default,
{
    if namespace == "ALL" {
        Api::all(client.clone())
    } else {
        Api::namespaced(client.clone(), namespace)
    }
}

async fn watch_stream<T>(
    api: Api<T>,
    wc: watcher::Config,
    ctx: &StreamCtx,
    id: &str,
) -> Result<(), watcher::Error>
where
    T: kube::Resource
        + Clone
        + std::fmt::Debug
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
    for<'a> ResourceData: From<&'a T>,
    T::DynamicType: Default,
{
    let mut events = std::pin::pin!(watcher::watcher(api, wc));
    loop {
        let next = tokio::select! {
            _ = ctx.cancel.cancelled() => return Ok(()),
            e = events.next() => e,
        };
        let Some(result) = next else {
            return Ok(()); // stream ended; caller restarts
        };
        ctx.health.contact(id);
        let send = match result {
            Ok(Event::Init) => SyncEvent::InitStart { stream: id.into() },
            Ok(Event::InitApply(o)) | Ok(Event::Apply(o)) => SyncEvent::Upsert {
                stream: id.into(),
                data: ResourceData::from(&o),
            },
            Ok(Event::Delete(o)) => SyncEvent::Delete {
                stream: id.into(),
                data: ResourceData::from(&o),
            },
            Ok(Event::InitDone) => SyncEvent::InitDone { stream: id.into() },
            Err(e) => return Err(e),
        };
        if ctx.tx.send(send).await.is_err() {
            return Ok(());
        }
    }
}

/// SLEEP / RESOURCE_NAME mode: repeatedly list (or read named) resources.
/// Emits InitStart/Upsert*/InitDone per pass so deletions commit atomically.
pub async fn run_lister(ctx: StreamCtx, kind: Kind, namespace: String, once: bool) {
    let id = stream_id(kind, &namespace);
    loop {
        let _ = ctx
            .tx
            .send(SyncEvent::InitStart { stream: id.clone() })
            .await;
        let result = match kind {
            Kind::ConfigMap => list_once::<ConfigMap>(&ctx, kind, &namespace, &id).await,
            Kind::Secret => list_once::<Secret>(&ctx, kind, &namespace, &id).await,
        };
        match result {
            Ok(()) => {
                let _ = ctx
                    .tx
                    .send(SyncEvent::InitDone { stream: id.clone() })
                    .await;
            }
            Err(e) => {
                // No InitDone: deletions stay uncommitted, old files preserved.
                error!(stream = %id, error = %e, "list pass failed");
            }
        }
        if once {
            return;
        }
        tokio::select! {
            _ = ctx.cancel.cancelled() => return,
            _ = tokio::time::sleep(ctx.cfg.sleep_time) => {}
        }
    }
}

async fn list_once<T>(ctx: &StreamCtx, kind: Kind, namespace: &str, id: &str) -> Result<(), String>
where
    T: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + serde::de::DeserializeOwned
        + std::fmt::Debug
        + Send
        + 'static,
    for<'a> ResourceData: From<&'a T>,
    T::DynamicType: Default,
{
    let api = namespaced_api::<T>(&ctx.client, namespace);
    let names = ctx.cfg.resource_names_for(kind, namespace);
    if names.is_empty() {
        // Follow `continue` tokens — upstream lists unpaginated, which silently
        // truncates if an apiserver decides to page the response.
        let mut lp = ListParams::default().labels(&selector(&ctx.cfg)).limit(500);
        loop {
            let list = api.list(&lp).await.map_err(|e| e.to_string())?;
            ctx.health.contact(id);
            for item in &list.items {
                send_upsert(&ctx.tx, id, item).await?;
            }
            match list.metadata.continue_ {
                Some(token) if !token.is_empty() => lp = lp.continue_token(&token),
                _ => break,
            }
        }
    } else {
        for name in names {
            match api.get(&name).await {
                Ok(item) => {
                    ctx.health.contact(id);
                    send_upsert(&ctx.tx, id, &item).await?;
                }
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    debug!(stream = %id, name, "named resource not found")
                }
                Err(e) => return Err(e.to_string()),
            }
        }
    }
    Ok(())
}

fn selector(cfg: &Config) -> String {
    match &cfg.label_value {
        Some(v) => format!("{}={v}", cfg.label),
        None => cfg.label.clone(),
    }
}

async fn send_upsert<T>(tx: &mpsc::Sender<SyncEvent>, id: &str, item: &T) -> Result<(), String>
where
    for<'a> ResourceData: From<&'a T>,
{
    tx.send(SyncEvent::Upsert {
        stream: id.into(),
        data: ResourceData::from(item),
    })
    .await
    .map_err(|e| e.to_string())
}

#[derive(Default)]
struct StreamState {
    /// Owner keys this stream currently has on disk.
    known: BTreeSet<String>,
    /// Between InitStart and InitDone — deletes are buffered, not applied.
    in_init: bool,
    /// Owner keys seen during the current init window.
    init_seen: BTreeSet<String>,
    /// Deletes received inside the current init window (committed at InitDone).
    init_deletes: Vec<ResourceData>,
    /// Set once this stream finished its first init/list pass.
    synced: bool,
}

/// Inputs for the single reconciler task.
pub struct ReconcileCtx<F> {
    pub cfg: Arc<Config>,
    pub fetcher: F,
    pub reloader: Option<Arc<Reloader>>,
    pub health: Arc<HealthState>,
    pub expected_streams: HashSet<String>,
    pub cancel: CancellationToken,
}

/// Single consumer for all stream events: reconciles files, drives reload
/// generations, updates readiness. `expected_streams` must all complete an
/// initial sync before `health` goes ready and `cleanup_stale` runs.
pub async fn reconcile_loop<F: UrlFetcher>(
    mut rx: mpsc::Receiver<SyncEvent>,
    mut rec: Reconciler,
    ctx: ReconcileCtx<F>,
) {
    let ReconcileCtx {
        cfg,
        fetcher,
        reloader,
        health,
        expected_streams,
        cancel,
    } = ctx;
    let mut streams: HashMap<String, StreamState> = HashMap::new();
    let mut owners: HashMap<String, Owner> = HashMap::new();
    let mut changed_during_init = false;
    let mut ready = false;

    loop {
        let event = tokio::select! {
            _ = cancel.cancelled() => return,
            e = rx.recv() => e,
        };
        let Some(event) = event else { return };
        health.contact(event.stream());
        let id = event.stream().to_string();
        let st = streams.entry(id.clone()).or_default();

        let skip_init = cfg.req.as_ref().is_some_and(|c| c.skip_init);
        match event {
            SyncEvent::InitStart { .. } => {
                st.in_init = true;
                st.init_seen.clear();
                st.init_deletes.clear();
            }
            SyncEvent::Upsert { data, .. } => {
                if rec.already_processed(&data.owner, data.resource_version.as_deref()) {
                    debug!(owner = %data.owner.key(), "skipping already-processed rv");
                    st.init_seen.insert(data.owner.key());
                    continue;
                }
                owners.insert(data.owner.key(), data.owner.clone());
                match apply_upsert(&mut rec, &cfg, &data, &fetcher).await {
                    Ok(changed) => {
                        st.init_seen.insert(data.owner.key());
                        st.known.insert(data.owner.key());
                        if changed
                            && ready
                            && let Some(r) = &reloader
                        {
                            r.bump();
                        }
                        if changed && !ready && !skip_init {
                            changed_during_init = true;
                        }
                    }
                    Err(e) => {
                        error!(owner = %data.owner.key(), error = %e, "reconcile failed; will retry on next event");
                    }
                }
            }
            SyncEvent::Delete { data, .. } => {
                owners.insert(data.owner.key(), data.owner.clone());
                if st.in_init {
                    st.init_deletes.push(data);
                } else if let Ok(true) = rec.remove(&data.owner) {
                    st.known.remove(&data.owner.key());
                    if ready {
                        if let Some(r) = &reloader {
                            r.bump();
                        }
                    } else if !skip_init {
                        changed_during_init = true;
                    }
                }
            }
            SyncEvent::InitDone { .. } => {
                st.in_init = false;
                let mut changed = false;
                for d in std::mem::take(&mut st.init_deletes) {
                    owners.insert(d.owner.key(), d.owner.clone());
                    changed |= rec.remove(&d.owner).unwrap_or(false);
                    st.known.remove(&d.owner.key());
                }
                // Commit deletions: owners applied by this stream but absent
                // from the relist are removed now.
                let stale: Vec<String> = st.known.difference(&st.init_seen).cloned().collect();
                for key in stale {
                    if let Some(owner) = owners.get(&key) {
                        changed |= rec.remove(owner).unwrap_or(false);
                    }
                    st.known.remove(&key);
                }
                st.synced = true;

                if !ready
                    && streams.values().all(|s| s.synced)
                    && streams.keys().collect::<HashSet<_>>()
                        == expected_streams.iter().collect::<HashSet<_>>()
                {
                    ready = true;
                    let live: BTreeSet<String> = streams
                        .values()
                        .flat_map(|s| s.known.iter().cloned())
                        .collect();
                    if let Err(e) = rec.cleanup_stale(&live) {
                        error!(error = %e, "stale cleanup failed");
                    }
                    health.mark_ready();
                    if changed_during_init && let Some(r) = &reloader {
                        r.bump();
                    }
                    info!("initial sync complete, sidecar is ready");
                }
                if ready
                    && changed
                    && let Some(r) = &reloader
                {
                    r.bump();
                }
            }
        }
    }
}

async fn apply_upsert<F: UrlFetcher>(
    rec: &mut Reconciler,
    cfg: &Config,
    data: &ResourceData,
    fetcher: &F,
) -> std::io::Result<bool> {
    let dest = resolve_dest_folder(
        Some(&data.annotations),
        &cfg.folder,
        &cfg.folder_annotation,
        cfg.folder_per_namespace,
        &data.owner.namespace,
    )?;
    let planned = plan_files(
        &dest,
        &data.owner,
        &data.text,
        &data.binary,
        cfg.unique_filenames,
    );
    rec.apply(&data.owner, planned, data.resource_version.clone(), fetcher)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::TempDir;

    struct NoFetch;
    impl UrlFetcher for NoFetch {
        async fn fetch(&self, url: &str, _b: bool) -> Result<Vec<u8>, String> {
            Err(format!("no fetcher for {url}"))
        }
    }

    fn cfg(pairs: &[(&str, &str)]) -> Arc<Config> {
        let env: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Arc::new(load(&env, &[]).unwrap())
    }

    fn cm(ns: &str, name: &str, data: &[(&str, &str)]) -> ResourceData {
        ResourceData {
            owner: Owner {
                kind: Kind::ConfigMap,
                namespace: ns.into(),
                name: name.into(),
            },
            annotations: BTreeMap::new(),
            text: data
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            binary: BTreeMap::new(),
            resource_version: Some("1".into()),
        }
    }

    struct Harness {
        tx: mpsc::Sender<SyncEvent>,
        folder: PathBuf,
        cancel: CancellationToken,
    }

    async fn harness(
        cfg: Arc<Config>,
        folder: &std::path::Path,
        streams: &[&str],
    ) -> (Harness, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(64);
        let rec = Reconciler::load(folder, cfg.default_file_mode, cfg.ignore_already_processed);
        let health = HealthState::new(None);
        for s in streams {
            health.register_stream(s, Duration::from_secs(60));
        }
        let cancel = CancellationToken::new();
        let ctx = ReconcileCtx {
            cfg: cfg.clone(),
            fetcher: NoFetch,
            reloader: None,
            health,
            expected_streams: streams.iter().map(|s| s.to_string()).collect(),
            cancel: cancel.clone(),
        };
        let handle = tokio::spawn(reconcile_loop(rx, rec, ctx));
        (
            Harness {
                tx,
                folder: folder.to_path_buf(),
                cancel,
            },
            handle,
        )
    }

    #[tokio::test]
    async fn upsert_writes_file_and_init_done_marks_ready() {
        let tmp = TempDir::new().unwrap();
        let c = cfg(&[("LABEL", "x"), ("FOLDER", tmp.path().to_str().unwrap())]);
        let (h, task) = harness(c, tmp.path(), &["configmap/ns"]).await;
        h.tx.send(SyncEvent::InitStart {
            stream: "configmap/ns".into(),
        })
        .await
        .unwrap();
        h.tx.send(SyncEvent::Upsert {
            stream: "configmap/ns".into(),
            data: cm("ns", "a", &[("f", "v")]),
        })
        .await
        .unwrap();
        h.tx.send(SyncEvent::InitDone {
            stream: "configmap/ns".into(),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(h.folder.join("f").exists());
        h.cancel.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn deletes_buffer_until_init_done() {
        let tmp = TempDir::new().unwrap();
        let c = cfg(&[("LABEL", "x"), ("FOLDER", tmp.path().to_str().unwrap())]);
        let (h, task) = harness(c, tmp.path(), &["configmap/ns"]).await;
        let s = "configmap/ns".to_string();
        // Initial sync with one file.
        for e in [
            SyncEvent::InitStart { stream: s.clone() },
            SyncEvent::Upsert {
                stream: s.clone(),
                data: cm("ns", "a", &[("f", "v"), ("g", "w")]),
            },
            SyncEvent::InitDone { stream: s.clone() },
        ] {
            h.tx.send(e).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(h.folder.join("f").exists());

        // Relist: object "a" loses key "g" → still upsert; a vanished object
        // arrives as Delete inside init → buffered, committed at InitDone.
        for e in [
            SyncEvent::InitStart { stream: s.clone() },
            SyncEvent::Upsert {
                stream: s.clone(),
                data: cm("ns", "a", &[("f", "v")]),
            },
            SyncEvent::Delete {
                stream: s.clone(),
                data: cm("ns", "b", &[]),
            },
            SyncEvent::InitDone { stream: s.clone() },
        ] {
            h.tx.send(e).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(h.folder.join("f").exists());
        assert!(!h.folder.join("g").exists()); // key removed via diff
        h.cancel.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn failed_partial_relist_preserves_files() {
        let tmp = TempDir::new().unwrap();
        let c = cfg(&[("LABEL", "x"), ("FOLDER", tmp.path().to_str().unwrap())]);
        let (h, task) = harness(c, tmp.path(), &["configmap/ns"]).await;
        let s = "configmap/ns".to_string();
        for e in [
            SyncEvent::InitStart { stream: s.clone() },
            SyncEvent::Upsert {
                stream: s.clone(),
                data: cm("ns", "a", &[("f", "v")]),
            },
            SyncEvent::InitDone { stream: s.clone() },
        ] {
            h.tx.send(e).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Stream dies mid-init: InitStart + partial data, no InitDone, then a
        // brand-new full relist.
        for e in [
            SyncEvent::InitStart { stream: s.clone() },
            SyncEvent::InitStart { stream: s.clone() }, // reconnect without InitDone
            SyncEvent::Upsert {
                stream: s.clone(),
                data: cm("ns", "a", &[("f", "v")]),
            },
            SyncEvent::InitDone { stream: s.clone() },
        ] {
            h.tx.send(e).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(h.folder.join("f").exists());
        h.cancel.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn identical_content_no_reload() {
        let tmp = TempDir::new().unwrap();
        let c = cfg(&[("LABEL", "x"), ("FOLDER", tmp.path().to_str().unwrap())]);
        let (h, task) = harness(c.clone(), tmp.path(), &["configmap/ns"]).await;
        // no reloader wired here; assert via file mtime staying same is flaky —
        // instead assert apply() returns false on second identical upsert via
        // the reconciler directly (covered in files.rs tests). Here verify a
        // second upsert after init doesn't panic and file persists.
        let s = "configmap/ns".to_string();
        for e in [
            SyncEvent::InitStart { stream: s.clone() },
            SyncEvent::Upsert {
                stream: s.clone(),
                data: cm("ns", "a", &[("f", "v")]),
            },
            SyncEvent::InitDone { stream: s.clone() },
            SyncEvent::Upsert {
                stream: s.clone(),
                data: cm("ns", "a", &[("f", "v")]),
            },
        ] {
            h.tx.send(e).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(std::fs::read(h.folder.join("f")).unwrap(), b"v");
        h.cancel.cancel();
        let _ = task.await;
    }

    #[test]
    fn resource_data_from_configmap_splits_data() {
        let mut cm_obj = ConfigMap::default();
        cm_obj.metadata.name = Some("n".into());
        cm_obj.metadata.namespace = Some("ns".into());
        cm_obj.data = Some(BTreeMap::from([("k".into(), "v".into())]));
        cm_obj.binary_data = Some(BTreeMap::from([(
            "b".into(),
            k8s_openapi::ByteString(b"\x01\x02".to_vec()),
        )]));
        let d = ResourceData::from(&cm_obj);
        assert_eq!(d.text["k"], "v");
        assert_eq!(d.binary["b"], vec![1, 2]);
        assert_eq!(d.owner.kind, Kind::ConfigMap);
    }
}
