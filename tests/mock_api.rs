#![warn(
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::exit,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::unreachable,
    clippy::undocumented_unsafe_blocks,
    clippy::as_conversions
)]
#![allow(
    // Transitive duplicate versions are outside our control.
    clippy::multiple_crate_versions,
    // Error behaviour is documented at module level, not via per-fn
    // Errors sections; the public surface is consumed internally.
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    // Function length is governed by cognitive-complexity, not lines.
    clippy::too_many_lines,
    // Licence/keyword metadata is a maintainer decision, not a lint.
    clippy::cargo_common_metadata,
)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::unreachable,
        clippy::disallowed_methods,
        clippy::future_not_send,
        clippy::assert_is_empty,
        // Fake/test impls are async only because the real trait is.
        clippy::unused_async_trait_impl,
    )
)]
//! Mock Kubernetes API integration tests: real `run_watcher` / `run_lister`
//! / `reconcile_loop` against an in-process fake apiserver covering initial
//! sync, watch events, disconnect/reconnect, HTTP 410 relist recovery and
//! paginated lists.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::StreamExt;
use k8s_sidecar_rs::config::{self, Config, Kind};
use k8s_sidecar_rs::files::{Reconciler, UrlFetcher};
use k8s_sidecar_rs::health::HealthState;
use k8s_sidecar_rs::watch::{
    ReconcileCtx, StreamCtx, reconcile_loop, run_lister, run_watcher, stream_id,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------- mock API

struct Watcher {
    /// None = cluster-scoped watch (`/api/v1/<resource>`).
    ns: Option<String>,
    resource: String,
    tx: mpsc::UnboundedSender<String>,
}

#[derive(Default)]
struct Inner {
    /// (resource, ns, name) -> object JSON.
    items: BTreeMap<(String, String, String), Value>,
    rv: u64,
    watchers: Vec<Watcher>,
    /// When set, list responses paginate at this size via `continue` tokens.
    page_size: Option<usize>,
    /// One-shot failure code for the next watch request.
    fail_watch: Option<u16>,
    /// Persistent failure code for every API request (list + watch).
    fail_all: Option<u16>,
}

#[derive(Clone)]
struct MockKube {
    state: Arc<Mutex<Inner>>,
    url: String,
}

impl MockKube {
    async fn start() -> Self {
        let state = Arc::new(Mutex::new(Inner::default()));
        let app = Router::new()
            .route("/api/v1/{resource}", get(list_all))
            .route("/api/v1/namespaces/{ns}/{resource}", get(list_ns))
            .route("/api/v1/namespaces/{ns}/{resource}/{name}", get(get_one))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());
        Self {
            state,
            url: format!("http://{addr}"),
        }
    }

    fn upsert_cm(&self, ns: &str, name: &str, data: &[(&str, &str)]) {
        self.upsert("configmaps", ns, name, cm_json(ns, name, data));
    }

    fn upsert_secret(&self, ns: &str, name: &str, data: &[(&str, &[u8])]) {
        self.upsert("secrets", ns, name, secret_json(ns, name, data));
    }

    fn upsert(&self, resource: &str, ns: &str, name: &str, mut obj: Value) {
        let mut i = self.state.lock().unwrap();
        i.rv += 1;
        let key = (resource.to_string(), ns.to_string(), name.to_string());
        let typ = if i.items.contains_key(&key) {
            "MODIFIED"
        } else {
            "ADDED"
        };
        obj["metadata"]["resourceVersion"] = json!(i.rv.to_string());
        i.items.insert(key, obj.clone());
        broadcast(i, resource, ns, typ, &obj);
    }

    fn delete_cm(&self, ns: &str, name: &str) {
        let mut i = self.state.lock().unwrap();
        i.rv += 1;
        let key = ("configmaps".to_string(), ns.to_string(), name.to_string());
        if let Some(obj) = i.items.remove(&key) {
            broadcast(i, "configmaps", ns, "DELETED", &obj);
        }
    }

    /// Close every open watch stream — dropping the senders ends each stream,
    /// which forces kube-rs to relist and rewatch.
    fn close_watches(&self) {
        self.state.lock().unwrap().watchers.clear();
    }

    /// The next watch request fails with this HTTP status (one shot).
    fn fail_next_watch(&self, code: u16) {
        self.state.lock().unwrap().fail_watch = Some(code);
    }

    /// Every subsequent API request (list + watch) fails with this status.
    fn fail_everything(&self, code: u16) {
        self.state.lock().unwrap().fail_all = Some(code);
    }

    fn set_page_size(&self, n: usize) {
        self.state.lock().unwrap().page_size = Some(n);
    }

    fn watch_count(&self) -> usize {
        self.state.lock().unwrap().watchers.len()
    }
}

fn broadcast(
    mut i: std::sync::MutexGuard<Inner>,
    resource: &str,
    ns: &str,
    typ: &str,
    obj: &Value,
) {
    let line = format!("{}\n", json!({"type": typ, "object": obj}));
    i.watchers.retain(|w| {
        if w.resource != resource {
            return true;
        }
        if let Some(watched_ns) = &w.ns
            && watched_ns != ns
        {
            return true;
        }
        w.tx.send(line.clone()).is_ok()
    });
}

fn cm_json(ns: &str, name: &str, data: &[(&str, &str)]) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": ns,
            "uid": format!("uid-{ns}-{name}"),
            "labels": {"app": "x"},
        },
        "data": BTreeMap::from_iter(data.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
    })
}

fn secret_json(ns: &str, name: &str, data: &[(&str, &[u8])]) -> Value {
    let b64: BTreeMap<String, String> = data
        .iter()
        .map(|(k, v)| (k.to_string(), base64_encode(v)))
        .collect();
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": name,
            "namespace": ns,
            "uid": format!("uid-{ns}-{name}"),
            "labels": {"app": "x"},
        },
        "data": b64,
    })
}

fn base64_encode(b: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in b.chunks(3) {
        let n = chunk.iter().fold(0u32, |a, &b| (a << 8) | u32::from(b)) << (8 * (3 - chunk.len()));
        for i in 0..4 {
            let idx = usize::try_from((n >> (18 - 6 * i)) & 63).unwrap_or(0);
            out.push(if i < chunk.len() + 1 {
                char::from(T[idx])
            } else {
                '='
            });
        }
    }
    out
}

/// Minimal labelSelector matcher (`k=v`, `k==v`, `k`, `k!=v`, comma-joined).
fn label_matches(selector: &str, labels: &Value) -> bool {
    selector.split(',').all(|term| {
        let term = term.trim();
        if let Some((k, v)) = term.split_once("!=") {
            return labels.get(k) != Some(&json!(v));
        }
        if let Some((k, v)) = term.split_once('=') {
            return labels.get(k.trim_end_matches('=')) == Some(&json!(v));
        }
        labels.get(term).is_some()
    })
}

type Params = HashMap<String, String>;

async fn list_all(
    State(mock): State<Arc<Mutex<Inner>>>,
    AxPath(resource): AxPath<String>,
    Query(q): Query<Params>,
) -> Response {
    respond_list_or_watch(&mock, None, resource, &q)
}

async fn list_ns(
    State(mock): State<Arc<Mutex<Inner>>>,
    AxPath((ns, resource)): AxPath<(String, String)>,
    Query(q): Query<Params>,
) -> Response {
    respond_list_or_watch(&mock, Some(ns), resource, &q)
}

fn respond_list_or_watch(
    mock: &Mutex<Inner>,
    ns: Option<String>,
    resource: String,
    q: &Params,
) -> Response {
    let fail_all = mock.lock().unwrap().fail_all;
    if let Some(code) = fail_all {
        return (
            StatusCode::from_u16(code).unwrap(),
            axum::Json(json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "reason": "InternalError", "code": code,
                "message": "mock API failure",
            })),
        )
            .into_response();
    }
    if q.get("watch").is_some_and(|v| v == "true" || v == "1") {
        return watch_response(mock, ns, resource);
    }
    let (all_items, rv, page_size) = {
        let i = mock.lock().unwrap();
        (i.items.clone(), i.rv, i.page_size)
    };
    let selector = q.get("labelSelector").cloned().unwrap_or_default();
    let mut items: Vec<Value> = all_items
        .iter()
        .filter(|((r, ins, _), obj)| {
            r == &resource
                && ns.as_ref().is_none_or(|n| n == ins)
                && (selector.is_empty() || label_matches(&selector, &obj["metadata"]["labels"]))
        })
        .map(|(_, v)| v.clone())
        .collect();
    items.sort_by_key(|o| {
        o["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    });

    let limit = q.get("limit").and_then(|v| v.parse().ok()).or(page_size);
    let offset = q
        .get("continue")
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let (page, next) = match limit {
        Some(l) if offset + l < items.len() => {
            (items[offset..offset + l].to_vec(), Some(offset + l))
        }
        _ => (items[offset.min(items.len())..].to_vec(), None),
    };
    let mut meta = json!({"resourceVersion": rv.to_string()});
    if let Some(c) = next {
        meta["continue"] = json!(c.to_string());
    }
    let kind = if resource == "secrets" {
        "SecretList"
    } else {
        "ConfigMapList"
    };
    (
        StatusCode::OK,
        axum::Json(json!({
            "apiVersion": "v1",
            "kind": kind,
            "metadata": meta,
            "items": page,
        })),
    )
        .into_response()
}

fn watch_response(mock: &Mutex<Inner>, ns: Option<String>, resource: String) -> Response {
    let mut i = mock.lock().unwrap();
    if let Some(code) = i.fail_watch.take() {
        return (
            StatusCode::from_u16(code).unwrap(),
            axum::Json(json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "reason": "Expired", "code": code,
                "message": "resource version too old",
            })),
        )
            .into_response();
    }
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    i.watchers.push(Watcher { ns, resource, tx });
    drop(i);
    let stream =
        UnboundedReceiverStream::new(rx).map(|line| Ok::<Bytes, Infallible>(Bytes::from(line)));
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn get_one(
    State(mock): State<Arc<Mutex<Inner>>>,
    AxPath((ns, resource, name)): AxPath<(String, String, String)>,
) -> Response {
    let i = mock.lock().unwrap();
    i.items.get(&(resource, ns, name)).map_or_else(
        || {
            (
                StatusCode::NOT_FOUND,
                axum::Json(json!({
                    "kind": "Status", "apiVersion": "v1", "status": "Failure",
                    "reason": "NotFound", "code": 404,
                    "message": "not found",
                })),
            )
                .into_response()
        },
        |obj| (StatusCode::OK, axum::Json(obj.clone())).into_response(),
    )
}

// ------------------------------------------------------------ test harness

struct NoFetch;
impl UrlFetcher for NoFetch {
    async fn fetch(&self, url: &str, _binary: bool) -> Result<Vec<u8>, String> {
        Err(format!("no fetcher in mock-api tests: {url}"))
    }
}

struct Sidecar {
    dir: TempDir,
    cancel: CancellationToken,
    health: Arc<HealthState>,
    _tasks: JoinSet<()>,
}

fn cfg_for(folder: &Path, extra: &[(&str, &str)]) -> Arc<Config> {
    let mut env: HashMap<String, String> = HashMap::from([
        ("LABEL".into(), "app".into()),
        ("LABEL_VALUE".into(), "x".into()),
        ("FOLDER".into(), folder.to_str().unwrap().into()),
        ("NAMESPACE".into(), "ns1".into()),
        ("ERROR_THROTTLE_SLEEP".into(), "1".into()),
    ]);
    for (k, v) in extra {
        env.insert(k.to_string(), v.to_string());
    }
    Arc::new(config::load(&env, &[]).unwrap())
}

fn client_to(url: &str) -> kube::Client {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let kcfg = kube::Config::new(url.parse().unwrap());
    kube::Client::try_from(kcfg).unwrap()
}

/// Wire one (kind, namespace) stream + the reconcile loop, mirroring main.rs:
/// `effective_method` picks watcher vs lister; `METHOD=LIST` runs a single pass.
fn spawn_sidecar(mock: &MockKube, cfg: &Config, kind: Kind, ns: &str) -> Sidecar {
    let dir = tempfile::tempdir().unwrap();
    let cfg = {
        let mut c = cfg.clone();
        c.folder = dir.path().to_path_buf();
        Arc::new(c)
    };
    let cancel = CancellationToken::new();
    let health = HealthState::new(None);
    let id = stream_id(kind, ns);
    health.register_stream(&id, Duration::from_secs(60));
    let (tx, rx) = mpsc::channel(64);

    let mut tasks = JoinSet::new();
    let stream_ctx = StreamCtx {
        client: client_to(&mock.url),
        cfg: cfg.clone(),
        tx: tx.clone(),
        health: health.clone(),
        cancel: cancel.clone(),
    };
    let nso = ns.to_string();
    let lister = cfg.effective_method(ns) != config::Method::Watch;
    let once = cfg.method == config::Method::List;
    tasks.spawn(async move {
        if lister {
            run_lister(stream_ctx, kind, nso, once).await;
        } else {
            run_watcher(stream_ctx, kind, nso).await;
        }
    });
    drop(tx);

    let rec = Reconciler::load(
        &cfg.folder,
        cfg.default_file_mode,
        cfg.ignore_already_processed,
    );
    let rctx = ReconcileCtx {
        cfg,
        fetcher: NoFetch,
        reloader: None,
        health: health.clone(),
        expected_streams: HashSet::from([id]),
        cancel: cancel.clone(),
    };
    tasks.spawn(async move { reconcile_loop(rx, rec, rctx).await });
    Sidecar {
        dir,
        cancel,
        health,
        _tasks: tasks,
    }
}

fn file(dir: &TempDir, name: &str) -> PathBuf {
    dir.path().join(name)
}

async fn wait_for(cond: impl Fn() -> bool) -> bool {
    for _ in 0..200 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn wait_file(dir: &TempDir, name: &str) -> String {
    let p = file(dir, name);
    assert!(
        wait_for(|| p.exists()).await,
        "timed out waiting for {name}"
    );
    std::fs::read_to_string(&p).unwrap()
}

async fn wait_content(dir: &TempDir, name: &str, expected: &str) {
    let p = file(dir, name);
    assert!(
        wait_for(|| p.exists() && std::fs::read_to_string(&p).unwrap() == expected).await,
        "timed out waiting for {name} == {expected:?}"
    );
}

async fn wait_gone(dir: &TempDir, name: &str) {
    let p = file(dir, name);
    assert!(
        wait_for(|| !p.exists()).await,
        "timed out waiting for {name} removal"
    );
}

// ------------------------------------------------------------------ tests

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_initial_sync_writes_files() {
    let mock = MockKube::start().await;
    mock.upsert_cm("ns1", "cm-a", &[("a.json", "{\"a\":1}")]);
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(dir.path(), &[]);
    let sc = spawn_sidecar(&mock, &cfg, Kind::ConfigMap, "ns1");

    assert_eq!(wait_file(&sc.dir, "a.json").await, "{\"a\":1}");
    sc.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_add_modify_delete_events() {
    let mock = MockKube::start().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(dir.path(), &[]);
    let sc = spawn_sidecar(&mock, &cfg, Kind::ConfigMap, "ns1");

    mock.upsert_cm("ns1", "cm-b", &[("b.txt", "one")]);
    wait_file(&sc.dir, "b.txt").await;

    mock.upsert_cm("ns1", "cm-b", &[("b.txt", "two")]);
    wait_content(&sc.dir, "b.txt", "two").await;

    mock.delete_cm("ns1", "cm-b");
    wait_gone(&sc.dir, "b.txt").await;
    sc.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_secret_bytes_written() {
    let mock = MockKube::start().await;
    mock.upsert_secret("ns1", "s1", &[("bin", b"\x00\x01\xff")]);
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(dir.path(), &[("RESOURCE", "both")]);
    let sc = spawn_sidecar(&mock, &cfg, Kind::Secret, "ns1");

    let p = file(&sc.dir, "bin");
    assert!(wait_for(|| p.exists()).await);
    assert_eq!(std::fs::read(&p).unwrap(), b"\x00\x01\xff");
    sc.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_reconnects_after_stream_close() {
    let mock = MockKube::start().await;
    mock.upsert_cm("ns1", "cm-a", &[("a.json", "v1")]);
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(dir.path(), &[]);
    let sc = spawn_sidecar(&mock, &cfg, Kind::ConfigMap, "ns1");
    wait_file(&sc.dir, "a.json").await;
    assert!(wait_for(|| mock.watch_count() > 0).await);

    mock.close_watches();
    // kube-rs relists internally and opens a fresh watch.
    assert!(wait_for(|| mock.watch_count() > 0).await);
    mock.upsert_cm("ns1", "cm-c", &[("c.json", "after-reconnect")]);
    assert_eq!(wait_file(&sc.dir, "c.json").await, "after-reconnect");
    sc.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watcher_recovers_from_410_gone() {
    let mock = MockKube::start().await;
    mock.upsert_cm("ns1", "cm-a", &[("a.json", "v1")]);
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(dir.path(), &[]);
    let sc = spawn_sidecar(&mock, &cfg, Kind::ConfigMap, "ns1");
    wait_file(&sc.dir, "a.json").await;
    assert!(wait_for(|| mock.watch_count() > 0).await);

    // Resource-version expiry on the next watch establish; must relist.
    mock.fail_next_watch(410);
    mock.close_watches();
    mock.upsert_cm("ns1", "cm-d", &[("d.json", "post-410")]);
    assert_eq!(wait_file(&sc.dir, "d.json").await, "post-410");
    sc.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lister_follows_paginated_list() {
    let mock = MockKube::start().await;
    for n in 0..5 {
        let file_name = format!("f{n}.json");
        let content = format!("v{n}");
        mock.upsert_cm("ns1", &format!("cm-{n}"), &[(&file_name, content.as_str())]);
    }
    mock.set_page_size(2); // 5 items over pages of 2 -> exercises continue tokens
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(dir.path(), &[("METHOD", "SLEEP"), ("SLEEP_TIME", "3600")]);
    let sc = spawn_sidecar(&mock, &cfg, Kind::ConfigMap, "ns1");

    for n in 0..5 {
        assert_eq!(
            wait_file(&sc.dir, &format!("f{n}.json")).await,
            format!("v{n}")
        );
    }
    sc.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lister_resource_name_get_and_404() {
    let mock = MockKube::start().await;
    mock.upsert_cm("ns1", "wanted", &[("w.json", "named")]);
    // "missing" is requested but absent -> exercises the 404 skip path.
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(
        dir.path(),
        &[("METHOD", "LIST"), ("RESOURCE_NAME", "wanted,missing")],
    );
    let sc = spawn_sidecar(&mock, &cfg, Kind::ConfigMap, "ns1");
    assert_eq!(wait_file(&sc.dir, "w.json").await, "named");
    sc.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_stream_stays_dead_across_reconnects() {
    // With the API hard-down, the watcher loops reconnect attempts. A restart
    // must not revive liveness on its own — only real events may — or a dead
    // stream would flap healthy on every retry.
    let mock = MockKube::start().await;
    mock.upsert_cm("ns1", "cm-a", &[("a.json", "v1")]);
    let dir = tempfile::tempdir().unwrap();
    let cfg = cfg_for(dir.path(), &[]);
    let sc = spawn_sidecar(&mock, &cfg, Kind::ConfigMap, "ns1");
    wait_file(&sc.dir, "a.json").await;
    assert!(
        wait_for(|| sc.health.probe().0 == 200).await,
        "timed out waiting for ready+live"
    );

    mock.fail_everything(500);
    mock.close_watches();

    assert!(
        wait_for(|| sc.health.probe().0 == 503).await,
        "dead stream must drop liveness"
    );
    // Stay dead through several reconnect cycles (throttle is 1s in tests).
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        sc.health.probe().0,
        503,
        "reconnect attempts revived liveness without events"
    );
    sc.cancel.cancel();
}
