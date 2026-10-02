use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use kube::Client;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use k8s_sidecar_rs::config::{self, Config, LogFormat, Method, Namespaces};
use k8s_sidecar_rs::files::{Reconciler, UrlFetcher};
use k8s_sidecar_rs::health::{self, HealthState};
use k8s_sidecar_rs::http::build_req_client;
use k8s_sidecar_rs::reload::{self, Reloader};
use k8s_sidecar_rs::watch::{self, SyncEvent, reconcile_loop, run_lister, run_watcher, stream_id};

const SA_NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
/// Bounded queue between streams and the single file reconciler.
const EVENT_QUEUE: usize = 256;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    std::process::exit(rt.block_on(run()));
}

async fn run() -> i32 {
    let env: HashMap<String, String> = std::env::vars().collect();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cfg = match config::load(&env, &args) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("configuration error: {e}");
            return 1;
        }
    };
    init_logging(&cfg);
    info!("starting collector");
    if cfg.resource_name_ignored() {
        warn!("RESOURCE_NAME has no effect with NAMESPACE=ALL; selectors are ignored");
    }
    if let Some(tz) = env.get("LOG_TZ")
        && !tz.eq_ignore_ascii_case("UTC")
        && !tz.eq_ignore_ascii_case("LOCAL")
    {
        info!(value = %tz, "unrecognised LOG_TZ; using local time");
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let namespaces = match resolve_namespaces(&cfg) {
        Ok(n) => n,
        Err(e) => {
            error!(error = %e, "cannot resolve namespace");
            return 1;
        }
    };

    let client = match build_client(&cfg).await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "cannot build kubernetes client");
            return 1;
        }
    };

    let http = match build_req_client(&cfg) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "cannot build http client");
            return 1;
        }
    };

    let health = HealthState::new(cfg.k8s_contact_threshold);
    let cancel = CancellationToken::new();

    let (tx, rx) = mpsc::channel::<SyncEvent>(EVENT_QUEUE);
    let mut tasks = JoinSet::new();

    // Expected stream ids: one per (resource kind, namespace); ALL collapses
    // to a single cluster-wide stream per kind, like upstream.
    let mut expected: HashSet<(String, Duration)> = HashSet::new();
    for kind in &cfg.resources {
        match &namespaces {
            Namespaces::All => {
                expected.insert((stream_id(*kind, "ALL"), heartbeat_for(&cfg, "ALL")));
            }
            Namespaces::List(list) => {
                for ns in list {
                    expected.insert((stream_id(*kind, ns), heartbeat_for(&cfg, ns)));
                }
            }
            Namespaces::PodNamespace => unreachable!("resolved above"),
        }
    }
    // Upstream 2.11.2: each stream's heartbeat interval is SLEEP_TIME for
    // polling streams, WATCH_SERVER_TIMEOUT for watchers; staleness = 2× that.
    for (s, heartbeat) in &expected {
        health.register_stream(s, *heartbeat);
    }

    let fetcher = HttpFetcher {
        http: http.clone(),
        settings: cfg.fetch.clone(),
    };
    let reloader = cfg.req.clone().map(|r| Reloader::new(r, http.clone()));

    let once = cfg.method == Method::List;
    for kind in cfg.resources.clone() {
        let ns_list: Vec<String> = match &namespaces {
            Namespaces::All => vec!["ALL".to_string()],
            Namespaces::List(list) => list.clone(),
            Namespaces::PodNamespace => unreachable!(),
        };
        for ns in ns_list {
            let stream_ctx = watch::StreamCtx {
                client: client.clone(),
                cfg: cfg.clone(),
                tx: tx.clone(),
                health: health.clone(),
                cancel: cancel.clone(),
            };
            match cfg.effective_method(&ns) {
                Method::Watch => {
                    tasks.spawn(async move { run_watcher(stream_ctx, kind, ns).await })
                }
                Method::Sleep | Method::List => {
                    tasks.spawn(async move { run_lister(stream_ctx, kind, ns, once).await })
                }
            };
        }
    }
    drop(tx); // channel closes when all streams stop

    {
        let rec = Reconciler::load(
            &cfg.folder,
            cfg.default_file_mode,
            cfg.ignore_already_processed,
        );
        let ctx = watch::ReconcileCtx {
            cfg: cfg.clone(),
            fetcher,
            reloader: reloader.clone(),
            health: health.clone(),
            expected_streams: expected.iter().map(|(s, _)| s.clone()).collect(),
            cancel: cancel.clone(),
        };
        tasks.spawn(async move { reconcile_loop(rx, rec, ctx).await });
    }

    if cfg.method != Method::List {
        if let Some(r) = reloader {
            let (c, pause) = (cancel.clone(), cfg.error_throttle_sleep);
            tasks.spawn(async move { r.run(c, pause).await });
        }
        let (state, port, c) = (health.clone(), cfg.health_port, cancel.clone());
        tasks.spawn(async move { health::serve(state, port, c).await });
    } else {
        // LIST exits once every stream did a single pass and the queue drains.
        while tasks.join_next().await.is_some() {}
        // The reloader loop never ran — deliver the pending callback once.
        if let Some(r) = &reloader {
            r.flush().await;
        }
        info!("list pass complete, exiting");
        return 0;
    }

    // Upstream 2.11.2 semantics: any worker dying is fatal — the process exits
    // nonzero so the container runtime restarts it. No task should complete
    // before cancellation.
    tokio::select! {
        _ = wait_for_shutdown() => {
            info!("shutdown signal received, stopping");
        }
        res = tasks.join_next() => {
            error!(result = ?res, "worker task exited unexpectedly; stopping");
            cancel.cancel();
            let _ = tokio::time::timeout(SHUTDOWN_GRACE, async {
                while tasks.join_next().await.is_some() {}
            })
            .await;
            return 1;
        }
    }
    cancel.cancel();

    let drain = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while tasks.join_next().await.is_some() {}
    });
    match drain.await {
        Ok(()) => 0,
        Err(_) => {
            error!("tasks did not stop within grace period");
            1
        }
    }
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async { term.as_mut().unwrap().recv().await }, if term.is_some() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Upstream 2.11.2 `heartbeat_interval`: polling streams report in every
/// SLEEP_TIME; watch streams get a heartbeat whenever the server closes the
/// watch (WATCH_SERVER_TIMEOUT) or an event arrives.
fn heartbeat_for(cfg: &Config, ns: &str) -> Duration {
    match cfg.effective_method(ns) {
        Method::Sleep | Method::List => cfg.sleep_time,
        Method::Watch => Duration::from_secs(cfg.watch_server_timeout),
    }
}

fn resolve_namespaces(cfg: &Config) -> Result<Namespaces, String> {
    match &cfg.namespaces {
        Namespaces::PodNamespace => {
            let ns = std::fs::read_to_string(Path::new(SA_NAMESPACE_FILE))
                .map_err(|e| format!("read {SA_NAMESPACE_FILE}: {e}"))?;
            let ns = ns.trim();
            if ns.is_empty() {
                return Err("service-account namespace file is empty".into());
            }
            Ok(Namespaces::List(vec![ns.to_string()]))
        }
        other => Ok(other.clone()),
    }
}

async fn build_client(cfg: &Config) -> Result<Client, String> {
    let mut kcfg = match &cfg.kubeconfig {
        Some(path) => {
            let kc = kube::config::Kubeconfig::read_from(path)
                .map_err(|e| format!("kubeconfig {path}: {e}"))?;
            kube::Config::from_custom_kubeconfig(kc, &kube::config::KubeConfigOptions::default())
                .await
                .map_err(|e| e.to_string())?
        }
        None => kube::Config::infer().await.map_err(|e| e.to_string())?,
    };
    if cfg.skip_tls_verify {
        kcfg.accept_invalid_certs = true;
    }
    kcfg.read_timeout = Some(Duration::from_secs(cfg.watch_client_timeout));
    Client::try_from(kcfg).map_err(|e| e.to_string())
}

/// `.url` key downloads — same shared HTTP session semantics as upstream:
/// GET with the REQ_* auth/retry/timeout budget; response bodies are written
/// verbatim (including 4xx); 5xx bodies are only written when ENABLE_5XX.
struct HttpFetcher {
    http: reqwest::Client,
    settings: config::FetchSettings,
}

impl UrlFetcher for HttpFetcher {
    async fn fetch(&self, url: &str, _binary: bool) -> Result<Vec<u8>, String> {
        let mut tracker = self.settings.retries.tracker();
        let mut delay = Duration::ZERO;
        loop {
            if tracker.retries_taken() > 0 {
                tokio::time::sleep(delay).await;
            }
            let mut req = self.http.get(url).timeout(self.settings.timeout);
            if let Some(h) = reload::basic_auth_header(&self.settings) {
                req = req.header(reqwest::header::AUTHORIZATION, h);
            }
            match req.send().await {
                Ok(resp) => {
                    if resp.status().is_server_error() && !self.settings.enable_5xx {
                        match tracker.failed(config::FailureKind::Status) {
                            Some(d) => {
                                delay = d;
                                continue;
                            }
                            None => return Err(format!("{url} returned {}", resp.status())),
                        }
                    }
                    return resp
                        .bytes()
                        .await
                        .map(|b| b.to_vec())
                        .map_err(|e| e.to_string());
                }
                Err(e) => {
                    let kind = if e.is_connect() {
                        config::FailureKind::Connect
                    } else {
                        config::FailureKind::Read
                    };
                    match tracker.failed(kind) {
                        Some(d) => {
                            delay = d;
                            continue;
                        }
                        None => return Err(e.to_string()),
                    }
                }
            }
        }
    }
}

fn init_logging(cfg: &Config) {
    use tracing_subscriber::fmt::time::LocalTime;
    let filter = EnvFilter::try_new(&cfg.log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    match cfg.log_format {
        LogFormat::Json => {
            let fmt = tracing_subscriber::fmt().json().with_env_filter(filter);
            if cfg.log_tz_utc {
                fmt.init();
            } else {
                fmt.with_timer(LocalTime::rfc_3339()).init();
            }
        }
        LogFormat::Logfmt => {
            let fmt = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_ansi(false);
            if cfg.log_tz_utc {
                fmt.init();
            } else {
                fmt.with_timer(LocalTime::rfc_3339()).init();
            }
        }
    }
}
