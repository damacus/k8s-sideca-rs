//! `/healthz` endpoint. Readiness: all watch streams completed their initial
//! sync. Liveness: every stream contacted the Kubernetes API within the last
//! `K8S_CONTACT_THRESHOLD` and no stream task has died. Mirrors upstream
//! semantics (per-stream contact tracking — the 2.5.0 supervisor loop
//! unconditionally refreshed the shared timestamp every 5s, making the
//! contact check ineffective; we track contact per stream instead).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Default)]
struct Inner {
    /// stream id -> (last successful API contact, staleness threshold).
    /// Threshold is 2× the stream's heartbeat interval (SLEEP_TIME for pollers,
    /// WATCH_SERVER_TIMEOUT for watchers) — upstream 2.11.2 semantics.
    contact: HashMap<String, (Instant, Duration)>,
    /// stream id -> alive.
    alive: HashMap<String, bool>,
}

pub struct HealthState {
    ready: AtomicBool,
    override_threshold: Option<Duration>,
    inner: Mutex<Inner>,
}

impl HealthState {
    /// `threshold_override` mirrors `K8S_CONTACT_THRESHOLD_SECONDS`.
    pub fn new(threshold_override: Option<Duration>) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            override_threshold: threshold_override,
            inner: Mutex::new(Inner::default()),
        })
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    /// Register a stream; `heartbeat` is how often a healthy stream reports in.
    /// The staleness threshold is 2× that, matching upstream's derivation.
    pub fn register_stream(&self, stream: &str, heartbeat: Duration) {
        let mut i = self.inner.lock().unwrap();
        i.alive.insert(stream.to_string(), true);
        i.contact
            .insert(stream.to_string(), (Instant::now(), 2 * heartbeat));
    }

    pub fn contact(&self, stream: &str) {
        if let Ok(mut i) = self.inner.lock()
            && let Some(entry) = i.contact.get_mut(stream)
        {
            entry.0 = Instant::now();
        }
    }

    pub fn stream_dead(&self, stream: &str) {
        if let Ok(mut i) = self.inner.lock() {
            i.alive.insert(stream.to_string(), false);
        }
    }

    /// (status_code, body)
    fn probe(&self) -> (u16, &'static str) {
        if !self.ready.load(Ordering::SeqCst) {
            return (503, "NOT READY");
        }
        let inner = self.inner.lock().unwrap();
        if inner.alive.values().any(|a| !*a) {
            return (503, "NOT LIVE (watcher thread died)");
        }
        let now = Instant::now();
        let stale = inner.contact.values().any(|(t, threshold)| {
            now.duration_since(*t) > self.override_threshold.unwrap_or(*threshold)
        });
        if stale {
            return (503, "NOT LIVE (K8s contact lost)");
        }
        (200, "OK")
    }
}

/// Serves `/healthz` on `port`. Binds IPv6 dual-stack first, falls back to
/// IPv4 (matches upstream's fix for clusters without IPv6).
pub async fn serve(state: Arc<HealthState>, port: u16, cancel: CancellationToken) {
    let listener = match bind(port) {
        Some(l) => l,
        None => {
            error!(port, "health server failed to bind");
            return;
        }
    };
    info!(port, "health server listening");

    // Translate accept into tokio via a blocking thread + channel. The
    // listener is nonblocking so the thread can observe cancellation even
    // when no probes are arriving — a blocking accept() would otherwise
    // leak the thread (and the bound port) past shutdown.
    if let Err(e) = listener.set_nonblocking(true) {
        error!(port, error = %e, "health server failed to configure listener");
        return;
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::net::TcpStream>(16);
    let accept_cancel = cancel.clone();
    let accept_thread = std::thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((s, _)) => {
                    // Accepted sockets may inherit nonblocking mode depending on
                    // the platform; handle() needs blocking reads with a timeout.
                    let _ = s.set_nonblocking(false);
                    if tx.blocking_send(s).is_err() {
                        return;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if accept_cancel.is_cancelled() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => return,
            }
        }
    });

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            Some(stream) = rx.recv() => {
                let st = state.clone();
                tokio::spawn(async move {
                    tokio::task::spawn_blocking(move || handle(stream, st)).await.ok();
                });
            }
        }
    }

    // Wait for the accept thread to notice cancellation and drop the listener.
    let _ = tokio::task::spawn_blocking(move || accept_thread.join()).await;
}

fn bind(port: u16) -> Option<TcpListener> {
    TcpListener::bind(("::", port))
        .or_else(|_| TcpListener::bind(("0.0.0.0", port)))
        .ok()
}

/// Cap on bytes read from a probe connection — a client sending a huge
/// header line must not make the handler buffer unboundedly.
const MAX_REQUEST_BYTES: u64 = 8 * 1024;

fn handle(stream: std::net::TcpStream, state: Arc<HealthState>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new((&stream).take(MAX_REQUEST_BYTES));
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
        return;
    }
    let mut parts = request_line.split_whitespace();
    let path = if parts.next() == Some("GET") {
        parts.next().unwrap_or("")
    } else {
        ""
    };
    // Drain headers so the connection stays well-formed.
    for line in reader.lines() {
        match line {
            Ok(l) if l.is_empty() => break,
            Ok(_) => continue,
            Err(_) => return,
        }
    }

    let (status, body) = if path == "/healthz" {
        state.probe()
    } else {
        (404, "Not Found")
    };
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Service Unavailable",
    };
    let _ = write!(
        &mut &stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(port: u16) -> (u16, String) {
        let resp = reqwest::get(format!("http://127.0.0.1:{port}/healthz"))
            .await
            .unwrap();
        (resp.status().as_u16(), resp.text().await.unwrap())
    }

    async fn start(state: Arc<HealthState>) -> (u16, CancellationToken) {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let cancel = CancellationToken::new();
        tokio::spawn(serve(state, port, cancel.clone()));
        tokio::time::sleep(Duration::from_millis(50)).await;
        (port, cancel)
    }

    #[tokio::test]
    async fn not_ready_until_marked() {
        let state = HealthState::new(None);
        state.register_stream("cm/ns", Duration::from_secs(60));
        let (port, cancel) = start(state.clone()).await;
        let (status, _) = get(port).await;
        assert_eq!(status, 503);
        state.mark_ready();
        let (status, body) = get(port).await;
        assert_eq!(status, 200);
        assert_eq!(body, "OK");
        cancel.cancel();
    }

    #[tokio::test]
    async fn dead_stream_fails_liveness() {
        let state = HealthState::new(None);
        state.register_stream("cm/ns", Duration::from_secs(60));
        state.mark_ready();
        let (port, cancel) = start(state.clone()).await;
        assert_eq!(get(port).await.0, 200);
        state.stream_dead("cm/ns");
        let (status, body) = get(port).await;
        assert_eq!(status, 503);
        assert!(body.contains("watcher"));
        cancel.cancel();
    }

    #[test]
    fn stale_contact_fails_liveness() {
        let state = HealthState::new(None);
        // Heartbeat 10ms → threshold 20ms.
        state.register_stream("cm/ns", Duration::from_millis(10));
        state.mark_ready();
        std::thread::sleep(Duration::from_millis(40));
        let (status, body) = state.probe();
        assert_eq!(status, 503);
        assert!(body.contains("contact"));
    }

    #[test]
    fn threshold_env_override_wins() {
        // 10ms heartbeat would normally go stale in 20ms; a 1h override keeps it live.
        let state = HealthState::new(Some(Duration::from_secs(3600)));
        state.register_stream("cm/ns", Duration::from_millis(10));
        state.mark_ready();
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(state.probe().0, 200);
    }

    #[test]
    fn fresh_contact_within_threshold_stays_live() {
        let state = HealthState::new(None);
        state.register_stream("cm/ns", Duration::from_secs(60));
        state.mark_ready();
        state.contact("cm/ns");
        assert_eq!(state.probe().0, 200);
    }

    #[tokio::test]
    async fn unknown_path_404() {
        let state = HealthState::new(None);
        state.mark_ready();
        let (port, cancel) = start(state).await;
        let resp = reqwest::get(format!("http://127.0.0.1:{port}/nope"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 404);
        cancel.cancel();
    }

    #[tokio::test]
    async fn cancel_releases_the_listener() {
        // The accept thread must notice cancellation, exit, and drop the
        // listener — a leaked thread would keep the port bound forever.
        let state = HealthState::new(None);
        state.mark_ready();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let cancel = CancellationToken::new();
        let server = tokio::spawn(serve(state, port, cancel.clone()));
        tokio::time::sleep(Duration::from_millis(50)).await;

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("serve should return once the accept thread exits")
            .unwrap();

        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err(),
            "listener should be released after cancel"
        );
    }

    #[tokio::test]
    async fn oversized_request_line_is_bounded() {
        // A client sending a huge header line without a newline must not make
        // the handler buffer unboundedly — it answers and closes.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let state = HealthState::new(None);
        state.mark_ready();
        let (port, cancel) = start(state).await;

        let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        sock.write_all(&vec![b'A'; 128 * 1024]).await.unwrap();
        sock.write_all(b"\r\n\r\n").await.unwrap();

        let mut resp = Vec::new();
        let outcome = tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut resp))
            .await
            .expect("handler should close the connection promptly");
        match outcome {
            Ok(_) => assert!(resp.starts_with(b"HTTP/1.1")),
            Err(e) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset,
                "over-limit request should be answered or reset, not {e:?}"
            ),
        }
        cancel.cancel();
    }
}
