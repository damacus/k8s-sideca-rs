//! Reload callbacks: file changes publish a generation number; a single
//! worker performs `REQ_URL` calls with upstream-style retries. A failed
//! callback stays pending and is retried without needing another resource
//! update (deliberate improvement over upstream, which drops it).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{BasicAuthEncoding, Payload, ReqConfig, ReqMethod};

pub struct Reloader {
    cfg: ReqConfig,
    http: reqwest::Client,
    pending: AtomicU64,
    applied: AtomicU64,
    notify: Notify,
}

impl Reloader {
    pub fn new(cfg: ReqConfig, http: reqwest::Client) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            http,
            pending: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            notify: Notify::new(),
        })
    }

    /// Record that file state changed; coalesces bursts — only the latest
    /// pending generation matters.
    pub fn bump(&self) {
        self.pending.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_one();
    }

    #[cfg(test)]
    pub fn applied(&self) -> u64 {
        self.applied.load(Ordering::SeqCst)
    }

    /// Fire the callback once if a generation is pending. METHOD=LIST never
    /// runs the loop — without this the pending callback was silently dropped
    /// on exit.
    pub async fn flush(&self) {
        let pending = self.pending.load(Ordering::SeqCst);
        if pending <= self.applied.load(Ordering::SeqCst) {
            return;
        }
        match self.attempt().await {
            Ok(()) => {
                self.applied.store(pending, Ordering::SeqCst);
            }
            Err(e) => {
                error!(url = %self.cfg.url, error = %e, "reload callback failed");
            }
        }
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken, retry_pause: Duration) {
        loop {
            let pending = self.pending.load(Ordering::SeqCst);
            let applied = self.applied.load(Ordering::SeqCst);
            if pending > applied {
                match self.attempt().await {
                    Ok(()) => {
                        self.applied.store(pending, Ordering::SeqCst);
                        info!(url = %self.cfg.url, generation = pending, "reload callback delivered");
                    }
                    Err(e) => {
                        // Keep the generation pending; pause before the next
                        // round so a dead endpoint doesn't hot-loop.
                        error!(url = %self.cfg.url, error = %e, "reload callback failed; will retry");
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep(retry_pause) => {}
                        }
                        continue;
                    }
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = self.notify.notified() => {}
            }
        }
    }

    async fn attempt(&self) -> Result<(), String> {
        let retries = &self.cfg.common.retries;
        let mut delay = Duration::ZERO;
        for attempt in 0..=retries.total {
            if attempt > 0 {
                tokio::time::sleep(delay).await;
            }
            match self.send_once().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_server_error() && !self.cfg.common.enable_5xx {
                        if attempt < retries.total {
                            delay = retries.backoff_delay(attempt);
                            warn!(status = %status, "reload returned 5xx; retrying");
                            continue;
                        }
                        return Err(format!("server returned {status}"));
                    }
                    return Ok(());
                }
                Err(e) => {
                    if attempt < retries.total {
                        delay = retries.backoff_delay(attempt);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        unreachable!()
    }

    async fn send_once(&self) -> Result<reqwest::Response, String> {
        let mut builder = match self.cfg.method {
            ReqMethod::Get => self.http.get(&self.cfg.url),
            ReqMethod::Post => {
                let b = self.http.post(&self.cfg.url);
                match &self.cfg.payload {
                    Some(Payload::Json(v)) => b.json(v),
                    Some(Payload::Text(t)) => b
                        .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                        .body(t.clone()),
                    None => b,
                }
            }
        };
        if let Some(header) = basic_auth_header(&self.cfg.common) {
            builder = builder.header(reqwest::header::AUTHORIZATION, header);
        }
        builder
            .timeout(self.cfg.common.timeout)
            .send()
            .await
            .map_err(|e| e.to_string())
    }
}

/// Build the `Authorization: Basic …` header, re-reading credential files on
/// every attempt (upstream does the same — supports rotation).
pub fn basic_auth_header(cfg: &crate::config::FetchSettings) -> Option<String> {
    let mut username = cfg.username.clone();
    let mut password = cfg.password.clone();
    if let Some(p) = &cfg.username_file {
        match std::fs::read_to_string(p) {
            Ok(s) => username = Some(s.trim().to_string()),
            Err(e) => warn!(path = %p.display(), error = %e, "cannot read REQ_USERNAME_FILE"),
        }
    }
    if let Some(p) = &cfg.password_file {
        match std::fs::read_to_string(p) {
            Ok(s) => password = Some(s.trim().to_string()),
            Err(e) => warn!(path = %p.display(), error = %e, "cannot read REQ_PASSWORD_FILE"),
        }
    }
    let (u, p) = (username?, password?);
    Some(format!(
        "Basic {}",
        encode_basic(&u, &p, cfg.basic_auth_encoding)
    ))
}

fn encode_basic(user: &str, pass: &str, enc: BasicAuthEncoding) -> String {
    use base64::Engine;
    let raw = match enc {
        BasicAuthEncoding::Utf8 => format!("{user}:{pass}").into_bytes(),
        // latin1: each char maps to one byte; chars > 0xFF can't encode.
        BasicAuthEncoding::Latin1 => format!("{user}:{pass}")
            .chars()
            .map(|c| u32::from(c).min(0xFF) as u8)
            .collect(),
    };
    base64::engine::general_purpose::STANDARD.encode(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RetryConfig;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::AtomicUsize;

    fn cfg(url: String) -> ReqConfig {
        ReqConfig {
            url,
            method: ReqMethod::Get,
            payload: None,
            skip_init: false,
            common: crate::config::FetchSettings {
                username: None,
                password: None,
                username_file: None,
                password_file: None,
                basic_auth_encoding: BasicAuthEncoding::Latin1,
                retries: RetryConfig {
                    total: 2,
                    connect: 2,
                    read: 2,
                    backoff_factor: 0.0,
                },
                timeout: Duration::from_secs(2),
                enable_5xx: false,
            },
        }
    }

    /// Serve `statuses` responses in order, then keep serving the last one.
    fn serve(statuses: Vec<u16>, hits: Arc<AtomicUsize>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut served = 0usize;
            while let Ok((mut s, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = s.read(&mut buf);
                let status = statuses[served.min(statuses.len() - 1)];
                served += 1;
                hits.fetch_add(1, Ordering::SeqCst);
                let body = format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n\r\n");
                let _ = s.write_all(body.as_bytes());
            }
        });
        format!("http://{addr}/reload")
    }

    fn client() -> reqwest::Client {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        reqwest::Client::builder().build().unwrap()
    }

    /// Accept one connection, reply `status`, and hand back the raw request.
    fn capture_once(status: u16) -> (String, std::sync::mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let Ok((mut s, _)) = listener.accept() else {
                return;
            };
            let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                match s.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                        if request_complete(&buf) {
                            break;
                        }
                    }
                }
            }
            let _ =
                s.write_all(format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n\r\n").as_bytes());
            let _ = tx.send(String::from_utf8_lossy(&buf).into_owned());
        });
        (format!("http://{addr}/reload"), rx)
    }

    /// Headers done + `Content-Length` body bytes all present.
    fn request_complete(buf: &[u8]) -> bool {
        let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4) else {
            return false;
        };
        let head = String::from_utf8_lossy(&buf[..head_end]);
        let len = head
            .lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        buf.len() >= head_end + len
    }

    #[test]
    fn basic_auth_latin1_encoding() {
        // "u:p" → dTpw; non-ASCII char maps to its latin1 byte.
        assert_eq!(encode_basic("u", "p", BasicAuthEncoding::Latin1), "dTpw");
        assert_eq!(encode_basic("ü", "p", BasicAuthEncoding::Latin1), "/Dpw");
    }

    #[tokio::test]
    async fn successful_bump_marks_applied() {
        let hits = Arc::new(AtomicUsize::new(0));
        let url = serve(vec![200], hits.clone());
        let r = Reloader::new(cfg(url), client());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(r.clone().run(cancel.clone(), Duration::from_millis(10)));
        r.bump();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(r.applied(), 1);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        cancel.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn burst_coalesces_to_latest_generation() {
        let hits = Arc::new(AtomicUsize::new(0));
        let url = serve(vec![200], hits.clone());
        let r = Reloader::new(cfg(url), client());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(r.clone().run(cancel.clone(), Duration::from_millis(10)));
        r.bump();
        r.bump();
        r.bump();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(r.applied(), 3);
        // First bump consumed immediately, others coalesced.
        assert!(hits.load(Ordering::SeqCst) <= 2);
        cancel.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn failed_callback_retries_and_stays_pending() {
        let hits = Arc::new(AtomicUsize::new(0));
        // First two attempts get 500, then success.
        let url = serve(vec![500, 500, 200], hits.clone());
        let r = Reloader::new(cfg(url), client());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(r.clone().run(cancel.clone(), Duration::from_millis(10)));
        r.bump();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(r.applied(), 1);
        assert!(hits.load(Ordering::SeqCst) >= 3);
        cancel.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn text_payload_is_sent_verbatim_without_json_content_type() {
        // A non-JSON REQ_PAYLOAD must not be sent as application/json —
        // receivers that inspect Content-Type (or validate the body) would
        // otherwise try to parse raw text as JSON.
        let (url, rx) = capture_once(200);
        let mut c = cfg(url);
        c.method = ReqMethod::Post;
        c.payload = Some(Payload::Text("not json {".into()));
        let r = Reloader::new(c, client());
        r.attempt().await.unwrap();

        let req = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let head = req.split("\r\n\r\n").next().unwrap().to_lowercase();
        let body = req.split("\r\n\r\n").nth(1).unwrap();
        assert_eq!(body, "not json {");
        assert!(
            head.contains("content-type: text/plain"),
            "expected text/plain content type, got:\n{head}"
        );
        assert!(!head.contains("application/json"));
    }

    #[tokio::test]
    async fn json_payload_keeps_json_content_type() {
        let (url, rx) = capture_once(200);
        let mut c = cfg(url);
        c.method = ReqMethod::Post;
        c.payload = Some(Payload::Json(serde_json::json!({"a": 1})));
        let r = Reloader::new(c, client());
        r.attempt().await.unwrap();

        let req = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let head = req.split("\r\n\r\n").next().unwrap().to_lowercase();
        let body = req.split("\r\n\r\n").nth(1).unwrap();
        assert_eq!(body, "{\"a\":1}");
        assert!(head.contains("content-type: application/json"));
    }

    #[tokio::test]
    async fn flush_delivers_pending_generation_once() {
        // METHOD=LIST never runs the reloader loop — flush() is its exit path.
        let hits = Arc::new(AtomicUsize::new(0));
        let url = serve(vec![200], hits.clone());
        let r = Reloader::new(cfg(url), client());
        r.flush().await;
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        r.bump();
        r.flush().await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(r.applied(), 1);
        r.flush().await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_run_exits() {
        let r = Reloader::new(cfg("http://127.0.0.1:1/".into()), client());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(r.clone().run(cancel.clone(), Duration::from_millis(10)));
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }
}
