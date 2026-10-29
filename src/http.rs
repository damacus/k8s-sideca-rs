//! reqwest client construction for `REQ_URL` callbacks and `*.url` downloads.

use std::sync::Arc;
use std::time::Duration;

use crate::config::{Config, FailureKind, FetchSettings, RetryConfig};
use crate::files::UrlFetcher;

/// reqwest client rooted on bundled webpki roots — the scratch image ships no
/// CA bundle. `REQ_SKIP_TLS_VERIFY` disables verification for these calls
/// only (kube API TLS is governed by `SKIP_TLS_VERIFY`).
pub fn build_req_client(cfg: &Config) -> Result<reqwest::Client, String> {
    if cfg.req_skip_tls_verify {
        // reqwest silently ignores danger_accept_invalid_certs when a
        // preconfigured TLS config is installed — take reqwest's default
        // TLS path so the flag actually disables verification.
        return reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|e| e.to_string());
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .build()
        .map_err(|e| e.to_string())
}

/// `.url` key downloads — GET with the REQ_* retry/timeout/5xx budget.
///
/// Unlike upstream (one shared authenticated session), REQ_* basic-auth
/// credentials are never attached: the URL comes from resource data, so
/// anyone able to create a labeled `ConfigMap` or Secret could otherwise
/// exfiltrate the credentials to an arbitrary endpoint.
pub struct HttpFetcher {
    http: reqwest::Client,
    retries: RetryConfig,
    timeout: Duration,
    enable_5xx: bool,
}

impl HttpFetcher {
    #[must_use]
    pub fn new(http: reqwest::Client, settings: &FetchSettings) -> Self {
        Self {
            http,
            retries: settings.retries.clone(),
            timeout: settings.timeout,
            enable_5xx: settings.enable_5xx,
        }
    }
}

impl UrlFetcher for HttpFetcher {
    async fn fetch(&self, url: &str, _binary: bool) -> Result<Vec<u8>, String> {
        let mut tracker = self.retries.tracker();
        let mut delay = Duration::ZERO;
        loop {
            if tracker.retries_taken() > 0 {
                tokio::time::sleep(delay).await;
            }
            let req = self.http.get(url).timeout(self.timeout);
            match req.send().await {
                Ok(resp) => {
                    if resp.status().is_server_error() && !self.enable_5xx {
                        let Some(d) = tracker.failed(FailureKind::Status) else {
                            return Err(format!("{url} returned {}", resp.status()));
                        };
                        delay = d;
                        continue;
                    }
                    return resp
                        .bytes()
                        .await
                        .map(|b| b.to_vec())
                        .map_err(|e| e.to_string());
                }
                Err(e) => {
                    let kind = if e.is_connect() {
                        FailureKind::Connect
                    } else {
                        FailureKind::Read
                    };
                    let Some(d) = tracker.failed(kind) else {
                        return Err(e.to_string());
                    };
                    delay = d;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BasicAuthEncoding, RetryConfig};
    use axum::Router;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::get;
    use std::sync::Mutex;

    fn settings() -> FetchSettings {
        FetchSettings {
            username: Some("sidecar".into()),
            password: Some("s3cret".into()),
            username_file: None,
            password_file: None,
            basic_auth_encoding: BasicAuthEncoding::Latin1,
            retries: RetryConfig {
                total: 0,
                connect: 0,
                read: 0,
                backoff_factor: 0.0,
            },
            timeout: Duration::from_secs(2),
            enable_5xx: false,
        }
    }

    async fn record(
        State(seen): State<Arc<Mutex<Vec<HeaderMap>>>>,
        headers: HeaderMap,
    ) -> &'static str {
        seen.lock().unwrap().push(headers);
        "body"
    }

    /// Anyone able to create a labeled `ConfigMap` controls the `*.url`
    /// target. REQ_* credentials authenticate the sidecar to `REQ_URL` only —
    /// they must never reach a resource-chosen URL.
    #[tokio::test]
    async fn url_fetch_does_not_forward_req_basic_auth() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/d", get(record))
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());

        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let client = reqwest::Client::builder().build().unwrap();
        let fetcher = HttpFetcher::new(client, &settings());
        let body = fetcher
            .fetch(&format!("http://{addr}/d"), false)
            .await
            .unwrap();
        assert_eq!(body, b"body");

        let headers: Vec<HeaderMap> = seen.lock().unwrap().clone();
        assert_eq!(headers.len(), 1);
        assert!(
            !headers[0].contains_key(reqwest::header::AUTHORIZATION),
            "basic-auth credentials leaked to .url fetch: {:?}",
            headers[0]
        );
    }
}
