//! reqwest client construction for `REQ_URL` callbacks and `*.url` downloads.

use std::sync::Arc;

use crate::config::Config;

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
