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
//! `REQ_SKIP_TLS_VERIFY` must actually disable verification — the flag
//! used to be silently ignored because `use_preconfigured_tls` overrides
//! `danger_accept_invalid_certs` inside reqwest.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use k8s_sidecar_rs::config::{self, Config};
use k8s_sidecar_rs::http::build_req_client;

/// Serve HTTPS on a self-signed "localhost" cert; reply 200 to each request.
fn serve_self_signed() -> String {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let tls = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into()),
            )
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        while let Ok((mut stream, _)) = listener.accept() {
            let Ok(mut conn) = rustls::ServerConnection::new(Arc::clone(&tls)) else {
                continue;
            };
            while conn.is_handshaking() {
                if conn.complete_io(&mut stream).is_err() {
                    break;
                }
            }
            if conn.is_handshaking() {
                continue; // handshake failed (e.g. rejected cert) — next client
            }
            let mut s = rustls::Stream::new(&mut conn, &mut stream);
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf);
            let _ =
                s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
    });
    format!("https://localhost:{port}/")
}

fn cfg_with(pairs: &[(&str, &str)]) -> Config {
    let mut env: HashMap<String, String> = HashMap::from([
        ("LABEL".into(), "x".into()),
        ("FOLDER".into(), "/tmp".into()),
    ]);
    for (k, v) in pairs {
        env.insert(k.to_string(), v.to_string());
    }
    config::load(&env, &[]).unwrap()
}

#[tokio::test]
async fn req_skip_tls_verify_accepts_self_signed() {
    let url = serve_self_signed();
    let cfg = cfg_with(&[("REQ_SKIP_TLS_VERIFY", "true")]);
    let client = build_req_client(&cfg).unwrap();
    client
        .get(url)
        .send()
        .await
        .expect("self-signed cert should be accepted when REQ_SKIP_TLS_VERIFY=true");
}

#[tokio::test]
async fn self_signed_rejected_without_skip() {
    let url = serve_self_signed();
    let cfg = cfg_with(&[]);
    let client = build_req_client(&cfg).unwrap();
    assert!(
        client.get(url).send().await.is_err(),
        "self-signed cert must be rejected without REQ_SKIP_TLS_VERIFY"
    );
}
