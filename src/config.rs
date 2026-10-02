//! Environment variable and CLI flag parsing, mirroring kiwigrid/k8s-sidecar
//! env-var surface (pinned to upstream 1.30.2 / 2.5.0).

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

pub const DEFAULT_FOLDER_ANNOTATION: &str = "k8s-sidecar-target-directory";
pub const MANIFEST_FILENAME: &str = ".k8s-sidecar-rs.manifest.json";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("required environment variable {0} is not set")]
    Missing(&'static str),
    #[error("invalid value for {var}: {value}")]
    Invalid { var: &'static str, value: String },
    #[error("{0} is not supported by this implementation")]
    Unsupported(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    ConfigMap,
    Secret,
}

impl Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::ConfigMap => "configmap",
            Kind::Secret => "secret",
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// List once and exit.
    List,
    /// List, sleep `SLEEP_TIME`, repeat.
    Sleep,
    /// Continuous watch (default for anything unrecognised, like upstream).
    Watch,
}

/// Parsed `RESOURCE_NAME` entry: `name`, `kind/name` or `namespace/kind/name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceNameSelector {
    pub name: String,
    pub kind: Option<Kind>,
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Namespaces {
    /// Watch all namespaces (single cluster-wide stream per resource kind).
    All,
    /// One stream per listed namespace.
    List(Vec<String>),
    /// Resolve from the pod service-account namespace file at runtime.
    PodNamespace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReqMethod {
    Get,
    Post,
}

/// Basic-auth credential encoding (RFC 7617 leaves it undefined; upstream
/// defaults to latin1 via `requests`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasicAuthEncoding {
    Latin1,
    Utf8,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Payload {
    Json(serde_json::Value),
    Text(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    pub total: u32,
    pub connect: u32,
    pub read: u32,
    pub backoff_factor: f64,
}

impl RetryConfig {
    /// urllib3-style exponential backoff for retry `attempt` (1-based),
    /// capped at urllib3's `Retry.BACKOFF_MAX` (120s). The cap also bounds
    /// the delay when the exponent overflows f64 (huge REQ_RETRY_TOTAL) —
    /// uncapped, `from_secs_f64(inf)` panics.
    // f64::max/min pass the non-NaN operand through — a hand-built
    // RetryConfig can carry a NaN factor (pub fields), and f64::clamp
    // would propagate NaN into from_secs_f64 where it panics.
    #[allow(clippy::manual_clamp)]
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        const BACKOFF_MAX_SECS: f64 = 120.0;
        let secs = self.backoff_factor * 2f64.powi(attempt.saturating_sub(1) as i32);
        Duration::from_secs_f64(secs.max(0.0).min(BACKOFF_MAX_SECS))
    }

    /// Per-request retry state — urllib3's connect/read budgets are separate
    /// counters that each also consume `total`.
    pub fn tracker(&self) -> RetryTracker<'_> {
        RetryTracker {
            cfg: self,
            retries_taken: 0,
            connect_used: 0,
            read_used: 0,
        }
    }
}

/// Which budget a failure consumes (all failures also consume `total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// TCP/TLS connect failure (`REQ_RETRY_CONNECT`).
    Connect,
    /// Other send/receive failure — timeouts, resets (`REQ_RETRY_READ`).
    Read,
    /// Server 5xx with retries enabled (`total` only).
    Status,
}

pub struct RetryTracker<'a> {
    cfg: &'a RetryConfig,
    retries_taken: u32,
    connect_used: u32,
    read_used: u32,
}

impl RetryTracker<'_> {
    /// Retries granted so far (the current attempt number).
    pub fn retries_taken(&self) -> u32 {
        self.retries_taken
    }

    /// Record a failure; `Some(delay)` when another attempt is allowed.
    pub fn failed(&mut self, kind: FailureKind) -> Option<Duration> {
        let within_budget = match kind {
            FailureKind::Connect => {
                self.connect_used += 1;
                self.connect_used <= self.cfg.connect
            }
            FailureKind::Read => {
                self.read_used += 1;
                self.read_used <= self.cfg.read
            }
            FailureKind::Status => true,
        };
        if !within_budget || self.retries_taken >= self.cfg.total {
            return None;
        }
        let delay = self.cfg.backoff_delay(self.retries_taken);
        self.retries_taken += 1;
        Some(delay)
    }
}

/// Shared HTTP settings for `REQ_URL` callbacks *and* `*.url` downloads —
/// upstream uses one `requests` session and the `REQ_*` budget for both.
/// Parsed even when `REQ_URL` is unset (`*.url` fetching still works).
#[derive(Debug, Clone, PartialEq)]
pub struct FetchSettings {
    pub retries: RetryConfig,
    pub timeout: Duration,
    pub username: Option<String>,
    pub password: Option<String>,
    pub username_file: Option<PathBuf>,
    pub password_file: Option<PathBuf>,
    pub basic_auth_encoding: BasicAuthEncoding,
    /// Write 5xx response bodies instead of treating them as failures (`.url`
    /// fetches), and do not retry them.
    pub enable_5xx: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReqConfig {
    pub url: String,
    pub method: ReqMethod,
    pub payload: Option<Payload>,
    pub skip_init: bool,
    pub common: FetchSettings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Logfmt,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub label: String,
    pub label_value: Option<String>,
    pub folder: PathBuf,
    pub folder_annotation: String,
    pub folder_per_namespace: bool,
    pub namespaces: Namespaces,
    pub resources: Vec<Kind>,
    pub resource_names: Vec<ResourceNameSelector>,
    pub method: Method,
    pub sleep_time: Duration,
    pub error_throttle_sleep: Duration,
    pub req: Option<ReqConfig>,
    /// HTTP settings for `*.url` downloads (and shared by `req` when set).
    pub fetch: FetchSettings,
    /// Skip TLS verification for Kubernetes API calls (`SKIP_TLS_VERIFY`).
    pub skip_tls_verify: bool,
    /// Skip TLS verification for `REQ_URL`/`*.url` HTTP calls
    /// (`REQ_SKIP_TLS_VERIFY`).
    pub req_skip_tls_verify: bool,
    pub unique_filenames: bool,
    pub default_file_mode: Option<u32>,
    pub kubeconfig: Option<String>,
    pub watch_server_timeout: u64,
    pub watch_client_timeout: u64,
    pub ignore_already_processed: bool,
    /// `K8S_CONTACT_THRESHOLD_SECONDS` — liveness staleness override; when
    /// unset each stream uses 2× its heartbeat interval (upstream 2.11.2).
    pub k8s_contact_threshold: Option<Duration>,
    pub health_port: u16,
    pub log_level: String,
    pub log_format: LogFormat,
    pub log_tz_utc: bool,
}

impl Config {
    /// Effective method for one concrete namespace ("ALL" is passed verbatim).
    /// Upstream: `SLEEP` mode, or `RESOURCE_NAME` set on a namespaced stream,
    /// polls via repeated list instead of watching.
    pub fn effective_method(&self, namespace: &str) -> Method {
        if self.method == Method::Sleep || (namespace != "ALL" && !self.resource_names.is_empty()) {
            Method::Sleep
        } else {
            self.method
        }
    }

    /// `RESOURCE_NAME` entries applicable to one (kind, namespace) stream.
    pub fn resource_names_for(&self, kind: Kind, namespace: &str) -> Vec<String> {
        self.resource_names
            .iter()
            .filter(|s| s.namespace.as_deref().is_none_or(|n| n == namespace))
            .filter(|s| s.kind.is_none_or(|k| k == kind))
            .map(|s| s.name.clone())
            .collect()
    }

    /// `RESOURCE_NAME` has no effect in cluster-wide mode (`NAMESPACE=ALL`):
    /// namespaced selectors never match the single "ALL" stream, and unscoped
    /// names `GET` on a cluster-scoped `Api::all` URL, which 404s for
    /// namespaced kinds. Upstream has the same limitation but says nothing;
    /// we warn at startup.
    pub fn resource_name_ignored(&self) -> bool {
        self.namespaces == Namespaces::All && !self.resource_names.is_empty()
    }
}

fn parse_bool(value: Option<&String>) -> bool {
    value.is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

fn parse_u64(
    env: &HashMap<String, String>,
    var: &'static str,
    default: u64,
) -> Result<u64, ConfigError> {
    match env.get(var) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| ConfigError::Invalid {
            var,
            value: v.clone(),
        }),
    }
}

fn parse_f64(
    env: &HashMap<String, String>,
    var: &'static str,
    default: f64,
) -> Result<f64, ConfigError> {
    match env.get(var) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| ConfigError::Invalid {
            var,
            value: v.clone(),
        }),
    }
}

/// A positive, finite f64 — negative/NaN/inf values would panic inside
/// `Duration::from_secs_f64` or produce nonsensical backoff math.
fn parse_f64_positive(
    env: &HashMap<String, String>,
    var: &'static str,
    default: f64,
) -> Result<f64, ConfigError> {
    let v = parse_f64(env, var, default)?;
    if !v.is_finite() || v <= 0.0 {
        return Err(ConfigError::Invalid {
            var,
            value: env.get(var).cloned().unwrap_or_default(),
        });
    }
    Ok(v)
}

/// A finite, non-negative f64 (backoff factor — 0 disables the delay).
fn parse_f64_nonnegative(
    env: &HashMap<String, String>,
    var: &'static str,
    default: f64,
) -> Result<f64, ConfigError> {
    let v = parse_f64(env, var, default)?;
    if !v.is_finite() || v < 0.0 {
        return Err(ConfigError::Invalid {
            var,
            value: env.get(var).cloned().unwrap_or_default(),
        });
    }
    Ok(v)
}

/// Watch timeouts feed a `u32` apiserver parameter and the liveness
/// heartbeat. `0` is not "no timeout": the server closes the watch
/// immediately (hot reconnect loop) and a zero client read-timeout fails
/// every request. Above `u32::MAX` silently truncates. Bound both.
fn parse_watch_timeout(
    env: &HashMap<String, String>,
    var: &'static str,
    default: u64,
) -> Result<u64, ConfigError> {
    let v = parse_u64(env, var, default)?;
    if v == 0 || v > u64::from(u32::MAX) {
        return Err(ConfigError::Invalid {
            var,
            value: env.get(var).cloned().unwrap_or_default(),
        });
    }
    Ok(v)
}

fn parse_u32(
    env: &HashMap<String, String>,
    var: &'static str,
    default: u32,
) -> Result<u32, ConfigError> {
    match env.get(var) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| ConfigError::Invalid {
            var,
            value: v.clone(),
        }),
    }
}

fn parse_resource_name(value: &str) -> Result<ResourceNameSelector, ConfigError> {
    let mut parts: Vec<&str> = value.rsplitn(3, '/').collect();
    parts.reverse();
    match parts.as_slice() {
        [name] => Ok(ResourceNameSelector {
            name: (*name).to_string(),
            kind: None,
            namespace: None,
        }),
        [kind, name] => {
            let kind = match *kind {
                "configmap" | "configmaps" => Kind::ConfigMap,
                "secret" | "secrets" => Kind::Secret,
                _ => {
                    return Err(ConfigError::Invalid {
                        var: "RESOURCE_NAME",
                        value: value.to_string(),
                    });
                }
            };
            Ok(ResourceNameSelector {
                name: (*name).to_string(),
                kind: Some(kind),
                namespace: None,
            })
        }
        [ns, kind, name] => {
            let kind = match *kind {
                "configmap" | "configmaps" => Kind::ConfigMap,
                "secret" | "secrets" => Kind::Secret,
                _ => {
                    return Err(ConfigError::Invalid {
                        var: "RESOURCE_NAME",
                        value: value.to_string(),
                    });
                }
            };
            Ok(ResourceNameSelector {
                name: (*name).to_string(),
                kind: Some(kind),
                namespace: Some((*ns).to_string()),
            })
        }
        _ => Err(ConfigError::Invalid {
            var: "RESOURCE_NAME",
            value: value.to_string(),
        }),
    }
}

fn parse_file_mode(env: &HashMap<String, String>) -> Result<Option<u32>, ConfigError> {
    match env.get("DEFAULT_FILE_MODE") {
        None => Ok(None),
        Some(v) => {
            if v.len() != 3 || !v.chars().all(|c| ('0'..='7').contains(&c)) {
                return Err(ConfigError::Invalid {
                    var: "DEFAULT_FILE_MODE",
                    value: v.clone(),
                });
            }
            u32::from_str_radix(v, 8)
                .map(Some)
                .map_err(|_| ConfigError::Invalid {
                    var: "DEFAULT_FILE_MODE",
                    value: v.clone(),
                })
        }
    }
}

fn parse_payload(raw: &str) -> Payload {
    match serde_json::from_str(raw) {
        Ok(v) => Payload::Json(v),
        Err(_) => Payload::Text(raw.to_string()),
    }
}

fn parse_args(args: &[String]) -> HashMap<String, String> {
    // Upstream uses argparse with --req-username-file / --req-password-file.
    let mut out = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        if let Some((key, inline)) = args[i].split_once('=') {
            out.insert(key.to_string(), inline.to_string());
        } else if i + 1 < args.len() {
            out.insert(args[i].clone(), args[i + 1].clone());
            i += 1;
        }
        i += 1;
    }
    out
}

/// Build a Config from an environment map and CLI args (injectable for tests).
pub fn load(env: &HashMap<String, String>, args: &[String]) -> Result<Config, ConfigError> {
    // Deliberate unsupported features — fail loudly rather than silently
    // dropping behaviour a deployment relies on.
    if env.get("SCRIPT").is_some_and(|v| !v.is_empty()) {
        return Err(ConfigError::Unsupported("SCRIPT"));
    }
    if parse_bool(env.get("DISABLE_X509_STRICT_VERIFICATION")) {
        return Err(ConfigError::Unsupported("DISABLE_X509_STRICT_VERIFICATION"));
    }

    let label = env
        .get("LABEL")
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or(ConfigError::Missing("LABEL"))?;
    let folder = env
        .get("FOLDER")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .ok_or(ConfigError::Missing("FOLDER"))?;

    let label_value = env.get("LABEL_VALUE").filter(|v| !v.is_empty()).cloned();
    let folder_annotation = env
        .get("FOLDER_ANNOTATION")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| DEFAULT_FOLDER_ANNOTATION.to_string());
    let folder_per_namespace = parse_bool(env.get("FOLDER_PER_NAMESPACE"));

    let namespaces = match env.get("NAMESPACE").filter(|v| !v.is_empty()) {
        None => Namespaces::PodNamespace,
        Some(v) if v == "ALL" => Namespaces::All,
        Some(v) => Namespaces::List(v.split(',').map(|s| s.trim().to_string()).collect()),
    };

    let resources = match env
        .get("RESOURCE")
        .map(String::as_str)
        .unwrap_or("configmap")
    {
        "configmap" => vec![Kind::ConfigMap],
        "secret" => vec![Kind::Secret],
        // Upstream iterates ("secret", "configmap") for `both`.
        "both" => vec![Kind::Secret, Kind::ConfigMap],
        other => {
            return Err(ConfigError::Invalid {
                var: "RESOURCE",
                value: other.to_string(),
            });
        }
    };

    let resource_names = env
        .get("RESOURCE_NAME")
        .map(|v| {
            v.split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| parse_resource_name(s.trim()))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    let method = match env.get("METHOD").map(String::as_str) {
        Some("LIST") => Method::List,
        Some("SLEEP") => Method::Sleep,
        _ => Method::Watch,
    };

    let cli = parse_args(args);
    let username_file = cli
        .get("--req-username-file")
        .or_else(|| env.get("REQ_USERNAME_FILE"))
        .map(PathBuf::from);
    let password_file = cli
        .get("--req-password-file")
        .or_else(|| env.get("REQ_PASSWORD_FILE"))
        .map(PathBuf::from);

    let basic_auth_encoding = match env.get("REQ_BASIC_AUTH_ENCODING").map(String::as_str) {
        None | Some("latin1") | Some("latin-1") | Some("iso-8859-1") => BasicAuthEncoding::Latin1,
        Some("utf-8") | Some("utf8") => BasicAuthEncoding::Utf8,
        Some(other) => {
            return Err(ConfigError::Invalid {
                var: "REQ_BASIC_AUTH_ENCODING",
                value: other.to_string(),
            });
        }
    };
    let fetch = FetchSettings {
        username: env.get("REQ_USERNAME").cloned(),
        password: env.get("REQ_PASSWORD").cloned(),
        username_file,
        password_file,
        basic_auth_encoding,
        enable_5xx: parse_bool(env.get("ENABLE_5XX")),
        retries: RetryConfig {
            total: parse_u32(env, "REQ_RETRY_TOTAL", 5)?,
            connect: parse_u32(env, "REQ_RETRY_CONNECT", 10)?,
            read: parse_u32(env, "REQ_RETRY_READ", 5)?,
            backoff_factor: parse_f64_nonnegative(env, "REQ_RETRY_BACKOFF_FACTOR", 1.1)?,
        },
        timeout: Duration::from_secs_f64(parse_f64_positive(env, "REQ_TIMEOUT", 10.0)?),
    };

    let req = match env.get("REQ_URL").filter(|v| !v.is_empty()) {
        None => None,
        Some(url) => {
            let method = match env.get("REQ_METHOD").map(String::as_str) {
                None | Some("GET") => ReqMethod::Get,
                Some("POST") => ReqMethod::Post,
                Some(other) => {
                    return Err(ConfigError::Invalid {
                        var: "REQ_METHOD",
                        value: other.to_string(),
                    });
                }
            };
            Some(ReqConfig {
                url: url.clone(),
                method,
                payload: env.get("REQ_PAYLOAD").map(|p| parse_payload(p)),
                skip_init: parse_bool(env.get("REQ_SKIP_INIT")),
                common: fetch.clone(),
            })
        }
    };

    // Case-insensitive like upstream's free-form env parsing.
    let log_format = match env.get("LOG_FORMAT").map(|s| s.to_ascii_uppercase()) {
        None => LogFormat::Json,
        Some(ref v) if v == "JSON" => LogFormat::Json,
        Some(ref v) if v == "LOGFMT" => LogFormat::Logfmt,
        Some(_) => {
            return Err(ConfigError::Invalid {
                var: "LOG_FORMAT",
                value: env.get("LOG_FORMAT").cloned().unwrap_or_default(),
            });
        }
    };

    Ok(Config {
        label,
        label_value,
        folder,
        folder_annotation,
        folder_per_namespace,
        namespaces,
        resources,
        resource_names,
        method,
        // Zero would hot-loop the poll/restart paths — clamp to 1s.
        sleep_time: Duration::from_secs(parse_u64(env, "SLEEP_TIME", 60)?.max(1)),
        error_throttle_sleep: Duration::from_secs(
            parse_u64(env, "ERROR_THROTTLE_SLEEP", 5)?.max(1),
        ),
        req,
        fetch,
        skip_tls_verify: parse_bool(env.get("SKIP_TLS_VERIFY")),
        req_skip_tls_verify: parse_bool(env.get("REQ_SKIP_TLS_VERIFY")),
        unique_filenames: parse_bool(env.get("UNIQUE_FILENAMES")),
        default_file_mode: parse_file_mode(env)?,
        kubeconfig: env.get("KUBECONFIG").cloned(),
        watch_server_timeout: parse_watch_timeout(env, "WATCH_SERVER_TIMEOUT", 60)?,
        watch_client_timeout: parse_watch_timeout(env, "WATCH_CLIENT_TIMEOUT", 66)?,
        ignore_already_processed: parse_bool(env.get("IGNORE_ALREADY_PROCESSED")),
        k8s_contact_threshold: env
            .get("K8S_CONTACT_THRESHOLD_SECONDS")
            .map(|v| {
                v.parse::<u64>()
                    .map(Duration::from_secs)
                    .map_err(|_| ConfigError::Invalid {
                        var: "K8S_CONTACT_THRESHOLD_SECONDS",
                        value: v.clone(),
                    })
            })
            .transpose()?,
        // u16 via u64 parse: reject 0 (binds an ephemeral port — probes
        // would never find it) and >65535 (silently truncated before).
        health_port: {
            let p = parse_u64(env, "HEALTH_PORT", 8080)?;
            if p == 0 || p > u64::from(u16::MAX) {
                return Err(ConfigError::Invalid {
                    var: "HEALTH_PORT",
                    value: env.get("HEALTH_PORT").cloned().unwrap_or_default(),
                });
            }
            p as u16
        },
        log_level: env
            .get("LOG_LEVEL")
            .cloned()
            .unwrap_or_else(|| "INFO".into()),
        log_format,
        log_tz_utc: env.get("LOG_TZ").is_some_and(|v| v == "UTC"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn base() -> HashMap<String, String> {
        env(&[
            ("LABEL", "grafana_dashboard"),
            ("FOLDER", "/tmp/dashboards"),
        ])
    }

    #[test]
    fn missing_label_fails() {
        let e = env(&[("FOLDER", "/tmp")]);
        assert_eq!(load(&e, &[]), Err(ConfigError::Missing("LABEL")));
    }

    #[test]
    fn missing_folder_fails() {
        let e = env(&[("LABEL", "x")]);
        assert_eq!(load(&e, &[]), Err(ConfigError::Missing("FOLDER")));
    }

    #[test]
    fn defaults_match_upstream() {
        let cfg = load(&base(), &[]).unwrap();
        assert_eq!(cfg.folder_annotation, "k8s-sidecar-target-directory");
        assert_eq!(cfg.namespaces, Namespaces::PodNamespace);
        assert_eq!(cfg.resources, vec![Kind::ConfigMap]);
        assert_eq!(cfg.method, Method::Watch);
        assert_eq!(cfg.sleep_time, Duration::from_secs(60));
        assert_eq!(cfg.error_throttle_sleep, Duration::from_secs(5));
        assert_eq!(cfg.watch_server_timeout, 60);
        assert_eq!(cfg.watch_client_timeout, 66);
        assert_eq!(cfg.health_port, 8080);
        assert!(!cfg.unique_filenames);
        assert!(!cfg.ignore_already_processed);
        assert_eq!(cfg.default_file_mode, None);
        assert!(cfg.req.is_none());
    }

    #[test]
    fn resource_name_with_namespace_all_is_flagged() {
        let mut e = base();
        e.insert("NAMESPACE".into(), "ALL".into());
        e.insert("RESOURCE_NAME".into(), "grafana".into());
        assert!(load(&e, &[]).unwrap().resource_name_ignored());
        e.remove("RESOURCE_NAME");
        assert!(!load(&e, &[]).unwrap().resource_name_ignored());
    }

    #[test]
    fn resource_name_with_single_namespace_is_not_ignored() {
        let mut e = base();
        e.insert("NAMESPACE".into(), "default".into());
        e.insert("RESOURCE_NAME".into(), "grafana".into());
        assert!(!load(&e, &[]).unwrap().resource_name_ignored());
    }

    #[test]
    fn resource_both_orders_secret_first() {
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("RESOURCE", "both")]);
        assert_eq!(
            load(&e, &[]).unwrap().resources,
            vec![Kind::Secret, Kind::ConfigMap]
        );
    }

    #[test]
    fn invalid_resource_fails() {
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("RESOURCE", "pod")]);
        assert!(matches!(
            load(&e, &[]),
            Err(ConfigError::Invalid {
                var: "RESOURCE",
                ..
            })
        ));
    }

    #[test]
    fn namespace_all_and_list() {
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("NAMESPACE", "ALL")]);
        assert_eq!(load(&e, &[]).unwrap().namespaces, Namespaces::All);
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("NAMESPACE", "a, b")]);
        assert_eq!(
            load(&e, &[]).unwrap().namespaces,
            Namespaces::List(vec!["a".into(), "b".into()])
        );
    }

    #[test]
    fn method_list_and_sleep() {
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("METHOD", "LIST")]);
        assert_eq!(load(&e, &[]).unwrap().method, Method::List);
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("METHOD", "SLEEP")]);
        assert_eq!(load(&e, &[]).unwrap().method, Method::Sleep);
        // Anything else is a watch, like upstream.
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("METHOD", "bogus")]);
        assert_eq!(load(&e, &[]).unwrap().method, Method::Watch);
    }

    #[test]
    fn resource_name_parsing() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("RESOURCE_NAME", "plain,secret/s,ns/configmap/cm"),
        ]);
        let cfg = load(&e, &[]).unwrap();
        assert_eq!(
            cfg.resource_names,
            vec![
                ResourceNameSelector {
                    name: "plain".into(),
                    kind: None,
                    namespace: None
                },
                ResourceNameSelector {
                    name: "s".into(),
                    kind: Some(Kind::Secret),
                    namespace: None
                },
                ResourceNameSelector {
                    name: "cm".into(),
                    kind: Some(Kind::ConfigMap),
                    namespace: Some("ns".into())
                },
            ]
        );
    }

    #[test]
    fn resource_name_forces_sleep_per_namespace() {
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("RESOURCE_NAME", "cm1")]);
        let cfg = load(&e, &[]).unwrap();
        assert_eq!(cfg.method, Method::Watch);
        assert_eq!(cfg.effective_method("myns"), Method::Sleep);
        // Cluster-wide stream ignores RESOURCE_NAME like upstream.
        assert_eq!(cfg.effective_method("ALL"), Method::Watch);
    }

    #[test]
    fn resource_names_for_filters_kind_and_namespace() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            (
                "RESOURCE_NAME",
                "a,secret/s1,ns1/configmap/cm1,ns2/configmap/cm2",
            ),
        ]);
        let cfg = load(&e, &[]).unwrap();
        assert_eq!(
            cfg.resource_names_for(Kind::ConfigMap, "ns1"),
            vec!["a".to_string(), "cm1".to_string()]
        );
        assert_eq!(
            cfg.resource_names_for(Kind::Secret, "ns1"),
            vec!["a".to_string(), "s1".to_string()]
        );
        assert_eq!(
            cfg.resource_names_for(Kind::ConfigMap, "ns2"),
            vec!["a".to_string(), "cm2".to_string()]
        );
    }

    #[test]
    fn script_is_unsupported() {
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("SCRIPT", "/x.sh")]);
        assert_eq!(load(&e, &[]), Err(ConfigError::Unsupported("SCRIPT")));
    }

    #[test]
    fn strict_x509_disable_is_unsupported() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("DISABLE_X509_STRICT_VERIFICATION", "true"),
        ]);
        assert_eq!(
            load(&e, &[]),
            Err(ConfigError::Unsupported("DISABLE_X509_STRICT_VERIFICATION"))
        );
    }

    #[test]
    fn file_mode_must_be_octal_triplet() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("DEFAULT_FILE_MODE", "640"),
        ]);
        assert_eq!(load(&e, &[]).unwrap().default_file_mode, Some(0o640));
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("DEFAULT_FILE_MODE", "999"),
        ]);
        assert!(matches!(
            load(&e, &[]),
            Err(ConfigError::Invalid {
                var: "DEFAULT_FILE_MODE",
                ..
            })
        ));
    }

    #[test]
    fn req_config_full_surface() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            (
                "REQ_URL",
                "http://grafana:3000/api/admin/provisioning/dashboards/reload",
            ),
            ("REQ_METHOD", "POST"),
            ("REQ_PAYLOAD", "{\"k\": 1}"),
            ("REQ_USERNAME", "u"),
            ("REQ_PASSWORD", "p"),
            ("REQ_SKIP_INIT", "true"),
            ("ENABLE_5XX", "true"),
            ("REQ_RETRY_TOTAL", "7"),
            ("REQ_TIMEOUT", "2.5"),
        ]);
        let cfg = load(&e, &[]).unwrap();
        let req = cfg.req.unwrap();
        assert_eq!(req.method, ReqMethod::Post);
        assert_eq!(req.common.username.as_deref(), Some("u"));
        assert!(req.skip_init);
        assert!(req.common.enable_5xx);
        assert_eq!(req.common.retries.total, 7);
        assert_eq!(req.common.retries.connect, 10);
        assert_eq!(req.common.timeout, Duration::from_millis(2500));
        assert!(matches!(req.payload, Some(Payload::Json(_))));
    }

    #[test]
    fn req_payload_non_json_becomes_text() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("REQ_URL", "http://x"),
            ("REQ_PAYLOAD", "not json {"),
        ]);
        let req = load(&e, &[]).unwrap().req.unwrap();
        assert!(matches!(req.payload, Some(Payload::Text(ref t)) if t == "not json {"));
    }

    #[test]
    fn req_method_invalid_fails() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("REQ_URL", "http://x"),
            ("REQ_METHOD", "DELETE"),
        ]);
        assert!(matches!(
            load(&e, &[]),
            Err(ConfigError::Invalid {
                var: "REQ_METHOD",
                ..
            })
        ));
    }

    #[test]
    fn credential_file_precedence_flag_then_env() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("REQ_URL", "http://x"),
            ("REQ_USERNAME_FILE", "/env/user"),
        ]);
        let args = vec!["--req-username-file".to_string(), "/cli/user".to_string()];
        let req = load(&e, &args).unwrap().req.unwrap();
        // CLI flag wins over env (upstream only had the flag at 2.5.0).
        assert_eq!(req.common.username_file, Some(PathBuf::from("/cli/user")));
    }

    #[test]
    fn basic_auth_encoding_latin1_default() {
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("REQ_URL", "http://x")]);
        let req = load(&e, &[]).unwrap().req.unwrap();
        assert_eq!(req.common.basic_auth_encoding, BasicAuthEncoding::Latin1);
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("REQ_URL", "http://x"),
            ("REQ_BASIC_AUTH_ENCODING", "utf-8"),
        ]);
        assert_eq!(
            load(&e, &[])
                .unwrap()
                .req
                .unwrap()
                .common
                .basic_auth_encoding,
            BasicAuthEncoding::Utf8
        );
    }

    #[test]
    fn folder_per_namespace_flag() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("FOLDER_PER_NAMESPACE", "true"),
        ]);
        assert!(load(&e, &[]).unwrap().folder_per_namespace);
    }

    #[test]
    fn zero_sleep_intervals_clamp_to_one_second() {
        // A zero poll/throttle interval is a CPU hot-loop, not "instant".
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("SLEEP_TIME", "0"),
            ("ERROR_THROTTLE_SLEEP", "0"),
        ]);
        let cfg = load(&e, &[]).unwrap();
        assert_eq!(cfg.sleep_time, Duration::from_secs(1));
        assert_eq!(cfg.error_throttle_sleep, Duration::from_secs(1));
    }

    #[test]
    fn zero_watch_timeouts_rejected() {
        for var in ["WATCH_SERVER_TIMEOUT", "WATCH_CLIENT_TIMEOUT"] {
            let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), (var, "0")]);
            assert!(
                load(&e, &[]).is_err(),
                "{var}=0 must be rejected — it hot-loops the stream"
            );
        }
    }

    #[test]
    fn watch_timeout_above_u32_rejected() {
        // watch_server_timeout is cast into a u32 apiserver parameter —
        // larger values used to silently truncate.
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("WATCH_SERVER_TIMEOUT", "4294967296"),
        ]);
        assert!(load(&e, &[]).is_err());
    }

    #[test]
    fn health_port_range_validated() {
        for bad in ["0", "65536", "99999"] {
            let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("HEALTH_PORT", bad)]);
            assert!(load(&e, &[]).is_err(), "HEALTH_PORT={bad} must be rejected");
        }
        let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("HEALTH_PORT", "9090")]);
        assert_eq!(load(&e, &[]).unwrap().health_port, 9090);
    }

    #[test]
    fn nonpositive_or_nonfinite_timeouts_rejected() {
        // These used to panic inside Duration::from_secs_f64.
        for bad in ["-1", "0", "inf", "-inf", "NaN"] {
            let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("REQ_TIMEOUT", bad)]);
            assert!(load(&e, &[]).is_err(), "REQ_TIMEOUT={bad} must be rejected");
        }
    }

    #[test]
    fn negative_backoff_factor_rejected() {
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("REQ_RETRY_BACKOFF_FACTOR", "-1.5"),
        ]);
        assert!(load(&e, &[]).is_err());
        // 0 is legitimate: retry immediately.
        let e = env(&[
            ("LABEL", "x"),
            ("FOLDER", "/tmp"),
            ("REQ_RETRY_BACKOFF_FACTOR", "0"),
        ]);
        assert!(load(&e, &[]).is_ok());
    }

    #[test]
    fn backoff_delay_is_exponential_from_attempt_one() {
        let retries = RetryConfig {
            total: 3,
            connect: 3,
            read: 3,
            backoff_factor: 0.5,
        };
        assert_eq!(retries.backoff_delay(1), Duration::from_secs_f64(0.5));
        assert_eq!(retries.backoff_delay(2), Duration::from_secs_f64(1.0));
        assert_eq!(retries.backoff_delay(3), Duration::from_secs_f64(2.0));
        // attempt 0 is not a real call site but must not panic or go negative.
        assert_eq!(retries.backoff_delay(0), Duration::from_secs_f64(0.5));
    }

    #[test]
    fn log_format_is_case_insensitive() {
        for v in ["json", "Json", "JSON", "logfmt", "Logfmt", "LOGFMT"] {
            let e = env(&[("LABEL", "x"), ("FOLDER", "/tmp"), ("LOG_FORMAT", v)]);
            let fmt = load(&e, &[]).unwrap().log_format;
            let expected = if v.eq_ignore_ascii_case("json") {
                LogFormat::Json
            } else {
                LogFormat::Logfmt
            };
            assert_eq!(fmt, expected, "LOG_FORMAT={v}");
        }
    }

    #[test]
    fn backoff_delay_caps_at_urllib3_max() {
        // urllib3 Retry.BACKOFF_MAX = 120s. Also guards the f64 overflow
        // path: 2^2000 saturates to inf, which used to panic in
        // Duration::from_secs_f64.
        let retries = RetryConfig {
            total: 3000,
            connect: 3,
            read: 3,
            backoff_factor: 1.1,
        };
        assert_eq!(retries.backoff_delay(2000), Duration::from_secs(120));
    }

    #[test]
    fn retry_tracker_honours_connect_and_total_budgets() {
        // urllib3 semantics: a connect failure consumes its own budget AND
        // total — a tight REQ_RETRY_CONNECT caps retries even when total
        // remains.
        let retries = RetryConfig {
            total: 5,
            connect: 1,
            read: 5,
            backoff_factor: 0.0,
        };
        let mut t = retries.tracker();
        assert!(t.failed(FailureKind::Connect).is_some());
        assert!(
            t.failed(FailureKind::Connect).is_none(),
            "connect budget exhausted must stop retries despite total"
        );
    }

    #[test]
    fn retry_tracker_read_and_status_share_total() {
        let retries = RetryConfig {
            total: 2,
            connect: 10,
            read: 5,
            backoff_factor: 0.0,
        };
        let mut t = retries.tracker();
        assert!(t.failed(FailureKind::Read).is_some());
        assert!(t.failed(FailureKind::Status).is_some());
        assert!(t.failed(FailureKind::Read).is_none(), "total exhausted");
    }

    #[test]
    fn backoff_delay_clamps_negative_factor() {
        let retries = RetryConfig {
            total: 3,
            connect: 3,
            read: 3,
            backoff_factor: -1.0,
        };
        assert_eq!(retries.backoff_delay(1), Duration::ZERO);
    }
}
