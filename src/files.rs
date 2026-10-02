//! File reconciliation: map ConfigMap/Secret payloads to files on disk,
//! write them atomically, and track ownership so stale files can be removed
//! without touching anything the sidecar didn't create.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, error, info, warn};

use crate::config::Kind;

/// Identity of a resource that owns files on disk.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Owner {
    pub kind: Kind,
    pub namespace: String,
    pub name: String,
}

impl Owner {
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.kind, self.namespace, self.name)
    }
}

impl serde::Serialize for Kind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Kind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match <String>::deserialize(d)?.as_str() {
            "configmap" => Ok(Kind::ConfigMap),
            "secret" => Ok(Kind::Secret),
            other => Err(serde::de::Error::custom(format!("unknown kind {other}"))),
        }
    }
}

/// Content for one output file. `Url` is resolved by the reconciler at apply
/// time (upstream `*.url` feature: the value is a URL, the written file is the
/// fetched body).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileContent {
    Bytes(Vec<u8>),
    /// URL to fetch; `binary` mirrors upstream: binary payloads are written
    /// raw, text payloads as UTF-8.
    Url {
        url: String,
        binary: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFile {
    pub path: PathBuf,
    pub content: FileContent,
}

/// Filename prefix used when UNIQUE_FILENAMES is set — upstream format:
/// `namespace_{ns}.{kind}_{name}.{filename}`.
pub fn unique_filename(filename: &str, namespace: &str, kind: Kind, name: &str) -> String {
    format!("namespace_{namespace}.{kind}_{name}.{filename}")
}

/// Resolve the destination folder for a resource, honouring the folder
/// annotation (absolute paths used verbatim, relative resolved against
/// `default_folder`) and FOLDER_PER_NAMESPACE.
///
/// Unlike upstream, a relative annotation that escapes `default_folder` is
/// rejected — deliberate hardening, see SPEC/deliberate-differences.
pub fn resolve_dest_folder(
    annotations: Option<&BTreeMap<String, String>>,
    default_folder: &Path,
    folder_annotation: &str,
    folder_per_namespace: bool,
    namespace: &str,
) -> Result<PathBuf, io::Error> {
    let mut dest = match annotations.and_then(|a| a.get(folder_annotation)) {
        Some(v) => {
            let p = Path::new(v);
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                let joined = default_folder.join(p);
                let normalised = normalize(&joined);
                if !normalised.starts_with(normalize(default_folder)) {
                    warn!(
                        annotation = folder_annotation,
                        value = v,
                        "folder annotation escapes FOLDER; ignoring"
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("annotation {folder_annotation} escapes destination root"),
                    ));
                }
                normalised
            }
        }
        None => default_folder.to_path_buf(),
    };
    if folder_per_namespace {
        dest = dest.join(namespace);
    }
    Ok(dest)
}

fn normalize(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Map one resource's data keys to planned files at `dest`.
/// Keys ending `.url` produce fetch intents with the `.url` suffix stripped.
pub fn plan_files(
    dest: &Path,
    owner: &Owner,
    text_data: &BTreeMap<String, String>,
    binary_data: &BTreeMap<String, Vec<u8>>,
    unique_filenames: bool,
) -> Vec<PlannedFile> {
    let mut out = Vec::with_capacity(text_data.len() + binary_data.len());
    for (key, value) in text_data {
        let (filename, content) = match key.strip_suffix(".url") {
            Some(base) => (
                base.to_string(),
                FileContent::Url {
                    url: value.clone(),
                    binary: false,
                },
            ),
            None => (key.clone(), FileContent::Bytes(value.clone().into_bytes())),
        };
        let filename = if unique_filenames {
            unique_filename(&filename, &owner.namespace, owner.kind, &owner.name)
        } else {
            filename
        };
        out.push(PlannedFile {
            path: dest.join(filename),
            content,
        });
    }
    for (key, value) in binary_data {
        let (filename, content) = match key.strip_suffix(".url") {
            Some(base) => (
                base.to_string(),
                FileContent::Url {
                    // binaryData values arrive already base64-decoded; the
                    // decoded bytes are the URL string.
                    url: String::from_utf8_lossy(value).to_string(),
                    binary: true,
                },
            ),
            None => (key.clone(), FileContent::Bytes(value.clone())),
        };
        let filename = if unique_filenames {
            unique_filename(&filename, &owner.namespace, owner.kind, &owner.name)
        } else {
            filename
        };
        out.push(PlannedFile {
            path: dest.join(filename),
            content,
        });
    }
    out
}

/// Fetches a `.url` target. Implemented by the reqwest client in production;
/// stubbed in tests.
pub trait UrlFetcher {
    fn fetch(
        &self,
        url: &str,
        binary: bool,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, String>> + Send;
}

/// Persisted ownership record: absolute path -> owner key. Lets a restarted
/// sidecar distinguish "my stale files" from unrelated content.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(flatten)]
    pub files: BTreeMap<PathBuf, Owner>,
}

/// Per-resource remembered state for move/remove computation (mirrors
/// upstream `_resources_object_map` + `_resources_dest_folder_map`).
struct ResourceState {
    paths: BTreeSet<PathBuf>,
    /// Latest resource_version seen — drives IGNORE_ALREADY_PROCESSED.
    resource_version: Option<String>,
}

pub struct Reconciler {
    manifest_path: PathBuf,
    manifest: Manifest,
    state: HashMap<String, ResourceState>,
    default_file_mode: Option<u32>,
    ignore_already_processed: bool,
}

impl Reconciler {
    pub fn load(
        folder: &Path,
        default_file_mode: Option<u32>,
        ignore_already_processed: bool,
    ) -> Self {
        let manifest_path = folder.join(crate::config::MANIFEST_FILENAME);
        let manifest = match std::fs::read(&manifest_path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                // Fail closed: a corrupt manifest means "own nothing" rather
                // than risking deleting unrelated files.
                error!(error = %e, "manifest corrupt; starting with empty ownership set");
                Manifest::default()
            }),
            Err(_) => Manifest::default(),
        };
        Self {
            manifest_path,
            manifest,
            state: HashMap::new(),
            default_file_mode,
            ignore_already_processed,
        }
    }

    /// `true` when this resource_version was already applied
    /// (IGNORE_ALREADY_PROCESSED semantics — upstream dedupes on rv).
    pub fn already_processed(&self, owner: &Owner, resource_version: Option<&str>) -> bool {
        if !self.ignore_already_processed {
            return false;
        }
        match (resource_version, self.state.get(&owner.key())) {
            (Some(rv), Some(st)) => st.resource_version.as_deref() == Some(rv),
            _ => false,
        }
    }

    /// Apply a resource's planned files: write new/changed, remove files the
    /// resource used to own that are no longer desired (key removal, empty
    /// data, annotation folder move).
    ///
    /// Returns `true` if any file on disk changed.
    pub async fn apply<F: UrlFetcher>(
        &mut self,
        owner: &Owner,
        planned: Vec<PlannedFile>,
        resource_version: Option<String>,
        fetcher: &F,
    ) -> io::Result<bool> {
        let mut changed = false;
        let mut new_paths = BTreeSet::new();

        for file in planned {
            new_paths.insert(file.path.clone());
            let bytes = match &file.content {
                FileContent::Bytes(b) => b.clone(),
                FileContent::Url { url, binary } => match fetcher.fetch(url, *binary).await {
                    Ok(b) => b,
                    Err(e) => {
                        // Deliberate difference: upstream writes the empty
                        // response body; we keep the previous file content.
                        error!(url = %url, error = %e, "url fetch failed; keeping previous file");
                        continue;
                    }
                },
            };
            changed |= write_if_changed(&file.path, &bytes, self.default_file_mode)?;
        }

        let old = self
            .state
            .insert(
                owner.key(),
                ResourceState {
                    paths: new_paths.clone(),
                    resource_version,
                },
            )
            .map(|s| s.paths)
            .unwrap_or_default();

        for stale in old.difference(&new_paths) {
            // Only remove what this resource still owns in the manifest — a
            // path may have been taken over by another resource (collision).
            if self.manifest.files.get(stale) == Some(owner) {
                changed |= remove_file(stale);
            }
        }

        for p in &new_paths {
            self.manifest.files.insert(p.clone(), owner.clone());
        }
        self.persist_manifest()?;
        Ok(changed)
    }

    /// Remove all files owned by this resource (Deleted event / vanished on
    /// relist).
    pub fn remove(&mut self, owner: &Owner) -> io::Result<bool> {
        let mut changed = false;
        if let Some(st) = self.state.remove(&owner.key()) {
            for p in st.paths {
                if self.manifest.files.get(&p) == Some(owner) {
                    changed |= remove_file(&p);
                }
            }
        }
        self.manifest.files.retain(|_, o| o != owner);
        self.persist_manifest()?;
        Ok(changed)
    }

    /// After initial sync: drop manifest entries (and their files) whose owner
    /// was not observed — restart cleanup of owned stale files. Anything not
    /// in the manifest is left alone.
    pub fn cleanup_stale(&mut self, live_owners: &BTreeSet<String>) -> io::Result<()> {
        let stale: Vec<(PathBuf, Owner)> = self
            .manifest
            .files
            .iter()
            .filter(|(_, o)| !live_owners.contains(&o.key()))
            .map(|(p, o)| (p.clone(), o.clone()))
            .collect();
        for (p, o) in stale {
            info!(path = %p.display(), owner = %o.key(), "removing stale owned file");
            remove_file(&p);
            self.manifest.files.remove(&p);
        }
        self.persist_manifest()
    }

    fn persist_manifest(&self) -> io::Result<()> {
        if let Some(parent) = self.manifest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_vec(&self.manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        atomic_write(&self.manifest_path, &data, None)
    }

    #[cfg(test)]
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

fn existing_hash(path: &Path) -> Option<[u8; 32]> {
    let data = std::fs::read(path).ok()?;
    Some(sha256(&data))
}

/// Write `data` to `path` only when content differs; returns whether a write
/// happened.
fn write_if_changed(path: &Path, data: &[u8], mode: Option<u32>) -> io::Result<bool> {
    if existing_hash(path) == Some(sha256(data)) {
        debug!(path = %path.display(), "content unchanged, skipping write");
        return Ok(false);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    info!(path = %path.display(), "writing file");
    atomic_write(path, data, mode)?;
    Ok(true)
}

/// Write-temp-then-rename so readers never see a partial file
/// (upstream writes in place; this is a deliberate improvement).
pub fn atomic_write(path: &Path, data: &[u8], mode: Option<u32>) -> io::Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, data)?;
    sync_file(&tmp)?;
    if let Some(m) = mode {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(m))?;
        }
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent).ok();
    }
    Ok(())
}

fn sync_file(path: &Path) -> io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn remove_file(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => {
            info!(path = %path.display(), "removed file");
            true
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            debug!(path = %path.display(), "file already gone");
            false
        }
        Err(e) => {
            error!(path = %path.display(), error = %e, "failed to remove file");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct NoFetch;
    impl UrlFetcher for NoFetch {
        async fn fetch(&self, url: &str, _binary: bool) -> Result<Vec<u8>, String> {
            Err(format!("no fetcher for {url}"))
        }
    }

    struct FakeFetch(BTreeMap<String, Vec<u8>>);
    impl UrlFetcher for FakeFetch {
        async fn fetch(&self, url: &str, _binary: bool) -> Result<Vec<u8>, String> {
            self.0.get(url).cloned().ok_or_else(|| "404".to_string())
        }
    }

    fn owner() -> Owner {
        Owner {
            kind: Kind::ConfigMap,
            namespace: "ns".into(),
            name: "cm".into(),
        }
    }

    fn texts(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn unique_filename_format_matches_upstream() {
        assert_eq!(
            unique_filename("a.yaml", "prod", Kind::Secret, "s1"),
            "namespace_prod.secret_s1.a.yaml"
        );
    }

    #[test]
    fn relative_annotation_resolves_under_folder() {
        let ann = BTreeMap::from([(
            "k8s-sidecar-target-directory".to_string(),
            "sub/dir".to_string(),
        )]);
        let dest = resolve_dest_folder(
            Some(&ann),
            Path::new("/folder"),
            "k8s-sidecar-target-directory",
            false,
            "ns",
        )
        .unwrap();
        assert_eq!(dest, PathBuf::from("/folder/sub/dir"));
    }

    #[test]
    fn absolute_annotation_used_verbatim() {
        let ann = BTreeMap::from([(
            "k8s-sidecar-target-directory".to_string(),
            "/elsewhere".to_string(),
        )]);
        let dest = resolve_dest_folder(
            Some(&ann),
            Path::new("/folder"),
            "k8s-sidecar-target-directory",
            false,
            "ns",
        )
        .unwrap();
        assert_eq!(dest, PathBuf::from("/elsewhere"));
    }

    #[test]
    fn relative_annotation_escaping_folder_rejected() {
        let ann = BTreeMap::from([(
            "k8s-sidecar-target-directory".to_string(),
            "../escape".to_string(),
        )]);
        assert!(
            resolve_dest_folder(
                Some(&ann),
                Path::new("/folder"),
                "k8s-sidecar-target-directory",
                false,
                "ns",
            )
            .is_err()
        );
    }

    #[test]
    fn folder_per_namespace_appends_namespace() {
        let dest = resolve_dest_folder(None, Path::new("/folder"), "x", true, "prod").unwrap();
        assert_eq!(dest, PathBuf::from("/folder/prod"));
    }

    #[test]
    fn url_keys_strip_suffix_and_become_fetch_intents() {
        let planned = plan_files(
            Path::new("/d"),
            &owner(),
            &texts(&[("dash.url", "http://x/y"), ("plain.json", "{}")]),
            &BTreeMap::new(),
            false,
        );
        assert_eq!(planned.len(), 2);
        assert!(planned.iter().any(|p| p.path.as_path() == Path::new("/d/dash")
            && matches!(&p.content, FileContent::Url { url, binary: false } if url == "http://x/y")));
        assert!(
            planned
                .iter()
                .any(|p| p.path.as_path() == Path::new("/d/plain.json")
                    && p.content == FileContent::Bytes(b"{}".to_vec()))
        );
    }

    #[test]
    fn binary_url_key_decoded_to_url_string() {
        let bin = BTreeMap::from([("dl.url".to_string(), b"http://dl".to_vec())]);
        let planned = plan_files(Path::new("/d"), &owner(), &BTreeMap::new(), &bin, false);
        assert!(
            matches!(&planned[0].content, FileContent::Url { url, binary: true } if url == "http://dl")
        );
        assert_eq!(planned[0].path, Path::new("/d/dl"));
    }

    #[tokio::test]
    async fn apply_writes_removes_and_moves() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().join("out");
        let mut rec = Reconciler::load(&folder, None, false);

        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("a", "1"), ("b", "2")]),
            &BTreeMap::new(),
            false,
        );
        assert!(
            rec.apply(&owner(), planned, Some("1".into()), &NoFetch)
                .await
                .unwrap()
        );
        assert_eq!(std::fs::read(folder.join("a")).unwrap(), b"1");
        assert_eq!(std::fs::read(folder.join("b")).unwrap(), b"2");

        // Same content → no change.
        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("a", "1"), ("b", "2")]),
            &BTreeMap::new(),
            false,
        );
        assert!(
            !rec.apply(&owner(), planned, Some("1".into()), &NoFetch)
                .await
                .unwrap()
        );

        // Key removed → file removed.
        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("a", "1")]),
            &BTreeMap::new(),
            false,
        );
        assert!(
            rec.apply(&owner(), planned, Some("2".into()), &NoFetch)
                .await
                .unwrap()
        );
        assert!(!folder.join("b").exists());

        // Folder move: files appear at new dest, removed at old.
        let newdest = folder.join("sub");
        let planned = plan_files(
            &newdest,
            &owner(),
            &texts(&[("a", "1")]),
            &BTreeMap::new(),
            false,
        );
        rec.apply(&owner(), planned, Some("3".into()), &NoFetch)
            .await
            .unwrap();
        assert!(newdest.join("a").exists());
        assert!(!folder.join("a").exists());
    }

    #[tokio::test]
    async fn remove_deletes_owned_files_only() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().to_path_buf();
        let mut rec = Reconciler::load(&folder, None, false);
        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("a", "1")]),
            &BTreeMap::new(),
            false,
        );
        rec.apply(&owner(), planned, None, &NoFetch).await.unwrap();

        // Unrelated file survives.
        std::fs::write(folder.join("other"), b"x").unwrap();
        rec.remove(&owner()).unwrap();
        assert!(!folder.join("a").exists());
        assert!(folder.join("other").exists());
    }

    #[tokio::test]
    async fn url_fetch_failure_keeps_previous_file() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().to_path_buf();
        let fetch = FakeFetch(BTreeMap::from([("http://x".to_string(), b"v1".to_vec())]));
        let mut rec = Reconciler::load(&folder, None, false);
        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("f.url", "http://x")]),
            &BTreeMap::new(),
            false,
        );
        rec.apply(&owner(), planned, None, &fetch).await.unwrap();
        assert_eq!(std::fs::read(folder.join("f")).unwrap(), b"v1");

        // URL now fails → previous content preserved.
        let fetch = FakeFetch(BTreeMap::new());
        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("f.url", "http://x")]),
            &BTreeMap::new(),
            false,
        );
        rec.apply(&owner(), planned, None, &fetch).await.unwrap();
        assert_eq!(std::fs::read(folder.join("f")).unwrap(), b"v1");
    }

    #[tokio::test]
    async fn restart_cleanup_removes_owned_stale_files() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().to_path_buf();
        {
            let mut rec = Reconciler::load(&folder, None, false);
            let planned = plan_files(
                &folder,
                &owner(),
                &texts(&[("a", "1")]),
                &BTreeMap::new(),
                false,
            );
            rec.apply(&owner(), planned, None, &NoFetch).await.unwrap();
        }
        // "Restart": new reconciler; owner never seen → stale file removed.
        let mut rec = Reconciler::load(&folder, None, false);
        rec.cleanup_stale(&BTreeSet::new()).unwrap();
        assert!(!folder.join("a").exists());
    }

    #[tokio::test]
    async fn corrupt_manifest_fails_closed() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().to_path_buf();
        std::fs::write(folder.join(crate::config::MANIFEST_FILENAME), b"{not json").unwrap();
        let mut rec = Reconciler::load(&folder, None, false);
        assert!(rec.manifest().files.is_empty());
        // Doesn't delete anything it doesn't own.
        std::fs::write(folder.join("keep"), b"x").unwrap();
        rec.cleanup_stale(&BTreeSet::new()).unwrap();
        assert!(folder.join("keep").exists());
    }

    #[tokio::test]
    async fn already_processed_respects_resource_version() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().to_path_buf();
        let mut rec = Reconciler::load(&folder, None, true);
        assert!(!rec.already_processed(&owner(), Some("5")));
        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("a", "1")]),
            &BTreeMap::new(),
            false,
        );
        rec.apply(&owner(), planned, Some("5".into()), &NoFetch)
            .await
            .unwrap();
        assert!(rec.already_processed(&owner(), Some("5")));
        assert!(!rec.already_processed(&owner(), Some("6")));
    }

    #[tokio::test]
    async fn default_file_mode_applied() {
        let tmp = TempDir::new().unwrap();
        let folder = tmp.path().to_path_buf();
        let mut rec = Reconciler::load(&folder, Some(0o640), false);
        let planned = plan_files(
            &folder,
            &owner(),
            &texts(&[("a", "1")]),
            &BTreeMap::new(),
            false,
        );
        rec.apply(&owner(), planned, None, &NoFetch).await.unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(folder.join("a"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o640
            );
        }
    }
}
