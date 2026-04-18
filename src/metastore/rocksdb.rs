//! RocksDB-backed metadata store.
//!
//! Five column families:
//!   - `projects`        — project records keyed by project_id
//!   - `branch_handles`  — stable external branch records: `project\0branch_id` → BranchHandle
//!   - `branch_nodes`    — internal COW tree nodes: `project\0hex(node_id)` → BranchNode
//!   - `objects`         — object versions: `project\0hex(node_id)\0path\0inverted_lsn`
//!   - `snapshots`       — snapshots: `project\0branch_id\0name`
//!
//! Keys use 0x00 (SEP) as the segment separator  and user input containing NUL bytes
//! is rejected at the engine layer.

use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use dashmap::DashSet;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Direction,
    IteratorMode, Options, WriteBatch,
};

use crate::{
    config::RocksDbMetastoreConfig,
    error::{Error, Result},
    id::ProcessUniqueId,
    types::{BranchHandle, BranchNode, ObjectMeta, Page, Project, SnapshotRecord},
};

use super::{MetadataStore, ObjectRecordEntry};

const CF_PROJECTS: &str = "projects";
const CF_BRANCH_HANDLES: &str = "branch_handles";
const CF_BRANCH_NODES: &str = "branch_nodes";
const CF_OBJECTS: &str = "objects";
const CF_SNAPSHOTS: &str = "snapshots";
const SEP: u8 = 0;

fn join_keys<'a, I>(parts: I) -> Vec<u8>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut out = Vec::new();
    for part in parts {
        if !out.is_empty() {
            out.push(SEP);
        }
        out.extend_from_slice(part);
    }
    out
}

/// Build the full key for a branch node record: `project\0hex(node_id)`.
fn branch_node_key(project: &str, node_id: ProcessUniqueId) -> Result<Vec<u8>> {
    let seg = hex::encode(bincode::serialize(&node_id)?).into_bytes();
    Ok(join_keys([project.as_bytes(), &seg]))
}

/// Build the prefix used to scan all object versions for a given node:
/// `project\0hex(node_id)\0`.
fn object_node_prefix(project: &str, node_id: ProcessUniqueId) -> Result<Vec<u8>> {
    let seg = hex::encode(bincode::serialize(&node_id)?).into_bytes();
    let mut prefix = join_keys([project.as_bytes(), &seg]);
    prefix.push(SEP);
    Ok(prefix)
}

/// Build the prefix for a specific object path on a node:
/// `project\0hex(node_id)\0path\0`.
fn object_path_prefix(project: &str, node_id: ProcessUniqueId, path: &str) -> Result<Vec<u8>> {
    let seg = hex::encode(bincode::serialize(&node_id)?).into_bytes();
    let mut prefix = join_keys([project.as_bytes(), &seg, path.as_bytes()]);
    prefix.push(SEP);
    Ok(prefix)
}

fn parse_object_key(key: &[u8]) -> (String, u64) {
    // Key format: project\0hex(node_id)\0path\0inverted_lsn
    // All errors here indicate database corruption
    let last_sep = key
        .iter()
        .rposition(|b| *b == SEP)
        .expect("object key missing final separator — database corrupted");
    let path_sep = key[..last_sep]
        .iter()
        .rposition(|b| *b == SEP)
        .expect("object key missing path separator — database corrupted");
    let path = std::str::from_utf8(&key[path_sep + 1..last_sep])
        .expect("object key path is not valid utf8 — database corrupted")
        .to_string();
    let stored = std::str::from_utf8(&key[last_sep + 1..])
        .expect("object key lsn is not valid utf8 — database corrupted")
        .parse::<u64>()
        .expect("object key lsn is not a valid u64 — database corrupted");
    (path, u64::MAX - stored)
}

/// Build the full key for a single object version:
/// `project\0hex(node_id)\0path\0inverted_lsn`
fn object_key(project: &str, node_id: ProcessUniqueId, path: &str, lsn: u64) -> Result<Vec<u8>> {
    let node_seg = hex::encode(bincode::serialize(&node_id)?).into_bytes();
    let inv = format!("{:020}", u64::MAX - lsn);
    Ok(join_keys([
        project.as_bytes(),
        &node_seg,
        path.as_bytes(),
        inv.as_bytes(),
    ]))
}

fn get_handle_blocking(db: &DB, project: &str, branch_id: &str) -> Result<Option<BranchHandle>> {
    let cf = db
        .cf_handle(CF_BRANCH_HANDLES)
        .expect("CF_BRANCH_HANDLES not registered");
    let Some(bytes) = db
        .get_cf(cf, join_keys([project.as_bytes(), branch_id.as_bytes()]))
        .map_err(|err| Error::Storage(format!("failed to get branch handle: {err}")))?
    else {
        return Ok(None);
    };
    Ok(Some(bincode::deserialize(&bytes)?))
}

fn get_node_blocking(
    db: &DB,
    project: &str,
    node_id: ProcessUniqueId,
) -> Result<Option<BranchNode>> {
    let key = branch_node_key(project, node_id)?;
    let cf = db
        .cf_handle(CF_BRANCH_NODES)
        .expect("CF_BRANCH_NODES not registered");
    let Some(bytes) = db
        .get_cf(cf, &key)
        .map_err(|err| Error::Storage(format!("failed to get branch node: {err}")))?
    else {
        return Ok(None);
    };
    Ok(Some(bincode::deserialize(&bytes)?))
}

fn list_children_blocking(db: &DB, project: &str, parent_branch_id: &str) -> Result<Vec<String>> {
    let cf = db
        .cf_handle(CF_BRANCH_HANDLES)
        .expect("CF_BRANCH_HANDLES not registered");
    let mut prefix = project.as_bytes().to_vec();
    prefix.push(SEP);
    let mut children = Vec::new();

    for entry in db.iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward)) {
        let (key, value) =
            entry.map_err(|err| Error::Storage(format!("branch scan failed: {err}")))?;
        if !key.starts_with(&prefix) {
            break;
        }
        let handle: BranchHandle = bincode::deserialize(&value)?;
        if handle.parent_branch_id.as_deref() == Some(parent_branch_id) {
            children.push(handle.branch_id);
        }
    }

    Ok(children)
}

pub struct RocksDbMetadataStore {
    db: Arc<DB>,
    mutation_leases: DashSet<String>,
}

impl TryFrom<RocksDbMetastoreConfig> for RocksDbMetadataStore {
    type Error = Error;

    fn try_from(config: RocksDbMetastoreConfig) -> Result<Self> {
        Self::open(
            &config.path,
            config.block_cache_mb,
            config.rate_limit_mb_sec,
        )
    }
}

impl RocksDbMetadataStore {
    fn open(path: &str, block_cache_mb: usize, rate_limit_mb_sec: usize) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(128 * 1024 * 1024);
        db_opts.set_max_write_buffer_number(4);
        db_opts.set_level_compaction_dynamic_level_bytes(true);

        db_opts.set_ratelimiter((rate_limit_mb_sec as i64) * 1024 * 1024, 100_000, 10);

        let cache = Cache::new_lru_cache(block_cache_mb * 1024 * 1024);

        let mut cf_options = Options::default();
        let mut block = BlockBasedOptions::default();
        block.set_bloom_filter(10.0, false);
        block.set_block_cache(&cache);
        cf_options.set_block_based_table_factory(&block);
        cf_options.set_compression_type(DBCompressionType::Lz4);
        cf_options.set_bottommost_compression_type(DBCompressionType::Zstd);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_PROJECTS, cf_options.clone()),
            ColumnFamilyDescriptor::new(CF_BRANCH_HANDLES, cf_options.clone()),
            ColumnFamilyDescriptor::new(CF_BRANCH_NODES, cf_options.clone()),
            ColumnFamilyDescriptor::new(CF_OBJECTS, cf_options.clone()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOTS, cf_options),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)
            .map_err(|err| Error::Storage(format!("failed to open rocksdb: {err}")))?;

        Ok(Self {
            db: Arc::new(db),
            mutation_leases: DashSet::new(),
        })
    }
}

#[async_trait]
impl MetadataStore for RocksDbMetadataStore {
    async fn create_project(
        &self,
        project: &Project,
        root_handle: &BranchHandle,
        root_node: &BranchNode,
    ) -> Result<()> {
        let project_bytes = bincode::serialize(project)?;
        let handle_bytes = bincode::serialize(root_handle)?;
        let node_bytes = bincode::serialize(root_node)?;
        let project_id = project.project_id.clone();
        let branch_id = root_handle.branch_id.clone();
        let node_key = branch_node_key(&project_id, root_node.node_id)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf_projects = db
                .cf_handle(CF_PROJECTS)
                .expect("CF_PROJECTS not registered");
            if db
                .get_cf(cf_projects, project_id.as_bytes())
                .map_err(|err| Error::Storage(format!("failed to check project existence: {err}")))?
                .is_some()
            {
                return Err(Error::ProjectAlreadyExists);
            }

            let cf_handles = db
                .cf_handle(CF_BRANCH_HANDLES)
                .expect("CF_BRANCH_HANDLES not registered");
            let cf_nodes = db
                .cf_handle(CF_BRANCH_NODES)
                .expect("CF_BRANCH_NODES not registered");
            let mut batch = WriteBatch::default();
            batch.put_cf(cf_projects, project_id.as_bytes(), project_bytes);
            batch.put_cf(
                cf_handles,
                join_keys([project_id.as_bytes(), branch_id.as_bytes()]),
                handle_bytes,
            );
            batch.put_cf(cf_nodes, node_key, node_bytes);
            db.write(batch)
                .map_err(|err| Error::Storage(format!("failed to create project with root: {err}")))
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn get_project(&self, project_id: &str) -> Result<Option<Project>> {
        let project_id = project_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_PROJECTS)
                .expect("CF_PROJECTS not registered");
            let Some(bytes) = db
                .get_cf(cf, project_id.as_bytes())
                .map_err(|err| Error::Storage(format!("failed to get project: {err}")))?
            else {
                return Ok(None);
            };
            Ok(Some(bincode::deserialize::<Project>(&bytes)?))
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn list_projects(&self, cursor: Option<&str>, limit: usize) -> Result<Page<Project>> {
        let cursor_key = cursor
            .map(|c| STANDARD.decode(c).map_err(|_| Error::InvalidCursor))
            .transpose()?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_PROJECTS)
                .expect("CF_PROJECTS not registered");
            if let Some(cursor_key) = cursor_key.as_ref()
                && db
                    .get_cf(cf, cursor_key)
                    .map_err(|err| Error::Storage(format!("failed to validate cursor: {err}")))?
                    .is_none()
            {
                return Err(Error::InvalidCursor);
            }
            let mode = if let Some(key) = cursor_key.as_ref() {
                IteratorMode::From(key, Direction::Forward)
            } else {
                IteratorMode::Start
            };

            let mut items = Vec::new();
            let mut next_cursor = None;
            let mut cursor_pending = cursor_key.is_some();
            let mut last_returned_key: Option<Vec<u8>> = None;

            for entry in db.iterator_cf(cf, mode) {
                let (key, value) =
                    entry.map_err(|err| Error::Storage(format!("project scan failed: {err}")))?;
                if let Some(cursor_key) = cursor_key.as_ref()
                    && cursor_pending
                    && key.as_ref() == cursor_key.as_slice()
                {
                    cursor_pending = false;
                    continue;
                }
                if items.len() == limit {
                    if let Some(last_key) = last_returned_key.as_ref() {
                        next_cursor = Some(STANDARD.encode(last_key));
                    }
                    break;
                }
                items.push(bincode::deserialize::<Project>(&value)?);
                last_returned_key = Some(key.to_vec());
            }

            Ok(Page {
                has_more: next_cursor.is_some(),
                items,
                next_cursor,
            })
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn create_branch(
        &self,
        project: &str,
        handle: &BranchHandle,
        node: &BranchNode,
    ) -> Result<()> {
        let handle_key = join_keys([project.as_bytes(), handle.branch_id.as_bytes()]);
        let handle_bytes = bincode::serialize(handle)?;
        let node_key = branch_node_key(project, node.node_id)?;
        let node_bytes = bincode::serialize(node)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf_handles = db
                .cf_handle(CF_BRANCH_HANDLES)
                .expect("CF_BRANCH_HANDLES not registered");
            let cf_nodes = db
                .cf_handle(CF_BRANCH_NODES)
                .expect("CF_BRANCH_NODES not registered");
            let mut batch = WriteBatch::default();
            batch.put_cf(cf_handles, handle_key, handle_bytes);
            batch.put_cf(cf_nodes, node_key, node_bytes);
            db.write(batch)
                .map_err(|err| Error::Storage(format!("failed to create branch: {err}")))
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn get_branch(
        &self,
        project: &str,
        branch_id: &str,
    ) -> Result<Option<(BranchHandle, BranchNode)>> {
        let project = project.to_string();
        let branch_id = branch_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let Some(handle) = get_handle_blocking(&db, &project, &branch_id)? else {
                return Ok(None);
            };
            let node = get_node_blocking(&db, &project, handle.node_id)?
                .expect("branch handle points to missing node — database corrupted");
            Ok(Some((handle, node)))
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn list_branches(
        &self,
        project: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<(BranchHandle, BranchNode)>> {
        let mut project_prefix = project.as_bytes().to_vec();
        project_prefix.push(SEP);
        let cursor_key = cursor
            .map(|c| STANDARD.decode(c).map_err(|_| Error::InvalidCursor))
            .transpose()?;
        let project = project.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf_handles = db
                .cf_handle(CF_BRANCH_HANDLES)
                .expect("CF_BRANCH_HANDLES not registered");
            if let Some(cursor_key) = cursor_key.as_ref() {
                if !cursor_key.starts_with(&project_prefix) {
                    return Err(Error::InvalidCursor);
                }
                if db
                    .get_cf(cf_handles, cursor_key)
                    .map_err(|err| Error::Storage(format!("failed to validate cursor: {err}")))?
                    .is_none()
                {
                    return Err(Error::InvalidCursor);
                }
            }
            let mode = if let Some(key) = cursor_key.as_ref() {
                IteratorMode::From(key, Direction::Forward)
            } else {
                IteratorMode::From(&project_prefix, Direction::Forward)
            };

            let mut items: Vec<(BranchHandle, BranchNode)> = Vec::new();
            let mut next_cursor = None;
            let mut cursor_pending = cursor_key.is_some();
            let mut last_returned_key: Option<Vec<u8>> = None;

            for entry in db.iterator_cf(cf_handles, mode) {
                let (key, value) =
                    entry.map_err(|err| Error::Storage(format!("branch scan failed: {err}")))?;
                if !key.starts_with(&project_prefix) {
                    break;
                }

                if let Some(cursor_key) = cursor_key.as_ref()
                    && cursor_pending
                    && key.as_ref() == cursor_key.as_slice()
                {
                    cursor_pending = false;
                    continue;
                }

                if items.len() == limit {
                    if let Some(last_key) = last_returned_key.as_ref() {
                        next_cursor = Some(STANDARD.encode(last_key));
                    }
                    break;
                }

                let handle: BranchHandle = bincode::deserialize(&value)?;
                let node = get_node_blocking(&db, &project, handle.node_id)?
                    .expect("branch handle points to missing node — database corrupted");
                last_returned_key = Some(key.to_vec());
                items.push((handle, node));
            }

            Ok(Page {
                has_more: next_cursor.is_some(),
                items,
                next_cursor,
            })
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn reset_branch_node(
        &self,
        project: &str,
        branch_id: &str,
        new_node: &BranchNode,
    ) -> Result<()> {
        let new_node = new_node.clone();
        let node_key = branch_node_key(project, new_node.node_id)?;
        let node_bytes = bincode::serialize(&new_node)?;
        let handle_key = join_keys([project.as_bytes(), branch_id.as_bytes()]);
        let project = project.to_string();
        let branch_id = branch_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let mut handle =
                get_handle_blocking(&db, &project, &branch_id)?.ok_or(Error::BranchNotFound)?;

            handle.node_id = new_node.node_id;
            let handle_bytes = bincode::serialize(&handle)?;

            let cf_handles = db
                .cf_handle(CF_BRANCH_HANDLES)
                .expect("CF_BRANCH_HANDLES not registered");
            let cf_nodes = db
                .cf_handle(CF_BRANCH_NODES)
                .expect("CF_BRANCH_NODES not registered");
            let mut batch = WriteBatch::default();
            // Write the new node first, then atomically update the handle pointer.
            batch.put_cf(cf_nodes, node_key, node_bytes);
            batch.put_cf(cf_handles, handle_key, handle_bytes);
            db.write(batch)
                .map_err(|err| Error::Storage(format!("failed to reset branch node: {err}")))
            // Old node remains in CF_BRANCH_NODES, orphaned. Future GC will clean it up.
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn get_branch_node(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
    ) -> Result<Option<BranchNode>> {
        let project = project.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || get_node_blocking(&db, &project, node_id))
            .await
            .expect("spawn_blocking panicked")
    }

    async fn put_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        lsn: u64,
        meta: &ObjectMeta,
    ) -> Result<()> {
        let key = object_key(project, node_id, path, lsn)?;
        let bytes = bincode::serialize(meta)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_OBJECTS).expect("CF_OBJECTS not registered");
            db.put_cf(cf, key, bytes)
                .map_err(|err| Error::Storage(format!("failed to put object meta: {err}")))
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn get_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        max_lsn: Option<u64>,
    ) -> Result<Option<(u64, ObjectMeta)>> {
        let prefix = object_path_prefix(project, node_id, path)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_OBJECTS).expect("CF_OBJECTS not registered");
            for entry in db.iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward)) {
                let (key, value) =
                    entry.map_err(|err| Error::Storage(format!("object scan failed: {err}")))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let (_, lsn) = parse_object_key(&key);
                if max_lsn.is_none_or(|cap| lsn <= cap) {
                    return Ok(Some((lsn, bincode::deserialize(&value)?)));
                }
            }
            Ok(None)
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn list_objects(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        prefix: Option<&str>,
        max_lsn: Option<u64>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<ObjectRecordEntry>> {
        // Decode the opaque cursor to the path of the last item returned by the
        // previous page. The cursor is base64(path_bytes)
        // it encodes a path, not a full RocksDB key, so the same cursor value can
        // be applied across multiple ancestor nodes in the k-way merge
        let after_path = cursor
            .map(|c| {
                let raw = STANDARD.decode(c).map_err(|_| Error::InvalidCursor)?;
                String::from_utf8(raw).map_err(|_| Error::InvalidCursor)
            })
            .transpose()?;

        let node_prefix = object_node_prefix(project, node_id)?;
        let mut scan_prefix = node_prefix.clone();
        if let Some(path_prefix) = prefix {
            scan_prefix.extend_from_slice(path_prefix.as_bytes());
        }

        // Seek directly to the start of the cursor path's entries. The `seen_path`
        // initialisation below then skips all LSN versions of that path so the first
        // item returned is the one immediately after the cursor in path order.
        let seek_key = after_path.as_deref().map(|path| {
            let mut key = node_prefix;
            key.extend_from_slice(path.as_bytes());
            key.push(SEP);
            key
        });

        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_OBJECTS).expect("CF_OBJECTS not registered");
            let start = seek_key.as_deref().unwrap_or(&scan_prefix);
            let mode = IteratorMode::From(start, Direction::Forward);

            let mut items = Vec::new();
            let mut last_path: Option<String> = None;
            let mut seen_path = after_path;

            for entry in db.iterator_cf(cf, mode) {
                let (key, value) =
                    entry.map_err(|err| Error::Storage(format!("object scan failed: {err}")))?;
                if !key.starts_with(&scan_prefix) {
                    break;
                }

                let (path, lsn) = parse_object_key(&key);
                if max_lsn.is_some_and(|cap| lsn > cap) {
                    continue;
                }
                if seen_path.as_deref() == Some(path.as_str()) {
                    continue;
                }

                seen_path = Some(path.clone());
                let meta: ObjectMeta = bincode::deserialize(&value)?;

                if items.len() == limit {
                    return Ok(Page {
                        has_more: true,
                        items,
                        next_cursor: last_path.map(|p| STANDARD.encode(p.as_bytes())),
                    });
                }

                last_path = Some(path.clone());
                items.push(ObjectRecordEntry {
                    path,
                    lsn,
                    size: meta.size,
                    content_type: meta.content_type,
                    tombstone: meta.tombstone,
                    chunks: meta.chunks,
                    created_at: meta.created_at,
                });
            }

            Ok(Page {
                has_more: false,
                items,
                next_cursor: None,
            })
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn list_children(&self, project: &str, branch_id: &str) -> Result<Vec<String>> {
        let project = project.to_string();
        let branch_id = branch_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || list_children_blocking(&db, &project, &branch_id))
            .await
            .expect("spawn_blocking panicked")
    }

    async fn get_max_lsn_for_node(&self, project: &str, node_id: ProcessUniqueId) -> Result<u64> {
        let scan_prefix = object_node_prefix(project, node_id)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_OBJECTS).expect("CF_OBJECTS not registered");
            let mut max_lsn: u64 = 0;
            for entry in db.iterator_cf(cf, IteratorMode::From(&scan_prefix, Direction::Forward)) {
                let (key, _) =
                    entry.map_err(|err| Error::Storage(format!("lsn scan failed: {err}")))?;
                if !key.starts_with(&scan_prefix) {
                    break;
                }
                let (_, lsn) = parse_object_key(&key);
                max_lsn = max_lsn.max(lsn);
            }
            Ok(max_lsn)
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn create_snapshot(
        &self,
        project: &str,
        branch_id: &str,
        snapshot: &SnapshotRecord,
    ) -> Result<()> {
        let key = join_keys([
            project.as_bytes(),
            branch_id.as_bytes(),
            snapshot.snapshot_id.as_bytes(),
        ]);
        let bytes = bincode::serialize(snapshot)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_SNAPSHOTS)
                .expect("CF_SNAPSHOTS not registered");
            db.put_cf(cf, key, bytes)
                .map_err(|err| Error::Storage(format!("failed to put snapshot: {err}")))
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn get_snapshot(
        &self,
        project: &str,
        branch_id: &str,
        name: &str,
    ) -> Result<Option<SnapshotRecord>> {
        let key = join_keys([project.as_bytes(), branch_id.as_bytes(), name.as_bytes()]);
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_SNAPSHOTS)
                .expect("CF_SNAPSHOTS not registered");
            let Some(bytes) = db
                .get_cf(cf, &key)
                .map_err(|err| Error::Storage(format!("failed to get snapshot: {err}")))?
            else {
                return Ok(None);
            };
            Ok(Some(bincode::deserialize(&bytes)?))
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn list_snapshots(
        &self,
        project: &str,
        branch_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<SnapshotRecord>> {
        let mut prefix = join_keys([project.as_bytes(), branch_id.as_bytes()]);
        prefix.push(SEP);

        let cursor_key = cursor
            .map(|c| STANDARD.decode(c).map_err(|_| Error::InvalidCursor))
            .transpose()?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_SNAPSHOTS)
                .expect("CF_SNAPSHOTS not registered");
            if let Some(cursor_key) = cursor_key.as_ref() {
                if !cursor_key.starts_with(&prefix) {
                    return Err(Error::InvalidCursor);
                }
                if db
                    .get_cf(cf, cursor_key)
                    .map_err(|err| Error::Storage(format!("failed to validate cursor: {err}")))?
                    .is_none()
                {
                    return Err(Error::InvalidCursor);
                }
            }
            let mode = if let Some(key) = cursor_key.as_ref() {
                IteratorMode::From(key, Direction::Forward)
            } else {
                IteratorMode::From(&prefix, Direction::Forward)
            };

            let mut items = Vec::new();
            let mut next_cursor = None;
            let mut cursor_pending = cursor_key.is_some();
            let mut last_returned_key: Option<Vec<u8>> = None;

            for entry in db.iterator_cf(cf, mode) {
                let (key, value) =
                    entry.map_err(|err| Error::Storage(format!("snapshot scan failed: {err}")))?;
                if !key.starts_with(&prefix) {
                    break;
                }

                if let Some(cursor_key) = cursor_key.as_ref()
                    && cursor_pending
                    && key.as_ref() == cursor_key.as_slice()
                {
                    cursor_pending = false;
                    continue;
                }

                if items.len() == limit {
                    if let Some(last_key) = last_returned_key.as_ref() {
                        next_cursor = Some(STANDARD.encode(last_key));
                    }
                    break;
                }

                items.push(bincode::deserialize(&value)?);
                last_returned_key = Some(key.to_vec());
            }

            Ok(Page {
                has_more: next_cursor.is_some(),
                items,
                next_cursor,
            })
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn merge_objects(
        &self,
        project: &str,
        source_branch_id: &str,
        target_node_id: ProcessUniqueId,
        base_lsn: u64,
        objects: Vec<ObjectRecordEntry>,
    ) -> Result<()> {
        let lease_key = format!("{project}\0{source_branch_id}");
        if !self.mutation_leases.insert(lease_key.clone()) {
            return Err(Error::Conflict);
        }

        let project = project.to_string();
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_OBJECTS).expect("CF_OBJECTS not registered");
            let mut batch = WriteBatch::default();
            for (i, obj) in objects.into_iter().enumerate() {
                let lsn = base_lsn + i as u64;
                let key = object_key(&project, target_node_id, &obj.path, lsn)?;
                batch.put_cf(cf, key, bincode::serialize(&ObjectMeta::from(obj))?);
            }
            db.write(batch)
                .map_err(|err| Error::Storage(format!("merge_objects failed: {err}")))
        })
        .await;

        self.mutation_leases.remove(&lease_key);
        result.expect("spawn_blocking panicked")
    }
}
