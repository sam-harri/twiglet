//! RocksDB-backed metadata store.
//!
//! Five column families:
//!   - `projects`        — project records keyed by project_id
//!   - `branch_handles`  — stable external branch records: `project\0branch_id` → BranchHandle
//!   - `branch_nodes`    — internal COW tree nodes: `project\0hex(node_id)` → BranchNode
//!   - `objects`         — object versions: `project\0hex(node_id)\0path\0inverted_lsn`
//!   - `snapshots`       — snapshots: `project\0branch_id\0name`
//!
//! ## Key encoding
//!
//! Keys use 0x00 (SEP) as the segment separator — user input containing NUL bytes
//! is rejected at the engine layer.
//!
//! `ProcessUniqueId` (Snowflake) node IDs are bincode-serialized and then hex-encoded
//! before embedding in keys. Hex strings contain only `[0-9a-f]`, so they never
//! accidentally introduce a SEP byte into the key stream. The hex-encoded form is
//! always fixed-length (32 chars for the 16-byte bincode output), which means keys
//! scan and compare correctly.
//!
//! Object keys store LSNs as `u64::MAX - lsn` zero-padded to 20 digits so that the
//! newest version appears first in a forward scan.
//!
//! Structural mutations (reset, delete branch, fork) acquire an ephemeral lease via a
//! `DashSet` + RAII guard (`MutationLease`). Object appends and reads are lock-free.
//!
//! All RocksDB operations run inside `spawn_blocking` to avoid blocking the tokio
//! runtime — even point reads can stall on disk I/O or compaction contention.

use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use dashmap::DashSet;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType,
    Direction, IteratorMode, Options, WriteBatch,
};

use crate::{
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Concatenate byte slices with SEP (0x00) between segments.
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

fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes)
        .map_err(|err| Error::Storage(format!("failed to deserialize value: {err}")))
}

fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value)
        .map_err(|err| Error::Storage(format!("failed to serialize value: {err}")))
}

fn cf_handle<'a>(db: &'a DB, name: &str) -> Result<&'a ColumnFamily> {
    db.cf_handle(name)
        .ok_or_else(|| Error::Storage(format!("missing column family: {name}")))
}

/// Encode a `ProcessUniqueId` as a fixed-length hex string safe for use as a RocksDB
/// key segment. Hex strings are always 32 chars for the 16-byte bincode output and
/// never contain the SEP byte (0x00).
fn node_id_to_key_seg(node_id: ProcessUniqueId) -> Result<Vec<u8>> {
    let raw = serialize(&node_id)?;
    Ok(hex::encode(&raw).into_bytes())
}

/// Build the full key for a branch node record: `project\0hex(node_id)`.
fn branch_node_key(project: &str, node_id: ProcessUniqueId) -> Result<Vec<u8>> {
    let seg = node_id_to_key_seg(node_id)?;
    Ok(join_keys([project.as_bytes(), &seg]))
}

/// Build the prefix used to scan all object versions for a given node:
/// `project\0hex(node_id)\0`.
fn object_node_prefix(project: &str, node_id: ProcessUniqueId) -> Result<Vec<u8>> {
    let seg = node_id_to_key_seg(node_id)?;
    let mut prefix = join_keys([project.as_bytes(), &seg]);
    prefix.push(SEP);
    Ok(prefix)
}

/// Build the prefix for a specific object path on a node:
/// `project\0hex(node_id)\0path\0`.
fn object_path_prefix(project: &str, node_id: ProcessUniqueId, path: &str) -> Result<Vec<u8>> {
    let seg = node_id_to_key_seg(node_id)?;
    let mut prefix = join_keys([project.as_bytes(), &seg, path.as_bytes()]);
    prefix.push(SEP);
    Ok(prefix)
}

fn parse_object_key(key: &[u8]) -> Result<(String, u64)> {
    // Key format: project\0hex(node_id)\0path\0inverted_lsn — extract path and LSN from the right.
    let last_sep = key
        .iter()
        .rposition(|b| *b == SEP)
        .ok_or_else(|| Error::Storage("invalid object key: missing separator".into()))?;
    let inv_bytes = &key[last_sep + 1..];
    let path_sep = key[..last_sep]
        .iter()
        .rposition(|b| *b == SEP)
        .ok_or_else(|| Error::Storage("invalid object key: missing path separator".into()))?;
    let path_bytes = &key[path_sep + 1..last_sep];

    let path = std::str::from_utf8(path_bytes)
        .map_err(|err| Error::Storage(format!("invalid utf8 in path: {err}")))?
        .to_string();
    let inverted = std::str::from_utf8(inv_bytes)
        .map_err(|err| Error::Storage(format!("invalid utf8 in lsn: {err}")))?;

    let stored = inverted
        .parse::<u64>()
        .map_err(|err| Error::Storage(format!("invalid inverted lsn {inverted}: {err}")))?;
    Ok((path, u64::MAX - stored))
}

// ---------------------------------------------------------------------------
// Synchronous helpers (called inside spawn_blocking)
// ---------------------------------------------------------------------------

fn get_handle_blocking(db: &DB, project: &str, branch_id: &str) -> Result<Option<BranchHandle>> {
    let cf = cf_handle(db, CF_BRANCH_HANDLES)?;
    let Some(bytes) = db
        .get_cf(cf, join_keys([project.as_bytes(), branch_id.as_bytes()]))
        .map_err(|err| Error::Storage(format!("failed to get branch handle: {err}")))?
    else {
        return Ok(None);
    };
    Ok(Some(deserialize(&bytes)?))
}

fn get_node_blocking(
    db: &DB,
    project: &str,
    node_id: ProcessUniqueId,
) -> Result<Option<BranchNode>> {
    let key = branch_node_key(project, node_id)?;
    let cf = cf_handle(db, CF_BRANCH_NODES)?;
    let Some(bytes) = db
        .get_cf(cf, &key)
        .map_err(|err| Error::Storage(format!("failed to get branch node: {err}")))?
    else {
        return Ok(None);
    };
    Ok(Some(deserialize(&bytes)?))
}

/// Scan all handles for a project and return branch_ids of children
/// (handles whose `parent_branch_id` equals `parent_branch_id` argument).
fn list_children_blocking(db: &DB, project: &str, parent_branch_id: &str) -> Result<Vec<String>> {
    let cf = cf_handle(db, CF_BRANCH_HANDLES)?;
    let mut prefix = project.as_bytes().to_vec();
    prefix.push(SEP);
    let mut children = Vec::new();

    for entry in db.iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward)) {
        let (key, value) =
            entry.map_err(|err| Error::Storage(format!("branch scan failed: {err}")))?;
        if !key.starts_with(&prefix) {
            break;
        }
        let handle: BranchHandle = deserialize(&value)?;
        if handle.parent_branch_id.as_deref() == Some(parent_branch_id) {
            children.push(handle.branch_id);
        }
    }

    Ok(children)
}

// ---------------------------------------------------------------------------
// MutationLease
// ---------------------------------------------------------------------------

/// RAII guard that removes a branch ID from the mutation lease set on drop.
struct MutationLease<'a> {
    set: &'a DashSet<String>,
    key: String,
}

impl Drop for MutationLease<'_> {
    fn drop(&mut self) {
        self.set.remove(&self.key);
    }
}

// ---------------------------------------------------------------------------
// RocksDbMetadataStore
// ---------------------------------------------------------------------------

pub struct RocksDbMetadataStore {
    db: Arc<DB>,
    /// Ephemeral leases for structural branch mutations (reset, delete, fork).
    /// A branch ID is inserted before the mutation and removed on completion
    /// via the `MutationLease` RAII guard. Appends and reads never touch this.
    mutation_leases: DashSet<String>,
}

impl RocksDbMetadataStore {
    pub fn open(path: &str, block_cache_mb: usize, rate_limit_mb_sec: usize) -> Result<Self> {
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

    fn try_acquire_lease(&self, branch_id: &str) -> Result<MutationLease<'_>> {
        let key = branch_id.to_string();
        if !self.mutation_leases.insert(key.clone()) {
            return Err(Error::Conflict);
        }
        Ok(MutationLease {
            set: &self.mutation_leases,
            key,
        })
    }
}

// ---------------------------------------------------------------------------
// MetadataStore impl
// ---------------------------------------------------------------------------

#[async_trait]
impl MetadataStore for RocksDbMetadataStore {
    async fn create_project(
        &self,
        project: &Project,
        root_handle: &BranchHandle,
        root_node: &BranchNode,
    ) -> Result<()> {
        let project_bytes = serialize(project)?;
        let handle_bytes = serialize(root_handle)?;
        let node_bytes = serialize(root_node)?;
        let project_id = project.project_id.clone();
        let branch_id = root_handle.branch_id.clone();
        let node_key = branch_node_key(&project_id, root_node.node_id)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf_projects = cf_handle(&db, CF_PROJECTS)?;
            if db
                .get_cf(cf_projects, project_id.as_bytes())
                .map_err(|err| Error::Storage(format!("failed to check project existence: {err}")))?
                .is_some()
            {
                return Err(Error::ProjectAlreadyExists);
            }

            let cf_handles = cf_handle(&db, CF_BRANCH_HANDLES)?;
            let cf_nodes = cf_handle(&db, CF_BRANCH_NODES)?;
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
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn get_project(&self, project_id: &str) -> Result<Option<Project>> {
        let project_id = project_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_PROJECTS)?;
            let Some(bytes) = db
                .get_cf(cf, project_id.as_bytes())
                .map_err(|err| Error::Storage(format!("failed to get project: {err}")))?
            else {
                return Ok(None);
            };
            Ok(Some(deserialize::<Project>(&bytes)?))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn delete_project(&self, project_id: &str) -> Result<()> {
        let project_id = project_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_PROJECTS)?;
            if db
                .get_cf(cf, project_id.as_bytes())
                .map_err(|err| Error::Storage(format!("failed to check project: {err}")))?
                .is_none()
            {
                return Err(Error::ProjectNotFound);
            }
            db.delete_cf(cf, project_id.as_bytes())
                .map_err(|err| Error::Storage(format!("failed to delete project: {err}")))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn list_projects(&self, cursor: Option<&str>, limit: usize) -> Result<Page<Project>> {
        let cursor_key = cursor
            .map(|c| STANDARD.decode(c).map_err(|_| Error::InvalidCursor))
            .transpose()?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_PROJECTS)?;
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
                items.push(deserialize::<Project>(&value)?);
                last_returned_key = Some(key.to_vec());
            }

            Ok(Page {
                has_more: next_cursor.is_some(),
                items,
                next_cursor,
            })
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn create_branch(
        &self,
        project: &str,
        handle: &BranchHandle,
        node: &BranchNode,
    ) -> Result<()> {
        let handle_key = join_keys([project.as_bytes(), handle.branch_id.as_bytes()]);
        let handle_bytes = serialize(handle)?;
        let node_key = branch_node_key(project, node.node_id)?;
        let node_bytes = serialize(node)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf_handles = cf_handle(&db, CF_BRANCH_HANDLES)?;
            let cf_nodes = cf_handle(&db, CF_BRANCH_NODES)?;
            let mut batch = WriteBatch::default();
            batch.put_cf(cf_handles, handle_key, handle_bytes);
            batch.put_cf(cf_nodes, node_key, node_bytes);
            db.write(batch)
                .map_err(|err| Error::Storage(format!("failed to create branch: {err}")))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn fork_branch(
        &self,
        project: &str,
        source_branch_id: &str,
        new_handle: &BranchHandle,
        new_node: &BranchNode,
    ) -> Result<()> {
        // Hold the source lease so a concurrent delete can't remove the parent
        // between our existence check and the child write.
        let _source_lease = self.try_acquire_lease(source_branch_id)?;

        let handle_key = join_keys([project.as_bytes(), new_handle.branch_id.as_bytes()]);
        let handle_bytes = serialize(new_handle)?;
        let node_key = branch_node_key(project, new_node.node_id)?;
        let node_bytes = serialize(new_node)?;
        let project = project.to_string();
        let source_branch_id = source_branch_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            get_handle_blocking(&db, &project, &source_branch_id)?.ok_or(Error::BranchNotFound)?;
            let cf_handles = cf_handle(&db, CF_BRANCH_HANDLES)?;
            let cf_nodes = cf_handle(&db, CF_BRANCH_NODES)?;
            let mut batch = WriteBatch::default();
            batch.put_cf(cf_handles, handle_key, handle_bytes);
            batch.put_cf(cf_nodes, node_key, node_bytes);
            db.write(batch)
                .map_err(|err| Error::Storage(format!("failed to fork branch: {err}")))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
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
            let node = get_node_blocking(&db, &project, handle.node_id)?.ok_or_else(|| {
                Error::Storage(format!(
                    "branch handle {} points to missing node {:?}",
                    branch_id, handle.node_id
                ))
            })?;
            Ok(Some((handle, node)))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn delete_branch(&self, project: &str, branch_id: &str) -> Result<()> {
        let _lease = self.try_acquire_lease(branch_id)?;

        let project = project.to_string();
        let branch_id = branch_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            if get_handle_blocking(&db, &project, &branch_id)?.is_none() {
                return Err(Error::BranchNotFound);
            }
            if !list_children_blocking(&db, &project, &branch_id)?.is_empty() {
                return Err(Error::BranchHasChildren);
            }

            let cf = cf_handle(&db, CF_BRANCH_HANDLES)?;
            db.delete_cf(cf, join_keys([project.as_bytes(), branch_id.as_bytes()]))
                .map_err(|err| Error::Storage(format!("failed to delete branch handle: {err}")))
            // Note: the BranchNode is intentionally left in place — it becomes
            // orphaned and is a candidate for future GC.
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
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
            let cf_handles = cf_handle(&db, CF_BRANCH_HANDLES)?;
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

                let handle: BranchHandle = deserialize(&value)?;
                let node = get_node_blocking(&db, &project, handle.node_id)?.ok_or_else(|| {
                    Error::Storage(format!(
                        "branch handle {} points to missing node",
                        handle.branch_id
                    ))
                })?;
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
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn reset_branch_node(
        &self,
        project: &str,
        branch_id: &str,
        new_node: &BranchNode,
    ) -> Result<()> {
        // Acquire a mutation lease to prevent concurrent reset/delete on this branch.
        let _lease = self.try_acquire_lease(branch_id)?;

        let new_node = new_node.clone();
        let node_key = branch_node_key(project, new_node.node_id)?;
        let node_bytes = serialize(&new_node)?;
        let handle_key = join_keys([project.as_bytes(), branch_id.as_bytes()]);
        let project = project.to_string();
        let branch_id = branch_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let mut handle =
                get_handle_blocking(&db, &project, &branch_id)?.ok_or(Error::BranchNotFound)?;

            handle.node_id = new_node.node_id;
            let handle_bytes = serialize(&handle)?;

            let cf_handles = cf_handle(&db, CF_BRANCH_HANDLES)?;
            let cf_nodes = cf_handle(&db, CF_BRANCH_NODES)?;
            let mut batch = WriteBatch::default();
            // Write the new node first, then atomically update the handle pointer.
            batch.put_cf(cf_nodes, node_key, node_bytes);
            batch.put_cf(cf_handles, handle_key, handle_bytes);
            db.write(batch)
                .map_err(|err| Error::Storage(format!("failed to reset branch node: {err}")))
            // Old node remains in CF_BRANCH_NODES, orphaned. Future GC will clean it up.
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
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
            .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn put_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        lsn: u64,
        meta: &ObjectMeta,
    ) -> Result<()> {
        // u64::MAX - lsn, zero-padded to 20 digits — newest version sorts first in lexicographic scans
        let inv = format!("{:020}", u64::MAX - lsn);
        let node_seg = node_id_to_key_seg(node_id)?;
        let key = join_keys([
            project.as_bytes(),
            &node_seg,
            path.as_bytes(),
            inv.as_bytes(),
        ]);
        let bytes = serialize(meta)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_OBJECTS)?;
            db.put_cf(cf, key, bytes)
                .map_err(|err| Error::Storage(format!("failed to put object meta: {err}")))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
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
            let cf = cf_handle(&db, CF_OBJECTS)?;
            for entry in db.iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward)) {
                let (key, value) =
                    entry.map_err(|err| Error::Storage(format!("object scan failed: {err}")))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let (_, lsn) = parse_object_key(&key)?;
                if max_lsn.is_none_or(|cap| lsn <= cap) {
                    return Ok(Some((lsn, deserialize(&value)?)));
                }
            }
            Ok(None)
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn list_objects(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        prefix: Option<&str>,
        max_lsn: Option<u64>,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Page<ObjectRecordEntry>> {
        let mut scan_prefix = object_node_prefix(project, node_id)?;
        if let Some(path_prefix) = prefix {
            scan_prefix.extend_from_slice(path_prefix.as_bytes());
        }

        let seek_key = if let Some(path) = start_after {
            let mut key = object_node_prefix(project, node_id)?;
            // Remove the trailing SEP we added in object_node_prefix, then rebuild with path
            key.pop();
            key.push(SEP);
            key.extend_from_slice(path.as_bytes());
            key.push(SEP);
            Some(key)
        } else {
            None
        };
        let seen_path_init = start_after.map(|p| p.to_string());

        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_OBJECTS)?;
            let start = seek_key.as_deref().unwrap_or(&scan_prefix);
            let mode = IteratorMode::From(start, Direction::Forward);

            let mut items = Vec::new();
            let mut last_path: Option<String> = None;
            let mut seen_path = seen_path_init;

            for entry in db.iterator_cf(cf, mode) {
                let (key, value) =
                    entry.map_err(|err| Error::Storage(format!("object scan failed: {err}")))?;
                if !key.starts_with(&scan_prefix) {
                    break;
                }

                let (path, lsn) = parse_object_key(&key)?;
                if max_lsn.is_some_and(|cap| lsn > cap) {
                    continue;
                }
                if seen_path.as_deref() == Some(path.as_str()) {
                    continue;
                }

                seen_path = Some(path.clone());
                let meta: ObjectMeta = deserialize(&value)?;

                if items.len() == limit {
                    return Ok(Page {
                        has_more: true,
                        items,
                        next_cursor: last_path,
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
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn list_children(&self, project: &str, branch_id: &str) -> Result<Vec<String>> {
        let project = project.to_string();
        let branch_id = branch_id.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || list_children_blocking(&db, &project, &branch_id))
            .await
            .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn bulk_put_objects(
        &self,
        project: &str,
        target_node_id: ProcessUniqueId,
        base_lsn: u64,
        objects: Vec<ObjectRecordEntry>,
    ) -> Result<()> {
        let project = project.to_string();
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let node_seg = node_id_to_key_seg(target_node_id)?;
            let cf = cf_handle(&db, CF_OBJECTS)?;
            let mut batch = WriteBatch::default();
            for (i, obj) in objects.iter().enumerate() {
                let lsn = base_lsn + i as u64;
                let inv = format!("{:020}", u64::MAX - lsn);
                let key = join_keys([
                    project.as_bytes(),
                    &node_seg,
                    obj.path.as_bytes(),
                    inv.as_bytes(),
                ]);
                let meta = ObjectMeta {
                    chunks: obj.chunks.clone(),
                    size: obj.size,
                    content_type: obj.content_type.clone(),
                    tombstone: obj.tombstone,
                    created_at: obj.created_at,
                };
                batch.put_cf(cf, key, serialize(&meta)?);
            }
            db.write(batch)
                .map_err(|err| Error::Storage(format!("bulk_put_objects failed: {err}")))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn get_max_lsn(&self, project: &str) -> Result<u64> {
        let mut scan_prefix = project.as_bytes().to_vec();
        scan_prefix.push(SEP);
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_OBJECTS)?;
            let mut max_lsn: u64 = 0;
            for entry in db.iterator_cf(cf, IteratorMode::From(&scan_prefix, Direction::Forward)) {
                let (key, _) =
                    entry.map_err(|err| Error::Storage(format!("lsn scan failed: {err}")))?;
                if !key.starts_with(&scan_prefix) {
                    break;
                }
                let (_, lsn) = parse_object_key(&key)?;
                max_lsn = max_lsn.max(lsn);
            }
            Ok(max_lsn)
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn get_max_lsn_for_node(&self, project: &str, node_id: ProcessUniqueId) -> Result<u64> {
        let scan_prefix = object_node_prefix(project, node_id)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_OBJECTS)?;
            let mut max_lsn: u64 = 0;
            for entry in db.iterator_cf(cf, IteratorMode::From(&scan_prefix, Direction::Forward)) {
                let (key, _) =
                    entry.map_err(|err| Error::Storage(format!("lsn scan failed: {err}")))?;
                if !key.starts_with(&scan_prefix) {
                    break;
                }
                let (_, lsn) = parse_object_key(&key)?;
                max_lsn = max_lsn.max(lsn);
            }
            Ok(max_lsn)
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
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
        let bytes = serialize(snapshot)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_SNAPSHOTS)?;
            db.put_cf(cf, key, bytes)
                .map_err(|err| Error::Storage(format!("failed to put snapshot: {err}")))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
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
            let cf = cf_handle(&db, CF_SNAPSHOTS)?;
            let Some(bytes) = db
                .get_cf(cf, &key)
                .map_err(|err| Error::Storage(format!("failed to get snapshot: {err}")))?
            else {
                return Ok(None);
            };
            Ok(Some(deserialize(&bytes)?))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
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
            let cf = cf_handle(&db, CF_SNAPSHOTS)?;
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

                items.push(deserialize(&value)?);
                last_returned_key = Some(key.to_vec());
            }

            Ok(Page {
                has_more: next_cursor.is_some(),
                items,
                next_cursor,
            })
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }

    async fn delete_snapshot(&self, project: &str, branch_id: &str, name: &str) -> Result<()> {
        let key = join_keys([project.as_bytes(), branch_id.as_bytes(), name.as_bytes()]);
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let cf = cf_handle(&db, CF_SNAPSHOTS)?;
            db.delete_cf(cf, key)
                .map_err(|err| Error::Storage(format!("failed to delete snapshot: {err}")))
        })
        .await
        .map_err(|err| Error::Storage(format!("blocking task join failed: {err}")))?
    }
}
