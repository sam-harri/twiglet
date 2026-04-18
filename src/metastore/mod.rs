//! Metadata storage abstraction.
//!
//! Each method on [`MetadataStore`] owns the consistency guarantees that only a storage
//! backend can provide atomically. See `engine.rs` for the logic/decision on whether something
//! should be in the metastore or the engine

mod rocksdb;

pub use self::rocksdb::RocksDbMetadataStore;

use async_trait::async_trait;

use crate::{
    error::Result,
    id::ProcessUniqueId,
    types::{BranchHandle, BranchNode, ChunkHash, ObjectMeta, Page, Project, SnapshotRecord},
};

#[derive(Clone, Debug)]
pub struct ObjectRecordEntry {
    pub path: String,
    pub lsn: u64,
    pub size: u64,
    pub content_type: String,
    pub tombstone: bool,
    pub chunks: Vec<ChunkHash>,
    pub created_at: i64,
}

impl From<ObjectRecordEntry> for ObjectMeta {
    fn from(e: ObjectRecordEntry) -> Self {
        Self {
            chunks: e.chunks,
            size: e.size,
            content_type: e.content_type,
            tombstone: e.tombstone,
            created_at: e.created_at,
        }
    }
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Atomically creates a project record together with its root branch handle and node.
    async fn create_project(
        &self,
        project: &Project,
        root_handle: &BranchHandle,
        root_node: &BranchNode,
    ) -> Result<()>;

    async fn get_project(&self, project_id: &str) -> Result<Option<Project>>;

    async fn list_projects(&self, cursor: Option<&str>, limit: usize) -> Result<Page<Project>>;

    /// Atomically writes a branch handle and its node.
    async fn create_branch(
        &self,
        project: &str,
        handle: &BranchHandle,
        node: &BranchNode,
    ) -> Result<()>;

    async fn get_branch(
        &self,
        project: &str,
        branch_id: &str,
    ) -> Result<Option<(BranchHandle, BranchNode)>>;

    async fn list_branches(
        &self,
        project: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<(BranchHandle, BranchNode)>>;

    /// Returns the stable external branch IDs of the direct children of `branch_id`.
    async fn list_children(&self, project: &str, branch_id: &str) -> Result<Vec<String>>;

    /// Atomically swaps the node pointer on `branch_id` to `new_node`.
    async fn reset_branch_node(
        &self,
        project: &str,
        branch_id: &str,
        new_node: &BranchNode,
    ) -> Result<()>;

    // Get the internal node from its node id
    async fn get_branch_node(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
    ) -> Result<Option<BranchNode>>;

    /// Appends a single object version (or tombstone) at the given LSN.
    async fn put_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        lsn: u64,
        meta: &ObjectMeta,
    ) -> Result<()>;

    /// Returns the most recent version of `path` on `node_id` at or before `max_lsn`,
    /// along with the LSN at which that version was written.
    async fn get_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        max_lsn: Option<u64>,
    ) -> Result<Option<(u64, ObjectMeta)>>;

    /// Lists object versions on `node_id` (single layer, no ancestry traversal).
    ///
    /// `cursor` is an opaque pagination token returned by a previous call. Pass `None`
    /// to start from the beginning. The returned `Page::next_cursor` is an opaque token
    /// suitable for passing back here.
    async fn list_objects(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        prefix: Option<&str>,
        max_lsn: Option<u64>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<ObjectRecordEntry>>;

    async fn get_max_lsn_for_node(&self, project: &str, node_id: ProcessUniqueId) -> Result<u64>;

    /// Writes `objects` onto `target_node_id`, assigning LSNs starting at `base_lsn`
    /// and incrementing by one per record. Holds an exclusive lock on `source_branch_id`
    /// for the duration, returning `Err(Error::Conflict)` if a concurrent merge of
    /// that branch is already in progress. The locking mechanism is backend-specific.
    async fn merge_objects(
        &self,
        project: &str,
        source_branch_id: &str,
        target_node_id: ProcessUniqueId,
        base_lsn: u64,
        objects: Vec<ObjectRecordEntry>,
    ) -> Result<()>;

    async fn create_snapshot(
        &self,
        project: &str,
        branch_id: &str,
        snapshot: &SnapshotRecord,
    ) -> Result<()>;

    async fn get_snapshot(
        &self,
        project: &str,
        branch_id: &str,
        name: &str,
    ) -> Result<Option<SnapshotRecord>>;

    async fn list_snapshots(
        &self,
        project: &str,
        branch_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<SnapshotRecord>>;
}
