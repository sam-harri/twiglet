//! Metadata storage abstraction.
//!
//! Tdefines the interface for storing projects,
//! branches, objects, and snapshots.

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

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn create_project(
        &self,
        project: &Project,
        root_handle: &BranchHandle,
        root_node: &BranchNode,
    ) -> Result<()>;
    async fn get_project(&self, project_id: &str) -> Result<Option<Project>>;
    async fn delete_project(&self, project_id: &str) -> Result<()>;
    async fn list_projects(&self, cursor: Option<&str>, limit: usize) -> Result<Page<Project>>;

    async fn create_branch(
        &self,
        project: &str,
        handle: &BranchHandle,
        node: &BranchNode,
    ) -> Result<()>;

    /// Validate that `source_branch_id` exists, then atomically write the new
    /// handle and node. Returns the source node's `node_id` so the caller can
    /// use it as `parent_node_id` on the new node.
    async fn fork_branch(
        &self,
        project: &str,
        source_branch_id: &str,
        new_handle: &BranchHandle,
        new_node: &BranchNode,
    ) -> Result<()>;

    async fn get_branch(
        &self,
        project: &str,
        branch_id: &str,
    ) -> Result<Option<(BranchHandle, BranchNode)>>;

    async fn delete_branch(&self, project: &str, branch_id: &str) -> Result<()>;

    async fn list_branches(
        &self,
        project: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<(BranchHandle, BranchNode)>>;

    async fn list_children(&self, project: &str, branch_id: &str) -> Result<Vec<String>>;

    async fn reset_branch_node(
        &self,
        project: &str,
        branch_id: &str,
        new_node: &BranchNode,
    ) -> Result<()>;

    async fn get_branch_node(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
    ) -> Result<Option<BranchNode>>;

    async fn put_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        lsn: u64,
        meta: &ObjectMeta,
    ) -> Result<()>;

    async fn get_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        max_lsn: Option<u64>,
    ) -> Result<Option<(u64, ObjectMeta)>>;

    async fn list_objects(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        prefix: Option<&str>,
        max_lsn: Option<u64>,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Page<ObjectRecordEntry>>;

    async fn bulk_put_objects(
        &self,
        project: &str,
        target_node_id: ProcessUniqueId,
        base_lsn: u64,
        objects: Vec<ObjectRecordEntry>,
    ) -> Result<()>;

    async fn get_max_lsn(&self, project: &str) -> Result<u64>;

    async fn get_max_lsn_for_node(&self, project: &str, node_id: ProcessUniqueId) -> Result<u64>;

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
    async fn delete_snapshot(&self, project: &str, branch_id: &str, name: &str) -> Result<()>;
}
