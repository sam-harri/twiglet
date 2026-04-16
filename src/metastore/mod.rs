//! Metadata storage abstraction.
//!
//! Defines the interface for storing projects, branches, objects, and snapshots.

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
        ObjectMeta {
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
    /// Atomically write the project record, its root branch handle, and the root
    /// COW node in a single batch. Returns `ProjectAlreadyExists` if the project
    /// id is already present.
    async fn create_project(
        &self,
        project: &Project,
        root_handle: &BranchHandle,
        root_node: &BranchNode,
    ) -> Result<()>;

    /// Return the project record, or `None` if it does not exist.
    async fn get_project(&self, project_id: &str) -> Result<Option<Project>>;

    /// Delete the project record. Returns `ProjectNotFound` if absent. Does not
    /// cascade.
    async fn delete_project(&self, project_id: &str) -> Result<()>;

    /// Return a page of project records in insertion order. `cursor` is an opaque
    /// token returned by a previous call; pass `None` to start from the beginning.
    async fn list_projects(&self, cursor: Option<&str>, limit: usize) -> Result<Page<Project>>;

    /// Write a branch handle and its COW node atomically. Used for root branch
    /// creation and for writing the backup branch during a restore so it does not
    /// validate that any parent exists, caller is responsible.
    async fn create_branch(
        &self,
        project: &str,
        handle: &BranchHandle,
        node: &BranchNode,
    ) -> Result<()>;

    /// Validate that `source_branch_id` exists, then atomically write the new
    /// handle and node. Returns `BranchNotFound` if the source is absent.
    /// Implementations should hold a mutation lease on the source for the
    /// duration to prevent a concurrent parent delete from removing it mid-flight.
    async fn fork_branch(
        &self,
        project: &str,
        source_branch_id: &str,
        new_handle: &BranchHandle,
        new_node: &BranchNode,
    ) -> Result<()>;

    /// Return the handle and COW node for a branch, or `None` if the branch does
    /// not exist. Panics if the handle exists but its node is missing (maybe shouldnt
    /// but only way this happens is if there's unrecoverable corruption so idk)
    async fn get_branch(
        &self,
        project: &str,
        branch_id: &str,
    ) -> Result<Option<(BranchHandle, BranchNode)>>;

    /// Delete the branch handle. Returns `BranchNotFound` if absent and
    /// `BranchHasChildren` if any other branch has this branch as its
    /// `parent_branch_id`. The COW node is left in place as an orphan for
    /// future GC (if any?)
    async fn delete_branch(&self, project: &str, branch_id: &str) -> Result<()>;

    /// Return a page of (handle, node) pairs for all branches in the project,
    /// in key order. `cursor` is an opaque token from a previous call.
    async fn list_branches(
        &self,
        project: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<(BranchHandle, BranchNode)>>;

    /// Return the branch ids of all branches whose `parent_branch_id` is
    /// `branch_id`. Used to enforce deletion ordering.
    async fn list_children(&self, project: &str, branch_id: &str) -> Result<Vec<String>>;

    /// Atomically write `new_node` to the node store and update the branch handle
    /// to point at it. The old node is left orphaned for future GC. Used by
    /// restore and reset-from-parent to swap the branch's active COW segment.
    /// Implementations should hold a mutation lease on the branch for the duration.
    async fn reset_branch_node(
        &self,
        project: &str,
        branch_id: &str,
        new_node: &BranchNode,
    ) -> Result<()>;

    /// Return a single COW node by its internal id, or `None` if absent. Used
    /// by the ancestry resolver to walk the parent chain during reads.
    async fn get_branch_node(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
    ) -> Result<Option<BranchNode>>;

    /// Append one object version record to the store, keyed by
    /// `(project, node_id, path, inverted_lsn)`
    async fn put_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        lsn: u64,
        meta: &ObjectMeta,
    ) -> Result<()>;

    /// Return the newest object version for `path` on `node_id` whose LSN is
    /// ≤ `max_lsn` (or uncapped if `None`). Returns `None` if no matching
    /// version exists. It only looks at current node id, ancestrry resolver does the walk
    async fn get_object(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        path: &str,
        max_lsn: Option<u64>,
    ) -> Result<Option<(u64, ObjectMeta)>>;

    /// Return a page of the latest object versions on `node_id`, optionally
    /// filtered by path prefix, capped at `max_lsn`, and starting after
    /// `start_after`. Only the newest version within the LSN cap is returned. 
    /// Like get object, does not walk the COW ancestry chain
    async fn list_objects(
        &self,
        project: &str,
        node_id: ProcessUniqueId,
        prefix: Option<&str>,
        max_lsn: Option<u64>,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Page<ObjectRecordEntry>>;

    /// Write multiple object records in a single batch, assigning LSNs
    /// sequentially starting from `base_lsn`. Used by the merge operation to
    /// replay a source branch's direct mutations onto a target node atomically.
    async fn bulk_put_objects(
        &self,
        project: &str,
        target_node_id: ProcessUniqueId,
        base_lsn: u64,
        objects: Vec<ObjectRecordEntry>,
    ) -> Result<()>;

    /// Return the highest LSN written to the objects store for this project,
    /// across all nodes. Returns 0 if no objects exist. Used to initialise the
    /// per-project LSN counter on startup.
    async fn get_max_lsn(&self, project: &str) -> Result<u64>;

    /// Return the highest LSN written directly to `node_id`. Returns 0 if the
    /// node has no direct writes. Used to determine the head LSN of a branch
    /// after a fork or restore creates a new empty node.
    async fn get_max_lsn_for_node(&self, project: &str, node_id: ProcessUniqueId) -> Result<u64>;

    /// Write a snapshot record keyed by `(project, branch_id, snapshot_id)`.
    async fn create_snapshot(
        &self,
        project: &str,
        branch_id: &str,
        snapshot: &SnapshotRecord,
    ) -> Result<()>;

    /// Return a snapshot by its id, or `None` if absent.
    async fn get_snapshot(
        &self,
        project: &str,
        branch_id: &str,
        name: &str,
    ) -> Result<Option<SnapshotRecord>>;

    /// Return a page of snapshots for a branch in key order.
    async fn list_snapshots(
        &self,
        project: &str,
        branch_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<SnapshotRecord>>;

    /// Delete a snapshot record. Does not error if the snapshot is absent.
    async fn delete_snapshot(&self, project: &str, branch_id: &str, name: &str) -> Result<()>;
}
