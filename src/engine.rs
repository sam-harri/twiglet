//! Core engine that orchestrates all storage operations.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashSet;
use futures::{StreamExt, TryStreamExt};
use tokio::io::AsyncRead;

use crate::{
    ancestry_resolver::AncestryResolver,
    chunk_store::ChunkStore,
    chunker::Chunker,
    error::{Error, Result},
    id::{generate_branch_id, generate_node_id, generate_project_id, generate_snapshot_id},
    lsn::LsnGenerator,
    metastore::{MetadataStore, ObjectRecordEntry},
    types::{
        BranchHandle, BranchInfo, BranchNode, GetObjectResponse, ObjectListResponse, ObjectMeta,
        Page, Project, PutObjectResponse, ResolvedObject, SnapshotRecord,
    },
};

pub struct Engine {
    pub(crate) lsn: Arc<dyn LsnGenerator>,
    pub(crate) chunker: Arc<dyn Chunker>,
    pub(crate) chunks: Arc<dyn ChunkStore>,
    pub(crate) metadata: Arc<dyn MetadataStore>,
    pub(crate) resolver: Arc<AncestryResolver>,
    /// Guards against concurrent merges from the same source branch.
    /// Keyed by "project\0source_branch_id".
    merge_locks: DashSet<String>,
}

impl Engine {
    pub fn new(
        lsn: Arc<dyn LsnGenerator>,
        chunker: Arc<dyn Chunker>,
        chunks: Arc<dyn ChunkStore>,
        metadata: Arc<dyn MetadataStore>,
        resolver: Arc<AncestryResolver>,
    ) -> Self {
        Self {
            lsn,
            chunker,
            chunks,
            metadata,
            resolver,
            merge_locks: DashSet::new(),
        }
    }

    pub async fn create_project(&self) -> Result<Project> {
        let project_id = generate_project_id();
        let default_branch_id = generate_branch_id();
        let node_id = generate_node_id();
        let now = now_millis();

        let project = Project {
            project_id: project_id.clone(),
            default_branch_id: default_branch_id.clone(),
            created_at: now,
        };

        let root_handle = BranchHandle {
            branch_id: default_branch_id.clone(),
            node_id,
            parent_branch_id: None,
            created_at: now,
        };

        let root_node = BranchNode {
            node_id,
            parent_node_id: None,
            fork_lsn: None,
        };

        self.metadata
            .create_project(&project, &root_handle, &root_node)
            .await?;

        Ok(project)
    }

    pub async fn get_project(&self, project_id: &str) -> Result<Project> {
        self.metadata
            .get_project(project_id)
            .await?
            .ok_or(Error::ProjectNotFound)
    }

    pub async fn delete_project(&self, project_id: &str) -> Result<()> {
        self.metadata.delete_project(project_id).await
    }

    pub async fn list_projects(&self, cursor: Option<&str>, limit: usize) -> Result<Page<Project>> {
        self.metadata.list_projects(cursor, limit.max(1)).await
    }

    pub async fn create_branch(&self, project: &str, source_branch_id: &str) -> Result<BranchInfo> {
        self.get_project(project).await?;

        // Resolve the source branch handle to get the source node_id.
        let (_, source_node) = self
            .metadata
            .get_branch(project, source_branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let branch_id = generate_branch_id();
        let node_id = generate_node_id();
        // Fork LSN is the current tip of the source branch's counter. The child
        // counter seeds from this value, so its first write lands at fork_lsn + 1.
        let fork_lsn = self.lsn.current(project, source_branch_id).await?;
        let now = now_millis();

        let new_handle = BranchHandle {
            branch_id: branch_id.clone(),
            node_id,
            parent_branch_id: Some(source_branch_id.to_string()),
            created_at: now,
        };
        let new_node = BranchNode {
            node_id,
            parent_node_id: Some(source_node.node_id),
            fork_lsn: Some(fork_lsn),
        };

        self.metadata
            .fork_branch(project, source_branch_id, &new_handle, &new_node)
            .await?;

        // New branch starts at fork_lsn; no need for a round-trip to the store.
        Ok(branch_info_from_handle_and_node(
            new_handle, new_node, fork_lsn,
        ))
    }

    pub async fn get_branch(&self, project: &str, branch_id: &str) -> Result<BranchInfo> {
        self.get_project(project).await?;
        let (handle, node) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        let head_lsn = self.lsn.current(project, branch_id).await?;
        Ok(branch_info_from_handle_and_node(handle, node, head_lsn))
    }

    pub async fn delete_branch(&self, project: &str, branch_id: &str) -> Result<()> {
        let project_meta = self.get_project(project).await?;
        if project_meta.default_branch_id == branch_id {
            return Err(Error::CannotDeleteDefaultBranch);
        }

        self.metadata.delete_branch(project, branch_id).await
    }

    pub async fn list_branches(
        &self,
        project: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<BranchInfo>> {
        self.get_project(project).await?;
        let page = self
            .metadata
            .list_branches(project, cursor, limit.max(1))
            .await?;
        let mut items = Vec::with_capacity(page.items.len());
        for (handle, node) in page.items {
            let head_lsn = self.lsn.current(project, &handle.branch_id).await?;
            items.push(branch_info_from_handle_and_node(handle, node, head_lsn));
        }
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
        })
    }

    /// Fork a new child branch from `branch_id` at `lsn`, returning the new branch.
    ///
    /// The source branch is left completely untouched. The new branch sees only
    /// history up to `lsn` — writes to it advance from there.
    pub async fn fork_at_lsn(
        &self,
        project: &str,
        branch_id: &str,
        lsn: u64,
    ) -> Result<BranchInfo> {
        self.get_project(project).await?;
        let (source_handle, source_node) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let branch_lsn = self.lsn.current(project, branch_id).await?;
        if lsn > branch_lsn {
            return Err(Error::InvalidRestoreLsn);
        }

        let new_branch_id = generate_branch_id();
        let new_node_id = generate_node_id();
        let now = now_millis();

        let new_handle = BranchHandle {
            branch_id: new_branch_id,
            node_id: new_node_id,
            parent_branch_id: Some(source_handle.branch_id),
            created_at: now,
        };
        let new_node = BranchNode {
            node_id: new_node_id,
            parent_node_id: Some(source_node.node_id),
            fork_lsn: Some(lsn),
        };

        self.metadata
            .create_branch(project, &new_handle, &new_node)
            .await?;

        // New fork branch starts at lsn; no round-trip needed.
        Ok(branch_info_from_handle_and_node(new_handle, new_node, lsn))
    }

    /// Rewind `branch_id` in-place to `lsn`, atomically swapping its internal node.
    ///
    /// Before rewinding, a **backup branch** is created as a child of `branch_id`
    /// pointing at its current node, so no history is lost. The same external
    /// `branch_id` is preserved; future writes land on a fresh node that chains
    /// back through the old node capped at `lsn`.
    ///
    /// Returns `(restored_branch_info, backup_branch_info)`. The backup is a child
    /// of `branch_id`, which prevents accidental deletion of `branch_id` until the
    /// backup is explicitly removed.
    ///
    /// **Failure safety**: if the node swap fails after the backup is written, the
    /// original branch is untouched and only an orphaned backup exists.
    pub async fn restore(
        &self,
        project: &str,
        branch_id: &str,
        lsn: u64,
    ) -> Result<(BranchInfo, BranchInfo)> {
        self.get_project(project).await?;
        let (handle, node) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let branch_lsn = self.lsn.current(project, branch_id).await?;
        if lsn > branch_lsn {
            return Err(Error::InvalidRestoreLsn);
        }

        let now = now_millis();

        // Step 1: Create a backup branch pointing at the current node so that the
        // pre-restore state is never lost. Writing the same node record again is
        // idempotent in RocksDB (same key, same value).
        let backup_handle = BranchHandle {
            branch_id: generate_branch_id(),
            node_id: node.node_id,
            parent_branch_id: Some(branch_id.to_string()),
            created_at: now,
        };
        self.metadata
            .create_branch(project, &backup_handle, &node)
            .await?;

        // Step 2: Create a fresh node chaining to the current node capped at `lsn`,
        // then atomically swap the handle to point at it.
        let new_node = BranchNode {
            node_id: generate_node_id(),
            parent_node_id: Some(node.node_id),
            fork_lsn: Some(lsn),
        };
        self.metadata
            .reset_branch_node(project, branch_id, &new_node)
            .await?;

        let head_lsn = self.lsn.current(project, branch_id).await?;
        let backup_head_lsn = self.lsn.current(project, &backup_handle.branch_id).await?;
        let restored_info = branch_info_from_handle_and_node(handle, new_node, head_lsn);
        let backup_info = branch_info_from_handle_and_node(backup_handle, node, backup_head_lsn);
        Ok((restored_info, backup_info))
    }

    /// Reset a branch by re-pointing it to a new node forked from its parent at
    /// the current LSN. The same `branch_id` is preserved; old writes on the
    /// branch become invisible (the node that held them is orphaned).
    ///
    /// Returns `Error::BranchIsRoot` if the branch has no parent.
    pub async fn reset_from_parent(&self, project: &str, branch_id: &str) -> Result<BranchInfo> {
        self.get_project(project).await?;
        let (handle, node) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let parent_node_id = node.parent_node_id.ok_or(Error::BranchIsRoot)?;
        let parent_branch_id = handle
            .parent_branch_id
            .as_deref()
            .ok_or(Error::BranchIsRoot)?;
        let fork_lsn = self.lsn.current(project, parent_branch_id).await?;

        let new_node = BranchNode {
            node_id: generate_node_id(),
            parent_node_id: Some(parent_node_id),
            fork_lsn: Some(fork_lsn),
        };

        self.metadata
            .reset_branch_node(project, branch_id, &new_node)
            .await?;

        let head_lsn = self.lsn.current(project, branch_id).await?;
        Ok(branch_info_from_handle_and_node(handle, new_node, head_lsn))
    }

    pub async fn put_object<R: AsyncRead + Unpin + Send>(
        &self,
        project: &str,
        branch_id: &str,
        path: &str,
        mut stream: R,
        content_type: &str,
    ) -> Result<PutObjectResponse> {
        validate_no_nul("path", path)?;
        let (_, node) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        let node_id = node.node_id;

        // Check existence before chunking so we can return 200 vs 201.
        let existing = self
            .metadata
            .get_object(project, node_id, path, None)
            .await?;
        let created = existing.is_none_or(|(_, meta)| meta.tombstone);

        const UPLOAD_CONCURRENCY: usize = 256;

        let chunk_stream = self.chunker.chunk(&mut stream);

        let mut size: u64 = 0;
        let mut chunk_hashes = Vec::new();

        let mut uploads = chunk_stream
            .map_ok(|chunk| {
                let store = self.chunks.clone();
                async move {
                    store.put(&chunk.hash, chunk.data).await?;
                    Ok((chunk.hash, chunk.length))
                }
            })
            .try_buffered(UPLOAD_CONCURRENCY);

        while let Some((hash, length)) = uploads.try_next().await? {
            size += length as u64;
            chunk_hashes.push(hash);
        }

        let lsn = self.lsn.next(project, branch_id).await?;

        let meta = ObjectMeta {
            chunks: chunk_hashes,
            size,
            content_type: content_type.to_string(),
            tombstone: false,
            created_at: now_millis(),
        };

        self.metadata
            .put_object(project, node_id, path, lsn, &meta)
            .await?;

        Ok(PutObjectResponse { lsn, size, created })
    }

    pub async fn get_object(
        &self,
        project: &str,
        branch_id: &str,
        path: &str,
        at_lsn: Option<u64>,
    ) -> Result<Option<GetObjectResponse>> {
        validate_no_nul("path", path)?;
        let Some(resolved) = self
            .resolver
            .resolve_object(project, branch_id, path, at_lsn)
            .await?
        else {
            return Ok(None);
        };

        let hashes = resolved.meta.chunks.clone();
        let chunks = Arc::clone(&self.chunks);
        let stream = futures::stream::iter(hashes).then(move |hash| {
            let chunks = Arc::clone(&chunks);
            async move { chunks.get(&hash).await }
        });

        Ok(Some(GetObjectResponse {
            meta: resolved.meta,
            stream: Box::pin(stream),
        }))
    }

    pub async fn delete_object(&self, project: &str, branch_id: &str, path: &str) -> Result<()> {
        validate_no_nul("path", path)?;
        let (_, node) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        let node_id = node.node_id;
        let lsn = self.lsn.next(project, branch_id).await?;

        let tombstone = ObjectMeta {
            chunks: Vec::new(),
            size: 0,
            content_type: "application/octet-stream".to_string(),
            tombstone: true,
            created_at: now_millis(),
        };

        self.metadata
            .put_object(project, node_id, path, lsn, &tombstone)
            .await
    }

    pub async fn get_object_meta(
        &self,
        project: &str,
        branch_id: &str,
        path: &str,
        at_lsn: Option<u64>,
    ) -> Result<Option<ResolvedObject>> {
        validate_no_nul("path", path)?;
        self.resolver
            .resolve_object(project, branch_id, path, at_lsn)
            .await
    }

    pub async fn list_objects(
        &self,
        project: &str,
        branch_id: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
        at_lsn: Option<u64>,
    ) -> Result<ObjectListResponse> {
        if let Some(prefix) = prefix {
            validate_no_nul("prefix", prefix)?;
        }
        let page = self
            .resolver
            .resolve_listing(project, branch_id, prefix, cursor, limit.max(1), at_lsn)
            .await?;
        Ok(ObjectListResponse {
            objects: page.items,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
        })
    }

    pub async fn create_snapshot(&self, project: &str, branch_id: &str) -> Result<SnapshotRecord> {
        self.get_project(project).await?;
        self.metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let snapshot_lsn = self.lsn.current(project, branch_id).await?;

        let snapshot = SnapshotRecord {
            snapshot_id: generate_snapshot_id(),
            branch_id: branch_id.to_string(),
            lsn: snapshot_lsn,
            created_at: now_millis(),
        };

        self.metadata
            .create_snapshot(project, branch_id, &snapshot)
            .await?;

        Ok(snapshot)
    }

    pub async fn get_snapshot(
        &self,
        project: &str,
        branch_id: &str,
        name: &str,
    ) -> Result<SnapshotRecord> {
        self.get_project(project).await?;
        self.metadata
            .get_snapshot(project, branch_id, name)
            .await?
            .ok_or(Error::SnapshotNotFound)
    }

    pub async fn list_snapshots(
        &self,
        project: &str,
        branch_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<SnapshotRecord>> {
        self.get_project(project).await?;
        self.metadata
            .list_snapshots(project, branch_id, cursor, limit.max(1))
            .await
    }

    pub async fn delete_snapshot(&self, project: &str, branch_id: &str, name: &str) -> Result<()> {
        self.get_project(project).await?;
        self.metadata
            .delete_snapshot(project, branch_id, name)
            .await
    }

    /// Replay all of `source_branch_id`'s direct-layer writes (from its fork
    /// point up to the current LSN) onto `target_branch_id`.
    ///
    /// Constraints (validated before the merge proceeds):
    /// - Source and target must be different branches.
    /// - Source must be a **direct child** of target
    ///   (`source.parent_branch_id == Some(target_branch_id)`).
    /// - Source must have **no children** of its own (leaf branch only).
    ///
    /// After a successful merge, target's node contains all object versions that
    /// source wrote since it was forked. Source is left untouched and may be
    /// deleted by the caller once no longer needed.
    ///
    /// Returns the updated `BranchInfo` for target and the number of object
    /// records that were copied.
    pub async fn merge(
        &self,
        project: &str,
        source_branch_id: &str,
        target_branch_id: &str,
    ) -> Result<(BranchInfo, u64)> {
        self.get_project(project).await?;

        if source_branch_id == target_branch_id {
            return Err(Error::InvalidInput(
                "cannot merge a branch into itself".to_string(),
            ));
        }

        let (source_handle, source_node) = self
            .metadata
            .get_branch(project, source_branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        // Source must be a direct child of target.
        if source_handle.parent_branch_id.as_deref() != Some(target_branch_id) {
            return Err(Error::NotDirectChild);
        }

        // Source must have no children — merging non-leaf branches would leave
        // grandchildren dangling at a stale fork point.
        if !self
            .metadata
            .list_children(project, source_branch_id)
            .await?
            .is_empty()
        {
            return Err(Error::BranchHasChildren);
        }

        let (target_handle, target_node) = self
            .metadata
            .get_branch(project, target_branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        // Acquire a per-source merge lock to prevent concurrent merges from the
        // same child racing each other. The lock is an entry in an in-process
        // DashSet; it is held across the async scan so it cannot be a
        // MutationLease (which borrows the metastore's DashSet and cannot cross
        // await points).
        let lock_key = format!("{project}\0{source_branch_id}");
        if !self.merge_locks.insert(lock_key.clone()) {
            return Err(Error::Conflict);
        }

        let result: Result<(BranchInfo, u64)> = async {
            // Snapshot the source branch's LSN so we get a consistent picture of
            // its direct layer even if other writes land while we scan.
            let source_head_lsn = self.lsn.current(project, source_branch_id).await?;

            // Collect all direct-layer objects from source up to source_head_lsn.
            // Memory is O(N × object_meta_size); acceptable for V1.
            let mut all_objects: Vec<ObjectRecordEntry> = Vec::new();
            let mut start_after: Option<String> = None;
            loop {
                let page = self
                    .metadata
                    .list_objects(
                        project,
                        source_node.node_id,
                        None,
                        Some(source_head_lsn),
                        start_after.as_deref(),
                        500,
                    )
                    .await?;
                all_objects.extend(page.items);
                if !page.has_more {
                    break;
                }
                start_after = page.next_cursor;
            }

            let n = all_objects.len() as u64;
            if n == 0 {
                let head_lsn = self.lsn.current(project, target_branch_id).await?;
                return Ok((
                    branch_info_from_handle_and_node(target_handle, target_node, head_lsn),
                    0,
                ));
            }

            // Reserve a contiguous block of LSNs on the target branch.
            let base_lsn = self.lsn.next_n(project, target_branch_id, n).await?;

            // Write all objects atomically onto target via a single WriteBatch.
            self.metadata
                .bulk_put_objects(project, target_node.node_id, base_lsn, all_objects)
                .await?;

            let head_lsn = self.lsn.current(project, target_branch_id).await?;
            Ok((
                branch_info_from_handle_and_node(target_handle, target_node, head_lsn),
                n,
            ))
        }
        .await;

        // Always release the lock, regardless of success or failure.
        self.merge_locks.remove(&lock_key);

        result
    }
}

fn branch_info_from_handle_and_node(
    handle: BranchHandle,
    node: BranchNode,
    head_lsn: u64,
) -> BranchInfo {
    BranchInfo {
        branch_id: handle.branch_id,
        // Only expose fork_lsn when there is a parent; root branches that happen to
        // point at a node with fork_lsn set (e.g. after an internal swap) should not
        // leak internal tree structure through the API.
        fork_lsn: if handle.parent_branch_id.is_some() {
            node.fork_lsn
        } else {
            None
        },
        parent_id: handle.parent_branch_id,
        head_lsn,
        created_at: handle.created_at,
    }
}

fn validate_no_nul(field: &str, value: &str) -> Result<()> {
    if value.as_bytes().contains(&0) {
        return Err(Error::InvalidInput(format!(
            "{field} contains unsupported null byte"
        )));
    }
    Ok(())
}

fn now_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis();
    i64::try_from(millis).expect("system clock timestamp overflows i64")
}
