//! Core engine that orchestrates all storage operations.
//!
//! Separation of concerns between the engine and metastore was something I hem and haw-ed
//! about for a while. Here is the final decisions :
//!
//! "does this check need to be atomic with the storage mutation?"
//! i.e., will the implementation differ with different KV backends (Postgres vs RockDB vs ...)
//! If yes -> metastore, if no -> engine

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

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
    lsn_generator: Arc<dyn LsnGenerator>,
    chunker: Arc<dyn Chunker>,
    chunk_store: Arc<dyn ChunkStore>,
    metadata_store: Arc<dyn MetadataStore>,
    ancestry_resolver: Arc<AncestryResolver>,
}

impl Engine {
    pub fn new(
        lsn_generator: Arc<dyn LsnGenerator>,
        chunker: Arc<dyn Chunker>,
        chunk_store: Arc<dyn ChunkStore>,
        metadata_store: Arc<dyn MetadataStore>,
        ancestry_resolver: Arc<AncestryResolver>,
    ) -> Self {
        Self {
            lsn_generator,
            chunker,
            chunk_store,
            metadata_store,
            ancestry_resolver,
        }
    }

    pub async fn create_project(&self) -> Result<Project> {
        let project_id = generate_project_id();
        let root_branch_id = generate_branch_id();
        let node_id = generate_node_id();
        let now = now_millis();

        let project = Project {
            project_id: project_id.clone(),
            root_branch_id: root_branch_id.clone(),
            created_at: now,
        };

        let root_handle = BranchHandle {
            branch_id: root_branch_id.clone(),
            node_id,
            parent_branch_id: None,
            created_at: now,
        };

        let root_node = BranchNode {
            node_id,
            parent_node_id: None,
            fork_lsn: None,
        };

        self.metadata_store
            .create_project(&project, &root_handle, &root_node)
            .await?;

        Ok(project)
    }

    pub async fn get_project(&self, project_id: &str) -> Result<Project> {
        self.metadata_store
            .get_project(project_id)
            .await?
            .ok_or(Error::ProjectNotFound)
    }

    pub async fn list_projects(&self, cursor: Option<&str>, limit: usize) -> Result<Page<Project>> {
        self.metadata_store
            .list_projects(cursor, limit.max(1))
            .await
    }

    pub async fn create_branch(&self, project: &str, source_branch_id: &str) -> Result<BranchInfo> {
        self.get_project(project).await?;

        let (_, source_node) = self
            .metadata_store
            .get_branch(project, source_branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let branch_id = generate_branch_id();
        let node_id = generate_node_id();
        let fork_lsn = self
            .lsn_generator
            .current(project, source_branch_id)
            .await?;
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
        self.metadata_store
            .create_branch(project, &new_handle, &new_node)
            .await?;

        Ok(branch_info_from_handle_and_node(
            new_handle, new_node, fork_lsn,
        ))
    }

    pub async fn get_branch(&self, project: &str, branch_id: &str) -> Result<BranchInfo> {
        self.get_project(project).await?;
        let (handle, node) = self
            .metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        let head_lsn = self.lsn_generator.current(project, branch_id).await?;
        Ok(branch_info_from_handle_and_node(handle, node, head_lsn))
    }

    pub async fn list_branches(
        &self,
        project: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Page<BranchInfo>> {
        self.get_project(project).await?;
        let page = self
            .metadata_store
            .list_branches(project, cursor, limit.max(1))
            .await?;
        let items = futures::future::try_join_all(page.items.into_iter().map(
            |(handle, node)| async move {
                let head_lsn = self
                    .lsn_generator
                    .current(project, &handle.branch_id)
                    .await?;
                Ok::<_, Error>(branch_info_from_handle_and_node(handle, node, head_lsn))
            },
        ))
        .await?;
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
        })
    }

    pub async fn fork_at_lsn(
        &self,
        project: &str,
        branch_id: &str,
        lsn: u64,
    ) -> Result<BranchInfo> {
        self.get_project(project).await?;
        let (source_handle, source_node) = self
            .metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let branch_lsn = self.lsn_generator.current(project, branch_id).await?;
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

        self.metadata_store
            .create_branch(project, &new_handle, &new_node)
            .await?;

        // New fork branch starts at lsn; no round-trip needed.
        Ok(branch_info_from_handle_and_node(new_handle, new_node, lsn))
    }

    /// Rewind `branch_id` in-place to `lsn`, atomically swapping its internal node
    ///
    /// Before rewinding, a backup branch is created as a child of `branch_id`
    /// pointing at its current node, so no history is lost. The same external
    /// `branch_id` is preserved and future writes land on a fresh node that chains
    /// back through the old node capped at `lsn`.
    ///
    /// TODO make puts atomic as write group?
    /// If the node swap fails after the backup is written, the
    /// original branch is untouched and only an orphaned backup exists.
    pub async fn restore(
        &self,
        project: &str,
        branch_id: &str,
        lsn: u64,
    ) -> Result<(BranchInfo, BranchInfo)> {
        self.get_project(project).await?;
        let (handle, node) = self
            .metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let branch_lsn = self.lsn_generator.current(project, branch_id).await?;
        if lsn > branch_lsn {
            return Err(Error::InvalidRestoreLsn);
        }

        let now = now_millis();

        let backup_handle = BranchHandle {
            branch_id: generate_branch_id(),
            node_id: node.node_id,
            parent_branch_id: Some(branch_id.to_string()),
            created_at: now,
        };
        self.metadata_store
            .create_branch(project, &backup_handle, &node)
            .await?;

        let new_node = BranchNode {
            node_id: generate_node_id(),
            parent_node_id: Some(node.node_id),
            fork_lsn: Some(lsn),
        };
        self.metadata_store
            .reset_branch_node(project, branch_id, &new_node)
            .await?;

        let (head_lsn, backup_head_lsn) = futures::future::try_join(
            self.lsn_generator.current(project, branch_id),
            self.lsn_generator
                .current(project, &backup_handle.branch_id),
        )
        .await?;
        let restored_info = branch_info_from_handle_and_node(handle, new_node, head_lsn);
        let backup_info = branch_info_from_handle_and_node(backup_handle, node, backup_head_lsn);
        Ok((restored_info, backup_info))
    }

    /// Reparent `branch_id` in-place to its parent's current LSN.
    /// Essentially create a new branch from my parent but keep my external branch id stable
    pub async fn reset_from_parent(&self, project: &str, branch_id: &str) -> Result<BranchInfo> {
        self.get_project(project).await?;
        let (handle, node) = self
            .metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let parent_branch_id = handle
            .parent_branch_id
            .as_deref()
            .ok_or(Error::BranchIsRoot)?;
        let parent_node_id = node
            .parent_node_id
            .expect("parent_branch_id and parent_node_id are always set together");
        let fork_lsn = self
            .lsn_generator
            .current(project, parent_branch_id)
            .await?;

        let new_node = BranchNode {
            node_id: generate_node_id(),
            parent_node_id: Some(parent_node_id),
            fork_lsn: Some(fork_lsn),
        };

        self.metadata_store
            .reset_branch_node(project, branch_id, &new_node)
            .await?;

        let head_lsn = self.lsn_generator.current(project, branch_id).await?;
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
            .metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        let node_id = node.node_id;

        // TODO not sure what a correct value for this would be
        // Need to do some more looking around
        const UPLOAD_CONCURRENCY: usize = 256;

        let chunk_stream = self.chunker.chunk(&mut stream);

        let mut size: u64 = 0;
        let mut chunk_hashes = Vec::new();

        let mut uploads = chunk_stream
            .map_ok(|chunk| {
                let store = self.chunk_store.clone();
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

        let lsn = self.lsn_generator.next(project, branch_id).await?;

        let meta = ObjectMeta {
            chunks: chunk_hashes,
            size,
            content_type: content_type.to_string(),
            tombstone: false,
            created_at: now_millis(),
        };

        self.metadata_store
            .put_object(project, node_id, path, lsn, &meta)
            .await?;

        Ok(PutObjectResponse { lsn, size })
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
            .ancestry_resolver
            .resolve_object(project, branch_id, path, at_lsn)
            .await?
        else {
            return Ok(None);
        };

        let hashes = resolved.meta.chunks.clone();
        let chunks = Arc::clone(&self.chunk_store);
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
            .metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        let node_id = node.node_id;
        let lsn = self.lsn_generator.next(project, branch_id).await?;

        let tombstone = ObjectMeta {
            chunks: Vec::new(),
            size: 0,
            content_type: String::new(),
            tombstone: true,
            created_at: now_millis(),
        };

        self.metadata_store
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
        self.ancestry_resolver
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
            .ancestry_resolver
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
        self.metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let snapshot_lsn = self.lsn_generator.current(project, branch_id).await?;

        let snapshot = SnapshotRecord {
            snapshot_id: generate_snapshot_id(),
            branch_id: branch_id.to_string(),
            lsn: snapshot_lsn,
            created_at: now_millis(),
        };

        self.metadata_store
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
        self.metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        self.metadata_store
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
        self.metadata_store
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;
        self.metadata_store
            .list_snapshots(project, branch_id, cursor, limit.max(1))
            .await
    }

    /// Play all the post fork mutations of `source_branch_id` onto its parent by reserving however many
    /// mutations worth of LSNs as a contiguous chunk and moving over the metadata records
    ///
    /// TODO look into O(1) merge op using a metadata record that routes the parent through the child first
    /// Read overhead, not sure if its worth it
    pub async fn merge(&self, project: &str, source_branch_id: &str) -> Result<(BranchInfo, u64)> {
        self.get_project(project).await?;

        let (source_handle, source_node) = self
            .metadata_store
            .get_branch(project, source_branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        // Source must have a parent — root branches can't be merged.
        let target_branch_id = source_handle
            .parent_branch_id
            .as_deref()
            .ok_or(Error::BranchIsRoot)?;

        if !self
            .metadata_store
            .list_children(project, source_branch_id)
            .await?
            .is_empty()
        {
            return Err(Error::BranchHasChildren);
        }

        let (target_handle, target_node) = self
            .metadata_store
            .get_branch(project, target_branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let source_head_lsn = self
            .lsn_generator
            .current(project, source_branch_id)
            .await?;

        // Collect all direct-layer objects from source up to source_head_lsn.
        // Memory is O(N * object_meta_size), acceptable for now.
        let mut all_objects: Vec<ObjectRecordEntry> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .metadata_store
                .list_objects(
                    project,
                    source_node.node_id,
                    None,
                    Some(source_head_lsn),
                    cursor.as_deref(),
                    500,
                )
                .await?;
            all_objects.extend(page.items);
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor;
        }

        let n = all_objects.len() as u64;
        if n == 0 {
            let head_lsn = self
                .lsn_generator
                .current(project, target_branch_id)
                .await?;
            return Ok((
                branch_info_from_handle_and_node(target_handle, target_node, head_lsn),
                0,
            ));
        }

        let base_lsn = self
            .lsn_generator
            .next_n(project, target_branch_id, n)
            .await?;

        self.metadata_store
            .merge_objects(
                project,
                source_branch_id,
                target_node.node_id,
                base_lsn,
                all_objects,
            )
            .await?;

        let head_lsn = self
            .lsn_generator
            .current(project, target_branch_id)
            .await?;
        Ok((
            branch_info_from_handle_and_node(target_handle, target_node, head_lsn),
            n,
        ))
    }
}

fn branch_info_from_handle_and_node(
    handle: BranchHandle,
    node: BranchNode,
    head_lsn: u64,
) -> BranchInfo {
    BranchInfo {
        branch_id: handle.branch_id,
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
    if value.contains('\0') {
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
