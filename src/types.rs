use std::pin::Pin;

use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{error::Result, id::ProcessUniqueId};

pub type ChunkHash = [u8; 32];

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Chunk {
    pub hash: ChunkHash,
    pub data: Bytes,
    pub offset: u64,
    pub length: u32,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BranchHandle {
    pub branch_id: String,
    pub node_id: ProcessUniqueId,
    pub parent_branch_id: Option<String>,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BranchNode {
    pub node_id: ProcessUniqueId,
    pub parent_node_id: Option<ProcessUniqueId>,
    pub fork_lsn: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct BranchInfo {
    pub branch_id: String,
    pub parent_id: Option<String>,
    pub fork_lsn: Option<u64>,
    pub head_lsn: u64,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ObjectMeta {
    pub chunks: Vec<ChunkHash>,
    pub size: u64,
    pub content_type: String,
    pub tombstone: bool,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResolvedObject {
    pub meta: ObjectMeta,
    pub lsn: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct ObjectListEntry {
    pub path: String,
    pub lsn: u64,
    pub size: u64,
    pub content_type: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct SnapshotRecord {
    pub snapshot_id: String,
    pub branch_id: String,
    pub lsn: u64,
    pub created_at: i64,
}
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct Project {
    pub project_id: String,
    pub default_branch_id: String,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[schema(bound = "T: ToSchema")]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct PutObjectResponse {
    pub lsn: u64,
    pub size: u64,
    pub created: bool,
}

pub struct GetObjectResponse {
    pub meta: ObjectMeta,
    pub stream: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct ObjectListResponse {
    pub objects: Vec<ObjectListEntry>,
    pub common_prefixes: Vec<String>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}
