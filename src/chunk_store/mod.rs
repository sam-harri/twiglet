//! Content-addressable chunk storage
//!
//! defines the interface GET, PUT, and DELETE -ing
//! chunks from a storage backend
//!
//! Chunks are keyed by their BLAKE3 hash and sharded by the first 4 hex chars
//! so they spread across prefixes (S3 partitions by prefix for throughput,
//! and local filesystems dont like many files in one directory).

mod local_fs;
mod s3;

pub use self::local_fs::LocalFsChunkStore;
pub use self::s3::S3ChunkStore;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{error::Result, types::ChunkHash};

#[async_trait]
pub trait ChunkStore: Send + Sync {
    async fn put(&self, hash: &ChunkHash, data: Bytes) -> Result<()>;
    async fn get(&self, hash: &ChunkHash) -> Result<Bytes>;
    // I'll add this when I implement the garbage collector
    #[allow(dead_code)]
    async fn delete(&self, hash: &ChunkHash) -> Result<()>;
}

pub fn chunk_key(hash: &ChunkHash) -> String {
    let hex = hex::encode(hash);
    format!("chunks/{}/{}/{}", &hex[0..2], &hex[2..4], &hex)
}
