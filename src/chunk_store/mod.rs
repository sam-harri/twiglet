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
    /// Store a chunk by its content hash.
    /// Idempotent — if the chunk already exists the call succeeds
    /// without overwriting it.
    async fn put(&self, hash: &ChunkHash, data: Bytes) -> Result<()>;

    /// Retrieve a chunk by its content hash.
    async fn get(&self, hash: &ChunkHash) -> Result<Bytes>;

    /// Dead code, only needed when I implement the garbage collector (if ever)
    #[allow(dead_code)]
    async fn delete(&self, hash: &ChunkHash) -> Result<()>;
}

pub fn chunk_key(hash: &ChunkHash) -> String {
    let hex = hex::encode(hash);
    format!("chunks/{}/{}/{}", &hex[0..2], &hex[2..4], &hex)
}
