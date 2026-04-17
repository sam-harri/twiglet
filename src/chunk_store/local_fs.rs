//! Local-filesystem chunk store.
//!
//! Mostly useful for development and tests..

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use rand::{RngExt, distr::Alphanumeric};
use tokio::fs;

use crate::{
    config::LocalStorageConfig,
    error::{Error, Result},
    types::ChunkHash,
};

use super::{ChunkStore, chunk_key};

pub struct LocalFsChunkStore {
    base_dir: PathBuf,
}

impl From<LocalStorageConfig> for LocalFsChunkStore {
    fn from(config: LocalStorageConfig) -> Self {
        Self {
            base_dir: PathBuf::from(config.path),
        }
    }
}

impl LocalFsChunkStore {
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }

    fn file_path(&self, hash: &ChunkHash) -> PathBuf {
        self.base_dir.join(chunk_key(hash))
    }
}

#[async_trait]
impl ChunkStore for LocalFsChunkStore {
    async fn put(&self, hash: &ChunkHash, data: Bytes) -> Result<()> {
        let path = self.file_path(hash);
        // this doesnt prevent any races, just an early exit
        if fs::try_exists(&path)
            .await
            .map_err(|err| Error::ChunkStore(format!("failed to stat chunk: {err}")))?
        {
            return Ok(());
        }

        let parent = path.parent().expect("chunk path always has a parent");
        fs::create_dir_all(parent)
            .await
            .map_err(|err| Error::ChunkStore(format!("failed to create dirs: {err}")))?;

        // tmp file + atomic rename makes 2 writes to the same chunk race proof
        // both files would have the same content since they are content-addressed
        let tmp_name: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect();
        let tmp_path = path.with_file_name(format!("tmp-{tmp_name}"));
        fs::write(&tmp_path, data)
            .await
            .map_err(|err| Error::ChunkStore(format!("failed to write tmp chunk: {err}")))?;

        match fs::rename(&tmp_path, &path).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&tmp_path).await;
                Ok(())
            }
            Err(err) => {
                let _ = fs::remove_file(&tmp_path).await;
                Err(Error::ChunkStore(format!(
                    "failed to atomically move chunk into place: {err}"
                )))
            }
        }
    }

    async fn get(&self, hash: &ChunkHash) -> Result<Bytes> {
        let path = self.file_path(hash);
        let data = fs::read(path)
            .await
            .map_err(|err| Error::ChunkStore(format!("failed to read chunk: {err}")))?;
        Ok(Bytes::from(data))
    }

    async fn delete(&self, hash: &ChunkHash) -> Result<()> {
        let path = self.file_path(hash);
        match fs::remove_file(path).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Error::ChunkStore(format!("failed to delete chunk: {err}"))),
        }
    }
}
