//! S3-backed chunk store (or any S3 compatible storage)

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, aws::AmazonS3Builder, path::Path as ObjectPath};

use crate::{
    config::S3StorageConfig,
    error::{Error, Result},
    types::ChunkHash,
};

use super::{ChunkStore, chunk_key};

pub struct S3ChunkStore {
    store: Arc<dyn ObjectStore>,
}

impl TryFrom<S3StorageConfig> for S3ChunkStore {
    type Error = Error;

    fn try_from(config: S3StorageConfig) -> Result<Self> {
        let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
            builder = builder.with_allow_http(true);
        }

        if let Some(access_key) = &config.access_key_id {
            builder = builder.with_access_key_id(access_key);
        }

        if let Some(secret_key) = &config.secret_access_key {
            builder = builder.with_secret_access_key(secret_key);
        }

        let store = builder
            .build()
            .map_err(|err| Error::ChunkStore(format!("failed to create s3 store: {err}")))?;

        Ok(Self {
            store: Arc::new(store),
        })
    }
}

#[async_trait]
impl ChunkStore for S3ChunkStore {
    async fn put(&self, hash: &ChunkHash, data: Bytes) -> Result<()> {
        let key = ObjectPath::from(chunk_key(hash));
        // PutMode::Create means fail with object_store::Error::AlreadyExists if it already exists
        let result = self
            .store
            .put_opts(
                &key,
                PutPayload::from(data),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(err) => Err(Error::ChunkStore(format!("failed to put chunk: {err}"))),
        }
    }

    async fn get(&self, hash: &ChunkHash) -> Result<Bytes> {
        let key = ObjectPath::from(chunk_key(hash));
        let bytes = self
            .store
            .get(&key)
            .await
            .map_err(|err| Error::ChunkStore(format!("failed to get chunk: {err}")))?
            .bytes()
            .await
            .map_err(|err| Error::ChunkStore(format!("failed to read chunk bytes: {err}")))?;
        Ok(bytes)
    }

    async fn delete(&self, hash: &ChunkHash) -> Result<()> {
        let key = ObjectPath::from(chunk_key(hash));
        match self.store.delete(&key).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(Error::ChunkStore(format!("failed to delete chunk: {err}"))),
        }
    }
}
