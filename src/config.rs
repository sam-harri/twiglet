use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub metastore: MetastoreConfig,
    #[serde(default)]
    pub chunker: ChunkerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub auth: AuthConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        Figment::from(Serialized::defaults(AppConfig::default()))
            .merge(Env::prefixed("TWIGLET_").split("__"))
            .extract()
            .map_err(|e| Error::Internal(format!("failed to load config: {e}")))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String, // TWIGLET_SERVER__HOST
    pub port: u16,    // TWIGLET_SERVER__PORT
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 8080,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    pub username: String, // TWIGLET_AUTH__USERNAME
    pub password: String, // TWIGLET_AUTH__PASSWORD
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            username: "twigletadmin".into(),
            password: "twigletadmin".into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetastoreConfig {
    RocksDb(RocksDbMetastoreConfig),
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        Self::RocksDb(Default::default())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RocksDbMetastoreConfig {
    pub path: String,             // TWIGLET_METASTORE__PATH
    pub block_cache_mb: usize,    // TWIGLET_METASTORE__BLOCK_CACHE_MB
    pub rate_limit_mb_sec: usize, // TWIGLET_METASTORE__RATE_LIMIT_MB_SEC
}

impl Default for RocksDbMetastoreConfig {
    fn default() -> Self {
        Self {
            path: "./data/rocksdb".into(),
            block_cache_mb: 1024,
            rate_limit_mb_sec: 50,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChunkerConfig {
    Fixed(FixedSizeChunkerConfig),
    Cdc(CdcChunkerConfig),
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self::Fixed(Default::default())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FixedSizeChunkerConfig {
    pub chunk_size_bytes: usize, // TWIGLET_CHUNKER__CHUNK_SIZE_BYTES
}

impl Default for FixedSizeChunkerConfig {
    fn default() -> Self {
        Self {
            chunk_size_bytes: 8_388_608, // 8 MiB
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CdcChunkerConfig {
    pub min_chunk_bytes: usize, // TWIGLET_CHUNKER__MIN_CHUNK_BYTES
    pub avg_chunk_bytes: usize, // TWIGLET_CHUNKER__AVG_CHUNK_BYTES
    pub max_chunk_bytes: usize, // TWIGLET_CHUNKER__MAX_CHUNK_BYTES
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageConfig {
    Local(LocalStorageConfig),
    S3(S3StorageConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Local(Default::default())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LocalStorageConfig {
    pub path: String, // TWIGLET_STORAGE__PATH
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            path: "twiglet-chunks".into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3StorageConfig {
    pub bucket: String,                    // TWIGLET_STORAGE__BUCKET
    pub region: Option<String>,            // TWIGLET_STORAGE__REGION
    pub endpoint: Option<String>,          // TWIGLET_STORAGE__ENDPOINT
    pub access_key_id: Option<String>,     // TWIGLET_STORAGE__ACCESS_KEY_ID
    pub secret_access_key: Option<String>, // TWIGLET_STORAGE__SECRET_ACCESS_KEY
}
