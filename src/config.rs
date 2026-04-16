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

impl ServerConfig {
    fn default_host() -> String {
        "127.0.0.1".into()
    }
    fn default_port() -> u16 {
        8080
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    pub username: String, // TWIGLET_AUTH__USERNAME
    pub password: String, // TWIGLET_AUTH__PASSWORD
}

impl AuthConfig {
    fn default_username() -> String {
        "twigletadmin".into()
    }
    fn default_password() -> String {
        "twigletadmin".into()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            username: Self::default_username(),
            password: Self::default_password(),
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

impl RocksDbMetastoreConfig {
    fn default_path() -> String {
        "./data/rocksdb".into()
    }
    fn default_block_cache_mb() -> usize {
        1024
    }
    fn default_rate_limit_mb_sec() -> usize {
        50
    }
}

impl Default for RocksDbMetastoreConfig {
    fn default() -> Self {
        Self {
            path: Self::default_path(),
            block_cache_mb: Self::default_block_cache_mb(),
            rate_limit_mb_sec: Self::default_rate_limit_mb_sec(),
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

impl FixedSizeChunkerConfig {
    fn default_chunk_size_bytes() -> usize {
        8_388_608 // 8 MiB
    }
}

impl Default for FixedSizeChunkerConfig {
    fn default() -> Self {
        Self {
            chunk_size_bytes: Self::default_chunk_size_bytes(),
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

impl LocalStorageConfig {
    fn default_path() -> String {
        "twiglet-chunks".into()
    }
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            path: Self::default_path(),
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