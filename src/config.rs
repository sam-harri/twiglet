//! This config is a bunch of hot doodoo, API wise
//! TODO make config not hot doodoo
//! Will look for crate that does this well and copy them lmao

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_rocksdb_path")]
    pub rocksdb_path: String,
    #[serde(default = "default_block_cache_mb")]
    pub block_cache_mb: usize,
    #[serde(default = "default_rate_limit_mb_sec")]
    pub rate_limit_mb_sec: usize,
    #[serde(default = "default_chunk_store_type")]
    pub chunk_store_type: String,
    pub storage_region: Option<String>,
    pub storage_endpoint: Option<String>,
    pub storage_access_key_id: Option<String>,
    pub storage_secret_access_key: Option<String>,
    #[serde(default = "default_storage_bucket")]
    pub storage_bucket: String,
    #[serde(default = "default_chunk_size_bytes")]
    pub chunk_size_bytes: usize,
    #[serde(default = "default_admin_username")]
    pub admin_username: String,
    #[serde(default = "default_admin_password")]
    pub admin_password: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        envy::prefixed("TWIGLET_")
            .from_env()
            .map_err(|e| Error::Internal(format!("failed to load config: {e}")))
    }
}

fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    8080
}
fn default_rocksdb_path() -> String {
    "./data/rocksdb".into()
}
fn default_block_cache_mb() -> usize {
    1024
}
fn default_rate_limit_mb_sec() -> usize {
    50
}
fn default_chunk_store_type() -> String {
    "local".into()
}
fn default_storage_bucket() -> String {
    "twiglet-chunks".into()
}
fn default_chunk_size_bytes() -> usize {
    8_388_608
}
fn default_admin_username() -> String {
    "twigletadmin".into()
}
fn default_admin_password() -> String {
    "twigletadmin".into()
}
