//! Per-branch LSN generators.
//!
//! Each branch has its own atomic counter seeded from the parent's `fork_lsn` at
//! creation time. This means:
//!
//! - concurrent writes to different branches never contend on the same atomic
//! - because child counters are seeded from the parent's current LSN at fork time , all
//!   possible ancestry traversals are strictly monotonically decreasing (like going from head to root)
//!
//! On the first access for a branch, `LazyAtomicLsnGenerator` gets the current node, then finds the highest LSN
//! already written on that node.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use dashmap::DashMap;

use crate::error::{Error, Result};
use crate::metastore::MetadataStore;

#[async_trait]
pub trait LsnGenerator: Send + Sync {
    async fn next(&self, project_id: &str, branch_id: &str) -> Result<u64>;
    async fn current(&self, project_id: &str, branch_id: &str) -> Result<u64>;
    async fn next_n(&self, project_id: &str, branch_id: &str, n: u64) -> Result<u64>;
}

struct LsnCounters {
    // map from branch_id -> LSN counter
    counters: DashMap<String, AtomicU64>,
}

impl LsnCounters {
    fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    fn seed(&self, branch_id: &str, value: u64) {
        self.counters
            .entry(branch_id.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_max(value, Ordering::SeqCst);
    }

    fn is_loaded(&self, branch_id: &str) -> bool {
        self.counters.contains_key(branch_id)
    }

    fn next(&self, branch_id: &str) -> u64 {
        self.counters
            .entry(branch_id.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::SeqCst)
            + 1 // fetch_add returns the old value, +1 gives us the new one
    }

    fn current(&self, branch_id: &str) -> u64 {
        self.counters
            .get(branch_id)
            .expect("current() called before counter was seeded via ensure_loaded()")
            .load(Ordering::SeqCst)
    }

    /// Atomically reserve a block of `n` LSNs. Returns the first LSN in the block.
    /// The caller owns [first, first + n - 1] exclusively
    /// TODO lowkey should take in a nonzero u64
    fn next_n(&self, branch_id: &str, n: u64) -> u64 {
        self.counters
            .entry(branch_id.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(n, Ordering::SeqCst)
            + 1 // fetch_add returns old value; first reserved LSN is old+1
    }
}

pub struct LazyAtomicLsnGenerator {
    counters: LsnCounters,
    metadata: Arc<dyn MetadataStore>,
}

impl LazyAtomicLsnGenerator {
    pub fn new(metadata: Arc<dyn MetadataStore>) -> Self {
        Self {
            counters: LsnCounters::new(),
            metadata,
        }
    }

    async fn ensure_loaded(&self, project_id: &str, branch_id: &str) -> Result<()> {
        if self.counters.is_loaded(branch_id) {
            return Ok(());
        }

        let (_, node) = self
            .metadata
            .get_branch(project_id, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let fork_lsn_baseline = node.fork_lsn.unwrap_or(0);
        let max_node_lsn = self
            .metadata
            .get_max_lsn_for_node(project_id, node.node_id)
            .await?;

        self.counters
            .seed(branch_id, fork_lsn_baseline.max(max_node_lsn));
        Ok(())
    }
}

#[async_trait]
impl LsnGenerator for LazyAtomicLsnGenerator {
    async fn next(&self, project_id: &str, branch_id: &str) -> Result<u64> {
        self.ensure_loaded(project_id, branch_id).await?;
        Ok(self.counters.next(branch_id))
    }

    async fn current(&self, project_id: &str, branch_id: &str) -> Result<u64> {
        self.ensure_loaded(project_id, branch_id).await?;
        Ok(self.counters.current(branch_id))
    }

    async fn next_n(&self, project_id: &str, branch_id: &str, n: u64) -> Result<u64> {
        self.ensure_loaded(project_id, branch_id).await?;
        Ok(self.counters.next_n(branch_id, n))
    }
}
