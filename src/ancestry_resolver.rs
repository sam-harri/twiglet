//! Resolves objects across branch ancestry.
//!
//! Basically try to resolve the read in your own branch history, if you cant,
//! go up to your parent and resolve the read between their own creation time and the time at
//! which you forked. Keep going up ancestry until you hit or return NotFound
//!
//! Listing operations use a k-way merge. Each node in the ancestry chain is queried
//! independently with a small batch size, and a min-heap merges them in sorted
//! path order. Child nodes have higher priority (lower chain index) so they
//! shadow parent versions and tombstones mask parent entries.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::{
    error::{Error, Result},
    id::ProcessUniqueId,
    metastore::{MetadataStore, ObjectRecordEntry},
    types::{ObjectListEntry, ObjectListResponse, Page, ResolvedObject},
};

/// Buffered stream of objects from a single node.
struct BranchStream {
    node_id: ProcessUniqueId,
    max_lsn: u64,
    chain_index: usize,
    batch_size: usize,
    buffer: Vec<ObjectRecordEntry>,
    buffer_pos: usize,
    next_cursor: Option<String>,
    exhausted: bool,
}

impl BranchStream {
    fn peek(&self) -> Option<&ObjectRecordEntry> {
        self.buffer.get(self.buffer_pos)
    }

    fn advance(&mut self) {
        self.buffer_pos += 1;
    }
}

/// Entry in the merge heap: sorted by (path, chain_index) so that for
/// the same path the leaf node (chain_index 0) is popped first.
#[derive(Eq, PartialEq)]
struct MergeEntry {
    path: String,
    chain_index: usize,
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.path, self.chain_index).cmp(&(&other.path, other.chain_index))
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct AncestryResolver {
    metadata: Arc<dyn MetadataStore>,
}

impl AncestryResolver {
    pub fn new(metadata: Arc<dyn MetadataStore>) -> Self {
        Self { metadata }
    }

    /// Returns `[(node_id, max_lsn)]` from leaf to root.
    ///
    /// For live queries `at_lsn` is `None` and the leaf max is `u64::MAX`,
    /// meaning all writes are visible. At each ancestor step the max LSN
    /// drops to the fork point so that the parent is never queried past
    /// where the child branched from it.
    async fn ancestry_chain(
        &self,
        project: &str,
        branch_id: &str,
        at_lsn: Option<u64>,
    ) -> Result<Vec<(ProcessUniqueId, u64)>> {
        // Resolve branch_id → leaf node via the handle.
        let (_, leaf) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let mut max_lsn = at_lsn.unwrap_or(u64::MAX);
        let mut chain = vec![(leaf.node_id, max_lsn)];
        let mut node = leaf;

        loop {
            let Some(parent_node_id) = node.parent_node_id else {
                break;
            };
            let fork_lsn = node
                .fork_lsn
                .expect("node has parent_node_id but no fork_lsn — integrity violation");
            // Never query the parent past the fork point; smaller wins for time-travel.
            max_lsn = max_lsn.min(fork_lsn);
            node = self
                .metadata
                .get_branch_node(project, parent_node_id)
                .await?
                .ok_or_else(|| {
                    Error::Storage(format!(
                        "parent node {:?} not found — data integrity violation",
                        parent_node_id
                    ))
                })?;
            chain.push((parent_node_id, max_lsn));
        }

        Ok(chain)
    }

    /// Walk the ancestry chain looking for the newest visible version of an object.
    pub async fn resolve_object(
        &self,
        project: &str,
        branch_id: &str,
        path: &str,
        at_lsn: Option<u64>,
    ) -> Result<Option<ResolvedObject>> {
        // Resolve branch_id → leaf node via the handle.
        let (_, leaf) = self
            .metadata
            .get_branch(project, branch_id)
            .await?
            .ok_or(Error::BranchNotFound)?;

        let mut current_node_id = leaf.node_id;
        let mut max_lsn = at_lsn.unwrap_or(u64::MAX);
        let mut node = leaf;

        loop {
            if let Some((lsn, meta)) = self
                .metadata
                .get_object(project, current_node_id, path, Some(max_lsn))
                .await?
            {
                if meta.tombstone {
                    return Ok(None);
                }
                return Ok(Some(ResolvedObject { meta, lsn }));
            }

            let Some(parent_node_id) = node.parent_node_id else {
                return Ok(None);
            };
            let fork_lsn = node
                .fork_lsn
                .expect("node has parent_node_id but no fork_lsn — data integrity violation");
            max_lsn = max_lsn.min(fork_lsn);
            node = self
                .metadata
                .get_branch_node(project, parent_node_id)
                .await?
                .ok_or_else(|| {
                    Error::Storage(format!(
                        "parent node {:?} not found — data integrity violation",
                        parent_node_id
                    ))
                })?;
            current_node_id = parent_node_id;
        }
    }

    /// Fetch the next batch from a stream, returning true if new data is available.
    async fn refill_stream(
        &self,
        stream: &mut BranchStream,
        project: &str,
        prefix: Option<&str>,
    ) -> Result<bool> {
        if stream.exhausted {
            return Ok(false);
        }
        let cursor = stream.next_cursor.take();
        let page = self
            .metadata
            .list_objects(
                project,
                stream.node_id,
                prefix,
                Some(stream.max_lsn),
                cursor.as_deref(),
                stream.batch_size,
            )
            .await?;
        stream.exhausted = !page.has_more;
        stream.next_cursor = page.next_cursor;
        stream.buffer = page.items;
        stream.buffer_pos = 0;
        Ok(!stream.buffer.is_empty())
    }

    /// Push the head of a stream onto the heap, refilling from the metastore if needed.
    async fn push_from_stream(
        &self,
        stream: &mut BranchStream,
        heap: &mut BinaryHeap<Reverse<MergeEntry>>,
        project: &str,
        prefix: Option<&str>,
    ) -> Result<()> {
        if stream.peek().is_none() {
            self.refill_stream(stream, project, prefix).await?;
        }
        if let Some(record) = stream.peek() {
            heap.push(Reverse(MergeEntry {
                path: record.path.clone(),
                chain_index: stream.chain_index,
            }));
        }
        Ok(())
    }

    /// K-way merge of object records from all ancestors into a sorted, deduplicated page.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_listing(
        &self,
        project: &str,
        branch_id: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
        at_lsn: Option<u64>,
    ) -> Result<Page<ObjectListEntry>> {
        let chain = self.ancestry_chain(project, branch_id, at_lsn).await?;
        let start_after = cursor
            .map(|c| {
                let raw = STANDARD.decode(c).map_err(|_| Error::InvalidCursor)?;
                String::from_utf8(raw).map_err(|_| Error::InvalidCursor)
            })
            .transpose()?;

        let batch_size = limit.clamp(64, 4096);

        let initial_pages =
            futures::future::try_join_all(chain.iter().map(|(node_id, max_lsn)| {
                self.metadata.list_objects(
                    project,
                    *node_id,
                    prefix,
                    Some(*max_lsn),
                    start_after.as_deref(),
                    batch_size,
                )
            }))
            .await?;

        let mut streams: Vec<BranchStream> = chain
            .into_iter()
            .enumerate()
            .zip(initial_pages)
            .map(|((i, (node_id, max_lsn)), page)| BranchStream {
                node_id,
                max_lsn,
                chain_index: i,
                batch_size,
                buffer: page.items,
                buffer_pos: 0,
                next_cursor: page.next_cursor,
                exhausted: !page.has_more,
            })
            .collect();

        let mut heap = BinaryHeap::new();
        for stream in &streams {
            if let Some(record) = stream.peek() {
                heap.push(Reverse(MergeEntry {
                    path: record.path.clone(),
                    chain_index: stream.chain_index,
                }));
            }
        }

        let mut result = Vec::with_capacity(limit.min(4096));

        while result.len() < limit {
            let Some(Reverse(entry)) = heap.pop() else {
                break;
            };
            let path = entry.path;
            let idx = entry.chain_index;

            let record = streams[idx].peek().unwrap().clone();
            streams[idx].advance();
            self.push_from_stream(&mut streams[idx], &mut heap, project, prefix)
                .await?;

            while let Some(Reverse(top)) = heap.peek() {
                if top.path != path {
                    break;
                }
                let dup_idx = top.chain_index;
                heap.pop();
                streams[dup_idx].advance();
                self.push_from_stream(&mut streams[dup_idx], &mut heap, project, prefix)
                    .await?;
            }

            if record.tombstone {
                continue;
            }

            result.push(ObjectListEntry {
                path: record.path,
                lsn: record.lsn,
                size: record.size,
                content_type: record.content_type,
            });
        }

        let has_more =
            result.len() == limit && (!heap.is_empty() || streams.iter().any(|s| !s.exhausted));
        let next_cursor = if has_more {
            result
                .last()
                .map(|item| STANDARD.encode(item.path.as_bytes()))
        } else {
            None
        };

        Ok(Page {
            items: result,
            next_cursor,
            has_more,
        })
    }

    /// List objects with optional S3-style delimiter support.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_object_listing(
        &self,
        project: &str,
        branch_id: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
        at_lsn: Option<u64>,
    ) -> Result<ObjectListResponse> {
        let Some(delimiter) = delimiter else {
            let page = self
                .resolve_listing(project, branch_id, prefix, cursor, limit, at_lsn)
                .await?;
            return Ok(ObjectListResponse {
                objects: page.items,
                common_prefixes: Vec::new(),
                next_cursor: page.next_cursor,
                has_more: page.has_more,
            });
        };

        enum Atom {
            Object(ObjectListEntry),
            Prefix(String),
        }

        let page = self
            .resolve_listing(project, branch_id, prefix, None, usize::MAX, at_lsn)
            .await?;

        let prefix_filter = prefix.unwrap_or_default();
        let mut prefixes = BTreeSet::new();
        let mut atoms = Vec::new();

        for item in page.items {
            if let Some(suffix) = item.path.strip_prefix(prefix_filter)
                && let Some(index) = suffix.find(delimiter)
            {
                let end = index + delimiter.len();
                let common = format!("{}{}", prefix_filter, &suffix[..end]);
                prefixes.insert(common);
                continue;
            }
            atoms.push(Atom::Object(item));
        }
        atoms.extend(prefixes.into_iter().map(Atom::Prefix));
        atoms.sort_by(|a, b| {
            fn key(atom: &Atom) -> (&str, u8) {
                match atom {
                    Atom::Prefix(p) => (p.as_str(), 0),
                    Atom::Object(o) => (o.path.as_str(), 1),
                }
            }
            key(a).cmp(&key(b))
        });

        let cursor_key = |a: &Atom| match a {
            Atom::Object(item) => format!("o:{}", item.path),
            Atom::Prefix(p) => format!("p:{p}"),
        };

        let cursor_value = cursor
            .map(|c| {
                let raw = STANDARD.decode(c).map_err(|_| Error::InvalidCursor)?;
                String::from_utf8(raw).map_err(|_| Error::InvalidCursor)
            })
            .transpose()?;
        let start = if let Some(cursor_value) = cursor_value {
            atoms
                .iter()
                .position(|atom| cursor_key(atom) == cursor_value)
                .map(|idx| idx + 1)
                .ok_or(Error::InvalidCursor)?
        } else {
            0
        };
        let end = (start + limit).min(atoms.len());

        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        for atom in &atoms[start..end] {
            match atom {
                Atom::Object(item) => objects.push(item.clone()),
                Atom::Prefix(prefix) => common_prefixes.push(prefix.clone()),
            }
        }

        let has_more = end < atoms.len();
        let next_cursor = has_more.then(|| STANDARD.encode(cursor_key(&atoms[end - 1]).as_bytes()));

        Ok(ObjectListResponse {
            objects,
            common_prefixes,
            next_cursor,
            has_more,
        })
    }
}
