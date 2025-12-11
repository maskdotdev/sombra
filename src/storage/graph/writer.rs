use std::num::NonZeroUsize;

use lru::LruCache;

use crate::primitives::pager::WriteGuard;
use crate::storage::EdgeSpec;
use crate::types::Result;
use crate::storage::mvcc::CommitId;
use crate::types::{EdgeId, NodeId, SombraError};

use super::Graph;

/// Options controlling how [`GraphWriter`] inserts edges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateEdgeOptions {
    /// Whether edge endpoints have been validated externally and can skip lookups.
    pub trusted_endpoints: bool,
    /// Capacity of the node-existence cache when validation is required.
    pub exists_cache_capacity: usize,
}

impl Default for CreateEdgeOptions {
    fn default() -> Self {
        Self {
            trusted_endpoints: false,
            exists_cache_capacity: 1024,
        }
    }
}

/// Aggregate statistics captured by [`GraphWriter`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GraphWriterStats {
    /// Number of cache hits for endpoint existence checks.
    pub exists_cache_hits: u64,
    /// Number of cache misses for endpoint existence checks.
    pub exists_cache_misses: u64,
    /// Number of edges inserted using trusted endpoints.
    pub trusted_edges: u64,
    /// Oldest reader commit observed when stats were captured.
    pub oldest_reader_commit: CommitId,
}

/// Validator used by [`GraphWriter`] to confirm endpoints exist before trusting batches.
pub trait BulkEdgeValidator {
    /// Validates a batch of `(src, dst)` pairs before inserts begin.
    fn validate_batch(&self, edges: &[(NodeId, NodeId)]) -> Result<()>;
}

/// Batched edge writer that amortizes endpoint probes and supports trusted inserts.
pub struct GraphWriter<'a> {
    pub(crate) graph: &'a Graph,
    opts: CreateEdgeOptions,
    exists_cache: Option<LruCache<NodeId, bool>>,
    validator: Option<Box<dyn BulkEdgeValidator + 'a>>,
    stats: GraphWriterStats,
    trust_budget: usize,
}

impl<'a> GraphWriter<'a> {
    /// Constructs a new writer for the provided [`Graph`].
    pub fn try_new(
        graph: &'a Graph,
        opts: CreateEdgeOptions,
        validator: Option<Box<dyn BulkEdgeValidator + 'a>>,
    ) -> Result<Self> {
        if opts.trusted_endpoints && validator.is_none() {
            return Err(SombraError::Invalid(super::TRUST_VALIDATOR_REQUIRED));
        }
        let exists_cache = NonZeroUsize::new(opts.exists_cache_capacity).map(LruCache::new);
        Ok(Self {
            graph,
            opts,
            exists_cache,
            validator,
            stats: GraphWriterStats::default(),
            trust_budget: 0,
        })
    }

    /// Returns the options associated with this writer.
    pub fn options(&self) -> &CreateEdgeOptions {
        &self.opts
    }

    /// Returns current statistics collected by the writer.
    pub fn stats(&self) -> GraphWriterStats {
        let mut stats = self.stats;
        if let Some(oldest) = self.graph.oldest_reader_commit() {
            stats.oldest_reader_commit = oldest;
        }
        stats
    }

    /// Validates a batch of edges before inserting them in trusted mode.
    pub fn validate_trusted_batch(&mut self, edges: &[(NodeId, NodeId)]) -> Result<()> {
        if !self.opts.trusted_endpoints {
            return Ok(());
        }
        let Some(validator) = self.validator.as_ref() else {
            return Err(SombraError::Invalid(super::TRUST_VALIDATOR_REQUIRED));
        };
        validator.validate_batch(edges)?;
        self.trust_budget = edges.len();
        Ok(())
    }

    /// Creates an edge with the configured validation strategy.
    pub fn create_edge(&mut self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        if self.opts.trusted_endpoints {
            if self.trust_budget == 0 {
                return Err(SombraError::Invalid(super::TRUST_BATCH_REQUIRED));
            }
            self.trust_budget -= 1;
            self.stats.trusted_edges = self.stats.trusted_edges.saturating_add(1);
        } else {
            self.ensure_endpoint(tx, spec.src, "edge source node missing")?;
            self.ensure_endpoint(tx, spec.dst, "edge destination node missing")?;
        }
        self.graph.insert_edge_unchecked(tx, spec)
    }

    fn ensure_endpoint(
        &mut self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        context: &'static str,
    ) -> Result<()> {
        if let Some(cache) = self.exists_cache.as_mut() {
            if let Some(hit) = cache.get(&node).copied() {
                self.stats.exists_cache_hits = self.stats.exists_cache_hits.saturating_add(1);
                if hit {
                    return Ok(());
                }
                return Err(SombraError::Invalid(context));
            }
        }
        let exists = self.graph.node_exists_with_write(tx, node)?;
        if let Some(cache) = self.exists_cache.as_mut() {
            cache.put(node, exists);
        }
        self.stats.exists_cache_misses = self.stats.exists_cache_misses.saturating_add(1);
        if exists {
            Ok(())
        } else {
            Err(SombraError::Invalid(context))
        }
    }
}
