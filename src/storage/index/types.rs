use crate::types::{LabelId, NodeId, PropId, Result, SombraError};

/// Backend storage implementation choices for property indexes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexKind {
    /// Chunked postings lists stored in compressed segments.
    Chunked,
    /// Fallback B+ tree postings keyed by `(label, prop, value, node)`.
    BTree,
}

/// Logical type of the indexed property value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypeTag {
    Null,
    Bool,
    Int,
    Float,
    String,
    Bytes,
    Date,
    DateTime,
}

/// Definition supplied when creating a property index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexDef {
    pub label: LabelId,
    pub prop: PropId,
    pub kind: IndexKind,
    pub ty: TypeTag,
}

/// Streaming interface over sorted, unique `NodeId`s.
pub trait PostingStream {
    /// Pushes up to `max` identifiers into `out`, returning `true` when additional
    /// data remains and `false` once the stream is exhausted.
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool>;

    /// Convenience helper to fetch the next identifier, if any.
    fn next(&mut self) -> Result<Option<NodeId>> {
        let mut buf = Vec::with_capacity(1);
        loop {
            buf.clear();
            let has_more = self.next_batch(&mut buf, 1)?;
            if let Some(node) = buf.pop() {
                return Ok(Some(node));
            }
            if !has_more {
                return Ok(None);
            }
        }
    }
}

/// A `PostingStream` that yields no results.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyPostingStream;

impl EmptyPostingStream {
    pub fn new() -> Self {
        Self
    }
}

impl PostingStream for EmptyPostingStream {
    fn next_batch(&mut self, _out: &mut Vec<NodeId>, _max: usize) -> Result<bool> {
        Ok(false)
    }
}

/// Lightweight adapter over an in-memory slice of sorted node identifiers.
pub struct VecPostingStream<'a> {
    nodes: &'a [NodeId],
    pos: usize,
}

impl<'a> VecPostingStream<'a> {
    #[allow(dead_code)]
    pub fn new(nodes: &'a [NodeId]) -> Self {
        Self { nodes, pos: 0 }
    }
}

impl PostingStream for VecPostingStream<'_> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            return Ok(self.pos < self.nodes.len());
        }
        let remaining = self.nodes.len().saturating_sub(self.pos);
        if remaining == 0 {
            return Ok(false);
        }
        let take = remaining.min(max);
        out.extend_from_slice(&self.nodes[self.pos..self.pos + take]);
        self.pos += take;
        Ok(self.pos < self.nodes.len())
    }
}

/// Drains an entire posting stream into `out`, appending in sorted order.
pub fn collect_all(stream: &mut dyn PostingStream, out: &mut Vec<NodeId>) -> Result<()> {
    const DEFAULT_BATCH: usize = 256;
    let mut batch = Vec::with_capacity(DEFAULT_BATCH);
    loop {
        batch.clear();
        let has_more = stream.next_batch(&mut batch, DEFAULT_BATCH)?;
        if batch.is_empty() {
            if !has_more {
                break;
            }
            return Err(SombraError::Corruption(
                "posting stream yielded empty batch while reporting more data",
            ));
        }
        out.extend_from_slice(&batch);
        if !has_more {
            break;
        }
    }
    Ok(())
}

/// Intersects two sorted posting streams, appending the common identifiers to `out`.
pub fn intersect_sorted(
    left: &mut dyn PostingStream,
    right: &mut dyn PostingStream,
    out: &mut Vec<NodeId>,
) -> Result<()> {
    let mut next_left = left.next()?;
    let mut next_right = right.next()?;
    while let (Some(l), Some(r)) = (next_left, next_right) {
        if l.0 == r.0 {
            out.push(l);
            next_left = left.next()?;
            next_right = right.next()?;
        } else if l.0 < r.0 {
            next_left = left.next()?;
        } else {
            next_right = right.next()?;
        }
    }
    Ok(())
}

/// Intersects `k` sorted posting streams. Results are appended to `out`.
///
/// The algorithm keeps one cursor per stream, advancing lagging inputs toward the
/// current maximum head so matches are emitted without staging intermediate vectors.
pub fn intersect_k(streams: &mut [&mut dyn PostingStream], out: &mut Vec<NodeId>) -> Result<()> {
    out.clear();
    if streams.is_empty() {
        return Ok(());
    }

    let mut heads = Vec::with_capacity(streams.len());
    for stream in streams.iter_mut() {
        heads.push(stream.next()?);
    }
    if heads.iter().any(|head| head.is_none()) {
        return Ok(());
    }

    loop {
        let mut target = match heads[0] {
            Some(node) => node,
            None => return Ok(()),
        };
        let mut all_equal = true;
        for head in heads.iter().skip(1) {
            let node = match head {
                Some(node) => *node,
                None => return Ok(()),
            };
            if node.0 != target.0 {
                all_equal = false;
            }
            if node.0 > target.0 {
                target = node;
            }
        }

        if all_equal {
            out.push(target);
            for (head, stream) in heads.iter_mut().zip(streams.iter_mut()) {
                *head = stream.next()?;
            }
            if heads.iter().any(|head| head.is_none()) {
                return Ok(());
            }
            continue;
        }

        let target_id = target.0;
        for (head, stream) in heads.iter_mut().zip(streams.iter_mut()) {
            loop {
                let node = match head {
                    Some(node) => *node,
                    None => return Ok(()),
                };
                if node.0 >= target_id {
                    break;
                }
                *head = stream.next()?;
                if head.is_none() {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersect_k_single_stream() {
        let nodes = [NodeId(1), NodeId(3), NodeId(5)];
        let mut stream = VecPostingStream::new(&nodes);
        let mut out = Vec::new();
        let mut streams: [&mut dyn PostingStream; 1] = [&mut stream];
        intersect_k(&mut streams, &mut out).expect("single stream intersection");
        assert_eq!(out.as_slice(), &nodes);
    }

    #[test]
    fn intersect_k_multiple_streams() {
        let a = [NodeId(1), NodeId(3), NodeId(5), NodeId(7)];
        let b = [NodeId(2), NodeId(3), NodeId(4), NodeId(7)];
        let c = [NodeId(3), NodeId(6), NodeId(7), NodeId(9)];

        let mut stream_a = VecPostingStream::new(&a);
        let mut stream_b = VecPostingStream::new(&b);
        let mut stream_c = VecPostingStream::new(&c);

        let mut out = Vec::new();
        let mut streams: [&mut dyn PostingStream; 3] =
            [&mut stream_a, &mut stream_b, &mut stream_c];

        intersect_k(&mut streams, &mut out).expect("multi-way intersection");
        assert_eq!(out, vec![NodeId(3), NodeId(7)]);
    }

    #[test]
    fn intersect_k_empty_result() {
        let a = [NodeId(1), NodeId(2)];
        let b = [NodeId(3), NodeId(4)];

        let mut stream_a = VecPostingStream::new(&a);
        let mut stream_b = VecPostingStream::new(&b);

        let mut out = Vec::new();
        let mut streams: [&mut dyn PostingStream; 2] = [&mut stream_a, &mut stream_b];

        intersect_k(&mut streams, &mut out).expect("disjoint intersection");
        assert!(out.is_empty());
    }
}
