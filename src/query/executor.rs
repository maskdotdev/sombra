//! Query executor scaffolding.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::primitives::pager::{Pager, ReadGuard};
use crate::storage::index::{collect_all, PostingStream};
use crate::storage::{
    Dir as StorageDir, ExpandOpts, Graph, NeighborCursor, NodeData, PropValueOwned,
};
use crate::types::{LabelId, NodeId, PropId, Result, SombraError, TypeId};

use crate::query::ast::Var;
use crate::query::metadata::MetadataProvider;
use crate::query::physical::{
    InLookup, LiteralValue, PhysicalBoolExpr, PhysicalComparison, PhysicalNode, PhysicalOp,
    PhysicalPlan, ProjectField, PropPredicate as PhysicalPredicate, ValueKey,
};
use crate::query::profile::{
    profile_timer as query_profile_timer, record_profile_timer as record_query_profile_timer,
    QueryProfileKind,
};

/// Materialised result returned by `execute`.
#[derive(Debug, Default)]
pub struct QueryResult {
    /// The rows returned by the query.
    pub rows: Vec<Row>,
}

/// Single output row represented as a mapping from alias to value.
pub type Row = BTreeMap<String, Value>;

/// Runtime value flowing through the executor.
#[derive(Clone, Debug)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value
    Int(i64),
    /// Floating-point value
    Float(f64),
    /// String value
    String(String),
    /// Binary value (base64-encoded when serialized).
    Bytes(Vec<u8>),
    /// Date value represented as days since Unix epoch.
    Date(i64),
    /// DateTime value represented as milliseconds since Unix epoch.
    DateTime(i64),
    /// Node identifier
    NodeId(NodeId),
    /// Nested object value (used for var projections).
    Object(BTreeMap<String, Value>),
}

type NodeCache = Arc<Mutex<HashMap<NodeId, NodeData>>>;
type PropNameCache = Arc<Mutex<HashMap<PropId, String>>>;

trait BindingStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>>;
}

type BoxBindingStream = Box<dyn BindingStream>;

struct ReadContext {
    guard: ReadGuard,
}

impl ReadContext {
    fn new(guard: ReadGuard) -> Self {
        Self { guard }
    }

    fn guard(&self) -> &ReadGuard {
        &self.guard
    }
}

#[derive(Clone, Debug, Default)]
struct BindingRow {
    nodes: BTreeMap<String, NodeId>,
}

impl BindingRow {
    fn from_binding(var: &str, node: NodeId) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(var.to_owned(), node);
        Self { nodes }
    }

    fn get(&self, var: &str) -> Option<NodeId> {
        self.nodes.get(var).copied()
    }

    fn insert(&mut self, var: &str, node: NodeId) {
        self.nodes.insert(var.to_owned(), node);
    }
}

#[derive(Clone)]
enum RowMapper {
    All,
    Project {
        fields: Vec<ProjectField>,
        graph: Arc<Graph>,
        context: Arc<ReadContext>,
        cache: NodeCache,
        metadata: Arc<dyn MetadataProvider>,
        prop_names: PropNameCache,
    },
}

impl RowMapper {
    fn map(&self, binding: &BindingRow) -> Result<Row> {
        match self {
            RowMapper::All => project_all(binding),
            RowMapper::Project {
                fields,
                graph,
                context,
                cache,
                metadata,
                prop_names,
            } => apply_projection(binding, fields, graph, context, cache, metadata, prop_names),
        }
    }
}

/// Streaming handle over query rows.
pub struct ResultStream {
    bindings: BoxBindingStream,
    mapper: RowMapper,
    _context: Arc<ReadContext>,
    cancel_token: Option<Arc<AtomicBool>>,
}

impl ResultStream {
    fn new(
        bindings: BoxBindingStream,
        mapper: RowMapper,
        context: Arc<ReadContext>,
        cancel_token: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            bindings,
            mapper,
            _context: context,
            cancel_token,
        }
    }

    fn check_cancel(&self) -> Result<()> {
        if let Some(flag) = &self.cancel_token {
            if flag.load(Ordering::SeqCst) {
                return Err(SombraError::Cancelled);
            }
        }
        Ok(())
    }
}

impl Iterator for ResultStream {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = self.check_cancel() {
            return Some(Err(err));
        }
        match self.bindings.try_next() {
            Ok(Some(binding)) => Some(self.mapper.map(&binding)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

/// Query executor responsible for running physical query plans.
pub struct Executor {
    graph: Arc<Graph>,
    pager: Arc<Pager>,
    metadata: Arc<dyn MetadataProvider>,
}

impl Executor {
    /// Creates a new executor with the specified graph, pager, and metadata provider.
    pub fn new(graph: Arc<Graph>, pager: Arc<Pager>, metadata: Arc<dyn MetadataProvider>) -> Self {
        Self {
            graph,
            pager,
            metadata,
        }
    }

    /// Executes a physical plan and materializes all results into memory.
    pub fn execute(
        &self,
        plan: &PhysicalPlan,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<QueryResult> {
        let mut stream = self.stream_with_token(plan, cancel)?;
        let iter_timer = query_profile_timer();
        let rows: Vec<Row> = stream.by_ref().collect::<Result<_>>()?;
        record_query_profile_timer(QueryProfileKind::StreamIter, iter_timer);
        Ok(QueryResult { rows })
    }

    /// Executes a physical plan and returns a streaming iterator over results.
    pub fn stream(
        &self,
        plan: &PhysicalPlan,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<ResultStream> {
        self.stream_with_token(plan, cancel)
    }

    fn stream_with_token(
        &self,
        plan: &PhysicalPlan,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<ResultStream> {
        let guard_timer = query_profile_timer();
        let context = Arc::new(ReadContext::new(self.pager.begin_latest_committed_read()?));
        record_query_profile_timer(QueryProfileKind::ReadGuard, guard_timer);
        let cache: NodeCache = Arc::new(Mutex::new(HashMap::new()));
        let mut project_fields = None;
        let root = match &plan.root.op {
            PhysicalOp::Project { fields } => {
                if plan.root.inputs.len() != 1 {
                    return Err(SombraError::Invalid("project expects single input"));
                }
                project_fields = Some(fields.clone());
                &plan.root.inputs[0]
            }
            _ => &plan.root,
        };
        let build_timer = query_profile_timer();
        let bindings = self.build_stream(root, Arc::clone(&context), Arc::clone(&cache))?;
        record_query_profile_timer(QueryProfileKind::StreamBuild, build_timer);
        let mapper = match project_fields {
            Some(fields) => RowMapper::Project {
                fields,
                graph: Arc::clone(&self.graph),
                context: Arc::clone(&context),
                cache,
                metadata: Arc::clone(&self.metadata),
                prop_names: Arc::new(Mutex::new(HashMap::new())),
            },
            None => RowMapper::All,
        };
        Ok(ResultStream::new(bindings, mapper, context, cancel))
    }

    fn build_stream(
        &self,
        node: &PhysicalNode,
        context: Arc<ReadContext>,
        cache: NodeCache,
    ) -> Result<BoxBindingStream> {
        match &node.op {
            PhysicalOp::LabelScan { label, as_var, .. } => {
                let stream = self.graph.label_scan_stream(context.guard(), *label)?;
                Ok(Box::new(PostingBindingStream::from_stream(
                    as_var.0.clone(),
                    stream,
                )?))
            }
            PhysicalOp::PropIndexScan {
                label,
                prop,
                pred,
                as_var,
                ..
            } => self.build_prop_index_stream(*label, *prop, pred, &as_var.0, Arc::clone(&context)),
            PhysicalOp::Expand {
                from,
                to,
                dir,
                ty,
                distinct_nodes,
            } => {
                if node.inputs.len() != 1 {
                    return Err(SombraError::Invalid("expand expects single input child"));
                }
                let input =
                    self.build_stream(&node.inputs[0], Arc::clone(&context), cache.clone())?;
                Ok(Box::new(ExpandStream::new(
                    input,
                    self.graph.clone(),
                    Arc::clone(&context),
                    from.0.clone(),
                    to.0.clone(),
                    storage_dir(*dir),
                    *ty,
                    *distinct_nodes,
                )))
            }
            PhysicalOp::Filter { pred, .. } => {
                if node.inputs.len() != 1 {
                    return Err(SombraError::Invalid("filter expects single input child"));
                }
                let input =
                    self.build_stream(&node.inputs[0], Arc::clone(&context), cache.clone())?;
                let filter = FilterEval::Physical(pred.clone());
                Ok(Box::new(FilterStream::new(
                    input,
                    self.graph.clone(),
                    Arc::clone(&context),
                    cache,
                    filter,
                )))
            }
            PhysicalOp::Union { dedup, .. } => {
                if node.inputs.is_empty() {
                    return Err(SombraError::Invalid("union expects at least one child"));
                }
                let mut children = Vec::with_capacity(node.inputs.len());
                for child in &node.inputs {
                    children.push(self.build_stream(child, Arc::clone(&context), cache.clone())?);
                }
                Ok(Box::new(UnionStream::new(children, *dedup)))
            }
            PhysicalOp::BoolFilter { expr } => {
                if node.inputs.len() != 1 {
                    return Err(SombraError::Invalid("filter expects single input child"));
                }
                let input =
                    self.build_stream(&node.inputs[0], Arc::clone(&context), cache.clone())?;
                Ok(Box::new(FilterStream::new(
                    input,
                    self.graph.clone(),
                    Arc::clone(&context),
                    cache,
                    FilterEval::Bool(expr.clone()),
                )))
            }
            PhysicalOp::Intersect { vars } => {
                if node.inputs.is_empty() {
                    return Err(SombraError::Invalid("intersect expects inputs"));
                }
                let mut sets = Vec::new();
                for child in &node.inputs {
                    let mut stream =
                        self.build_stream(child, Arc::clone(&context), cache.clone())?;
                    sets.push(collect_bindings(&mut *stream)?);
                }
                let rows = intersect_rows(vars, sets)?;
                Ok(Box::new(VecBindingStream::new(rows)))
            }
            PhysicalOp::HashJoin { left, right } => {
                if node.inputs.len() != 2 {
                    return Err(SombraError::Invalid(
                        "hash join expects exactly two input children",
                    ));
                }
                let mut left_stream =
                    self.build_stream(&node.inputs[0], Arc::clone(&context), cache.clone())?;
                let right_stream =
                    self.build_stream(&node.inputs[1], Arc::clone(&context), cache)?;
                Ok(Box::new(HashJoinStream::new(
                    collect_bindings(&mut *left_stream)?,
                    right_stream,
                    left.0.clone(),
                    right.0.clone(),
                )?))
            }
            PhysicalOp::Distinct => {
                if node.inputs.len() != 1 {
                    return Err(SombraError::Invalid("distinct expects single input child"));
                }
                let input = self.build_stream(&node.inputs[0], context, cache)?;
                Ok(Box::new(DistinctStream::new(input)))
            }
            PhysicalOp::Project { .. } => {
                if node.inputs.len() != 1 {
                    return Err(SombraError::Invalid("project expects single input child"));
                }
                self.build_stream(&node.inputs[0], context, cache)
            }
        }
    }

    fn build_prop_index_stream(
        &self,
        label: LabelId,
        prop: PropId,
        pred: &PhysicalPredicate,
        var: &str,
        context: Arc<ReadContext>,
    ) -> Result<BoxBindingStream> {
        match pred {
            PhysicalPredicate::Eq { value, .. } => {
                let owned = literal_to_prop_value(value)?;
                let prop_timer = query_profile_timer();
                let stream =
                    self.graph
                        .property_scan_eq_stream(context.guard(), label, prop, &owned)?;
                record_query_profile_timer(QueryProfileKind::PropIndex, prop_timer);
                Ok(Box::new(PostingBindingStream::from_stream(
                    var.to_owned(),
                    stream,
                )?))
            }
            PhysicalPredicate::Range { lower, upper, .. } => {
                let lower_owned = bound_owned(lower)?;
                let upper_owned = bound_owned(upper)?;
                let lower_ref = bound_ref(lower, lower_owned.as_ref());
                let upper_ref = bound_ref(upper, upper_owned.as_ref());
                let prop_timer = query_profile_timer();
                let stream = self.graph.property_scan_range_stream(
                    context.guard(),
                    label,
                    prop,
                    lower_ref,
                    upper_ref,
                )?;
                record_query_profile_timer(QueryProfileKind::PropIndex, prop_timer);
                Ok(Box::new(PostingBindingStream::from_stream(
                    var.to_owned(),
                    stream,
                )?))
            }
        }
    }
}

enum FilterEval {
    Physical(PhysicalPredicate),
    Bool(PhysicalBoolExpr),
}

struct PostingBindingStream {
    var: String,
    nodes: Vec<NodeId>,
    index: usize,
}

impl PostingBindingStream {
    fn from_stream(var: String, mut stream: Box<dyn PostingStream + '_>) -> Result<Self> {
        let mut nodes = Vec::new();
        collect_all(&mut *stream, &mut nodes)?;
        nodes.sort_by_key(|node| node.0);
        nodes.dedup();
        Ok(Self {
            var,
            nodes,
            index: 0,
        })
    }
}

impl BindingStream for PostingBindingStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>> {
        if self.index >= self.nodes.len() {
            return Ok(None);
        }
        let node = self.nodes[self.index];
        self.index += 1;
        Ok(Some(BindingRow::from_binding(&self.var, node)))
    }
}

struct VecBindingStream {
    rows: Vec<BindingRow>,
    index: usize,
}

impl VecBindingStream {
    fn new(rows: Vec<BindingRow>) -> Self {
        Self { rows, index: 0 }
    }
}

impl BindingStream for VecBindingStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>> {
        if self.index >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.index].clone();
        self.index += 1;
        Ok(Some(row))
    }
}

struct DistinctStream {
    input: BoxBindingStream,
    seen: BTreeSet<BTreeMap<String, NodeId>>,
}

impl DistinctStream {
    fn new(input: BoxBindingStream) -> Self {
        Self {
            input,
            seen: BTreeSet::new(),
        }
    }
}

impl BindingStream for DistinctStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>> {
        while let Some(row) = self.input.try_next()? {
            if self.seen.insert(row.nodes.clone()) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

struct UnionStream {
    inputs: Vec<BoxBindingStream>,
    current: usize,
    seen: Option<BTreeSet<BTreeMap<String, NodeId>>>,
}

impl UnionStream {
    fn new(inputs: Vec<BoxBindingStream>, dedup: bool) -> Self {
        let seen = if dedup { Some(BTreeSet::new()) } else { None };
        Self {
            inputs,
            current: 0,
            seen,
        }
    }
}

impl BindingStream for UnionStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>> {
        while self.current < self.inputs.len() {
            match self.inputs[self.current].try_next()? {
                Some(row) => {
                    if let Some(seen) = self.seen.as_mut() {
                        if !seen.insert(row.nodes.clone()) {
                            continue;
                        }
                    }
                    return Ok(Some(row));
                }
                None => self.current += 1,
            }
        }
        Ok(None)
    }
}

struct ExpandStream {
    input: BoxBindingStream,
    graph: Arc<Graph>,
    context: Arc<ReadContext>,
    from: String,
    to: String,
    dir: StorageDir,
    ty: Option<TypeId>,
    distinct_nodes: bool,
    current_row: Option<BindingRow>,
    neighbors: Option<NeighborCursor>,
}

impl ExpandStream {
    fn new(
        input: BoxBindingStream,
        graph: Arc<Graph>,
        context: Arc<ReadContext>,
        from: String,
        to: String,
        dir: StorageDir,
        ty: Option<TypeId>,
        distinct_nodes: bool,
    ) -> Self {
        Self {
            input,
            graph,
            context,
            from,
            to,
            dir,
            ty,
            distinct_nodes,
            current_row: None,
            neighbors: None,
        }
    }
}

impl BindingStream for ExpandStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>> {
        let expand_timer = query_profile_timer();
        let result = self.try_next_inner();
        record_query_profile_timer(QueryProfileKind::Expand, expand_timer);
        result
    }
}

impl ExpandStream {
    fn try_next_inner(&mut self) -> Result<Option<BindingRow>> {
        loop {
            if let Some(cursor) = self.neighbors.as_mut() {
                if let Some(neighbor) = cursor.next() {
                    let Some(current) = self.current_row.as_ref() else {
                        return Err(SombraError::Invalid(
                            "expand missing current row during neighbor iteration",
                        ));
                    };
                    let mut row = current.clone();
                    row.insert(&self.to, neighbor.neighbor);
                    return Ok(Some(row));
                }
                self.neighbors = None;
                self.current_row = None;
            }

            let Some(row) = self.input.try_next()? else {
                return Ok(None);
            };
            let Some(node_id) = row.get(&self.from) else {
                return Err(SombraError::Invalid(
                    "expand missing source variable binding",
                ));
            };
            let cursor = self.graph.neighbors(
                self.context.guard(),
                node_id,
                self.dir,
                self.ty,
                ExpandOpts {
                    distinct_nodes: self.distinct_nodes,
                },
            )?;
            if cursor.is_empty() {
                continue;
            }
            self.current_row = Some(row);
            self.neighbors = Some(cursor);
        }
    }
}

struct FilterStream {
    input: BoxBindingStream,
    graph: Arc<Graph>,
    context: Arc<ReadContext>,
    cache: NodeCache,
    eval: FilterEval,
}

impl FilterStream {
    fn new(
        input: BoxBindingStream,
        graph: Arc<Graph>,
        context: Arc<ReadContext>,
        cache: NodeCache,
        eval: FilterEval,
    ) -> Self {
        Self {
            input,
            graph,
            context,
            cache,
            eval,
        }
    }
}

impl BindingStream for FilterStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>> {
        let filter_timer = query_profile_timer();
        let result = self.try_next_inner();
        record_query_profile_timer(QueryProfileKind::Filter, filter_timer);
        result
    }
}

impl FilterStream {
    fn try_next_inner(&mut self) -> Result<Option<BindingRow>> {
        loop {
            let Some(row) = self.input.try_next()? else {
                return Ok(None);
            };
            let matches = match &self.eval {
                FilterEval::Physical(pred) => match pred {
                    PhysicalPredicate::Eq { var, .. } | PhysicalPredicate::Range { var, .. } => {
                        let node_id = row
                            .get(&var.0)
                            .ok_or(SombraError::Invalid("filter variable missing from binding"))?;
                        let node_data =
                            fetch_node_data(&self.graph, &self.context, &self.cache, node_id)?;
                        evaluate_predicate(pred, &node_data)?
                    }
                },
                FilterEval::Bool(expr) => {
                    let mut resolver = ExecutorBoolResolver::new(
                        &row,
                        self.graph.clone(),
                        Arc::clone(&self.context),
                        self.cache.clone(),
                    );
                    evaluate_bool_expr(expr, &mut resolver)?
                }
            };
            if matches {
                return Ok(Some(row));
            }
        }
    }
}

trait BoolNodeResolver {
    fn resolve(&mut self, var: &Var) -> Result<NodeData>;
}

struct ExecutorBoolResolver<'a> {
    row: &'a BindingRow,
    graph: Arc<Graph>,
    context: Arc<ReadContext>,
    cache: NodeCache,
    loaded: HashMap<String, NodeData>,
}

impl<'a> ExecutorBoolResolver<'a> {
    fn new(
        row: &'a BindingRow,
        graph: Arc<Graph>,
        context: Arc<ReadContext>,
        cache: NodeCache,
    ) -> Self {
        Self {
            row,
            graph,
            context,
            cache,
            loaded: HashMap::new(),
        }
    }
}

impl BoolNodeResolver for ExecutorBoolResolver<'_> {
    fn resolve(&mut self, var: &Var) -> Result<NodeData> {
        if let Some(existing) = self.loaded.get(&var.0) {
            return Ok(existing.clone());
        }
        let node_id = self.row.get(&var.0).ok_or(SombraError::Invalid(
            "predicate variable missing from binding",
        ))?;
        let data = fetch_node_data(&self.graph, &self.context, &self.cache, node_id)?;
        self.loaded.insert(var.0.clone(), data.clone());
        Ok(data)
    }
}

fn evaluate_bool_expr<R: BoolNodeResolver>(
    expr: &PhysicalBoolExpr,
    resolver: &mut R,
) -> Result<bool> {
    match expr {
        PhysicalBoolExpr::Cmp(cmp) => evaluate_comparison(cmp, resolver),
        PhysicalBoolExpr::And(children) => {
            for child in children {
                if !evaluate_bool_expr(child, resolver)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        PhysicalBoolExpr::Or(children) => {
            for child in children {
                if evaluate_bool_expr(child, resolver)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        PhysicalBoolExpr::Not(child) => Ok(!evaluate_bool_expr(child, resolver)?),
    }
}

fn evaluate_comparison<R: BoolNodeResolver>(
    cmp: &PhysicalComparison,
    resolver: &mut R,
) -> Result<bool> {
    match cmp {
        PhysicalComparison::Eq {
            var, prop, value, ..
        } => {
            let node = resolver.resolve(var)?;
            eval_eq(&node, *prop, value)
        }
        PhysicalComparison::Ne {
            var, prop, value, ..
        } => {
            let node = resolver.resolve(var)?;
            eval_ne(&node, *prop, value)
        }
        PhysicalComparison::Lt {
            var, prop, value, ..
        } => {
            let node = resolver.resolve(var)?;
            compare_with(&node, *prop, value, |ord| ord.is_lt())
        }
        PhysicalComparison::Le {
            var, prop, value, ..
        } => {
            let node = resolver.resolve(var)?;
            compare_with(&node, *prop, value, |ord| ord.is_le())
        }
        PhysicalComparison::Gt {
            var, prop, value, ..
        } => {
            let node = resolver.resolve(var)?;
            compare_with(&node, *prop, value, |ord| ord.is_gt())
        }
        PhysicalComparison::Ge {
            var, prop, value, ..
        } => {
            let node = resolver.resolve(var)?;
            compare_with(&node, *prop, value, |ord| ord.is_ge())
        }
        PhysicalComparison::Between {
            var,
            prop,
            low,
            high,
            ..
        } => {
            let node = resolver.resolve(var)?;
            eval_between(&node, *prop, low, high)
        }
        PhysicalComparison::In {
            var,
            prop,
            values,
            lookup,
            ..
        } => {
            let node = resolver.resolve(var)?;
            eval_in(&node, *prop, values, lookup)
        }
        PhysicalComparison::Exists { var, prop, .. } => {
            let node = resolver.resolve(var)?;
            Ok(find_prop(&node, *prop).is_some())
        }
        PhysicalComparison::IsNull { var, prop, .. } => {
            let node = resolver.resolve(var)?;
            Ok(find_prop(&node, *prop)
                .map(|value| matches!(value, PropValueOwned::Null))
                .unwrap_or(true))
        }
        PhysicalComparison::IsNotNull { var, prop, .. } => {
            let node = resolver.resolve(var)?;
            Ok(find_prop(&node, *prop)
                .map(|value| !matches!(value, PropValueOwned::Null))
                .unwrap_or(false))
        }
    }
}

fn eval_eq(node: &NodeData, prop: PropId, literal: &LiteralValue) -> Result<bool> {
    if matches!(literal, LiteralValue::Null) {
        return Ok(find_prop(node, prop)
            .map(|value| matches!(value, PropValueOwned::Null))
            .unwrap_or(true));
    }
    let Some(value) = find_prop(node, prop) else {
        return Ok(false);
    };
    if matches!(value, PropValueOwned::Null) {
        return Ok(false);
    }
    Ok(compare_values(value, literal)?.is_eq())
}

fn eval_ne(node: &NodeData, prop: PropId, literal: &LiteralValue) -> Result<bool> {
    if matches!(literal, LiteralValue::Null) {
        return Ok(find_prop(node, prop)
            .map(|value| !matches!(value, PropValueOwned::Null))
            .unwrap_or(false));
    }
    let Some(value) = find_prop(node, prop) else {
        return Ok(false);
    };
    if matches!(value, PropValueOwned::Null) {
        return Ok(false);
    }
    Ok(!compare_values(value, literal)?.is_eq())
}

fn compare_with<F>(
    node: &NodeData,
    prop: PropId,
    literal: &LiteralValue,
    predicate: F,
) -> Result<bool>
where
    F: Fn(CompareOrdering) -> bool,
{
    if matches!(literal, LiteralValue::Null) {
        return Ok(false);
    }
    let Some(value) = find_prop(node, prop) else {
        return Ok(false);
    };
    if matches!(value, PropValueOwned::Null) {
        return Ok(false);
    }
    let ord = compare_values(value, literal)?;
    Ok(predicate(ord))
}

fn eval_between(
    node: &NodeData,
    prop: PropId,
    low: &Bound<LiteralValue>,
    high: &Bound<LiteralValue>,
) -> Result<bool> {
    let Some(value) = find_prop(node, prop) else {
        return Ok(false);
    };
    if matches!(value, PropValueOwned::Null) {
        return Ok(false);
    }
    let meets_low = match low {
        Bound::Unbounded => true,
        Bound::Included(lit) => {
            if matches!(lit, LiteralValue::Null) {
                return Ok(false);
            }
            compare_values(value, lit)?.is_ge()
        }
        Bound::Excluded(lit) => {
            if matches!(lit, LiteralValue::Null) {
                return Ok(false);
            }
            compare_values(value, lit)?.is_gt()
        }
    };
    if !meets_low {
        return Ok(false);
    }
    let meets_high = match high {
        Bound::Unbounded => true,
        Bound::Included(lit) => {
            if matches!(lit, LiteralValue::Null) {
                return Ok(false);
            }
            compare_values(value, lit)?.is_le()
        }
        Bound::Excluded(lit) => {
            if matches!(lit, LiteralValue::Null) {
                return Ok(false);
            }
            compare_values(value, lit)?.is_lt()
        }
    };
    Ok(meets_high)
}

fn eval_in(
    node: &NodeData,
    prop: PropId,
    values: &[LiteralValue],
    lookup: &InLookup,
) -> Result<bool> {
    let Some(actual) = find_prop(node, prop) else {
        return Ok(false);
    };
    if matches!(actual, PropValueOwned::Null) {
        return Ok(false);
    }
    if let Some(set) = lookup.hash_values() {
        if let Some(key) = ValueKey::from_property(actual) {
            return Ok(set.contains(&key));
        }
    }
    for literal in values {
        if matches!(literal, LiteralValue::Null) {
            continue;
        }
        if compare_values(actual, literal)?.is_eq() {
            return Ok(true);
        }
    }
    Ok(false)
}

struct HashJoinStream {
    build: HashMap<NodeId, Vec<BindingRow>>,
    probe: BoxBindingStream,
    right_var: String,
    pending: Vec<BindingRow>,
    pending_idx: usize,
}

impl HashJoinStream {
    fn new(
        left_rows: Vec<BindingRow>,
        probe: BoxBindingStream,
        left_var: String,
        right_var: String,
    ) -> Result<Self> {
        let mut build: HashMap<NodeId, Vec<BindingRow>> = HashMap::new();
        for row in left_rows {
            let Some(id) = row.get(&left_var) else {
                return Err(SombraError::Invalid(
                    "hash join missing left variable binding",
                ));
            };
            build.entry(id).or_default().push(row);
        }
        Ok(Self {
            build,
            probe,
            right_var,
            pending: Vec::new(),
            pending_idx: 0,
        })
    }
}

impl BindingStream for HashJoinStream {
    fn try_next(&mut self) -> Result<Option<BindingRow>> {
        loop {
            if self.pending_idx < self.pending.len() {
                let row = self.pending[self.pending_idx].clone();
                self.pending_idx += 1;
                return Ok(Some(row));
            }
            self.pending.clear();
            self.pending_idx = 0;

            let Some(row) = self.probe.try_next()? else {
                return Ok(None);
            };
            let Some(id) = row.get(&self.right_var) else {
                return Err(SombraError::Invalid(
                    "hash join missing right variable binding",
                ));
            };

            if let Some(candidates) = self.build.get(&id) {
                for left in candidates {
                    if let Some(merged) = merge_rows(left, &row) {
                        self.pending.push(merged);
                    }
                }
            }
        }
    }
}

fn collect_bindings(stream: &mut dyn BindingStream) -> Result<Vec<BindingRow>> {
    let mut rows = Vec::new();
    while let Some(row) = stream.try_next()? {
        rows.push(row);
    }
    Ok(rows)
}

fn intersect_rows(vars: &[Var], sets: Vec<Vec<BindingRow>>) -> Result<Vec<BindingRow>> {
    if vars.is_empty() {
        return Err(SombraError::Invalid(
            "intersect requires at least one variable",
        ));
    }
    if sets.len() < 2 {
        return Err(SombraError::Invalid(
            "intersect requires at least two inputs",
        ));
    }
    let key_var = &vars[0].0;
    let mut acc: Vec<BindingRow> = sets[0].clone();
    for set in sets.iter().skip(1) {
        let mut map: HashMap<NodeId, Vec<BindingRow>> = HashMap::new();
        for row in &acc {
            if let Some(id) = row.get(key_var) {
                map.entry(id).or_default().push(row.clone());
            }
        }
        let mut next_acc = Vec::new();
        for row in set {
            if let Some(id) = row.get(key_var) {
                if let Some(existing) = map.get(&id) {
                    for prefix in existing {
                        if let Some(merged) = merge_rows(prefix, row) {
                            next_acc.push(merged);
                        }
                    }
                }
            }
        }
        acc = next_acc;
    }
    Ok(acc)
}

fn storage_dir(dir: crate::query::physical::Dir) -> StorageDir {
    match dir {
        crate::query::physical::Dir::Out => StorageDir::Out,
        crate::query::physical::Dir::In => StorageDir::In,
        crate::query::physical::Dir::Both => StorageDir::Both,
    }
}

fn fetch_node_data(
    graph: &Arc<Graph>,
    context: &Arc<ReadContext>,
    cache: &NodeCache,
    id: NodeId,
) -> Result<NodeData> {
    let mut guard = cache
        .lock()
        .map_err(|_| SombraError::Invalid("node cache lock poisoned"))?;
    if let Some(existing) = guard.get(&id).cloned() {
        return Ok(existing);
    }
    let data = graph
        .get_node(context.guard(), id)?
        .ok_or(SombraError::Invalid("node missing during evaluation"))?;
    guard.insert(id, data.clone());
    Ok(data)
}

fn apply_projection(
    binding: &BindingRow,
    fields: &[ProjectField],
    graph: &Arc<Graph>,
    context: &Arc<ReadContext>,
    cache: &NodeCache,
    metadata: &Arc<dyn MetadataProvider>,
    prop_names: &PropNameCache,
) -> Result<Row> {
    let mut row = Row::new();
    for field in fields {
        match field {
            ProjectField::Var { var, alias } => {
                let Some(node) = binding.get(&var.0) else {
                    return Err(SombraError::Invalid("projection variable missing"));
                };
                let data = fetch_node_data(graph, context, cache, node)?;
                let mut props = BTreeMap::new();
                for (prop_id, prop_value) in &data.props {
                    let name = resolve_prop_name(metadata, prop_names, *prop_id)?;
                    props.insert(name, prop_value_to_exec_value(prop_value));
                }
                let key = alias.clone().unwrap_or_else(|| var.0.clone());
                let mut node_obj = BTreeMap::new();
                node_obj.insert("_id".into(), Value::NodeId(node));
                node_obj.insert("props".into(), Value::Object(props));
                row.insert(key, Value::Object(node_obj));
            }
            ProjectField::Prop {
                var,
                prop,
                prop_name,
                alias,
            } => {
                let node_id = binding
                    .get(&var.0)
                    .ok_or(SombraError::Invalid("projection variable missing"))?;
                let data = fetch_node_data(graph, context, cache, node_id)?;
                let value = find_prop(&data, *prop)
                    .map(prop_value_to_exec_value)
                    .unwrap_or(Value::Null);
                let key = alias.clone().unwrap_or_else(|| prop_name.clone());
                row.insert(key, value);
            }
        }
    }
    Ok(row)
}

fn resolve_prop_name(
    metadata: &Arc<dyn MetadataProvider>,
    cache: &PropNameCache,
    prop: PropId,
) -> Result<String> {
    let mut guard = cache
        .lock()
        .map_err(|_| SombraError::Invalid("prop name cache lock poisoned"))?;
    if let Some(name) = guard.get(&prop).cloned() {
        return Ok(name);
    }
    let name = metadata.property_name(prop)?;
    guard.insert(prop, name.clone());
    Ok(name)
}

fn project_all(binding: &BindingRow) -> Result<Row> {
    let mut row = Row::new();
    for (var, node) in &binding.nodes {
        row.insert(var.clone(), Value::NodeId(*node));
    }
    Ok(row)
}

fn merge_rows(left: &BindingRow, right: &BindingRow) -> Option<BindingRow> {
    for (var, node) in &right.nodes {
        if let Some(existing) = left.nodes.get(var) {
            if existing != node {
                return None;
            }
        }
    }
    let mut merged = left.clone();
    for (var, node) in &right.nodes {
        merged.insert(var, *node);
    }
    Some(merged)
}

fn evaluate_predicate(predicate: &PhysicalPredicate, node: &NodeData) -> Result<bool> {
    match predicate {
        PhysicalPredicate::Eq { prop, value, .. } => {
            if matches!(value, LiteralValue::Null) {
                return Ok(find_prop(node, *prop)
                    .map(|actual| matches!(actual, PropValueOwned::Null))
                    .unwrap_or(true));
            }
            let Some(actual) = find_prop(node, *prop) else {
                return Ok(false);
            };
            if matches!(actual, PropValueOwned::Null) {
                return Ok(false);
            }
            Ok(compare_values(actual, value)?.is_eq())
        }
        PhysicalPredicate::Range {
            prop, lower, upper, ..
        } => {
            let Some(actual) = find_prop(node, *prop) else {
                return Ok(false);
            };
            if matches!(actual, PropValueOwned::Null) {
                return Ok(false);
            }
            let lower_cmp = match lower {
                Bound::Unbounded => true,
                Bound::Included(lit) => {
                    if matches!(lit, LiteralValue::Null) {
                        return Ok(false);
                    }
                    compare_values(actual, lit)?.is_ge()
                }
                Bound::Excluded(lit) => {
                    if matches!(lit, LiteralValue::Null) {
                        return Ok(false);
                    }
                    compare_values(actual, lit)?.is_gt()
                }
            };
            let upper_cmp = match upper {
                Bound::Unbounded => true,
                Bound::Included(lit) => {
                    if matches!(lit, LiteralValue::Null) {
                        return Ok(false);
                    }
                    compare_values(actual, lit)?.is_le()
                }
                Bound::Excluded(lit) => {
                    if matches!(lit, LiteralValue::Null) {
                        return Ok(false);
                    }
                    compare_values(actual, lit)?.is_lt()
                }
            };
            Ok(lower_cmp && upper_cmp)
        }
    }
}

fn find_prop(node: &NodeData, prop: PropId) -> Option<&PropValueOwned> {
    node.props
        .iter()
        .find_map(|(id, value)| if *id == prop { Some(value) } else { None })
}

fn prop_value_to_exec_value(value: &PropValueOwned) -> Value {
    match value {
        PropValueOwned::Null => Value::Null,
        PropValueOwned::Bool(v) => Value::Bool(*v),
        PropValueOwned::Int(v) => Value::Int(*v),
        PropValueOwned::Float(v) => Value::Float(*v),
        PropValueOwned::Str(v) => Value::String(v.clone()),
        PropValueOwned::Bytes(v) => Value::Bytes(v.clone()),
        PropValueOwned::Date(v) => Value::Date(*v),
        PropValueOwned::DateTime(v) => Value::DateTime(*v),
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum CompareOrdering {
    Less,
    Equal,
    Greater,
}

impl CompareOrdering {
    fn is_eq(self) -> bool {
        matches!(self, CompareOrdering::Equal)
    }
    fn is_lt(self) -> bool {
        matches!(self, CompareOrdering::Less)
    }
    fn is_gt(self) -> bool {
        matches!(self, CompareOrdering::Greater)
    }
    fn is_le(self) -> bool {
        matches!(self, CompareOrdering::Less | CompareOrdering::Equal)
    }
    fn is_ge(self) -> bool {
        matches!(self, CompareOrdering::Greater | CompareOrdering::Equal)
    }
}

fn compare_values(value: &PropValueOwned, literal: &LiteralValue) -> Result<CompareOrdering> {
    use CompareOrdering::*;
    if let LiteralValue::Bytes(expected) = literal {
        match value {
            PropValueOwned::Bytes(actual) => {
                return Ok(match actual.cmp(expected) {
                    std::cmp::Ordering::Less => Less,
                    std::cmp::Ordering::Equal => Equal,
                    std::cmp::Ordering::Greater => Greater,
                })
            }
            _ => {
                return Err(SombraError::Invalid(
                    "binary property comparison unsupported for this property",
                ))
            }
        }
    }
    let left = comparable_from_prop(value)?;
    let right = comparable_from_literal(literal);
    match (left, right) {
        (ComparableValue::Null, ComparableValue::Null) => Ok(Equal),
        (ComparableValue::Bool(a), ComparableValue::Bool(b)) => Ok(match a.cmp(&b) {
            std::cmp::Ordering::Less => Less,
            std::cmp::Ordering::Equal => Equal,
            std::cmp::Ordering::Greater => Greater,
        }),
        (ComparableValue::Number(a), ComparableValue::Number(b)) => {
            match a
                .partial_cmp(&b)
                .ok_or(SombraError::Invalid("number comparison invalid"))?
            {
                std::cmp::Ordering::Less => Ok(Less),
                std::cmp::Ordering::Equal => Ok(Equal),
                std::cmp::Ordering::Greater => Ok(Greater),
            }
        }
        (ComparableValue::String(a), ComparableValue::String(b)) => Ok(match a.cmp(&b) {
            std::cmp::Ordering::Less => Less,
            std::cmp::Ordering::Equal => Equal,
            std::cmp::Ordering::Greater => Greater,
        }),
        _ => Err(SombraError::Invalid("incompatible property comparison")),
    }
}

fn literal_to_prop_value(value: &LiteralValue) -> Result<PropValueOwned> {
    match value {
        LiteralValue::Null => Ok(PropValueOwned::Null),
        LiteralValue::Bool(v) => Ok(PropValueOwned::Bool(*v)),
        LiteralValue::Int(v) => Ok(PropValueOwned::Int(*v)),
        LiteralValue::Float(v) => Ok(PropValueOwned::Float(*v)),
        LiteralValue::String(v) => Ok(PropValueOwned::Str(v.clone())),
        LiteralValue::Bytes(v) => Ok(PropValueOwned::Bytes(v.clone())),
        LiteralValue::DateTime(v) => Ok(PropValueOwned::DateTime(*v)),
    }
}

fn bound_owned(bound: &Bound<LiteralValue>) -> Result<Option<PropValueOwned>> {
    match bound {
        Bound::Unbounded => Ok(None),
        Bound::Included(lit) | Bound::Excluded(lit) => Ok(Some(literal_to_prop_value(lit)?)),
    }
}

#[derive(Clone, Debug)]
enum ComparableValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
}

fn comparable_from_prop(value: &PropValueOwned) -> Result<ComparableValue> {
    match value {
        PropValueOwned::Null => Ok(ComparableValue::Null),
        PropValueOwned::Bool(v) => Ok(ComparableValue::Bool(*v)),
        PropValueOwned::Int(v) => Ok(ComparableValue::Number(*v as f64)),
        PropValueOwned::Float(v) => Ok(ComparableValue::Number(*v)),
        PropValueOwned::Str(v) => Ok(ComparableValue::String(v.clone())),
        PropValueOwned::Date(v) => Ok(ComparableValue::Number(*v as f64)),
        PropValueOwned::DateTime(v) => Ok(ComparableValue::Number(*v as f64)),
        PropValueOwned::Bytes(_) => Err(SombraError::Invalid(
            "binary property comparison unsupported",
        )),
    }
}

fn comparable_from_literal(literal: &LiteralValue) -> ComparableValue {
    match literal {
        LiteralValue::Null => ComparableValue::Null,
        LiteralValue::Bool(v) => ComparableValue::Bool(*v),
        LiteralValue::Int(v) => ComparableValue::Number(*v as f64),
        LiteralValue::Float(v) => ComparableValue::Number(*v),
        LiteralValue::String(v) => ComparableValue::String(v.clone()),
        LiteralValue::DateTime(v) => ComparableValue::Number(*v as f64),
        LiteralValue::Bytes(_) => unreachable!("binary literal handled earlier"),
    }
}

fn bound_ref<'a>(
    source: &Bound<LiteralValue>,
    owned: Option<&'a PropValueOwned>,
) -> Bound<&'a PropValueOwned> {
    match (source, owned) {
        (Bound::Unbounded, _) => Bound::Unbounded,
        (Bound::Included(_), Some(value)) => Bound::Included(value),
        (Bound::Excluded(_), Some(value)) => Bound::Excluded(value),
        _ => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::pager::{PageStore, Pager, PagerOptions};
    use crate::query::ast::Var;
    use crate::query::builder::QueryBuilder;
    use crate::query::metadata::InMemoryMetadata;
    use crate::query::metadata::MetadataProvider;
    use crate::query::physical::{
        InLookup, LiteralValue, PhysicalNode, PhysicalOp, PhysicalPlan, ProjectField,
        PropPredicate as PhysicalPredicate,
    };
    use crate::query::planner::{Planner, PlannerConfig};
    use crate::storage::{GraphOptions, NodeSpec, PropEntry, PropValue};
    use crate::types::{LabelId, PropId, TypeId};
    use std::collections::HashMap;
    use std::ops::Bound;
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

    fn setup_graph() -> Result<(TempDir, Arc<Pager>, Arc<Graph>)> {
        let dir = tempdir()?;
        let path = dir.path().join("executor.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
        let store: Arc<dyn crate::primitives::pager::PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store))?;
        Ok((dir, pager, graph))
    }

    fn setup_metadata() -> Arc<dyn MetadataProvider> {
        Arc::new(
            InMemoryMetadata::new()
                .with_label("User", LabelId(1))
                .with_property("age", PropId(1))
                .with_edge_type("FOLLOWS", TypeId(1)),
        )
    }

    fn seed_users(pager: &Arc<Pager>, graph: &Arc<Graph>, ages: &[Option<i64>]) -> Result<()> {
        let mut write = pager.begin_write()?;
        for age in ages {
            match age {
                Some(value) => {
                    let props = [PropEntry::new(PropId(1), PropValue::Int(*value))];
                    graph.create_node(
                        &mut write,
                        NodeSpec {
                            labels: &[LabelId(1)],
                            props: &props,
                        },
                    )?;
                }
                None => {
                    graph.create_node(
                        &mut write,
                        NodeSpec {
                            labels: &[LabelId(1)],
                            props: &[],
                        },
                    )?;
                }
            }
        }
        graph.create_label_index(&mut write, LabelId(1))?;
        pager.commit(write)?;
        Ok(())
    }

    fn node_id_from(value: &Value) -> NodeId {
        match value {
            Value::NodeId(id) => *id,
            Value::Object(obj) => {
                if let Some(Value::NodeId(id)) = obj.get("_id") {
                    *id
                } else {
                    panic!("object missing _id field");
                }
            }
            _ => panic!("unexpected value type"),
        }
    }

    #[test]
    fn executor_scans_label() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();

        // Seed data.
        seed_users(&pager, &graph, &[Some(25), Some(30)])?;

        let ast = QueryBuilder::new().r#match("User").select(["a"]).build()?;
        let planner = Planner::new(PlannerConfig::default(), Arc::clone(&metadata));
        let plan = planner.plan(&ast)?;
        let executor = Executor::new(graph, pager, metadata);
        let result = executor.execute(&plan.plan, None)?;
        assert_eq!(result.rows.len(), 2);
        for row in result.rows {
            let value = row.get("a").expect("projected value");
            let id = node_id_from(value);
            assert!(id.0 > 0);
            if let Value::Object(obj) = value {
                assert!(obj.get("props").is_some());
            }
        }
        Ok(())
    }

    #[test]
    fn executor_hash_join_filters_rows() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();
        seed_users(&pager, &graph, &[Some(20), Some(35)])?;

        let left = PhysicalNode::new(PhysicalOp::LabelScan {
            label: LabelId(1),
            label_name: None,
            as_var: Var("a".into()),
        });

        let right_scan = PhysicalNode::new(PhysicalOp::LabelScan {
            label: LabelId(1),
            label_name: None,
            as_var: Var("b".into()),
        });

        let right_filter = PhysicalNode::with_inputs(
            PhysicalOp::Filter {
                pred: PhysicalPredicate::Range {
                    var: Var("b".into()),
                    prop: PropId(1),
                    prop_name: "age".into(),
                    lower: Bound::Included(LiteralValue::Int(30)),
                    upper: Bound::Unbounded,
                },
                selectivity: 0.3,
            },
            vec![right_scan],
        );

        let join = PhysicalNode::with_inputs(
            PhysicalOp::HashJoin {
                left: Var("a".into()),
                right: Var("b".into()),
            },
            vec![left, right_filter],
        );

        let project = PhysicalNode::with_inputs(
            PhysicalOp::Project {
                fields: vec![
                    ProjectField::Var {
                        var: Var("a".into()),
                        alias: None,
                    },
                    ProjectField::Var {
                        var: Var("b".into()),
                        alias: None,
                    },
                ],
            },
            vec![join],
        );

        let plan = PhysicalPlan::new(project);
        let executor = Executor::new(graph, pager, metadata);
        let rows = executor.execute(&plan, None)?.rows;
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        let a_id = node_id_from(row.get("a").expect("binding for a"));
        let b_id = node_id_from(row.get("b").expect("binding for b"));
        assert_eq!(a_id, b_id);
        Ok(())
    }

    #[test]
    fn executor_stream_materialises_rows() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();
        seed_users(&pager, &graph, &[Some(25), Some(30)])?;

        let ast = QueryBuilder::new().r#match("User").select(["a"]).build()?;
        let planner = Planner::new(PlannerConfig::default(), Arc::clone(&metadata));
        let plan = planner.plan(&ast)?;
        let executor = Executor::new(graph, pager, metadata);
        let stream = executor.stream(&plan.plan, None)?;
        let rows: Vec<Row> = stream.collect::<Result<_>>()?;
        assert_eq!(rows.len(), 2);
        Ok(())
    }

    #[test]
    fn executor_projects_property_values() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();
        seed_users(&pager, &graph, &[Some(42)])?;

        let scan = PhysicalNode::new(PhysicalOp::LabelScan {
            label: LabelId(1),
            label_name: None,
            as_var: Var("a".into()),
        });
        let project = PhysicalNode::with_inputs(
            PhysicalOp::Project {
                fields: vec![ProjectField::Prop {
                    var: Var("a".into()),
                    prop: PropId(1),
                    prop_name: "age".into(),
                    alias: None,
                }],
            },
            vec![scan],
        );
        let plan = PhysicalPlan::new(project);
        let executor = Executor::new(graph, pager, metadata);
        let rows = executor.execute(&plan, None)?.rows;
        assert_eq!(rows.len(), 1);
        let value = rows[0].get("age").expect("projected column");
        assert!(matches!(value, Value::Int(42)));
        Ok(())
    }

    #[test]
    fn union_stream_concatenates_inputs() -> Result<()> {
        let rows_left = vec![
            BindingRow::from_binding("a", NodeId(1)),
            BindingRow::from_binding("a", NodeId(2)),
        ];
        let rows_right = vec![BindingRow::from_binding("a", NodeId(3))];
        let mut stream = UnionStream::new(
            vec![
                Box::new(MockStream::new(rows_left)),
                Box::new(MockStream::new(rows_right)),
            ],
            false,
        );
        let mut seen = Vec::new();
        while let Some(row) = stream.try_next()? {
            seen.push(row.get("a").expect("binding present"));
        }
        assert_eq!(seen, vec![NodeId(1), NodeId(2), NodeId(3)]);
        Ok(())
    }

    #[test]
    fn union_stream_deduplicates_when_enabled() -> Result<()> {
        let rows_left = vec![
            BindingRow::from_binding("a", NodeId(1)),
            BindingRow::from_binding("a", NodeId(2)),
        ];
        let rows_right = vec![
            BindingRow::from_binding("a", NodeId(2)),
            BindingRow::from_binding("a", NodeId(3)),
        ];
        let mut stream = UnionStream::new(
            vec![
                Box::new(MockStream::new(rows_left)),
                Box::new(MockStream::new(rows_right)),
            ],
            true,
        );
        let mut seen = Vec::new();
        while let Some(row) = stream.try_next()? {
            seen.push(row.get("a").expect("binding present"));
        }
        assert_eq!(seen, vec![NodeId(1), NodeId(2), NodeId(3)]);
        Ok(())
    }

    struct MockStream {
        rows: Vec<BindingRow>,
        idx: usize,
    }

    impl MockStream {
        fn new(rows: Vec<BindingRow>) -> Self {
            Self { rows, idx: 0 }
        }
    }

    impl BindingStream for MockStream {
        fn try_next(&mut self) -> Result<Option<BindingRow>> {
            if self.idx >= self.rows.len() {
                return Ok(None);
            }
            let row = self.rows[self.idx].clone();
            self.idx += 1;
            Ok(Some(row))
        }
    }

    fn bool_node(props: Vec<(PropId, PropValueOwned)>) -> NodeData {
        NodeData {
            labels: Vec::<LabelId>::new(),
            props,
        }
    }

    struct TestResolver {
        nodes: HashMap<String, NodeData>,
    }

    impl TestResolver {
        fn new(entries: Vec<(&str, NodeData)>) -> Self {
            let mut nodes = HashMap::new();
            for (var, node) in entries {
                nodes.insert(var.to_owned(), node);
            }
            Self { nodes }
        }
    }

    impl BoolNodeResolver for TestResolver {
        fn resolve(&mut self, var: &Var) -> Result<NodeData> {
            self.nodes
                .get(&var.0)
                .cloned()
                .ok_or(SombraError::Invalid("missing test binding"))
        }
    }

    fn eval_cmp_with_props(cmp: PhysicalComparison, props: Vec<(PropId, PropValueOwned)>) -> bool {
        let expr = PhysicalBoolExpr::Cmp(cmp);
        let mut resolver = TestResolver::new(vec![("a", bool_node(props))]);
        evaluate_bool_expr(&expr, &mut resolver).unwrap()
    }

    #[test]
    fn bool_expr_or_evaluates() {
        let expr = PhysicalBoolExpr::Or(vec![
            PhysicalBoolExpr::Cmp(PhysicalComparison::Eq {
                var: Var("a".into()),
                prop: PropId(1),
                prop_name: "username".into(),
                value: LiteralValue::String("Ada".into()),
            }),
            PhysicalBoolExpr::Cmp(PhysicalComparison::Eq {
                var: Var("a".into()),
                prop: PropId(1),
                prop_name: "username".into(),
                value: LiteralValue::String("Bob".into()),
            }),
        ]);
        let mut resolver = TestResolver::new(vec![(
            "a",
            bool_node(vec![(PropId(1), PropValueOwned::Str("Bob".into()))]),
        )]);
        assert!(evaluate_bool_expr(&expr, &mut resolver).unwrap());
    }

    #[test]
    fn bool_expr_not_handles_in_with_nulls() {
        let values = vec![
            LiteralValue::Int(1),
            LiteralValue::Null,
            LiteralValue::Int(5),
        ];
        let expr = PhysicalBoolExpr::Not(Box::new(PhysicalBoolExpr::Cmp(PhysicalComparison::In {
            var: Var("a".into()),
            prop: PropId(2),
            prop_name: "scores".into(),
            lookup: InLookup::from_literals(&values),
            values,
        })));
        let mut resolver = TestResolver::new(vec![(
            "a",
            bool_node(vec![(PropId(2), PropValueOwned::Int(10))]),
        )]);
        assert!(evaluate_bool_expr(&expr, &mut resolver).unwrap());
    }

    #[test]
    fn eval_in_hash_lookup_matches_values() -> Result<()> {
        let values: Vec<LiteralValue> = (0..16).map(LiteralValue::Int).collect();
        let lookup = InLookup::from_literals(&values);
        assert!(matches!(lookup, InLookup::Hash(_)));

        let present = bool_node(vec![(PropId(30), PropValueOwned::Int(11))]);
        assert!(eval_in(&present, PropId(30), &values, &lookup)?);

        let absent = bool_node(vec![(PropId(30), PropValueOwned::Int(42))]);
        assert!(!eval_in(&absent, PropId(30), &values, &lookup)?);
        Ok(())
    }

    #[test]
    fn bool_expr_eq_null_matches_missing_property() {
        let expr = PhysicalBoolExpr::Cmp(PhysicalComparison::Eq {
            var: Var("a".into()),
            prop: PropId(3),
            prop_name: "optional".into(),
            value: LiteralValue::Null,
        });
        let mut resolver = TestResolver::new(vec![(
            "a",
            bool_node(vec![(PropId(4), PropValueOwned::Int(1))]),
        )]);
        assert!(evaluate_bool_expr(&expr, &mut resolver).unwrap());
    }

    #[test]
    fn bool_expr_exists_treats_null_as_present() {
        let cmp = PhysicalComparison::Exists {
            var: Var("a".into()),
            prop: PropId(10),
            prop_name: "flag".into(),
        };
        assert!(eval_cmp_with_props(
            cmp.clone(),
            vec![(PropId(10), PropValueOwned::Null)]
        ));
        assert!(!eval_cmp_with_props(cmp, vec![]));
    }

    #[test]
    fn bool_expr_is_null_handles_missing_and_null_values() {
        let cmp = PhysicalComparison::IsNull {
            var: Var("a".into()),
            prop: PropId(11),
            prop_name: "maybe".into(),
        };
        assert!(eval_cmp_with_props(cmp.clone(), vec![]));
        assert!(eval_cmp_with_props(
            cmp.clone(),
            vec![(PropId(11), PropValueOwned::Null)]
        ));
        assert!(!eval_cmp_with_props(
            cmp,
            vec![(PropId(11), PropValueOwned::Int(5))]
        ));
    }

    #[test]
    fn bool_expr_is_not_null_requires_value() {
        let cmp = PhysicalComparison::IsNotNull {
            var: Var("a".into()),
            prop: PropId(12),
            prop_name: "maybe".into(),
        };
        assert!(eval_cmp_with_props(
            cmp.clone(),
            vec![(PropId(12), PropValueOwned::Int(5))]
        ));
        assert!(!eval_cmp_with_props(
            cmp.clone(),
            vec![(PropId(12), PropValueOwned::Null)]
        ));
        assert!(!eval_cmp_with_props(cmp, vec![]));
    }

    #[test]
    fn bool_expr_eq_null_rejects_present_non_null_values() {
        let cmp = PhysicalComparison::Eq {
            var: Var("a".into()),
            prop: PropId(13),
            prop_name: "maybe".into(),
            value: LiteralValue::Null,
        };
        assert!(!eval_cmp_with_props(
            cmp,
            vec![(PropId(13), PropValueOwned::Int(7))]
        ));
    }

    #[test]
    fn bool_expr_ne_null_only_matches_present_values() {
        let cmp = PhysicalComparison::Ne {
            var: Var("a".into()),
            prop: PropId(14),
            prop_name: "maybe".into(),
            value: LiteralValue::Null,
        };
        assert!(eval_cmp_with_props(
            cmp.clone(),
            vec![(PropId(14), PropValueOwned::Int(1))]
        ));
        assert!(!eval_cmp_with_props(
            cmp.clone(),
            vec![(PropId(14), PropValueOwned::Null)]
        ));
        assert!(!eval_cmp_with_props(cmp, vec![]));
    }

    #[test]
    fn bool_expr_lt_with_null_literal_is_false() {
        let cmp = PhysicalComparison::Lt {
            var: Var("a".into()),
            prop: PropId(15),
            prop_name: "score".into(),
            value: LiteralValue::Null,
        };
        assert!(!eval_cmp_with_props(
            cmp,
            vec![(PropId(15), PropValueOwned::Int(10))]
        ));
    }

    #[test]
    fn bool_expr_between_rejects_null_bounds_and_values() {
        let cmp = PhysicalComparison::Between {
            var: Var("a".into()),
            prop: PropId(16),
            prop_name: "range".into(),
            low: Bound::Included(LiteralValue::Null),
            high: Bound::Excluded(LiteralValue::Int(5)),
        };
        assert!(!eval_cmp_with_props(
            cmp.clone(),
            vec![(PropId(16), PropValueOwned::Int(1))]
        ));

        let cmp_value_null = PhysicalComparison::Between {
            var: Var("a".into()),
            prop: PropId(16),
            prop_name: "range".into(),
            low: Bound::Unbounded,
            high: Bound::Excluded(LiteralValue::Int(5)),
        };
        assert!(!eval_cmp_with_props(
            cmp_value_null,
            vec![(PropId(16), PropValueOwned::Null)]
        ));
        assert!(!eval_cmp_with_props(cmp, vec![]));
    }
}
