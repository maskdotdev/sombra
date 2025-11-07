//! Query executor scaffolding.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use sombra_index::{collect_all, PostingStream};
use sombra_pager::{PageStore, Pager, ReadGuard};
use sombra_storage::{
    Dir as StorageDir, ExpandOpts, Graph, NeighborCursor, NodeData, PropValueOwned,
};
use sombra_types::{LabelId, NodeId, PropId, Result, SombraError, TypeId};

use crate::ast::Var;
use crate::metadata::MetadataProvider;
use crate::physical::{
    LiteralValue, PhysicalNode, PhysicalOp, PhysicalPlan, ProjectField,
    PropPredicate as PhysicalPredicate,
};
use crate::profile::{
    profile_timer as query_profile_timer, record_profile_timer as record_query_profile_timer,
    QueryProfileKind,
};

/// Materialised result returned by `execute`.
#[derive(Debug, Default)]
pub struct QueryResult {
    pub rows: Vec<Row>,
}

/// Single output row represented as a mapping from alias to value.
pub type Row = BTreeMap<String, Value>;

/// Runtime value flowing through the executor.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    NodeId(NodeId),
}

type NodeCache = Arc<Mutex<HashMap<NodeId, NodeData>>>;

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
    Project(Vec<ProjectField>),
}

impl RowMapper {
    fn map(&self, binding: &BindingRow) -> Result<Row> {
        match self {
            RowMapper::All => project_all(binding),
            RowMapper::Project(fields) => apply_projection(binding, fields),
        }
    }
}

/// Streaming handle over query rows.
pub struct ResultStream {
    bindings: BoxBindingStream,
    mapper: RowMapper,
    _context: Arc<ReadContext>,
}

impl ResultStream {
    fn new(bindings: BoxBindingStream, mapper: RowMapper, context: Arc<ReadContext>) -> Self {
        Self {
            bindings,
            mapper,
            _context: context,
        }
    }
}

impl Iterator for ResultStream {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.bindings.try_next() {
            Ok(Some(binding)) => Some(self.mapper.map(&binding)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

pub struct Executor {
    graph: Arc<Graph>,
    pager: Arc<Pager>,
    metadata: Arc<dyn MetadataProvider>,
}

impl Executor {
    pub fn new(graph: Arc<Graph>, pager: Arc<Pager>, metadata: Arc<dyn MetadataProvider>) -> Self {
        Self {
            graph,
            pager,
            metadata,
        }
    }

    pub fn execute(&self, plan: &PhysicalPlan) -> Result<QueryResult> {
        let mut stream = self.stream(plan)?;
        let iter_timer = query_profile_timer();
        let rows: Vec<Row> = stream.by_ref().collect::<Result<_>>()?;
        record_query_profile_timer(QueryProfileKind::StreamIter, iter_timer);
        Ok(QueryResult { rows })
    }

    pub fn stream(&self, plan: &PhysicalPlan) -> Result<ResultStream> {
        let guard_timer = query_profile_timer();
        let context = Arc::new(ReadContext::new(self.pager.begin_read()?));
        record_query_profile_timer(QueryProfileKind::ReadGuard, guard_timer);
        let cache: NodeCache = Arc::new(Mutex::new(HashMap::new()));

        let (mapper, root) = match &plan.root.op {
            PhysicalOp::Project { fields } => {
                if plan.root.inputs.len() != 1 {
                    return Err(SombraError::Invalid("project expects single input"));
                }
                (RowMapper::Project(fields.clone()), &plan.root.inputs[0])
            }
            _ => (RowMapper::All, &plan.root),
        };

        let build_timer = query_profile_timer();
        let bindings = self.build_stream(root, Arc::clone(&context), cache)?;
        record_query_profile_timer(QueryProfileKind::StreamBuild, build_timer);
        Ok(ResultStream::new(bindings, mapper, context))
    }

    fn build_stream(
        &self,
        node: &PhysicalNode,
        context: Arc<ReadContext>,
        cache: NodeCache,
    ) -> Result<BoxBindingStream> {
        match &node.op {
            PhysicalOp::LabelScan { label, as_var } => {
                let scan = self
                    .graph
                    .label_scan(context.guard(), *label)?
                    .ok_or(SombraError::Invalid("label index not found"))?;
                Ok(Box::new(PostingBindingStream::from_stream(
                    as_var.0.clone(),
                    Box::new(scan),
                )?))
            }
            PhysicalOp::PropIndexScan {
                label,
                prop,
                pred,
                as_var,
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
            PhysicalOp::Filter { pred } => {
                if node.inputs.len() != 1 {
                    return Err(SombraError::Invalid("filter expects single input child"));
                }
                let input =
                    self.build_stream(&node.inputs[0], Arc::clone(&context), cache.clone())?;
                let filter = match pred {
                    PhysicalPredicate::Eq { .. } | PhysicalPredicate::Range { .. } => {
                        FilterEval::Physical(pred.clone())
                    }
                    PhysicalPredicate::Custom { expr } => {
                        FilterEval::Custom(self.parse_custom_predicate(expr)?)
                    }
                };
                Ok(Box::new(FilterStream::new(
                    input,
                    self.graph.clone(),
                    Arc::clone(&context),
                    cache,
                    filter,
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
            PhysicalPredicate::Custom { .. } => Err(SombraError::Invalid(
                "custom property predicate not supported by index scan",
            )),
        }
    }

    fn parse_custom_predicate(&self, expr: &str) -> Result<ParsedCustomPredicate> {
        parse_custom_predicate(self.metadata.as_ref(), expr)
    }
}

enum FilterEval {
    Physical(PhysicalPredicate),
    Custom(ParsedCustomPredicate),
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
                    let mut row = self
                        .current_row
                        .as_ref()
                        .expect("current row set when neighbors available")
                        .clone();
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
            let node_id = match &self.eval {
                FilterEval::Physical(PhysicalPredicate::Eq { var, .. })
                | FilterEval::Physical(PhysicalPredicate::Range { var, .. }) => row
                    .get(&var.0)
                    .ok_or(SombraError::Invalid("filter variable missing from binding"))?,
                FilterEval::Physical(PhysicalPredicate::Custom { .. }) => {
                    return Err(SombraError::Invalid(
                        "custom predicate stored in physical filter",
                    ))
                }
                FilterEval::Custom(parsed) => row.get(&parsed.var).ok_or(SombraError::Invalid(
                    "custom predicate variable missing from binding",
                ))?,
            };
            let node_data = fetch_node_data(&self.graph, &self.context, &self.cache, node_id)?;
            let matches = match &self.eval {
                FilterEval::Physical(pred) => match pred {
                    PhysicalPredicate::Eq { .. } | PhysicalPredicate::Range { .. } => {
                        evaluate_predicate(pred, &node_data)?
                    }
                    PhysicalPredicate::Custom { .. } => unreachable!(),
                },
                FilterEval::Custom(parsed) => evaluate_custom_predicate(parsed, &node_data)?,
            };
            if matches {
                return Ok(Some(row));
            }
        }
    }
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

fn storage_dir(dir: crate::physical::Dir) -> StorageDir {
    match dir {
        crate::physical::Dir::Out => StorageDir::Out,
        crate::physical::Dir::In => StorageDir::In,
        crate::physical::Dir::Both => StorageDir::Both,
    }
}

fn fetch_node_data(
    graph: &Arc<Graph>,
    context: &Arc<ReadContext>,
    cache: &NodeCache,
    id: NodeId,
) -> Result<NodeData> {
    if let Some(existing) = cache.lock().unwrap().get(&id).cloned() {
        return Ok(existing);
    }
    let data = graph
        .get_node(context.guard(), id)?
        .ok_or(SombraError::Invalid("node missing during evaluation"))?;
    cache.lock().unwrap().insert(id, data.clone());
    Ok(data)
}

fn parse_custom_predicate(
    metadata: &dyn MetadataProvider,
    expr: &str,
) -> Result<ParsedCustomPredicate> {
    let tokens = tokenize_expression(expr);
    if tokens.len() < 2 {
        return Err(SombraError::Invalid(
            "custom predicate requires an operator",
        ));
    }

    let (var, prop_name) = split_var_prop(&tokens[0])?;
    let prop = metadata.resolve_property(&prop_name)?;

    if tokens[1].eq_ignore_ascii_case("IS") {
        if tokens.len() == 3 && tokens[2].eq_ignore_ascii_case("NULL") {
            return Ok(ParsedCustomPredicate {
                var,
                prop,
                op: CustomOp::IsNull,
                literal: None,
            });
        }
        if tokens.len() == 4
            && tokens[2].eq_ignore_ascii_case("NOT")
            && tokens[3].eq_ignore_ascii_case("NULL")
        {
            return Ok(ParsedCustomPredicate {
                var,
                prop,
                op: CustomOp::IsNotNull,
                literal: None,
            });
        }
        return Err(SombraError::Invalid(
            "custom predicate expected NULL after IS",
        ));
    }

    if tokens.len() < 3 {
        return Err(SombraError::Invalid(
            "custom predicate missing comparison literal",
        ));
    }

    let op = match tokens[1].as_str() {
        "=" | "==" => CustomOp::Eq,
        "!=" | "<>" => CustomOp::Ne,
        ">" => CustomOp::Gt,
        ">=" => CustomOp::Ge,
        "<" => CustomOp::Lt,
        "<=" => CustomOp::Le,
        _ => {
            return Err(SombraError::Invalid(
                "unsupported custom predicate operator",
            ))
        }
    };

    let literal_raw = tokens[2..].join(" ");
    let literal = parse_literal_value(&literal_raw)?;
    Ok(ParsedCustomPredicate {
        var,
        prop,
        op,
        literal: Some(literal),
    })
}

fn evaluate_custom_predicate(parsed: &ParsedCustomPredicate, node: &NodeData) -> Result<bool> {
    let prop_value = find_prop(node, parsed.prop);
    match parsed.op {
        CustomOp::IsNull => Ok(prop_value
            .map(|value| matches!(value, PropValueOwned::Null))
            .unwrap_or(true)),
        CustomOp::IsNotNull => Ok(prop_value
            .map(|value| !matches!(value, PropValueOwned::Null))
            .unwrap_or(false)),
        CustomOp::Eq | CustomOp::Ne | CustomOp::Lt | CustomOp::Le | CustomOp::Gt | CustomOp::Ge => {
            let Some(actual) = prop_value else {
                return Ok(false);
            };
            let literal = parsed
                .literal
                .as_ref()
                .ok_or_else(|| SombraError::Invalid("custom predicate missing literal"))?;
            let ord = compare_values(actual, literal)?;
            let matches = match parsed.op {
                CustomOp::Eq => ord.is_eq(),
                CustomOp::Ne => !ord.is_eq(),
                CustomOp::Lt => ord.is_lt(),
                CustomOp::Le => ord.is_le(),
                CustomOp::Gt => ord.is_gt(),
                CustomOp::Ge => ord.is_ge(),
                CustomOp::IsNull | CustomOp::IsNotNull => unreachable!(),
            };
            Ok(matches)
        }
    }
}

fn apply_projection(binding: &BindingRow, fields: &[ProjectField]) -> Result<Row> {
    let mut row = Row::new();
    for field in fields {
        match field {
            ProjectField::Var { var, alias } => {
                let Some(node) = binding.get(&var.0) else {
                    return Err(SombraError::Invalid("projection variable missing"));
                };
                let key = alias.clone().unwrap_or_else(|| var.0.clone());
                row.insert(key, Value::NodeId(node));
            }
            ProjectField::Expr { .. } => {
                return Err(SombraError::Invalid(
                    "expression projection not implemented",
                ));
            }
        }
    }
    Ok(row)
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
            let Some(actual) = find_prop(node, *prop) else {
                return Ok(false);
            };
            Ok(compare_values(actual, value)?.is_eq())
        }
        PhysicalPredicate::Range {
            prop, lower, upper, ..
        } => {
            let Some(actual) = find_prop(node, *prop) else {
                return Ok(false);
            };
            let lower_cmp = match lower {
                Bound::Unbounded => true,
                Bound::Included(lit) => compare_values(actual, lit)?.is_ge(),
                Bound::Excluded(lit) => compare_values(actual, lit)?.is_gt(),
            };
            let upper_cmp = match upper {
                Bound::Unbounded => true,
                Bound::Included(lit) => compare_values(actual, lit)?.is_le(),
                Bound::Excluded(lit) => compare_values(actual, lit)?.is_lt(),
            };
            Ok(lower_cmp && upper_cmp)
        }
        PhysicalPredicate::Custom { .. } => {
            Err(SombraError::Invalid("custom predicates not supported yet"))
        }
    }
}

fn find_prop<'a>(node: &'a NodeData, prop: PropId) -> Option<&'a PropValueOwned> {
    node.props
        .iter()
        .find_map(|(id, value)| if *id == prop { Some(value) } else { None })
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

#[derive(Clone, Debug)]
struct ParsedCustomPredicate {
    var: String,
    prop: PropId,
    op: CustomOp,
    literal: Option<LiteralValue>,
}

#[derive(Clone, Copy, Debug)]
enum CustomOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
}

fn tokenize_expression(expr: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut buf = String::new();
    let mut quote: Option<char> = None;
    let mut chars = expr.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(q) = quote {
            buf.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.next() {
                    buf.push(next);
                }
                continue;
            }
            if ch == q {
                quote = None;
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                if !buf.is_empty() {
                    tokens.push(std::mem::take(&mut buf));
                }
                quote = Some(ch);
                buf.push(ch);
            }
            c if c.is_whitespace() => {
                if !buf.is_empty() {
                    tokens.push(std::mem::take(&mut buf));
                }
            }
            '=' | '!' | '<' | '>' => {
                if !buf.is_empty() {
                    tokens.push(std::mem::take(&mut buf));
                }
                let mut op = String::new();
                op.push(ch);
                if let Some(next) = chars.peek() {
                    if (*next == '=')
                        || (ch == '<' && *next == '>')
                        || (ch == '>' && *next == '=')
                        || (ch == '!' && *next == '=')
                    {
                        op.push(*next);
                        chars.next();
                    }
                }
                tokens.push(op);
            }
            _ => buf.push(ch),
        }
    }

    if !buf.is_empty() {
        tokens.push(buf);
    }

    tokens
}

fn split_var_prop(token: &str) -> Result<(String, String)> {
    let trimmed = token.trim();
    let Some((var, prop)) = trimmed.split_once('.') else {
        return Err(SombraError::Invalid(
            "custom predicate must reference a var.prop pair",
        ));
    };
    if var.is_empty() || prop.is_empty() {
        return Err(SombraError::Invalid(
            "custom predicate requires non-empty var and property",
        ));
    }
    Ok((var.to_owned(), prop.to_owned()))
}

fn parse_literal_value(raw: &str) -> Result<LiteralValue> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(SombraError::Invalid(
            "custom predicate literal cannot be empty",
        ));
    }

    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        let inner = &trimmed[1..trimmed.len() - 1];
        let unescaped = unescape_quoted(inner)?;
        return Ok(LiteralValue::String(unescaped));
    }

    match trimmed.to_ascii_lowercase().as_str() {
        "null" => Ok(LiteralValue::Null),
        "true" => Ok(LiteralValue::Bool(true)),
        "false" => Ok(LiteralValue::Bool(false)),
        _ => {
            if let Ok(int) = trimmed.parse::<i64>() {
                return Ok(LiteralValue::Int(int));
            }
            if let Ok(float) = trimmed.parse::<f64>() {
                return Ok(LiteralValue::Float(float));
            }
            Err(SombraError::Invalid(
                "unable to parse custom predicate literal",
            ))
        }
    }
}

fn unescape_quoted(input: &str) -> Result<String> {
    let mut result = String::new();
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let Some(next) = chars.next() else {
                return Err(SombraError::Invalid("incomplete escape in literal"));
            };
            match next {
                '\\' => result.push('\\'),
                'n' => result.push('\n'),
                'r' => result.push('\r'),
                't' => result.push('\t'),
                '\'' => result.push('\''),
                '"' => result.push('"'),
                other => result.push(other),
            }
        } else {
            result.push(ch);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Var;
    use crate::builder::QueryBuilder;
    use crate::metadata::InMemoryMetadata;
    use crate::metadata::MetadataProvider;
    use crate::physical::{
        LiteralValue, PhysicalNode, PhysicalOp, PhysicalPlan, ProjectField,
        PropPredicate as PhysicalPredicate,
    };
    use crate::planner::{Planner, PlannerConfig};
    use sombra_pager::{Pager, PagerOptions};
    use sombra_storage::{GraphOptions, NodeSpec, PropEntry, PropValue};
    use sombra_types::{LabelId, PropId, TypeId};
    use std::ops::Bound;
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

    fn setup_graph() -> Result<(TempDir, Arc<Pager>, Arc<Graph>)> {
        let dir = tempdir()?;
        let path = dir.path().join("executor.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
        let store: Arc<dyn sombra_pager::PageStore> = pager.clone();
        let graph = Arc::new(Graph::open(GraphOptions::new(store))?);
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

    #[test]
    fn executor_scans_label() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();

        // Seed data.
        seed_users(&pager, &graph, &[Some(25), Some(30)])?;

        let ast = QueryBuilder::new().r#match("User").select(["a"]).build();
        let planner = Planner::new(PlannerConfig::default(), Arc::clone(&metadata));
        let plan = planner.plan(&ast)?;
        let executor = Executor::new(graph, pager, metadata);
        let result = executor.execute(&plan.plan)?;
        assert_eq!(result.rows.len(), 2);
        for row in result.rows {
            let value = row.get("a").expect("projected value");
            match value {
                Value::NodeId(id) => assert!(id.0 > 0),
                _ => panic!("unexpected value type"),
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
            as_var: Var("a".into()),
        });

        let right_scan = PhysicalNode::new(PhysicalOp::LabelScan {
            label: LabelId(1),
            as_var: Var("b".into()),
        });

        let right_filter = PhysicalNode::with_inputs(
            PhysicalOp::Filter {
                pred: PhysicalPredicate::Range {
                    var: Var("b".into()),
                    prop: PropId(1),
                    lower: Bound::Included(LiteralValue::Int(30)),
                    upper: Bound::Unbounded,
                },
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
        let rows = executor.execute(&plan)?.rows;
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        let Value::NodeId(a_id) = row.get("a").expect("binding for a") else {
            panic!("expected node id for a");
        };
        let Value::NodeId(b_id) = row.get("b").expect("binding for b") else {
            panic!("expected node id for b");
        };
        assert_eq!(a_id, b_id);
        Ok(())
    }

    #[test]
    fn executor_custom_predicate_filters() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();
        seed_users(&pager, &graph, &[Some(18), Some(42)])?;

        let scan = PhysicalNode::new(PhysicalOp::LabelScan {
            label: LabelId(1),
            as_var: Var("a".into()),
        });

        let filter = PhysicalNode::with_inputs(
            PhysicalOp::Filter {
                pred: PhysicalPredicate::Custom {
                    expr: "a.age >= 21".into(),
                },
            },
            vec![scan],
        );

        let project = PhysicalNode::with_inputs(
            PhysicalOp::Project {
                fields: vec![ProjectField::Var {
                    var: Var("a".into()),
                    alias: None,
                }],
            },
            vec![filter],
        );

        let plan = PhysicalPlan::new(project);
        let executor = Executor::new(graph, pager, metadata);
        let rows = executor.execute(&plan)?.rows;
        assert_eq!(rows.len(), 1);
        let Value::NodeId(node_id) = rows[0].get("a").expect("binding for a") else {
            panic!("expected node id");
        };
        assert!(node_id.0 > 0);
        Ok(())
    }

    #[test]
    fn executor_custom_predicate_supports_is_null() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();
        seed_users(&pager, &graph, &[None, Some(10)])?;

        let scan = PhysicalNode::new(PhysicalOp::LabelScan {
            label: LabelId(1),
            as_var: Var("a".into()),
        });

        let filter = PhysicalNode::with_inputs(
            PhysicalOp::Filter {
                pred: PhysicalPredicate::Custom {
                    expr: "a.age IS NULL".into(),
                },
            },
            vec![scan],
        );

        let project = PhysicalNode::with_inputs(
            PhysicalOp::Project {
                fields: vec![ProjectField::Var {
                    var: Var("a".into()),
                    alias: None,
                }],
            },
            vec![filter],
        );

        let plan = PhysicalPlan::new(project);
        let executor = Executor::new(graph, pager, metadata);
        let rows = executor.execute(&plan)?.rows;
        assert_eq!(rows.len(), 1);
        Ok(())
    }

    #[test]
    fn executor_stream_materialises_rows() -> Result<()> {
        let (_tmpdir, pager, graph) = setup_graph()?;
        let metadata = setup_metadata();
        seed_users(&pager, &graph, &[Some(25), Some(30)])?;

        let ast = QueryBuilder::new().r#match("User").select(["a"]).build();
        let planner = Planner::new(PlannerConfig::default(), Arc::clone(&metadata));
        let plan = planner.plan(&ast)?;
        let executor = Executor::new(graph, pager, metadata);
        let stream = executor.stream(&plan.plan)?;
        let rows: Vec<Row> = stream.collect::<Result<_>>()?;
        assert_eq!(rows.len(), 2);
        Ok(())
    }
}
