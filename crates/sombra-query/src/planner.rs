//! Rule-based planner scaffolding.

use crate::{
    ast::{EdgeDirection, MatchClause, Projection, PropPredicate as AstPredicate, QueryAst, Var},
    logical::{LogicalOp, LogicalPlan, PlanNode},
    metadata::MetadataProvider,
    physical::{
        Dir, LiteralValue, PhysicalNode, PhysicalOp, PhysicalPlan, ProjectField,
        PropPredicate as PhysicalPredicate,
    },
};
use sombra_index::IndexDef;
use sombra_types::{LabelId, PropId, Result, SombraError, TypeId};
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

/// Planner inputs that influence rule selection.
#[derive(Clone, Debug, Default)]
pub struct PlannerConfig {
    pub enable_hash_join: bool,
}

/// Planner output containing the chosen physical plan and explain tree.
#[derive(Clone, Debug)]
pub struct PlannerOutput {
    pub plan: PhysicalPlan,
    pub explain: PlanExplain,
}

/// Human-readable explain tree.
#[derive(Clone, Debug)]
pub struct PlanExplain {
    pub root: ExplainNode,
}

/// Explain node representing an operator with optional metadata.
#[derive(Clone, Debug)]
pub struct ExplainNode {
    pub op: String,
    pub props: Vec<(String, String)>,
    pub inputs: Vec<ExplainNode>,
}

impl ExplainNode {
    pub fn new(op: impl Into<String>) -> Self {
        Self {
            op: op.into(),
            props: Vec::new(),
            inputs: Vec::new(),
        }
    }
}

/// Planner facade.
pub struct Planner {
    metadata: Arc<dyn MetadataProvider>,
    _config: PlannerConfig,
}

impl Planner {
    pub fn new(config: PlannerConfig, metadata: Arc<dyn MetadataProvider>) -> Self {
        Self {
            metadata,
            _config: config,
        }
    }

    /// Converts an AST into a physical plan.
    pub fn plan(&self, ast: &QueryAst) -> Result<PlannerOutput> {
        let mut ctx = PlanContext::new(self.metadata.as_ref());
        let logical = self.build_logical_plan(ast, &mut ctx)?;
        let physical = self.lower_to_physical(&logical, &mut ctx)?;
        let explain = PlanExplain {
            root: build_explain_tree(&physical.root),
        };
        Ok(PlannerOutput {
            plan: physical,
            explain,
        })
    }

    fn build_logical_plan(&self, ast: &QueryAst, ctx: &mut PlanContext<'_>) -> Result<LogicalPlan> {
        if ast.matches.is_empty() {
            return Err(SombraError::Invalid(
                "query must include at least one match",
            ));
        }

        if ast.matches.iter().any(|m| m.label.is_none()) {
            return Err(SombraError::Invalid(
                "match clause requires a label in the current planner",
            ));
        }

        let mut preds_by_var: HashMap<Var, Vec<AstPredicate>> = HashMap::new();
        let mut custom_preds = Vec::new();
        for pred in &ast.predicates {
            match pred {
                AstPredicate::Eq { var, .. } | AstPredicate::Range { var, .. } => {
                    preds_by_var
                        .entry(var.clone())
                        .or_default()
                        .push(pred.clone());
                }
                AstPredicate::Custom { .. } => custom_preds.push(pred.clone()),
            }
        }

        let labels_by_var = self.resolve_label_ids(&ast.matches, ctx)?;
        let anchor_idx = self.select_anchor(&ast.matches, &labels_by_var, &preds_by_var, ctx)?;
        let anchor_match = &ast.matches[anchor_idx];
        let anchor_label = *labels_by_var
            .get(&anchor_match.var)
            .expect("missing label id for anchor");
        let indexed_preds =
            self.take_indexed_predicates(anchor_label, &anchor_match.var, &mut preds_by_var, ctx)?;
        let mut current = match indexed_preds.len() {
            0 => PlanNode::new(LogicalOp::LabelScan {
                label: anchor_match.label.clone(),
                as_var: anchor_match.var.clone(),
            }),
            1 => {
                let (predicate, prop) = indexed_preds.into_iter().next().unwrap();
                PlanNode::new(LogicalOp::PropIndexScan {
                    label: anchor_match.label.clone(),
                    prop,
                    predicate,
                    as_var: anchor_match.var.clone(),
                })
            }
            _ => {
                let children = indexed_preds
                    .into_iter()
                    .map(|(predicate, prop)| {
                        PlanNode::new(LogicalOp::PropIndexScan {
                            label: anchor_match.label.clone(),
                            prop,
                            predicate,
                            as_var: anchor_match.var.clone(),
                        })
                    })
                    .collect();
                PlanNode::with_inputs(
                    LogicalOp::Intersect {
                        vars: vec![anchor_match.var.clone()],
                    },
                    children,
                )
            }
        };

        current = self.apply_var_predicates(current, &anchor_match.var, &mut preds_by_var);

        let mut bound_vars: HashSet<Var> = HashSet::new();
        bound_vars.insert(anchor_match.var.clone());
        let mut remaining_edges = ast.edges.clone();

        while bound_vars.len() < ast.matches.len() {
            let Some((edge_idx, reverse)) =
                remaining_edges.iter().enumerate().find_map(|(idx, edge)| {
                    let from_bound = bound_vars.contains(&edge.from);
                    let to_bound = bound_vars.contains(&edge.to);
                    match (from_bound, to_bound) {
                        (true, false) => Some((idx, false)),
                        (false, true) => Some((idx, true)),
                        _ => None,
                    }
                })
            else {
                return Err(SombraError::Invalid(
                    "query pattern is disconnected; cannot plan edges",
                ));
            };

            let edge = remaining_edges.remove(edge_idx);
            let (expand_from, expand_to, direction) = if !reverse {
                (edge.from.clone(), edge.to.clone(), edge.direction)
            } else {
                (
                    edge.to.clone(),
                    edge.from.clone(),
                    invert_direction(edge.direction),
                )
            };

            current = PlanNode::with_inputs(
                LogicalOp::Expand {
                    from: expand_from.clone(),
                    to: expand_to.clone(),
                    direction,
                    edge_type: edge.edge_type.clone(),
                    distinct_nodes: false,
                },
                vec![current],
            );
            current = self.apply_var_predicates(current, &expand_to, &mut preds_by_var);
            bound_vars.insert(expand_to);
        }

        for pred in custom_preds {
            current = PlanNode::with_inputs(LogicalOp::Filter { predicate: pred }, vec![current]);
        }

        if ast.distinct {
            current = PlanNode::with_inputs(LogicalOp::Distinct, vec![current]);
        }

        if !ast.projections.is_empty() {
            current = PlanNode::with_inputs(
                LogicalOp::Project {
                    fields: ast.projections.clone(),
                },
                vec![current],
            );
        }

        Ok(LogicalPlan::new(current))
    }

    fn apply_var_predicates(
        &self,
        mut node: PlanNode,
        var: &Var,
        preds_by_var: &mut HashMap<Var, Vec<AstPredicate>>,
    ) -> PlanNode {
        if let Some(preds) = preds_by_var.remove(var) {
            for pred in preds {
                node = PlanNode::with_inputs(LogicalOp::Filter { predicate: pred }, vec![node]);
            }
        }
        node
    }

    fn resolve_label_ids(
        &self,
        matches: &[MatchClause],
        ctx: &mut PlanContext<'_>,
    ) -> Result<HashMap<Var, LabelId>> {
        let mut map = HashMap::new();
        for m in matches {
            let id = ctx.label(&m.label)?;
            map.insert(m.var.clone(), id);
        }
        Ok(map)
    }

    fn select_anchor(
        &self,
        matches: &[MatchClause],
        labels_by_var: &HashMap<Var, LabelId>,
        preds_by_var: &HashMap<Var, Vec<AstPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<usize> {
        let mut best_idx = 0;
        let mut best_score = AnchorScore::Label;
        for (idx, m) in matches.iter().enumerate() {
            let label = labels_by_var
                .get(&m.var)
                .copied()
                .expect("label id missing for match variable");
            let score = self.anchor_score(&m.var, label, preds_by_var, ctx)?;
            if score > best_score {
                best_score = score;
                best_idx = idx;
            }
        }
        Ok(best_idx)
    }

    fn anchor_score(
        &self,
        var: &Var,
        label: LabelId,
        preds_by_var: &HashMap<Var, Vec<AstPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<AnchorScore> {
        let Some(preds) = preds_by_var.get(var) else {
            return Ok(AnchorScore::Label);
        };
        let mut best = AnchorScore::Label;
        for pred in preds {
            let (prop_name, score_candidate) = match pred {
                AstPredicate::Eq { prop, .. } => (prop.as_str(), AnchorScore::Eq),
                AstPredicate::Range { prop, .. } => (prop.as_str(), AnchorScore::Range),
                AstPredicate::Custom { .. } => continue,
            };
            let prop_id = ctx.property(prop_name)?;
            if ctx.property_index(label, prop_id)?.is_some() {
                if score_candidate == AnchorScore::Eq {
                    return Ok(AnchorScore::Eq);
                }
                best = AnchorScore::Range;
            }
        }
        Ok(best)
    }

    fn take_indexed_predicates(
        &self,
        label_id: LabelId,
        var: &Var,
        preds_by_var: &mut HashMap<Var, Vec<AstPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<Vec<(AstPredicate, String)>> {
        let Some(preds) = preds_by_var.get_mut(var) else {
            return Ok(Vec::new());
        };

        let mut indexed_eq = Vec::new();
        let mut indexed_range = Vec::new();
        let mut remaining = Vec::new();

        for predicate in preds.drain(..) {
            let (prop_name, class) = match &predicate {
                AstPredicate::Eq { prop, .. } => (prop.as_str(), AnchorScore::Eq),
                AstPredicate::Range { prop, .. } => (prop.as_str(), AnchorScore::Range),
                AstPredicate::Custom { .. } => {
                    remaining.push(predicate);
                    continue;
                }
            };

            let prop_id = ctx.property(prop_name)?;
            if ctx.property_index(label_id, prop_id)?.is_some() {
                let prop = prop_name.to_owned();
                match class {
                    AnchorScore::Eq => indexed_eq.push((predicate, prop)),
                    AnchorScore::Range => indexed_range.push((predicate, prop)),
                    AnchorScore::Label => unreachable!("label score never assigned here"),
                }
            } else {
                remaining.push(predicate);
            }
        }

        if remaining.is_empty() {
            preds_by_var.remove(var);
        } else {
            *preds = remaining;
        }

        let mut indexed = indexed_eq;
        indexed.extend(indexed_range);
        Ok(indexed)
    }

    fn lower_to_physical(
        &self,
        logical: &LogicalPlan,
        ctx: &mut PlanContext<'_>,
    ) -> Result<PhysicalPlan> {
        let root = self.lower_node(&logical.root, ctx)?;
        Ok(PhysicalPlan::new(root))
    }

    fn lower_node(&self, node: &PlanNode, ctx: &mut PlanContext<'_>) -> Result<PhysicalNode> {
        let inputs = node
            .inputs
            .iter()
            .map(|child| self.lower_node(child, ctx))
            .collect::<Result<Vec<_>>>()?;

        let op = match &node.op {
            LogicalOp::LabelScan { label, as_var } => PhysicalOp::LabelScan {
                label: ctx.label(label)?,
                as_var: as_var.clone(),
            },
            LogicalOp::PropIndexScan {
                label,
                predicate,
                as_var,
                ..
            } => {
                let pred = self.convert_prop_predicate(predicate, ctx)?;
                let prop = prop_from_predicate(&pred).ok_or(SombraError::Invalid(
                    "property index scans require concrete predicates",
                ))?;
                PhysicalOp::PropIndexScan {
                    label: ctx.label(label)?,
                    prop,
                    pred,
                    as_var: as_var.clone(),
                }
            }
            LogicalOp::Expand {
                from,
                to,
                direction,
                edge_type,
                distinct_nodes,
            } => PhysicalOp::Expand {
                from: from.clone(),
                to: to.clone(),
                dir: convert_direction(*direction),
                ty: ctx.edge_type(edge_type)?,
                distinct_nodes: *distinct_nodes,
            },
            LogicalOp::Filter { predicate } => PhysicalOp::Filter {
                pred: self.convert_prop_predicate(predicate, ctx)?,
            },
            LogicalOp::Intersect { vars } => PhysicalOp::Intersect { vars: vars.clone() },
            LogicalOp::HashJoin { left, right } => PhysicalOp::HashJoin {
                left: left.clone(),
                right: right.clone(),
            },
            LogicalOp::Project { fields } => PhysicalOp::Project {
                fields: fields.iter().cloned().map(convert_projection).collect(),
            },
            LogicalOp::Distinct => PhysicalOp::Distinct,
        };

        Ok(PhysicalNode::with_inputs(op, inputs))
    }

    fn convert_prop_predicate(
        &self,
        predicate: &AstPredicate,
        ctx: &mut PlanContext<'_>,
    ) -> Result<PhysicalPredicate> {
        match predicate {
            AstPredicate::Eq { var, prop, value } => Ok(PhysicalPredicate::Eq {
                var: var.clone(),
                prop: ctx.property(prop)?,
                value: LiteralValue::from(value),
            }),
            AstPredicate::Range {
                var,
                prop,
                lower,
                upper,
            } => Ok(PhysicalPredicate::Range {
                var: var.clone(),
                prop: ctx.property(prop)?,
                lower: convert_bound(lower),
                upper: convert_bound(upper),
            }),
            AstPredicate::Custom { expr } => Ok(PhysicalPredicate::Custom { expr: expr.clone() }),
        }
    }
}

fn convert_direction(direction: EdgeDirection) -> Dir {
    match direction {
        EdgeDirection::Out => Dir::Out,
        EdgeDirection::In => Dir::In,
        EdgeDirection::Both => Dir::Both,
    }
}

fn invert_direction(direction: EdgeDirection) -> EdgeDirection {
    match direction {
        EdgeDirection::Out => EdgeDirection::In,
        EdgeDirection::In => EdgeDirection::Out,
        EdgeDirection::Both => EdgeDirection::Both,
    }
}

fn convert_bound(bound: &Bound<crate::ast::Literal>) -> Bound<LiteralValue> {
    match bound {
        Bound::Included(lit) => Bound::Included(LiteralValue::from(lit)),
        Bound::Excluded(lit) => Bound::Excluded(LiteralValue::from(lit)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn convert_projection(proj: Projection) -> ProjectField {
    match proj {
        Projection::Var { var, alias } => ProjectField::Var { var, alias },
        Projection::Expr { expr, alias } => ProjectField::Expr { expr, alias },
    }
}

struct PlanContext<'a> {
    metadata: &'a dyn MetadataProvider,
    labels: HashMap<String, LabelId>,
    props: HashMap<String, PropId>,
    edge_types: HashMap<String, TypeId>,
}

impl<'a> PlanContext<'a> {
    fn new(metadata: &'a dyn MetadataProvider) -> Self {
        Self {
            metadata,
            labels: HashMap::new(),
            props: HashMap::new(),
            edge_types: HashMap::new(),
        }
    }

    fn label(&mut self, label: &Option<String>) -> Result<LabelId> {
        match label {
            Some(name) => self.label_by_name(name),
            None => Err(SombraError::Invalid("label resolution requires a name")),
        }
    }

    fn property(&mut self, name: &str) -> Result<PropId> {
        if let Some(id) = self.props.get(name) {
            return Ok(*id);
        }
        let id = self.metadata.resolve_property(name)?;
        self.props.insert(name.to_owned(), id);
        Ok(id)
    }

    fn edge_type(&mut self, ty: &Option<String>) -> Result<Option<TypeId>> {
        match ty {
            Some(name) => {
                if let Some(id) = self.edge_types.get(name) {
                    return Ok(Some(*id));
                }
                let id = self.metadata.resolve_edge_type(name)?;
                self.edge_types.insert(name.to_owned(), id);
                Ok(Some(id))
            }
            None => Ok(None),
        }
    }

    fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        self.metadata.property_index(label, prop)
    }

    fn label_by_name(&mut self, name: &str) -> Result<LabelId> {
        if let Some(id) = self.labels.get(name) {
            return Ok(*id);
        }
        let id = self.metadata.resolve_label(name)?;
        self.labels.insert(name.to_owned(), id);
        Ok(id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum AnchorScore {
    Label,
    Range,
    Eq,
}

fn build_explain_tree(node: &PhysicalNode) -> ExplainNode {
    let mut explain = ExplainNode::new(op_name(&node.op));
    explain.props = op_props(&node.op);
    explain.inputs = node
        .inputs
        .iter()
        .map(|child| build_explain_tree(child))
        .collect();
    explain
}

fn op_name(op: &PhysicalOp) -> &'static str {
    match op {
        PhysicalOp::LabelScan { .. } => "LabelScan",
        PhysicalOp::PropIndexScan { .. } => "PropIndexScan",
        PhysicalOp::Expand { .. } => "Expand",
        PhysicalOp::Filter { .. } => "Filter",
        PhysicalOp::Intersect { .. } => "Intersect",
        PhysicalOp::HashJoin { .. } => "HashJoin",
        PhysicalOp::Distinct => "Distinct",
        PhysicalOp::Project { .. } => "Project",
    }
}

fn op_props(op: &PhysicalOp) -> Vec<(String, String)> {
    match op {
        PhysicalOp::LabelScan { label, as_var } => vec![
            ("label".into(), label.0.to_string()),
            ("as".into(), as_var.0.clone()),
        ],
        PhysicalOp::PropIndexScan {
            label,
            prop,
            pred,
            as_var,
        } => vec![
            ("label".into(), label.0.to_string()),
            ("prop".into(), prop.0.to_string()),
            ("as".into(), as_var.0.clone()),
            ("predicate".into(), describe_predicate(pred)),
        ],
        PhysicalOp::Expand {
            from,
            to,
            dir,
            ty,
            distinct_nodes,
        } => vec![
            ("from".into(), from.0.clone()),
            ("to".into(), to.0.clone()),
            ("dir".into(), format!("{dir:?}")),
            (
                "type".into(),
                ty.map(|t| t.0.to_string()).unwrap_or_else(|| "*".into()),
            ),
            ("distinct".into(), distinct_nodes.to_string()),
        ],
        PhysicalOp::Filter { pred } => vec![("predicate".into(), describe_predicate(pred))],
        PhysicalOp::Intersect { vars } => vec![(
            "vars".into(),
            vars.iter()
                .map(|v| v.0.clone())
                .collect::<Vec<_>>()
                .join(", "),
        )],
        PhysicalOp::HashJoin { left, right } => vec![
            ("left".into(), left.0.clone()),
            ("right".into(), right.0.clone()),
        ],
        PhysicalOp::Distinct => Vec::new(),
        PhysicalOp::Project { fields } => vec![(
            "fields".into(),
            fields
                .iter()
                .map(describe_field)
                .collect::<Vec<_>>()
                .join(", "),
        )],
    }
}

fn describe_predicate(pred: &PhysicalPredicate) -> String {
    match pred {
        PhysicalPredicate::Eq { var, prop, value } => {
            format!("{}.{} = {}", var.0, prop.0, literal_to_string(value))
        }
        PhysicalPredicate::Range {
            var,
            prop,
            lower,
            upper,
        } => format!(
            "{}.{} in {}..{}",
            var.0,
            prop.0,
            bound_to_string(lower),
            bound_to_string(upper)
        ),
        PhysicalPredicate::Custom { expr } => expr.clone(),
    }
}

fn literal_to_string(value: &LiteralValue) -> String {
    match value {
        LiteralValue::Null => "null".into(),
        LiteralValue::Bool(v) => v.to_string(),
        LiteralValue::Int(v) => v.to_string(),
        LiteralValue::Float(v) => v.to_string(),
        LiteralValue::String(v) => format!("{:?}", v),
    }
}

fn bound_to_string(bound: &Bound<LiteralValue>) -> String {
    match bound {
        Bound::Included(v) => format!("[{}]", literal_to_string(v)),
        Bound::Excluded(v) => format!("({})", literal_to_string(v)),
        Bound::Unbounded => "*".into(),
    }
}

fn describe_field(field: &ProjectField) -> String {
    match field {
        ProjectField::Var { var, alias } => match alias {
            Some(alias) => format!("{} as {}", var.0, alias),
            None => var.0.clone(),
        },
        ProjectField::Expr { expr, alias } => format!("{expr} as {alias}"),
    }
}

fn prop_from_predicate(pred: &PhysicalPredicate) -> Option<PropId> {
    match pred {
        PhysicalPredicate::Eq { prop, .. } => Some(*prop),
        PhysicalPredicate::Range { prop, .. } => Some(*prop),
        PhysicalPredicate::Custom { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Literal;
    use crate::builder::{PropOp, QueryBuilder};
    use crate::metadata::InMemoryMetadata;
    use sombra_types::{LabelId, PropId, TypeId};

    fn planner_with_metadata() -> Planner {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_label("Person", LabelId(2))
            .with_property("age", PropId(3))
            .with_property("name", PropId(4))
            .with_edge_type("FOLLOWS", TypeId(5));
        Planner::new(PlannerConfig::default(), Arc::new(metadata))
    }

    fn planner_with_indexed_metadata() -> Planner {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_property_index(LabelId(1), PropId(4));
        Planner::new(PlannerConfig::default(), Arc::new(metadata))
    }

    #[test]
    fn planner_builds_simple_plan() {
        let planner = planner_with_metadata();
        let ast = QueryBuilder::new().r#match("User").select(["a"]).build();
        let output = planner.plan(&ast).expect("plan succeeds");

        match &output.plan.root.op {
            PhysicalOp::Project { fields } => {
                assert_eq!(fields.len(), 1);
                assert!(matches!(fields[0], ProjectField::Var { .. }));
            }
            other => panic!("unexpected root op: {other:?}"),
        }
        assert_eq!(output.explain.root.op, "Project");
        assert_eq!(
            output.explain.root.inputs.first().map(|n| n.op.as_str()),
            Some("LabelScan")
        );
    }

    #[test]
    fn planner_applies_filters() {
        let planner = planner_with_metadata();
        let ast = QueryBuilder::new()
            .r#match("Person")
            .where_prop("a", "age", PropOp::Ge, 21_i64, None::<Literal>)
            .select(["a"])
            .build();
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::Filter { pred } => match pred {
                PhysicalPredicate::Range { var, prop, .. } => {
                    assert_eq!(var.0, "a");
                    assert_eq!(prop.0, 3);
                }
                other => panic!("unexpected predicate: {other:?}"),
            },
            other => panic!("expected filter, found {other:?}"),
        }
    }

    #[test]
    fn planner_prefers_prop_index_scan_when_available() {
        let planner = planner_with_indexed_metadata();
        let ast = QueryBuilder::new()
            .r#match("User")
            .where_prop("a", "name", PropOp::Eq, "Ada", None::<Literal>)
            .select(["a"])
            .build();
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::PropIndexScan { label, prop, .. } => {
                assert_eq!(label.0, 1);
                assert_eq!(prop.0, 4);
            }
            other => panic!("expected PropIndexScan, found {other:?}"),
        }
    }

    #[test]
    fn planner_intersects_multiple_indexed_predicates() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_property("status", PropId(6))
            .with_property_index(LabelId(1), PropId(4))
            .with_property_index(LabelId(1), PropId(6));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .r#match("User")
            .where_prop("a", "name", PropOp::Eq, "Ada", None::<Literal>)
            .where_prop("a", "status", PropOp::Eq, "active", None::<Literal>)
            .select(["a"])
            .build();
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::Intersect { vars } => {
                assert_eq!(vars.len(), 1);
                assert_eq!(vars[0].0, "a");
                assert_eq!(project_input.inputs.len(), 2);
                for child in &project_input.inputs {
                    match &child.op {
                        PhysicalOp::PropIndexScan { as_var, .. } => assert_eq!(as_var.0, "a"),
                        other => panic!("expected PropIndexScan child, found {other:?}"),
                    }
                }
            }
            other => panic!("expected Intersect, found {other:?}"),
        }
    }

    #[test]
    fn planner_can_reanchor_mid_chain_using_index() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_edge_type("FOLLOWS", TypeId(5))
            .with_property_index(LabelId(1), PropId(4));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .r#match(("a", "User"))
            .where_edge("FOLLOWS", ("b", "User"))
            .where_prop("b", "name", PropOp::Eq, "Ada", None::<Literal>)
            .select(["a", "b"])
            .build();
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::Expand { from, to, dir, .. } => {
                assert_eq!(from.0, "b");
                assert_eq!(to.0, "a");
                assert_eq!(*dir, Dir::In);
                let expand_input = project_input.inputs.first().expect("expand input");
                match &expand_input.op {
                    PhysicalOp::PropIndexScan { as_var, .. } => assert_eq!(as_var.0, "b"),
                    other => panic!("expected PropIndexScan, found {other:?}"),
                }
            }
            other => panic!("expected Expand, found {other:?}"),
        }
    }
}
