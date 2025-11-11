//! Rule-based planner scaffolding.

use crate::query::{
    analyze,
    ast::{BoolExpr, Comparison, EdgeDirection, MatchClause, Projection, QueryAst, Var},
    logical::{LogicalOp, LogicalPlan, PlanNode, PropPredicate as AstPredicate},
    metadata::MetadataProvider,
    physical::{
        Dir, LiteralValue, PhysicalBoolExpr, PhysicalComparison, PhysicalNode, PhysicalOp,
        PhysicalPlan, ProjectField, PropPredicate as PhysicalPredicate,
    },
    Value,
};
use crate::storage::index::IndexDef;
use crate::storage::{PropStats, PropValueOwned};
use crate::types::{LabelId, PropId, Result, SombraError, TypeId};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::ops::Bound;
use std::sync::Arc;

/// Planner inputs that influence rule selection.
#[derive(Clone, Debug, Default)]
pub struct PlannerConfig {
    /// Whether to enable hash join optimization
    pub enable_hash_join: bool,
}

/// Planner output containing the chosen physical plan and explain tree.
#[derive(Clone, Debug)]
pub struct PlannerOutput {
    /// The generated physical query plan
    pub plan: PhysicalPlan,
    /// Human-readable explain tree
    pub explain: PlanExplain,
}

/// Human-readable explain tree.
#[derive(Clone, Debug)]
pub struct PlanExplain {
    /// Root node of the explain tree
    pub root: ExplainNode,
}

/// Explain node representing an operator with optional metadata.
#[derive(Clone, Debug)]
pub struct ExplainNode {
    /// Operator name
    pub op: String,
    /// Additional properties describing the operator
    pub props: Vec<(String, String)>,
    /// Input operators
    pub inputs: Vec<ExplainNode>,
}

impl ExplainNode {
    /// Creates a new explain node with the given operator name.
    pub fn new(op: impl Into<String>) -> Self {
        Self {
            op: op.into(),
            props: Vec::new(),
            inputs: Vec::new(),
        }
    }
}

/// Query planner that converts AST to physical execution plans.
pub struct Planner {
    metadata: Arc<dyn MetadataProvider>,
    _config: PlannerConfig,
}

impl Planner {
    /// Creates a new planner with the given configuration and metadata provider.
    pub fn new(config: PlannerConfig, metadata: Arc<dyn MetadataProvider>) -> Self {
        Self {
            metadata,
            _config: config,
        }
    }

    /// Converts an AST into a physical plan.
    pub fn plan(&self, ast: &QueryAst) -> Result<PlannerOutput> {
        let normalized = analyze::normalize(ast)?;
        let mut ctx = PlanContext::new(self.metadata.as_ref());
        let logical = self.build_logical_plan(&normalized, &mut ctx)?;
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

        let mut preds_by_var: HashMap<Var, Vec<VarPredicate>> = HashMap::new();
        let mut residual_predicate = ast.predicate.clone();
        if let Some(expr) = ast.predicate.as_ref() {
            let mut pushdowns = Vec::new();
            residual_predicate = extract_pushdown_predicates(expr, &mut pushdowns);
            for pred in pushdowns {
                let key = predicate_var(&pred);
                let selectivity = predicate_selectivity(&pred, ctx)?;
                preds_by_var.entry(key).or_default().push(VarPredicate {
                    predicate: pred,
                    selectivity,
                });
            }
        }

        let labels_by_var = self.resolve_label_ids(&ast.matches, ctx)?;
        let anchor_idx = self.select_anchor(&ast.matches, &labels_by_var, &preds_by_var, ctx)?;
        let anchor_match = &ast.matches[anchor_idx];
        let anchor_label = labels_by_var
            .get(&anchor_match.var)
            .expect("missing label id for anchor")
            .id;
        let indexed_preds =
            self.take_indexed_predicates(anchor_label, &anchor_match.var, &mut preds_by_var, ctx)?;
        let mut current = match indexed_preds.len() {
            0 => PlanNode::new(LogicalOp::LabelScan {
                label: anchor_match.label.clone(),
                as_var: anchor_match.var.clone(),
            }),
            1 => {
                let (var_pred, prop) = indexed_preds.into_iter().next().unwrap();
                let VarPredicate {
                    predicate,
                    selectivity,
                } = var_pred;
                PlanNode::new(LogicalOp::PropIndexScan {
                    label: anchor_match.label.clone(),
                    prop,
                    predicate,
                    selectivity,
                    as_var: anchor_match.var.clone(),
                })
            }
            _ => {
                let children = indexed_preds
                    .into_iter()
                    .map(|(var_pred, prop)| {
                        let VarPredicate {
                            predicate,
                            selectivity,
                        } = var_pred;
                        PlanNode::new(LogicalOp::PropIndexScan {
                            label: anchor_match.label.clone(),
                            prop,
                            predicate,
                            selectivity,
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

        if let Some(expr) = &residual_predicate {
            current =
                PlanNode::with_inputs(LogicalOp::BoolFilter { expr: expr.clone() }, vec![current]);
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
        preds_by_var: &mut HashMap<Var, Vec<VarPredicate>>,
    ) -> PlanNode {
        if let Some(mut preds) = preds_by_var.remove(var) {
            preds.sort_by(|a, b| {
                a.selectivity
                    .partial_cmp(&b.selectivity)
                    .unwrap_or(Ordering::Equal)
            });
            for predicate in preds {
                let VarPredicate {
                    predicate,
                    selectivity,
                } = predicate;
                node = PlanNode::with_inputs(
                    LogicalOp::Filter {
                        predicate,
                        selectivity,
                    },
                    vec![node],
                );
            }
        }
        node
    }

    fn resolve_label_ids(
        &self,
        matches: &[MatchClause],
        ctx: &mut PlanContext<'_>,
    ) -> Result<HashMap<Var, VarLabel>> {
        let mut map = HashMap::new();
        for m in matches {
            let label_name = m
                .label
                .as_ref()
                .ok_or(SombraError::Invalid("match clause requires a label"))?
                .clone();
            let id = ctx.label(&m.label)?;
            let info = VarLabel {
                id,
                name: label_name,
            };
            ctx.record_var_label(&m.var, info.clone());
            map.insert(m.var.clone(), info);
        }
        Ok(map)
    }

    fn select_anchor(
        &self,
        matches: &[MatchClause],
        labels_by_var: &HashMap<Var, VarLabel>,
        preds_by_var: &HashMap<Var, Vec<VarPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<usize> {
        let mut best_idx = 0;
        let mut best_score = AnchorScore::Label;
        for (idx, m) in matches.iter().enumerate() {
            let label = labels_by_var
                .get(&m.var)
                .expect("label id missing for match variable");
            let score = self.anchor_score(&m.var, label.id, preds_by_var, ctx)?;
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
        preds_by_var: &HashMap<Var, Vec<VarPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<AnchorScore> {
        let Some(preds) = preds_by_var.get(var) else {
            return Ok(AnchorScore::Label);
        };
        let mut best = AnchorScore::Label;
        for pred in preds {
            let (prop_name, score_candidate) = match &pred.predicate {
                AstPredicate::Eq { prop, .. } => (prop.as_str(), AnchorScore::Eq),
                AstPredicate::Range { prop, .. } => (prop.as_str(), AnchorScore::Range),
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
        preds_by_var: &mut HashMap<Var, Vec<VarPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<Vec<(VarPredicate, String)>> {
        let Some(mut preds) = preds_by_var.remove(var) else {
            return Ok(Vec::new());
        };

        let mut indexed_eq: Vec<(VarPredicate, String)> = Vec::new();
        let mut indexed_range: Vec<(VarPredicate, String)> = Vec::new();
        let mut remaining: Vec<VarPredicate> = Vec::new();

        for predicate in preds.drain(..) {
            let (prop_name, class) = match &predicate.predicate {
                AstPredicate::Eq { prop, .. } => (prop.as_str(), AnchorScore::Eq),
                AstPredicate::Range { prop, .. } => (prop.as_str(), AnchorScore::Range),
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

        if !remaining.is_empty() {
            preds_by_var.insert(var.clone(), remaining);
        }

        let by_selectivity = |a: &(VarPredicate, String), b: &(VarPredicate, String)| {
            a.0.selectivity
                .partial_cmp(&b.0.selectivity)
                .unwrap_or(Ordering::Equal)
        };
        indexed_eq.sort_by(by_selectivity);
        indexed_range.sort_by(by_selectivity);

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
                selectivity,
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
                    selectivity: *selectivity,
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
            LogicalOp::Filter {
                predicate,
                selectivity,
            } => PhysicalOp::Filter {
                pred: self.convert_prop_predicate(predicate, ctx)?,
                selectivity: *selectivity,
            },
            LogicalOp::Intersect { vars } => PhysicalOp::Intersect { vars: vars.clone() },
            LogicalOp::HashJoin { left, right } => PhysicalOp::HashJoin {
                left: left.clone(),
                right: right.clone(),
            },
            LogicalOp::Project { fields } => {
                let projections = fields
                    .iter()
                    .cloned()
                    .map(|proj| convert_projection(proj, ctx))
                    .collect::<Result<Vec<_>>>()?;
                PhysicalOp::Project {
                    fields: projections,
                }
            }
            LogicalOp::Distinct => PhysicalOp::Distinct,
            LogicalOp::BoolFilter { expr } => PhysicalOp::BoolFilter {
                expr: self.convert_bool_expr(expr, ctx)?,
            },
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
        }
    }

    fn convert_bool_expr(
        &self,
        expr: &BoolExpr,
        ctx: &mut PlanContext<'_>,
    ) -> Result<PhysicalBoolExpr> {
        match expr {
            BoolExpr::Cmp(cmp) => Ok(PhysicalBoolExpr::Cmp(self.convert_comparison(cmp, ctx)?)),
            BoolExpr::And(children) => {
                let mut converted = Vec::with_capacity(children.len());
                for child in children {
                    converted.push(self.convert_bool_expr(child, ctx)?);
                }
                converted.sort_by(|a, b| {
                    let sa = bool_expr_selectivity(a);
                    let sb = bool_expr_selectivity(b);
                    sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(PhysicalBoolExpr::And(converted))
            }
            BoolExpr::Or(children) => {
                let mut converted = Vec::with_capacity(children.len());
                for child in children {
                    converted.push(self.convert_bool_expr(child, ctx)?);
                }
                converted.sort_by(|a, b| {
                    let sa = bool_expr_selectivity(a);
                    let sb = bool_expr_selectivity(b);
                    sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(PhysicalBoolExpr::Or(converted))
            }
            BoolExpr::Not(child) => {
                let inner = self.convert_bool_expr(child, ctx)?;
                Ok(PhysicalBoolExpr::Not(Box::new(inner)))
            }
        }
    }

    fn convert_comparison(
        &self,
        cmp: &Comparison,
        ctx: &mut PlanContext<'_>,
    ) -> Result<PhysicalComparison> {
        Ok(match cmp {
            Comparison::Eq { var, prop, value } => PhysicalComparison::Eq {
                var: var.clone(),
                prop: ctx.property(prop)?,
                value: LiteralValue::from(value),
            },
            Comparison::Ne { var, prop, value } => PhysicalComparison::Ne {
                var: var.clone(),
                prop: ctx.property(prop)?,
                value: LiteralValue::from(value),
            },
            Comparison::Lt { var, prop, value } => PhysicalComparison::Lt {
                var: var.clone(),
                prop: ctx.property(prop)?,
                value: LiteralValue::from(value),
            },
            Comparison::Le { var, prop, value } => PhysicalComparison::Le {
                var: var.clone(),
                prop: ctx.property(prop)?,
                value: LiteralValue::from(value),
            },
            Comparison::Gt { var, prop, value } => PhysicalComparison::Gt {
                var: var.clone(),
                prop: ctx.property(prop)?,
                value: LiteralValue::from(value),
            },
            Comparison::Ge { var, prop, value } => PhysicalComparison::Ge {
                var: var.clone(),
                prop: ctx.property(prop)?,
                value: LiteralValue::from(value),
            },
            Comparison::Between {
                var,
                prop,
                low,
                high,
            } => PhysicalComparison::Between {
                var: var.clone(),
                prop: ctx.property(prop)?,
                low: convert_bound(low),
                high: convert_bound(high),
            },
            Comparison::In { var, prop, values } => PhysicalComparison::In {
                var: var.clone(),
                prop: ctx.property(prop)?,
                values: values.iter().map(LiteralValue::from).collect(),
            },
            Comparison::Exists { var, prop } => PhysicalComparison::Exists {
                var: var.clone(),
                prop: ctx.property(prop)?,
            },
            Comparison::IsNull { var, prop } => PhysicalComparison::IsNull {
                var: var.clone(),
                prop: ctx.property(prop)?,
            },
            Comparison::IsNotNull { var, prop } => PhysicalComparison::IsNotNull {
                var: var.clone(),
                prop: ctx.property(prop)?,
            },
        })
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

fn convert_bound(bound: &Bound<Value>) -> Bound<LiteralValue> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(lit) => Bound::Included(LiteralValue::from(lit)),
        Bound::Excluded(lit) => Bound::Excluded(LiteralValue::from(lit)),
    }
}

fn convert_projection(proj: Projection, ctx: &mut PlanContext<'_>) -> Result<ProjectField> {
    match proj {
        Projection::Var { var, alias } => Ok(ProjectField::Var { var, alias }),
        Projection::Prop { var, prop, alias } => {
            let prop_id = ctx.property(&prop)?;
            Ok(ProjectField::Prop {
                var,
                prop: prop_id,
                prop_name: prop,
                alias,
            })
        }
    }
}

fn extract_pushdown_predicates(expr: &BoolExpr, out: &mut Vec<AstPredicate>) -> Option<BoolExpr> {
    match expr {
        BoolExpr::Cmp(cmp) => {
            if let Some(pred) = predicate_from_comparison(cmp) {
                out.push(pred);
                None
            } else {
                Some(expr.clone())
            }
        }
        BoolExpr::And(children) => {
            let mut residual = Vec::new();
            for child in children {
                match child {
                    BoolExpr::Or(_) | BoolExpr::Not(_) => residual.push(child.clone()),
                    _ => {
                        if let Some(rest) = extract_pushdown_predicates(child, out) {
                            residual.push(rest);
                        }
                    }
                }
            }
            match residual.len() {
                0 => None,
                1 => Some(residual.remove(0)),
                _ => Some(BoolExpr::And(residual)),
            }
        }
        BoolExpr::Or(_) | BoolExpr::Not(_) => Some(expr.clone()),
    }
}

fn predicate_from_comparison(cmp: &Comparison) -> Option<AstPredicate> {
    match cmp {
        Comparison::Eq { var, prop, value } => Some(AstPredicate::Eq {
            var: var.clone(),
            prop: prop.clone(),
            value: value.clone(),
        }),
        Comparison::Lt { var, prop, value } => Some(range_predicate(
            var,
            prop,
            &Bound::Unbounded,
            &Bound::Excluded(value.clone()),
        )),
        Comparison::Le { var, prop, value } => Some(range_predicate(
            var,
            prop,
            &Bound::Unbounded,
            &Bound::Included(value.clone()),
        )),
        Comparison::Gt { var, prop, value } => Some(range_predicate(
            var,
            prop,
            &Bound::Excluded(value.clone()),
            &Bound::Unbounded,
        )),
        Comparison::Ge { var, prop, value } => Some(range_predicate(
            var,
            prop,
            &Bound::Included(value.clone()),
            &Bound::Unbounded,
        )),
        Comparison::Between {
            var,
            prop,
            low,
            high,
        } => Some(range_predicate(var, prop, low, high)),
        _ => None,
    }
}

fn range_predicate(
    var: &Var,
    prop: &str,
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> AstPredicate {
    AstPredicate::Range {
        var: var.clone(),
        prop: prop.to_string(),
        lower: lower.clone(),
        upper: upper.clone(),
    }
}

fn predicate_var(pred: &AstPredicate) -> Var {
    match pred {
        AstPredicate::Eq { var, .. } | AstPredicate::Range { var, .. } => var.clone(),
    }
}

const DEFAULT_EQ_SELECTIVITY: f64 = 0.05;
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.3;
const MIN_SELECTIVITY: f64 = 1e-6;

fn predicate_selectivity(pred: &AstPredicate, ctx: &mut PlanContext<'_>) -> Result<f64> {
    let selectivity = match pred {
        AstPredicate::Eq { var, prop, .. } => {
            if let Some(stats) = ctx.property_stats_for(var, prop)? {
                eq_selectivity(stats.as_ref())
            } else {
                DEFAULT_EQ_SELECTIVITY
            }
        }
        AstPredicate::Range {
            var,
            prop,
            lower,
            upper,
        } => {
            if let Some(stats) = ctx.property_stats_for(var, prop)? {
                range_selectivity(stats.as_ref(), lower, upper)
            } else {
                DEFAULT_RANGE_SELECTIVITY
            }
        }
    };
    Ok(selectivity.clamp(MIN_SELECTIVITY, 1.0))
}

fn eq_selectivity(stats: &PropStats) -> f64 {
    if stats.row_count == 0 {
        return DEFAULT_EQ_SELECTIVITY;
    }
    let domain = stats.distinct_count.max(1) as f64;
    let non_null = stats.non_null_count.max(1) as f64;
    let base = non_null / stats.row_count as f64;
    (base / domain).max(MIN_SELECTIVITY)
}

fn range_selectivity(stats: &PropStats, lower: &Bound<Value>, upper: &Bound<Value>) -> f64 {
    if stats.row_count == 0 {
        return DEFAULT_RANGE_SELECTIVITY;
    }
    let density =
        (stats.non_null_count.max(1) as f64 / stats.row_count as f64).max(MIN_SELECTIVITY);
    if let Some(span) = numeric_range_fraction(stats, lower, upper) {
        return (density * span).clamp(MIN_SELECTIVITY, 1.0);
    }
    (density * DEFAULT_RANGE_SELECTIVITY)
        .max(DEFAULT_RANGE_SELECTIVITY)
        .clamp(MIN_SELECTIVITY, 1.0)
}

fn numeric_range_fraction(
    stats: &PropStats,
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Option<f64> {
    let min = prop_value_to_f64(stats.min.as_ref()?)?;
    let max = prop_value_to_f64(stats.max.as_ref()?)?;
    if min >= max {
        return Some(1.0);
    }
    let domain = max - min;
    if domain <= f64::EPSILON {
        return Some(1.0);
    }
    let lower_ratio = bound_numeric(lower)
        .map(|v| ((v - min) / domain).clamp(0.0, 1.0))
        .unwrap_or(0.0);
    let upper_ratio = bound_numeric(upper)
        .map(|v| ((v - min) / domain).clamp(0.0, 1.0))
        .unwrap_or(1.0);
    if upper_ratio <= lower_ratio {
        return Some(MIN_SELECTIVITY);
    }
    Some((upper_ratio - lower_ratio).clamp(MIN_SELECTIVITY, 1.0))
}

fn bound_numeric(bound: &Bound<Value>) -> Option<f64> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => {
            value_to_prop_value(value).and_then(|pv| prop_value_to_f64(&pv))
        }
        Bound::Unbounded => None,
    }
}

fn prop_value_to_f64(value: &PropValueOwned) -> Option<f64> {
    match value {
        PropValueOwned::Int(v) => Some(*v as f64),
        PropValueOwned::Float(v) => Some(*v),
        PropValueOwned::Date(v) => Some(*v as f64),
        PropValueOwned::DateTime(v) => Some(*v as f64),
        _ => None,
    }
}

fn value_to_prop_value(value: &Value) -> Option<PropValueOwned> {
    match value {
        Value::Null => Some(PropValueOwned::Null),
        Value::Bool(v) => Some(PropValueOwned::Bool(*v)),
        Value::Int(v) => Some(PropValueOwned::Int(*v)),
        Value::Float(v) => Some(PropValueOwned::Float(*v)),
        Value::String(v) => Some(PropValueOwned::Str(v.clone())),
        Value::Bytes(v) => Some(PropValueOwned::Bytes(v.clone())),
        Value::DateTime(v) => i64::try_from(*v).ok().map(PropValueOwned::DateTime),
    }
}

struct PlanContext<'a> {
    metadata: &'a dyn MetadataProvider,
    labels: HashMap<String, LabelId>,
    props: HashMap<String, PropId>,
    edge_types: HashMap<String, TypeId>,
    var_labels: HashMap<String, VarLabel>,
    prop_stats: HashMap<(LabelId, PropId), Arc<PropStats>>,
}

impl<'a> PlanContext<'a> {
    fn new(metadata: &'a dyn MetadataProvider) -> Self {
        Self {
            metadata,
            labels: HashMap::new(),
            props: HashMap::new(),
            edge_types: HashMap::new(),
            var_labels: HashMap::new(),
            prop_stats: HashMap::new(),
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

    fn record_var_label(&mut self, var: &Var, info: VarLabel) {
        self.var_labels.insert(var.0.clone(), info);
    }

    fn var_label_info(&self, var: &Var) -> Option<&VarLabel> {
        self.var_labels.get(&var.0)
    }

    fn property_stats_for(&mut self, var: &Var, prop: &str) -> Result<Option<Arc<PropStats>>> {
        let label_id = match self.var_label_info(var) {
            Some(info) => info.id,
            None => return Ok(None),
        };
        let prop_id = self.property(prop)?;
        self.property_stats_by_id(label_id, prop_id)
    }

    fn property_stats_by_id(
        &mut self,
        label: LabelId,
        prop: PropId,
    ) -> Result<Option<Arc<PropStats>>> {
        if let Some(stats) = self.prop_stats.get(&(label, prop)) {
            return Ok(Some(stats.clone()));
        }
        let stats = self.metadata.property_stats(label, prop)?;
        if let Some(stats) = stats {
            let arc = Arc::new(stats);
            self.prop_stats.insert((label, prop), arc.clone());
            Ok(Some(arc))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum AnchorScore {
    Label,
    Range,
    Eq,
}

#[derive(Clone)]
struct VarLabel {
    id: LabelId,
    name: String,
}

#[derive(Clone)]
struct VarPredicate {
    predicate: AstPredicate,
    selectivity: f64,
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
        PhysicalOp::BoolFilter { .. } => "BoolFilter",
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
            selectivity,
        } => {
            let mut props = vec![
                ("label".into(), label.0.to_string()),
                ("prop".into(), prop.0.to_string()),
                ("as".into(), as_var.0.clone()),
                ("predicate".into(), describe_predicate(pred)),
            ];
            props.push(("selectivity".into(), fmt_selectivity(*selectivity)));
            props
        }
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
        PhysicalOp::Filter { pred, selectivity } => {
            vec![
                ("predicate".into(), describe_predicate(pred)),
                ("selectivity".into(), fmt_selectivity(*selectivity)),
            ]
        }
        PhysicalOp::BoolFilter { expr } => vec![
            ("expr".into(), describe_bool_expr(expr)),
            (
                "selectivity".into(),
                fmt_selectivity(bool_expr_selectivity(expr)),
            ),
        ],
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
    }
}

fn describe_bool_expr(expr: &PhysicalBoolExpr) -> String {
    match expr {
        PhysicalBoolExpr::Cmp(cmp) => describe_comparison(cmp),
        PhysicalBoolExpr::And(children) => format!(
            "AND({})",
            children
                .iter()
                .map(describe_bool_expr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PhysicalBoolExpr::Or(children) => format!(
            "OR({})",
            children
                .iter()
                .map(describe_bool_expr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PhysicalBoolExpr::Not(child) => format!("NOT({})", describe_bool_expr(child)),
    }
}

fn describe_comparison(cmp: &PhysicalComparison) -> String {
    match cmp {
        PhysicalComparison::Eq { var, prop, value } => {
            format!("{}.{} = {}", var.0, prop.0, literal_to_string(value))
        }
        PhysicalComparison::Ne { var, prop, value } => {
            format!("{}.{} != {}", var.0, prop.0, literal_to_string(value))
        }
        PhysicalComparison::Lt { var, prop, value } => {
            format!("{}.{} < {}", var.0, prop.0, literal_to_string(value))
        }
        PhysicalComparison::Le { var, prop, value } => {
            format!("{}.{} <= {}", var.0, prop.0, literal_to_string(value))
        }
        PhysicalComparison::Gt { var, prop, value } => {
            format!("{}.{} > {}", var.0, prop.0, literal_to_string(value))
        }
        PhysicalComparison::Ge { var, prop, value } => {
            format!("{}.{} >= {}", var.0, prop.0, literal_to_string(value))
        }
        PhysicalComparison::Between {
            var,
            prop,
            low,
            high,
        } => format!(
            "{}.{} in {}..{}",
            var.0,
            prop.0,
            bound_to_string(low),
            bound_to_string(high)
        ),
        PhysicalComparison::In { var, prop, values } => format!(
            "{}.{} IN [{}]",
            var.0,
            prop.0,
            values
                .iter()
                .map(literal_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PhysicalComparison::Exists { var, prop } => format!("EXISTS({}.{})", var.0, prop.0),
        PhysicalComparison::IsNull { var, prop } => format!("{}.{} IS NULL", var.0, prop.0),
        PhysicalComparison::IsNotNull { var, prop } => {
            format!("{}.{} IS NOT NULL", var.0, prop.0)
        }
    }
}

fn literal_to_string(value: &LiteralValue) -> String {
    match value {
        LiteralValue::Null => "null".into(),
        LiteralValue::Bool(v) => v.to_string(),
        LiteralValue::Int(v) => v.to_string(),
        LiteralValue::Float(v) => v.to_string(),
        LiteralValue::String(v) => format!("{:?}", v),
        LiteralValue::Bytes(bytes) => format!("bytes(len={})", bytes.len()),
        LiteralValue::DateTime(ts) => format!("datetime({ts})"),
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
        ProjectField::Prop {
            var,
            prop_name,
            alias,
            ..
        } => match alias {
            Some(alias) => format!("{}.{} as {}", var.0, prop_name, alias),
            None => format!("{}.{}", var.0, prop_name),
        },
    }
}

fn prop_from_predicate(pred: &PhysicalPredicate) -> Option<PropId> {
    match pred {
        PhysicalPredicate::Eq { prop, .. } => Some(*prop),
        PhysicalPredicate::Range { prop, .. } => Some(*prop),
    }
}

fn bool_expr_selectivity(expr: &PhysicalBoolExpr) -> f64 {
    match expr {
        PhysicalBoolExpr::Cmp(cmp) => comparison_selectivity(cmp),
        PhysicalBoolExpr::And(children) => {
            let mut sel = 1.0;
            for child in children {
                sel *= bool_expr_selectivity(child);
            }
            sel.clamp(0.0, 1.0)
        }
        PhysicalBoolExpr::Or(children) => {
            let mut remaining = 1.0;
            for child in children {
                remaining *= 1.0 - bool_expr_selectivity(child);
            }
            (1.0 - remaining).clamp(0.0, 1.0)
        }
        PhysicalBoolExpr::Not(child) => (1.0 - bool_expr_selectivity(child)).clamp(0.0, 1.0),
    }
}

fn comparison_selectivity(cmp: &PhysicalComparison) -> f64 {
    match cmp {
        PhysicalComparison::Eq { .. } => 0.05,
        PhysicalComparison::Ne { .. } => 0.95,
        PhysicalComparison::Lt { .. } | PhysicalComparison::Le { .. } => 0.3,
        PhysicalComparison::Gt { .. } | PhysicalComparison::Ge { .. } => 0.3,
        PhysicalComparison::Between { .. } => 0.2,
        PhysicalComparison::In { values, .. } => (values.len() as f64 * 0.05).clamp(0.05, 1.0),
        PhysicalComparison::Exists { .. } => 0.5,
        PhysicalComparison::IsNull { .. } => 0.1,
        PhysicalComparison::IsNotNull { .. } => 0.9,
    }
}

fn fmt_selectivity(value: f64) -> String {
    format!("{:.4}", value.clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::builder::QueryBuilder;
    use crate::query::metadata::InMemoryMetadata;
    use crate::types::{LabelId, PropId, TypeId};

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
            .where_var("a", |pred| {
                pred.ge("age", 21_i64);
            })
            .select(["a"])
            .build();
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::Filter { pred, .. } => match pred {
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
            .where_var("a", |pred| {
                pred.eq("name", "Ada");
            })
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
            .where_var("a", |pred| {
                pred.eq("name", "Ada");
            })
            .where_var("a", |pred| {
                pred.eq("status", "active");
            })
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
            .where_var("b", |pred| {
                pred.eq("name", "Ada");
            })
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
