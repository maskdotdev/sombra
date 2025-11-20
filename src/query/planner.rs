//! Rule-based planner scaffolding.

use crate::query::{
    analyze::{
        self, AnalyzedComparison, AnalyzedExpr, AnalyzedProjection, AnalyzedQuery, Collation,
        PropRef, VarBinding, VarId,
    },
    ast::{EdgeDirection, QueryAst, Var},
    errors::AnalyzerErrorWithCode,
    logical::{LogicalOp, LogicalPlan, PlanNode, PropPredicate as AstPredicate},
    metadata::MetadataProvider,
    physical::{
        Dir, InLookup, LiteralValue, PhysicalBoolExpr, PhysicalComparison, PhysicalNode,
        PhysicalOp, PhysicalPlan, ProjectField, PropPredicate as PhysicalPredicate,
    },
    Value,
};
use crate::storage::index::{IndexDef, TypeTag};
use crate::storage::{PropStats, PropValueOwned};
use crate::types::{LabelId, PropId, Result, SombraError};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::Hasher;
use std::ops::Bound;
use std::sync::Arc;
use xxhash_rust::xxh64::Xxh64;

/// Planner inputs that influence rule selection.
#[derive(Clone, Debug, Default)]
pub struct PlannerConfig {
    /// Whether to enable hash join optimization
    pub enable_hash_join: bool,
}

/// Planner output containing the chosen physical plan and explain tree.
#[derive(Clone, Debug)]
pub struct PlannerOutput {
    /// Optional request identifier propagated from the AST.
    pub request_id: Option<String>,
    /// The generated physical query plan
    pub plan: PhysicalPlan,
    /// Human-readable explain tree
    pub explain: PlanExplain,
    /// Deterministic plan hash for explain/caching.
    pub plan_hash: u64,
}

/// Human-readable explain tree.
#[derive(Clone, Debug)]
pub struct PlanExplain {
    /// Root node of the explain tree
    pub root: ExplainNode,
    /// Deterministic hash for the plan.
    pub plan_hash: u64,
}

/// Explain node representing an operator with optional metadata.
#[derive(Clone, Debug)]
pub struct ExplainNode {
    /// Operator name
    pub op: String,
    /// Additional properties describing the operator
    pub props: Vec<ExplainProp>,
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

/// Single property associated with an [`ExplainNode`].
#[derive(Clone, Debug)]
pub struct ExplainProp {
    /// Property key.
    pub key: String,
    /// Property value serialized for display.
    pub value: String,
    /// Whether this property contains literal data that may be redacted.
    pub redactable: bool,
}

impl ExplainProp {
    fn plain(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            redactable: false,
        }
    }

    fn literal(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            redactable: true,
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
        let analyzed = analyze::analyze(ast, self.metadata.as_ref())
            .map_err(|err| SombraError::InvalidOwned(AnalyzerErrorWithCode(&err).to_string()))?;
        self.plan_analyzed(&analyzed)
    }

    /// Converts an analyzed query into a physical plan.
    pub fn plan_analyzed(&self, analyzed: &AnalyzedQuery) -> Result<PlannerOutput> {
        let mut ctx = PlanContext::new(self.metadata.as_ref());
        let logical = self.build_logical_plan(analyzed, &mut ctx)?;
        let physical = self.lower_to_physical(&logical, &mut ctx)?;
        let plan_hash = compute_plan_hash(analyzed, &physical, self.metadata.catalog_epoch());
        let explain = PlanExplain {
            root: build_explain_tree(&physical.root),
            plan_hash,
        };
        Ok(PlannerOutput {
            request_id: analyzed.request_id.clone(),
            plan: physical,
            explain,
            plan_hash,
        })
    }

    fn build_logical_plan(
        &self,
        analyzed: &AnalyzedQuery,
        ctx: &mut PlanContext<'_>,
    ) -> Result<LogicalPlan> {
        if analyzed.vars().is_empty() {
            return Err(SombraError::Invalid(
                "query must include at least one match",
            ));
        }

        let mut preds_by_var: HashMap<VarId, Vec<VarPredicate>> = HashMap::new();
        let mut residual_predicate = analyzed.predicate.clone();
        if let Some(expr) = analyzed.predicate.as_ref() {
            let mut pushdowns = Vec::new();
            residual_predicate =
                extract_pushdown_predicates(analyzed, expr.clone(), &mut pushdowns);
            for candidate in pushdowns {
                match candidate {
                    PushdownCandidate::Comparison(cmp) => {
                        let key = predicate_var(&cmp);
                        let selectivity = predicate_selectivity(analyzed, &cmp, ctx)?;
                        preds_by_var.entry(key).or_default().push(VarPredicate {
                            var: key,
                            selectivity,
                            kind: VarPredicateKind::Comparison(cmp),
                        });
                    }
                    PushdownCandidate::Union { var, expr, terms } => {
                        let mut union_terms = Vec::with_capacity(terms.len());
                        for cmp in terms {
                            let sel = predicate_selectivity(analyzed, &cmp, ctx)?;
                            union_terms.push(UnionTerm {
                                cmp,
                                selectivity: sel,
                            });
                        }
                        let selectivity = union_terms_selectivity(&union_terms);
                        preds_by_var.entry(var).or_default().push(VarPredicate {
                            var,
                            selectivity,
                            kind: VarPredicateKind::Union {
                                expr,
                                terms: union_terms,
                            },
                        });
                    }
                }
            }
        }

        let bindings = analyzed.vars();
        ctx.register_bindings(bindings);
        let anchor_idx = self.select_anchor(bindings, &preds_by_var, ctx)?;
        let anchor_binding = &bindings[anchor_idx];
        let anchor_label = anchor_binding.label_id;
        let mut indexed = self.take_indexed_predicates(anchor_binding, &mut preds_by_var, ctx)?;
        if let Some(expr) = indexed.union_fallback.take() {
            residual_predicate = merge_residual(residual_predicate, expr);
        }
        let mut current = if let Some(union_pred) = indexed.union {
            self.build_union_scan(analyzed, anchor_binding, union_pred, analyzed.distinct)?
        } else {
            match indexed.scans.len() {
                0 => PlanNode::new(LogicalOp::LabelScan {
                    label: anchor_binding.label.clone(),
                    label_id: anchor_label,
                    as_var: anchor_binding.var.clone(),
                }),
                1 => {
                    let pred = indexed.scans.into_iter().next().unwrap();
                    match pred.kind {
                        VarPredicateKind::Comparison(cmp) => {
                            PlanNode::new(LogicalOp::PropIndexScan {
                                label: anchor_binding.label.clone(),
                                label_id: anchor_label,
                                prop: prop_from_cmp(&cmp),
                                predicate: cmp_to_prop_predicate(analyzed, &cmp)?,
                                selectivity: pred.selectivity,
                                as_var: anchor_binding.var.clone(),
                            })
                        }
                        VarPredicateKind::Union { .. } => {
                            return Err(SombraError::Invalid(
                                "unexpected union predicate in indexed scans",
                            ))
                        }
                    }
                }
                _ => {
                    let children = indexed
                        .scans
                        .into_iter()
                        .map(|var_pred| -> Result<PlanNode> {
                            match var_pred.kind {
                                VarPredicateKind::Comparison(cmp) => {
                                    Ok(PlanNode::new(LogicalOp::PropIndexScan {
                                        label: anchor_binding.label.clone(),
                                        label_id: anchor_label,
                                        prop: prop_from_cmp(&cmp),
                                        predicate: cmp_to_prop_predicate(analyzed, &cmp)?,
                                        selectivity: var_pred.selectivity,
                                        as_var: anchor_binding.var.clone(),
                                    }))
                                }
                                VarPredicateKind::Union { .. } => Err(SombraError::Invalid(
                                    "unexpected union predicate in indexed scans",
                                )),
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    PlanNode::with_inputs(
                        LogicalOp::Intersect {
                            vars: vec![anchor_binding.var.clone()],
                        },
                        children,
                    )
                }
            }
        };

        current =
            self.apply_var_predicates(analyzed, current, anchor_binding.id, &mut preds_by_var)?;

        let mut bound_vars: HashSet<Var> = HashSet::new();
        bound_vars.insert(anchor_binding.var.clone());
        let mut remaining_edges = analyzed.edges.clone();

        while bound_vars.len() < bindings.len() {
            let Some((edge_idx, reverse)) =
                remaining_edges.iter().enumerate().find_map(|(idx, edge)| {
                    let from_binding = analyzed
                        .var_binding(edge.from)
                        .expect("edge references known var");
                    let to_binding = analyzed
                        .var_binding(edge.to)
                        .expect("edge references known var");
                    let from_bound = bound_vars.contains(&from_binding.var);
                    let to_bound = bound_vars.contains(&to_binding.var);
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
            let (expand_from, expand_to, direction, target_binding) = if !reverse {
                let from_binding = analyzed.var_binding(edge.from).expect("binding exists");
                let to_binding = analyzed.var_binding(edge.to).expect("binding exists");
                (
                    from_binding.var.clone(),
                    to_binding.var.clone(),
                    edge.direction,
                    to_binding,
                )
            } else {
                let from_binding = analyzed.var_binding(edge.to).expect("binding exists");
                let to_binding = analyzed.var_binding(edge.from).expect("binding exists");
                (
                    from_binding.var.clone(),
                    to_binding.var.clone(),
                    invert_direction(edge.direction),
                    from_binding,
                )
            };

            let base_input = current;
            current = match direction {
                EdgeDirection::Both => {
                    let forward = PlanNode::with_inputs(
                        LogicalOp::Expand {
                            from: expand_from.clone(),
                            to: expand_to.clone(),
                            direction: EdgeDirection::Out,
                            edge_type: edge.edge_type.clone(),
                            distinct_nodes: false,
                        },
                        vec![base_input.clone()],
                    );
                    let reverse = PlanNode::with_inputs(
                        LogicalOp::Expand {
                            from: expand_from.clone(),
                            to: expand_to.clone(),
                            direction: EdgeDirection::In,
                            edge_type: edge.edge_type.clone(),
                            distinct_nodes: false,
                        },
                        vec![base_input],
                    );
                    PlanNode::with_inputs(
                        LogicalOp::Union {
                            vars: vec![expand_from.clone(), expand_to.clone()],
                            dedup: false,
                        },
                        vec![forward, reverse],
                    )
                }
                _ => PlanNode::with_inputs(
                    LogicalOp::Expand {
                        from: expand_from.clone(),
                        to: expand_to.clone(),
                        direction,
                        edge_type: edge.edge_type.clone(),
                        distinct_nodes: false,
                    },
                    vec![base_input],
                ),
            };
            current =
                self.apply_var_predicates(analyzed, current, target_binding.id, &mut preds_by_var)?;
            bound_vars.insert(expand_to);
        }

        if let Some(expr) = &residual_predicate {
            current =
                PlanNode::with_inputs(LogicalOp::BoolFilter { expr: expr.clone() }, vec![current]);
        }

        if analyzed.distinct && !plan_is_inherently_distinct(&current) {
            current = PlanNode::with_inputs(LogicalOp::Distinct, vec![current]);
        }

        if !analyzed.projections.is_empty() {
            current = PlanNode::with_inputs(
                LogicalOp::Project {
                    fields: analyzed.projections.clone(),
                },
                vec![current],
            );
        }

        Ok(LogicalPlan::new(current))
    }

    fn apply_var_predicates(
        &self,
        analyzed: &AnalyzedQuery,
        mut node: PlanNode,
        var_id: VarId,
        preds_by_var: &mut HashMap<VarId, Vec<VarPredicate>>,
    ) -> Result<PlanNode> {
        if let Some(mut preds) = preds_by_var.remove(&var_id) {
            preds.sort_by(|a, b| {
                a.selectivity
                    .partial_cmp(&b.selectivity)
                    .unwrap_or(Ordering::Equal)
            });
            for predicate in preds {
                match predicate.kind {
                    VarPredicateKind::Comparison(cmp) => match cmp {
                        AnalyzedComparison::Eq { .. }
                        | AnalyzedComparison::Lt { .. }
                        | AnalyzedComparison::Le { .. }
                        | AnalyzedComparison::Gt { .. }
                        | AnalyzedComparison::Ge { .. }
                        | AnalyzedComparison::Between { .. } => {
                            node = PlanNode::with_inputs(
                                LogicalOp::Filter {
                                    predicate: cmp_to_prop_predicate(analyzed, &cmp)?,
                                    selectivity: predicate.selectivity,
                                },
                                vec![node],
                            );
                        }
                        _ => {
                            node = PlanNode::with_inputs(
                                LogicalOp::BoolFilter {
                                    expr: AnalyzedExpr::Cmp(cmp),
                                },
                                vec![node],
                            );
                        }
                    },
                    VarPredicateKind::Union { expr, .. } => {
                        node = PlanNode::with_inputs(LogicalOp::BoolFilter { expr }, vec![node]);
                    }
                }
            }
        }
        Ok(node)
    }

    fn select_anchor(
        &self,
        bindings: &[VarBinding],
        preds_by_var: &HashMap<VarId, Vec<VarPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<usize> {
        let mut best_idx = 0;
        let mut best_score = AnchorScore::Label;
        for (idx, binding) in bindings.iter().enumerate() {
            let score = self.anchor_score(binding, preds_by_var, ctx)?;
            if score > best_score {
                best_score = score;
                best_idx = idx;
            }
        }
        Ok(best_idx)
    }

    fn anchor_score(
        &self,
        binding: &VarBinding,
        preds_by_var: &HashMap<VarId, Vec<VarPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<AnchorScore> {
        let Some(preds) = preds_by_var.get(&binding.id) else {
            return Ok(AnchorScore::Label);
        };
        let mut best = AnchorScore::Label;
        for pred in preds {
            match &pred.kind {
                VarPredicateKind::Comparison(cmp) => {
                    if let Some((prop, score_candidate)) = cmp_anchor_class(cmp) {
                        if ctx.property_index(binding.label_id, prop.id)?.is_some() {
                            if score_candidate == AnchorScore::Eq {
                                return Ok(AnchorScore::Eq);
                            }
                            best = AnchorScore::Range;
                        }
                    }
                }
                VarPredicateKind::Union { terms, .. } => {
                    for term in terms {
                        if let Some((prop, score_candidate)) = cmp_anchor_class(&term.cmp) {
                            if ctx.property_index(binding.label_id, prop.id)?.is_some() {
                                if score_candidate == AnchorScore::Eq {
                                    return Ok(AnchorScore::Eq);
                                }
                                best = AnchorScore::Range;
                            }
                        }
                    }
                }
            }
        }
        Ok(best)
    }

    fn take_indexed_predicates(
        &self,
        binding: &VarBinding,
        preds_by_var: &mut HashMap<VarId, Vec<VarPredicate>>,
        ctx: &mut PlanContext<'_>,
    ) -> Result<IndexedSelection> {
        let Some(mut preds) = preds_by_var.remove(&binding.id) else {
            return Ok(IndexedSelection::default());
        };

        let mut selection = IndexedSelection::default();
        let mut indexed_eq: Vec<VarPredicate> = Vec::new();
        let mut indexed_range: Vec<VarPredicate> = Vec::new();
        let mut remaining: Vec<VarPredicate> = Vec::new();

        for predicate in preds.drain(..) {
            let VarPredicate {
                var,
                selectivity,
                kind,
            } = predicate;
            match kind {
                VarPredicateKind::Comparison(cmp) => {
                    let rebuilt = VarPredicate {
                        var,
                        selectivity,
                        kind: VarPredicateKind::Comparison(cmp.clone()),
                    };
                    if let Some((prop, class)) = cmp_anchor_class(&cmp) {
                        if ctx.property_index(binding.label_id, prop.id)?.is_some() {
                            match class {
                                AnchorScore::Eq => indexed_eq.push(rebuilt),
                                AnchorScore::Range => indexed_range.push(rebuilt),
                                AnchorScore::Label => unreachable!("label score not used here"),
                            }
                            continue;
                        }
                    }
                    remaining.push(rebuilt);
                }
                VarPredicateKind::Union { expr, terms } => {
                    if selection.union.is_some() {
                        selection.union_fallback =
                            merge_residual(selection.union_fallback.take(), expr);
                        continue;
                    }
                    if union_terms_indexed(binding, ctx, &terms)? {
                        selection.union = Some(VarPredicate {
                            var,
                            selectivity,
                            kind: VarPredicateKind::Union { expr, terms },
                        });
                    } else {
                        selection.union_fallback =
                            merge_residual(selection.union_fallback.take(), expr);
                    }
                }
            }
        }

        if !remaining.is_empty() {
            preds_by_var.insert(binding.id, remaining);
        }

        let by_selectivity = |a: &VarPredicate, b: &VarPredicate| {
            a.selectivity
                .partial_cmp(&b.selectivity)
                .unwrap_or(Ordering::Equal)
        };
        indexed_eq.sort_by(by_selectivity);
        indexed_range.sort_by(by_selectivity);
        selection.scans.extend(indexed_eq);
        selection.scans.extend(indexed_range);
        Ok(selection)
    }

    fn build_union_scan(
        &self,
        analyzed: &AnalyzedQuery,
        binding: &VarBinding,
        predicate: VarPredicate,
        dedup: bool,
    ) -> Result<PlanNode> {
        let VarPredicate {
            kind: VarPredicateKind::Union { terms, .. },
            ..
        } = predicate
        else {
            return Err(SombraError::Invalid(
                "expected union predicate when building union scan",
            ));
        };
        if terms.is_empty() {
            return Err(SombraError::Invalid("union predicate has no children"));
        }
        let mut children = Vec::with_capacity(terms.len());
        for term in terms {
            children.push(PlanNode::new(LogicalOp::PropIndexScan {
                label: binding.label.clone(),
                label_id: binding.label_id,
                prop: prop_from_cmp(&term.cmp),
                predicate: cmp_to_prop_predicate(analyzed, &term.cmp)?,
                selectivity: term.selectivity,
                as_var: binding.var.clone(),
            }));
        }
        Ok(PlanNode::with_inputs(
            LogicalOp::Union {
                vars: vec![binding.var.clone()],
                dedup,
            },
            children,
        ))
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
            LogicalOp::LabelScan {
                label,
                label_id,
                as_var,
            } => PhysicalOp::LabelScan {
                label: *label_id,
                label_name: label.clone(),
                as_var: as_var.clone(),
            },
            LogicalOp::PropIndexScan {
                label,
                label_id,
                prop: _,
                predicate,
                selectivity,
                as_var,
            } => {
                let pred = self.convert_prop_predicate(predicate, ctx)?;
                let prop = prop_from_predicate(&pred).ok_or(SombraError::Invalid(
                    "property index scans require concrete predicates",
                ))?;
                PhysicalOp::PropIndexScan {
                    label: *label_id,
                    label_name: label.clone(),
                    prop,
                    prop_name: prop_name_from_predicate(&pred),
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
                ty: edge_type.id,
                distinct_nodes: *distinct_nodes,
            },
            LogicalOp::Filter {
                predicate,
                selectivity,
            } => PhysicalOp::Filter {
                pred: self.convert_prop_predicate(predicate, ctx)?,
                selectivity: *selectivity,
            },
            LogicalOp::Union { vars, dedup } => PhysicalOp::Union {
                vars: vars.clone(),
                dedup: *dedup,
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
        _ctx: &mut PlanContext<'_>,
    ) -> Result<PhysicalPredicate> {
        match predicate {
            AstPredicate::Eq { var, prop, value } => Ok(PhysicalPredicate::Eq {
                var: var.clone(),
                prop: prop.id,
                prop_name: prop.name.clone(),
                value: LiteralValue::from(value),
            }),
            AstPredicate::Range {
                var,
                prop,
                lower,
                upper,
            } => Ok(PhysicalPredicate::Range {
                var: var.clone(),
                prop: prop.id,
                prop_name: prop.name.clone(),
                lower: convert_bound(lower),
                upper: convert_bound(upper),
            }),
        }
    }

    fn convert_bool_expr(
        &self,
        expr: &AnalyzedExpr,
        ctx: &mut PlanContext<'_>,
    ) -> Result<PhysicalBoolExpr> {
        match expr {
            AnalyzedExpr::Cmp(cmp) => Ok(PhysicalBoolExpr::Cmp(self.convert_comparison(cmp, ctx)?)),
            AnalyzedExpr::And(children) => {
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
            AnalyzedExpr::Or(children) => {
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
            AnalyzedExpr::Not(child) => {
                let inner = self.convert_bool_expr(child, ctx)?;
                Ok(PhysicalBoolExpr::Not(Box::new(inner)))
            }
        }
    }

    fn convert_comparison(
        &self,
        cmp: &AnalyzedComparison,
        ctx: &PlanContext<'_>,
    ) -> Result<PhysicalComparison> {
        Ok(match cmp {
            AnalyzedComparison::Eq { var, prop, value } => PhysicalComparison::Eq {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
                value: LiteralValue::from(value),
            },
            AnalyzedComparison::Ne { var, prop, value } => PhysicalComparison::Ne {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
                value: LiteralValue::from(value),
            },
            AnalyzedComparison::Lt { var, prop, value } => PhysicalComparison::Lt {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
                value: LiteralValue::from(value),
            },
            AnalyzedComparison::Le { var, prop, value } => PhysicalComparison::Le {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
                value: LiteralValue::from(value),
            },
            AnalyzedComparison::Gt { var, prop, value } => PhysicalComparison::Gt {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
                value: LiteralValue::from(value),
            },
            AnalyzedComparison::Ge { var, prop, value } => PhysicalComparison::Ge {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
                value: LiteralValue::from(value),
            },
            AnalyzedComparison::Between {
                var,
                prop,
                low,
                high,
            } => PhysicalComparison::Between {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
                low: convert_bound(low),
                high: convert_bound(high),
            },
            AnalyzedComparison::In { var, prop, values } => {
                let literals: Vec<LiteralValue> = values.iter().map(LiteralValue::from).collect();
                let lookup = InLookup::from_literals(&literals);
                PhysicalComparison::In {
                    var: ctx.var_for_id(*var),
                    prop: prop.id,
                    prop_name: prop.name.clone(),
                    values: literals,
                    lookup,
                }
            }
            AnalyzedComparison::Exists { var, prop } => PhysicalComparison::Exists {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
            },
            AnalyzedComparison::IsNull { var, prop } => PhysicalComparison::IsNull {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
            },
            AnalyzedComparison::IsNotNull { var, prop } => PhysicalComparison::IsNotNull {
                var: ctx.var_for_id(*var),
                prop: prop.id,
                prop_name: prop.name.clone(),
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

fn convert_projection(proj: AnalyzedProjection, ctx: &PlanContext<'_>) -> Result<ProjectField> {
    match proj {
        AnalyzedProjection::Var { var, alias } => Ok(ProjectField::Var {
            var: ctx.var_for_id(var),
            alias,
        }),
        AnalyzedProjection::Prop { var, prop, alias } => Ok(ProjectField::Prop {
            var: ctx.var_for_id(var),
            prop: prop.id,
            prop_name: prop.name.clone(),
            alias,
        }),
    }
}

fn extract_pushdown_predicates(
    query: &AnalyzedQuery,
    expr: AnalyzedExpr,
    out: &mut Vec<PushdownCandidate>,
) -> Option<AnalyzedExpr> {
    match expr {
        AnalyzedExpr::Cmp(cmp) => match cmp {
            AnalyzedComparison::In { ref values, .. } if values.len() <= MAX_SARGABLE_IN_VALUES => {
                let var = predicate_var(&cmp);
                let eq_terms = expand_in_terms(&cmp);
                out.push(PushdownCandidate::Union {
                    var,
                    expr: AnalyzedExpr::Cmp(cmp),
                    terms: eq_terms,
                });
                None
            }
            _ if is_pushdown_comparison(&cmp) => {
                out.push(PushdownCandidate::Comparison(cmp));
                None
            }
            _ => Some(AnalyzedExpr::Cmp(cmp)),
        },
        AnalyzedExpr::And(children) => {
            let mut residual = Vec::new();
            for child in children {
                match &child {
                    AnalyzedExpr::Not(_) => residual.push(child.clone()),
                    _ => {
                        if let Some(rest) = extract_pushdown_predicates(query, child.clone(), out) {
                            residual.push(rest);
                        }
                    }
                }
            }
            match residual.len() {
                0 => None,
                1 => Some(residual.remove(0)),
                _ => Some(AnalyzedExpr::And(residual)),
            }
        }
        AnalyzedExpr::Or(children) => {
            if let Some(candidate) = build_or_union_candidate(children.clone()) {
                out.push(candidate);
                None
            } else {
                let mut residual = Vec::new();
                for child in children {
                    if let Some(rest) = extract_pushdown_predicates(query, child, out) {
                        residual.push(rest);
                    }
                }
                match residual.len() {
                    0 => None,
                    1 => Some(residual.remove(0)),
                    _ => Some(AnalyzedExpr::Or(residual)),
                }
            }
        }
        AnalyzedExpr::Not(child) => Some(AnalyzedExpr::Not(child)),
    }
}

fn expand_in_terms(cmp: &AnalyzedComparison) -> Vec<AnalyzedComparison> {
    match cmp {
        AnalyzedComparison::In { var, prop, values } => values
            .iter()
            .map(|value| AnalyzedComparison::Eq {
                var: *var,
                prop: prop.clone(),
                value: value.clone(),
            })
            .collect(),
        _ => vec![cmp.clone()],
    }
}

fn build_or_union_candidate(children: Vec<AnalyzedExpr>) -> Option<PushdownCandidate> {
    let mut leaves = Vec::new();
    flatten_or(children.clone(), &mut leaves);
    if leaves.is_empty() {
        return None;
    }
    let fallback = AnalyzedExpr::Or(children);
    let mut terms = Vec::new();
    let mut var_id: Option<VarId> = None;
    for leaf in leaves {
        let cmp = match leaf {
            AnalyzedExpr::Cmp(cmp) => cmp,
            _ => return None,
        };
        let mut cmp_terms = match &cmp {
            AnalyzedComparison::In { values, .. } => {
                if values.len() > MAX_SARGABLE_IN_VALUES {
                    return None;
                }
                expand_in_terms(&cmp)
            }
            _ => {
                if !is_pushdown_comparison(&cmp) {
                    return None;
                }
                vec![cmp.clone()]
            }
        };
        for term in cmp_terms.drain(..) {
            if !is_pushdown_comparison(&term) {
                return None;
            }
            let this_var = predicate_var(&term);
            if let Some(expected) = var_id {
                if expected != this_var {
                    return None;
                }
            } else {
                var_id = Some(this_var);
            }
            terms.push(term);
        }
    }
    let var = var_id?;
    Some(PushdownCandidate::Union {
        var,
        expr: fallback,
        terms,
    })
}

fn flatten_or(exprs: Vec<AnalyzedExpr>, out: &mut Vec<AnalyzedExpr>) {
    for expr in exprs {
        match expr {
            AnalyzedExpr::Or(children) => flatten_or(children, out),
            other => out.push(other),
        }
    }
}

fn is_pushdown_comparison(cmp: &AnalyzedComparison) -> bool {
    match cmp {
        AnalyzedComparison::Eq { .. }
        | AnalyzedComparison::Lt { .. }
        | AnalyzedComparison::Le { .. }
        | AnalyzedComparison::Gt { .. }
        | AnalyzedComparison::Ge { .. }
        | AnalyzedComparison::Between { .. } => true,
        _ => false,
    }
}

fn predicate_var(cmp: &AnalyzedComparison) -> VarId {
    match cmp {
        AnalyzedComparison::Eq { var, .. }
        | AnalyzedComparison::Ne { var, .. }
        | AnalyzedComparison::Lt { var, .. }
        | AnalyzedComparison::Le { var, .. }
        | AnalyzedComparison::Gt { var, .. }
        | AnalyzedComparison::Ge { var, .. }
        | AnalyzedComparison::Between { var, .. }
        | AnalyzedComparison::In { var, .. }
        | AnalyzedComparison::Exists { var, .. }
        | AnalyzedComparison::IsNull { var, .. }
        | AnalyzedComparison::IsNotNull { var, .. } => *var,
    }
}

fn prop_from_cmp(cmp: &AnalyzedComparison) -> PropRef {
    match cmp {
        AnalyzedComparison::Eq { prop, .. }
        | AnalyzedComparison::Ne { prop, .. }
        | AnalyzedComparison::Lt { prop, .. }
        | AnalyzedComparison::Le { prop, .. }
        | AnalyzedComparison::Gt { prop, .. }
        | AnalyzedComparison::Ge { prop, .. }
        | AnalyzedComparison::Between { prop, .. }
        | AnalyzedComparison::In { prop, .. }
        | AnalyzedComparison::Exists { prop, .. }
        | AnalyzedComparison::IsNull { prop, .. }
        | AnalyzedComparison::IsNotNull { prop, .. } => prop.clone(),
    }
}

fn cmp_to_prop_predicate(
    analyzed: &AnalyzedQuery,
    cmp: &AnalyzedComparison,
) -> Result<AstPredicate> {
    let var = analyzed
        .var_binding(predicate_var(cmp))
        .expect("binding exists")
        .var
        .clone();
    match cmp {
        AnalyzedComparison::Eq { prop, value, .. } => Ok(AstPredicate::Eq {
            var,
            prop: prop.clone(),
            value: value.clone(),
        }),
        AnalyzedComparison::Lt { prop, value, .. } => Ok(AstPredicate::Range {
            var,
            prop: prop.clone(),
            lower: Bound::Unbounded,
            upper: Bound::Excluded(value.clone()),
        }),
        AnalyzedComparison::Le { prop, value, .. } => Ok(AstPredicate::Range {
            var,
            prop: prop.clone(),
            lower: Bound::Unbounded,
            upper: Bound::Included(value.clone()),
        }),
        AnalyzedComparison::Gt { prop, value, .. } => Ok(AstPredicate::Range {
            var,
            prop: prop.clone(),
            lower: Bound::Excluded(value.clone()),
            upper: Bound::Unbounded,
        }),
        AnalyzedComparison::Ge { prop, value, .. } => Ok(AstPredicate::Range {
            var,
            prop: prop.clone(),
            lower: Bound::Included(value.clone()),
            upper: Bound::Unbounded,
        }),
        AnalyzedComparison::Between {
            prop, low, high, ..
        } => Ok(AstPredicate::Range {
            var,
            prop: prop.clone(),
            lower: low.clone(),
            upper: high.clone(),
        }),
        _ => Err(SombraError::Invalid(
            "cannot convert comparison into property predicate",
        )),
    }
}

const DEFAULT_EQ_SELECTIVITY: f64 = 0.05;
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.3;
const DEFAULT_FILTER_SELECTIVITY: f64 = 0.25;
const MIN_SELECTIVITY: f64 = 1e-6;
const MAX_SARGABLE_IN_VALUES: usize = 8;

fn predicate_selectivity(
    analyzed: &AnalyzedQuery,
    cmp: &AnalyzedComparison,
    ctx: &mut PlanContext<'_>,
) -> Result<f64> {
    let binding = analyzed
        .var_binding(predicate_var(cmp))
        .expect("binding exists");
    let selectivity = match cmp {
        AnalyzedComparison::Eq { prop, .. } => {
            if let Some(stats) = ctx.property_stats_by_id(binding.label_id, prop.id)? {
                eq_selectivity(stats.as_ref())
            } else {
                DEFAULT_EQ_SELECTIVITY
            }
        }
        AnalyzedComparison::Lt { prop, value, .. } => {
            let upper = Bound::Excluded(value.clone());
            range_stats_selectivity(ctx, binding.label_id, prop.id, &Bound::Unbounded, &upper)?
        }
        AnalyzedComparison::Le { prop, value, .. } => {
            let upper = Bound::Included(value.clone());
            range_stats_selectivity(ctx, binding.label_id, prop.id, &Bound::Unbounded, &upper)?
        }
        AnalyzedComparison::Gt { prop, value, .. } => {
            let lower = Bound::Excluded(value.clone());
            range_stats_selectivity(ctx, binding.label_id, prop.id, &lower, &Bound::Unbounded)?
        }
        AnalyzedComparison::Ge { prop, value, .. } => {
            let lower = Bound::Included(value.clone());
            range_stats_selectivity(ctx, binding.label_id, prop.id, &lower, &Bound::Unbounded)?
        }
        AnalyzedComparison::Between {
            prop, low, high, ..
        } => range_stats_selectivity(ctx, binding.label_id, prop.id, low, high)?,
        AnalyzedComparison::In { prop, values, .. } => {
            let count = values.len().max(1) as f64;
            let per_value =
                if let Some(stats) = ctx.property_stats_by_id(binding.label_id, prop.id)? {
                    eq_selectivity(stats.as_ref())
                } else {
                    DEFAULT_EQ_SELECTIVITY
                };
            per_value * count
        }
        AnalyzedComparison::Exists { prop, .. } | AnalyzedComparison::IsNotNull { prop, .. } => {
            presence_selectivity(ctx, binding.label_id, prop.id)?
        }
        AnalyzedComparison::IsNull { prop, .. } => {
            null_selectivity(ctx, binding.label_id, prop.id)?
        }
        _ => DEFAULT_RANGE_SELECTIVITY,
    };
    Ok(selectivity.clamp(MIN_SELECTIVITY, 1.0))
}

fn union_terms_selectivity(terms: &[UnionTerm]) -> f64 {
    let mut remaining = 1.0;
    for term in terms {
        remaining *= 1.0 - term.selectivity.clamp(MIN_SELECTIVITY, 1.0);
    }
    (1.0 - remaining).clamp(MIN_SELECTIVITY, 1.0)
}

fn range_stats_selectivity(
    ctx: &mut PlanContext<'_>,
    label: LabelId,
    prop: PropId,
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<f64> {
    if let Some(stats) = ctx.property_stats_by_id(label, prop)? {
        Ok(range_selectivity(stats.as_ref(), lower, upper))
    } else {
        Ok(DEFAULT_RANGE_SELECTIVITY)
    }
}

fn presence_selectivity(ctx: &mut PlanContext<'_>, label: LabelId, prop: PropId) -> Result<f64> {
    if let Some(stats) = ctx.property_stats_by_id(label, prop)? {
        Ok(non_null_fraction(stats.as_ref()))
    } else {
        Ok(DEFAULT_FILTER_SELECTIVITY)
    }
}

fn null_selectivity(ctx: &mut PlanContext<'_>, label: LabelId, prop: PropId) -> Result<f64> {
    if let Some(stats) = ctx.property_stats_by_id(label, prop)? {
        Ok(property_null_fraction(stats.as_ref()))
    } else {
        Ok(DEFAULT_FILTER_SELECTIVITY)
    }
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

fn non_null_fraction(stats: &PropStats) -> f64 {
    if stats.row_count == 0 {
        return DEFAULT_FILTER_SELECTIVITY;
    }
    (stats.non_null_count as f64 / stats.row_count as f64).clamp(MIN_SELECTIVITY, 1.0)
}

fn property_null_fraction(stats: &PropStats) -> f64 {
    if stats.row_count == 0 {
        return DEFAULT_FILTER_SELECTIVITY;
    }
    (stats.null_count as f64 / stats.row_count as f64).clamp(MIN_SELECTIVITY, 1.0)
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
    prop_stats: HashMap<(LabelId, PropId), Arc<PropStats>>,
    var_names: HashMap<VarId, Var>,
}

impl<'a> PlanContext<'a> {
    fn new(metadata: &'a dyn MetadataProvider) -> Self {
        Self {
            metadata,
            prop_stats: HashMap::new(),
            var_names: HashMap::new(),
        }
    }

    fn register_bindings(&mut self, bindings: &[VarBinding]) {
        for binding in bindings {
            self.var_names.insert(binding.id, binding.var.clone());
        }
    }

    fn var_for_id(&self, id: VarId) -> Var {
        self.var_names
            .get(&id)
            .expect("unknown variable id")
            .clone()
    }

    fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        self.metadata.property_index(label, prop)
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
enum VarPredicateKind {
    Comparison(AnalyzedComparison),
    Union {
        expr: AnalyzedExpr,
        terms: Vec<UnionTerm>,
    },
}

#[derive(Clone)]
struct VarPredicate {
    var: VarId,
    selectivity: f64,
    kind: VarPredicateKind,
}

#[derive(Clone)]
struct UnionTerm {
    cmp: AnalyzedComparison,
    selectivity: f64,
}

#[derive(Clone, Default)]
struct IndexedSelection {
    scans: Vec<VarPredicate>,
    union: Option<VarPredicate>,
    union_fallback: Option<AnalyzedExpr>,
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

fn compute_plan_hash(analyzed: &AnalyzedQuery, plan: &PhysicalPlan, catalog_epoch: u64) -> u64 {
    let logical = hash_analyzed_query(analyzed, catalog_epoch);
    let physical = hash_physical_plan(plan);
    let mut hasher = Xxh64::new(0);
    hasher.write_u64(logical);
    hasher.write_u64(physical);
    hasher.finish()
}

fn hash_physical_plan(plan: &PhysicalPlan) -> u64 {
    let mut hasher = Xxh64::new(0);
    hash_physical_node(&plan.root, &mut hasher);
    hasher.finish()
}

fn hash_physical_node(node: &PhysicalNode, hasher: &mut Xxh64) {
    hasher.write(op_name(&node.op).as_bytes());
    for prop in op_props(&node.op) {
        hasher.write(prop.key.as_bytes());
        hasher.write(prop.value.as_bytes());
    }
    hasher.write_u64(node.inputs.len() as u64);
    for child in &node.inputs {
        hash_physical_node(child, hasher);
    }
}

fn hash_analyzed_query(analyzed: &AnalyzedQuery, catalog_epoch: u64) -> u64 {
    let mut hasher = Xxh64::new(0);
    hasher.write_u32(analyzed.schema_version());
    hasher.write_u64(catalog_epoch);
    hasher.write_u8(analyzed.distinct as u8);
    hasher.write_u64(analyzed.vars().len() as u64);
    for binding in analyzed.vars() {
        hasher.write(binding.var.0.as_bytes());
        if let Some(label) = &binding.label {
            hasher.write(label.as_bytes());
        }
        hasher.write_u32(binding.label_id.0);
    }
    hasher.write_u64(analyzed.edges.len() as u64);
    for edge in &analyzed.edges {
        hasher.write_u32(edge.from.0);
        hasher.write_u32(edge.to.0);
        hasher.write_u8(match edge.direction {
            EdgeDirection::Out => 0,
            EdgeDirection::In => 1,
            EdgeDirection::Both => 2,
        });
        if let Some(id) = edge.edge_type.id {
            hasher.write_u32(id.0);
        }
        if let Some(name) = &edge.edge_type.name {
            hasher.write(name.as_bytes());
        }
    }
    if let Some(expr) = &analyzed.predicate {
        hash_analyzed_expr(expr, &mut hasher);
    } else {
        hasher.write_u8(0xff);
    }
    hasher.write_u64(analyzed.projections.len() as u64);
    for projection in &analyzed.projections {
        hash_projection(projection, &mut hasher);
    }
    hasher.finish()
}

fn hash_projection(projection: &AnalyzedProjection, hasher: &mut Xxh64) {
    match projection {
        AnalyzedProjection::Var { var, alias } => {
            hasher.write_u8(0);
            hasher.write_u32(var.0);
            if let Some(alias) = alias {
                hasher.write(alias.as_bytes());
            }
        }
        AnalyzedProjection::Prop { var, prop, alias } => {
            hasher.write_u8(1);
            hasher.write_u32(var.0);
            hasher.write(prop.name.as_bytes());
            hasher.write_u32(prop.id.0);
            hasher.write_u8(match prop.collation {
                Collation::Binary => 0,
            });
            if let Some(alias) = alias {
                hasher.write(alias.as_bytes());
            }
        }
    }
}

fn hash_analyzed_expr(expr: &AnalyzedExpr, hasher: &mut Xxh64) {
    match expr {
        AnalyzedExpr::Cmp(cmp) => {
            hasher.write_u8(0);
            hash_analyzed_comparison(cmp, hasher);
        }
        AnalyzedExpr::And(children) => {
            hasher.write_u8(1);
            hasher.write_u64(children.len() as u64);
            for child in children {
                hash_analyzed_expr(child, hasher);
            }
        }
        AnalyzedExpr::Or(children) => {
            hasher.write_u8(2);
            hasher.write_u64(children.len() as u64);
            for child in children {
                hash_analyzed_expr(child, hasher);
            }
        }
        AnalyzedExpr::Not(child) => {
            hasher.write_u8(3);
            hash_analyzed_expr(child, hasher);
        }
    }
}

fn hash_analyzed_comparison(cmp: &AnalyzedComparison, hasher: &mut Xxh64) {
    match cmp {
        AnalyzedComparison::Eq { var, prop, value } => {
            hasher.write_u8(0);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hash_value(value, hasher);
        }
        AnalyzedComparison::Ne { var, prop, value } => {
            hasher.write_u8(1);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hash_value(value, hasher);
        }
        AnalyzedComparison::Lt { var, prop, value } => {
            hasher.write_u8(2);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hash_value(value, hasher);
        }
        AnalyzedComparison::Le { var, prop, value } => {
            hasher.write_u8(3);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hash_value(value, hasher);
        }
        AnalyzedComparison::Gt { var, prop, value } => {
            hasher.write_u8(4);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hash_value(value, hasher);
        }
        AnalyzedComparison::Ge { var, prop, value } => {
            hasher.write_u8(5);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hash_value(value, hasher);
        }
        AnalyzedComparison::Between {
            var,
            prop,
            low,
            high,
        } => {
            hasher.write_u8(6);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hash_bound_value(low, hasher);
            hash_bound_value(high, hasher);
        }
        AnalyzedComparison::In { var, prop, values } => {
            hasher.write_u8(7);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
            hasher.write_u64(values.len() as u64);
            for value in values {
                hash_value(value, hasher);
            }
        }
        AnalyzedComparison::Exists { var, prop } => {
            hasher.write_u8(8);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
        }
        AnalyzedComparison::IsNull { var, prop } => {
            hasher.write_u8(9);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
        }
        AnalyzedComparison::IsNotNull { var, prop } => {
            hasher.write_u8(10);
            hasher.write_u32(var.0);
            hash_prop(prop, hasher);
        }
    }
}

fn hash_prop(prop: &PropRef, hasher: &mut Xxh64) {
    hasher.write(prop.name.as_bytes());
    hasher.write_u32(prop.id.0);
    hasher.write_u8(match prop.collation {
        Collation::Binary => 0,
    });
    match prop.type_hint {
        Some(tag) => {
            hasher.write_u8(1);
            hasher.write_u8(type_tag_to_byte(tag));
        }
        None => hasher.write_u8(0),
    }
}

fn type_tag_to_byte(tag: TypeTag) -> u8 {
    match tag {
        TypeTag::Null => 0,
        TypeTag::Bool => 1,
        TypeTag::Int => 2,
        TypeTag::Float => 3,
        TypeTag::String => 4,
        TypeTag::Bytes => 5,
        TypeTag::Date => 6,
        TypeTag::DateTime => 7,
    }
}

fn hash_value(value: &Value, hasher: &mut Xxh64) {
    match value {
        Value::Null => hasher.write_u8(0),
        Value::Bool(v) => {
            hasher.write_u8(1);
            hasher.write_u8(*v as u8);
        }
        Value::Int(v) => {
            hasher.write_u8(2);
            hasher.write_i64(*v);
        }
        Value::Float(v) => {
            hasher.write_u8(3);
            hasher.write_u64(v.to_bits());
        }
        Value::String(s) => {
            hasher.write_u8(4);
            hasher.write(s.as_bytes());
        }
        Value::Bytes(bytes) => {
            hasher.write_u8(5);
            hasher.write_u64(bytes.len() as u64);
            hasher.write(bytes);
        }
        Value::DateTime(ts) => {
            hasher.write_u8(6);
            hasher.write(&ts.to_le_bytes());
        }
    }
}

fn hash_bound_value(bound: &Bound<Value>, hasher: &mut Xxh64) {
    match bound {
        Bound::Included(value) => {
            hasher.write_u8(0);
            hash_value(value, hasher);
        }
        Bound::Excluded(value) => {
            hasher.write_u8(1);
            hash_value(value, hasher);
        }
        Bound::Unbounded => hasher.write_u8(2),
    }
}

fn op_name(op: &PhysicalOp) -> &'static str {
    match op {
        PhysicalOp::LabelScan { .. } => "LabelScan",
        PhysicalOp::PropIndexScan { .. } => "PropIndexScan",
        PhysicalOp::Expand { .. } => "Expand",
        PhysicalOp::Filter { .. } => "Filter",
        PhysicalOp::BoolFilter { .. } => "BoolFilter",
        PhysicalOp::Union { .. } => "Union",
        PhysicalOp::Intersect { .. } => "Intersect",
        PhysicalOp::HashJoin { .. } => "HashJoin",
        PhysicalOp::Distinct => "Distinct",
        PhysicalOp::Project { .. } => "Project",
    }
}

fn op_props(op: &PhysicalOp) -> Vec<ExplainProp> {
    match op {
        PhysicalOp::LabelScan {
            label,
            label_name,
            as_var,
        } => {
            let mut props = vec![
                ExplainProp::plain("label_id", label.0.to_string()),
                ExplainProp::plain("as", as_var.0.clone()),
            ];
            if let Some(name) = label_name {
                props.insert(0, ExplainProp::plain("label", name.clone()));
            }
            props
        }
        PhysicalOp::PropIndexScan {
            label,
            label_name,
            prop,
            prop_name,
            pred,
            as_var,
            selectivity,
        } => {
            let mut props = vec![
                ExplainProp::plain("label_id", label.0.to_string()),
                ExplainProp::plain("prop_id", prop.0.to_string()),
                ExplainProp::plain("prop", prop_name.clone()),
                ExplainProp::plain("as", as_var.0.clone()),
                ExplainProp::literal("predicate", describe_predicate(pred)),
            ];
            if let Some(name) = label_name {
                props.insert(0, ExplainProp::plain("label", name.clone()));
            }
            props.push(ExplainProp::plain(
                "selectivity",
                fmt_selectivity(*selectivity),
            ));
            props
        }
        PhysicalOp::Expand {
            from,
            to,
            dir,
            ty,
            distinct_nodes,
        } => vec![
            ExplainProp::plain("from", from.0.clone()),
            ExplainProp::plain("to", to.0.clone()),
            ExplainProp::plain("dir", format!("{dir:?}")),
            ExplainProp::plain(
                "type",
                ty.map(|t| t.0.to_string()).unwrap_or_else(|| "*".into()),
            ),
            ExplainProp::plain("distinct", distinct_nodes.to_string()),
        ],
        PhysicalOp::Filter { pred, selectivity } => {
            vec![
                ExplainProp::literal("predicate", describe_predicate(pred)),
                ExplainProp::plain("selectivity", fmt_selectivity(*selectivity)),
            ]
        }
        PhysicalOp::Union { vars, dedup } => vec![
            ExplainProp::plain(
                "vars",
                vars.iter()
                    .map(|v| v.0.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            ExplainProp::plain("dedup", dedup.to_string()),
        ],
        PhysicalOp::BoolFilter { expr } => vec![
            ExplainProp::literal("expr", describe_bool_expr(expr)),
            ExplainProp::plain("selectivity", fmt_selectivity(bool_expr_selectivity(expr))),
        ],
        PhysicalOp::Intersect { vars } => vec![ExplainProp::plain(
            "vars",
            vars.iter()
                .map(|v| v.0.clone())
                .collect::<Vec<_>>()
                .join(", "),
        )],
        PhysicalOp::HashJoin { left, right } => vec![
            ExplainProp::plain("left", left.0.clone()),
            ExplainProp::plain("right", right.0.clone()),
        ],
        PhysicalOp::Distinct => Vec::new(),
        PhysicalOp::Project { fields } => vec![ExplainProp::plain(
            "fields",
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
        PhysicalPredicate::Eq {
            var,
            prop_name,
            value,
            ..
        } => format!("{}.{} = {}", var.0, prop_name, literal_to_string(value)),
        PhysicalPredicate::Range {
            var,
            prop_name,
            lower,
            upper,
            ..
        } => format!(
            "{}.{} in {}..{}",
            var.0,
            prop_name,
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
        PhysicalComparison::Eq {
            var,
            prop_name,
            value,
            ..
        } => format!("{}.{} = {}", var.0, prop_name, literal_to_string(value)),
        PhysicalComparison::Ne {
            var,
            prop_name,
            value,
            ..
        } => format!("{}.{} != {}", var.0, prop_name, literal_to_string(value)),
        PhysicalComparison::Lt {
            var,
            prop_name,
            value,
            ..
        } => format!("{}.{} < {}", var.0, prop_name, literal_to_string(value)),
        PhysicalComparison::Le {
            var,
            prop_name,
            value,
            ..
        } => format!("{}.{} <= {}", var.0, prop_name, literal_to_string(value)),
        PhysicalComparison::Gt {
            var,
            prop_name,
            value,
            ..
        } => format!("{}.{} > {}", var.0, prop_name, literal_to_string(value)),
        PhysicalComparison::Ge {
            var,
            prop_name,
            value,
            ..
        } => format!("{}.{} >= {}", var.0, prop_name, literal_to_string(value)),
        PhysicalComparison::Between {
            var,
            prop_name,
            low,
            high,
            ..
        } => format!(
            "{}.{} in {}..{}",
            var.0,
            prop_name,
            bound_to_string(low),
            bound_to_string(high)
        ),
        PhysicalComparison::In {
            var,
            prop_name,
            values,
            lookup: _,
            ..
        } => format!(
            "{}.{} IN [{}]",
            var.0,
            prop_name,
            values
                .iter()
                .map(literal_to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PhysicalComparison::Exists { var, prop_name, .. } => {
            format!("EXISTS({}.{})", var.0, prop_name)
        }
        PhysicalComparison::IsNull { var, prop_name, .. } => {
            format!("{}.{} IS NULL", var.0, prop_name)
        }
        PhysicalComparison::IsNotNull { var, prop_name, .. } => {
            format!("{}.{} IS NOT NULL", var.0, prop_name)
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

fn prop_name_from_predicate(pred: &PhysicalPredicate) -> String {
    match pred {
        PhysicalPredicate::Eq { prop_name, .. } | PhysicalPredicate::Range { prop_name, .. } => {
            prop_name.clone()
        }
    }
}

fn cmp_anchor_class(cmp: &AnalyzedComparison) -> Option<(PropRef, AnchorScore)> {
    match cmp {
        AnalyzedComparison::Eq { prop, .. } => Some((prop.clone(), AnchorScore::Eq)),
        AnalyzedComparison::Lt { prop, .. }
        | AnalyzedComparison::Le { prop, .. }
        | AnalyzedComparison::Gt { prop, .. }
        | AnalyzedComparison::Ge { prop, .. }
        | AnalyzedComparison::Between { prop, .. } => Some((prop.clone(), AnchorScore::Range)),
        _ => None,
    }
}

fn union_terms_indexed(
    binding: &VarBinding,
    ctx: &mut PlanContext<'_>,
    terms: &[UnionTerm],
) -> Result<bool> {
    for term in terms {
        let Some((prop, _)) = cmp_anchor_class(&term.cmp) else {
            return Ok(false);
        };
        if ctx.property_index(binding.label_id, prop.id)?.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn merge_residual(existing: Option<AnalyzedExpr>, extra: AnalyzedExpr) -> Option<AnalyzedExpr> {
    match existing {
        None => Some(extra),
        Some(current) => Some(AnalyzedExpr::And(vec![current, extra])),
    }
}

fn plan_is_inherently_distinct(node: &PlanNode) -> bool {
    match &node.op {
        LogicalOp::Project { .. } | LogicalOp::Filter { .. } | LogicalOp::BoolFilter { .. } => {
            node.inputs.len() == 1 && plan_is_inherently_distinct(&node.inputs[0])
        }
        LogicalOp::Union { dedup, .. } => *dedup,
        _ => false,
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
        PhysicalComparison::In {
            values, lookup: _, ..
        } => (values.len() as f64 * 0.05).clamp(0.05, 1.0),
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
        let ast = QueryBuilder::new()
            .r#match("User")
            .select(["a"])
            .build()
            .expect("builder succeeds");
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
            .build()
            .expect("builder succeeds");
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
            .build()
            .expect("builder succeeds");
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
            .build()
            .expect("builder succeeds");
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
    fn planner_pushes_down_in_list_as_union() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_property_index(LabelId(1), PropId(4));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .r#match("User")
            .where_var("a", |pred| {
                pred.in_list("name", ["Ada", "Grace"]);
            })
            .select(["a"])
            .build()
            .expect("builder succeeds");
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::Union { vars, dedup } => {
                assert_eq!(vars.len(), 1);
                assert_eq!(vars[0].0, "a");
                assert!(!dedup);
                assert_eq!(project_input.inputs.len(), 2);
                let mut seen = Vec::new();
                for child in &project_input.inputs {
                    match &child.op {
                        PhysicalOp::PropIndexScan { pred, .. } => match pred {
                            PhysicalPredicate::Eq { value, .. } => {
                                if let LiteralValue::String(name) = value {
                                    seen.push(name.clone());
                                } else {
                                    panic!("expected string literal in IN child");
                                }
                            }
                            other => panic!("expected eq predicate, found {other:?}"),
                        },
                        other => panic!("expected PropIndexScan child, found {other:?}"),
                    }
                }
                seen.sort();
                assert_eq!(seen, vec!["Ada".to_string(), "Grace".to_string()]);
            }
            other => panic!("expected Union, found {other:?}"),
        }
    }

    #[test]
    fn planner_lowers_or_expression_to_union() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_property_index(LabelId(1), PropId(4));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .r#match("User")
            .where_var("a", |pred| {
                pred.or_group(|or| {
                    or.eq("name", "Ada");
                    or.eq("name", "Grace");
                });
            })
            .select(["a"])
            .build()
            .expect("builder succeeds");
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::Union { vars, dedup } => {
                assert_eq!(vars, &[Var("a".into())]);
                assert!(!dedup);
                assert_eq!(project_input.inputs.len(), 2);
            }
            other => panic!("expected Union, found {other:?}"),
        }
    }

    #[test]
    fn planner_falls_back_to_filter_when_or_not_indexable() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .r#match("User")
            .where_var("a", |pred| {
                pred.or_group(|or| {
                    or.eq("name", "Ada");
                    or.eq("name", "Grace");
                });
            })
            .select(["a"])
            .build()
            .expect("builder succeeds");
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::BoolFilter { .. } => {}
            other => panic!("expected BoolFilter fallback, found {other:?}"),
        }
    }

    #[test]
    fn planner_marks_union_for_dedup_when_distinct() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_property_index(LabelId(1), PropId(4));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .distinct()
            .r#match("User")
            .where_var("a", |pred| {
                pred.or_group(|or| {
                    or.eq("name", "Ada");
                    or.eq("name", "Grace");
                });
            })
            .select(["a"])
            .build()
            .expect("builder succeeds");
        assert!(ast.distinct);
        let output = planner.plan(&ast).expect("plan succeeds");
        let project_input = output.plan.root.inputs.first().expect("project input");
        match &project_input.op {
            PhysicalOp::Union { dedup, .. } => assert!(*dedup),
            other => panic!("expected Union, found {other:?}"),
        }
    }

    #[test]
    fn planner_skips_distinct_when_union_dedups() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_property_index(LabelId(1), PropId(4));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .distinct()
            .r#match("User")
            .where_var("a", |pred| {
                pred.or_group(|or| {
                    or.eq("name", "Ada");
                    or.eq("name", "Grace");
                });
            })
            .select(["a"])
            .build()
            .expect("builder succeeds");
        assert!(ast.distinct);
        let output = planner.plan(&ast).expect("plan succeeds");
        match &output.plan.root.op {
            PhysicalOp::Project { .. } => {
                let child = output.plan.root.inputs.first().expect("project child");
                match &child.op {
                    PhysicalOp::Union { dedup, .. } => assert!(*dedup),
                    other => panic!("expected union child, found {other:?}"),
                }
            }
            other => panic!("expected project root, found {other:?}"),
        }
    }

    #[test]
    fn planner_keeps_distinct_when_pipeline_not_dedup_safe() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(4))
            .with_property("status", PropId(6))
            .with_edge_type("FOLLOWS", TypeId(5))
            .with_property_index(LabelId(1), PropId(4));
        let planner = Planner::new(PlannerConfig::default(), Arc::new(metadata));
        let ast = QueryBuilder::new()
            .distinct()
            .r#match(("a", "User"))
            .where_edge("FOLLOWS", ("b", "User"))
            .where_var("a", |pred| {
                pred.eq("name", "Ada");
            })
            .select(["a", "b"])
            .build()
            .expect("builder succeeds");
        assert!(ast.distinct);
        let output = planner.plan(&ast).expect("plan succeeds");
        match &output.plan.root.op {
            PhysicalOp::Project { .. } => {
                let distinct = output.plan.root.inputs.first().expect("project child");
                assert!(matches!(distinct.op, PhysicalOp::Distinct));
            }
            other => panic!("expected Project root, found {other:?}"),
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
            .build()
            .expect("builder succeeds");
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
#[derive(Clone)]
enum PushdownCandidate {
    Comparison(AnalyzedComparison),
    Union {
        var: VarId,
        expr: AnalyzedExpr,
        terms: Vec<AnalyzedComparison>,
    },
}
