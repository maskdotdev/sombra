#![forbid(unsafe_code)]
#![allow(missing_docs)]

//! Query normalization and semantic analysis (Phase 3).
//!
//! This module canonicalizes predicate trees, resolves catalog identifiers, and
//! enforces resource limits before planning begins. The resulting
//! [`AnalyzedQuery`] carries `VarId`, `LabelId`, and `PropId` metadata so later
//! planner stages no longer perform ad-hoc catalog lookups.

use crate::query::{
    ast::{
        BoolExpr, Comparison, EdgeClause, EdgeDirection, MatchClause, Projection, QueryAst, Var,
    },
    errors::AnalyzerError,
    metadata::MetadataProvider,
    Value,
};
use crate::storage::index::TypeTag;
use crate::types::{LabelId, PropId, TypeId};
use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

/// Maximum supported number of MATCH clauses per query.
pub const MAX_MATCHES: usize = 1_000;
/// Predicate node budget enforced during analysis.
pub const MAX_PREDICATE_NODES: usize = 10_000;
/// Predicate depth limit to guard against degenerate trees.
pub const MAX_PREDICATE_DEPTH: usize = 256;
/// Maximum number of literals allowed inside `IN` lists.
pub const MAX_IN_VALUES: usize = 10_000;
/// Maximum size for bytes literals (both standalone and aggregate).
pub const MAX_BYTES_LITERAL: usize = 1 << 20;

/// Convenience alias for analyzer results.
pub type AnalyzeResult<T> = std::result::Result<T, AnalyzerError>;

/// Internal identifier assigned to each declared match variable.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct VarId(pub u32);

/// Binding metadata for a match variable.
#[derive(Clone, Debug)]
pub struct VarBinding {
    /// Stable identifier assigned during analysis.
    pub id: VarId,
    /// User-supplied variable symbol.
    pub var: Var,
    /// Optional label string (retained for explain output).
    pub label: Option<String>,
    /// Catalog identifier for the label.
    pub label_id: LabelId,
}

/// Catalog-backed property reference.
#[derive(Clone, Debug)]
pub struct PropRef {
    /// Canonical property name.
    pub name: String,
    /// Property identifier from the catalog.
    pub id: PropId,
    /// Optional type hint supplied by the catalog.
    pub type_hint: Option<TypeTag>,
    /// Collation used for comparisons (binary by default).
    pub collation: Collation,
}

/// String collation identifier attached to predicates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Collation {
    /// Raw UTF-8 binary collation.
    Binary,
}

/// Edge type information for an expansion clause.
#[derive(Clone, Debug, Default)]
pub struct EdgeTypeRef {
    /// Edge type name as written by the caller.
    pub name: Option<String>,
    /// Resolved catalog identifier.
    pub id: Option<TypeId>,
}

/// Edge clause after variable/type resolution.
#[derive(Clone, Debug)]
pub struct AnalyzedEdge {
    /// Source binding identifier.
    pub from: VarId,
    /// Destination binding identifier.
    pub to: VarId,
    /// Traversal direction.
    pub direction: EdgeDirection,
    /// Optional edge type filter.
    pub edge_type: EdgeTypeRef,
}

/// Projection entry produced after analysis.
#[derive(Clone, Debug)]
pub enum AnalyzedProjection {
    /// Variable projection (`kind: "var"`).
    Var {
        /// Binding identifier.
        var: VarId,
        /// Optional alias.
        alias: Option<String>,
    },
    /// Property projection (`kind: "prop"`).
    Prop {
        /// Binding identifier.
        var: VarId,
        /// Resolved property metadata.
        prop: PropRef,
        /// Optional alias.
        alias: Option<String>,
    },
}

/// Typed boolean predicate tree.
#[derive(Clone, Debug)]
pub enum AnalyzedExpr {
    /// Comparison leaf.
    Cmp(AnalyzedComparison),
    /// Conjunction.
    And(Vec<AnalyzedExpr>),
    /// Disjunction.
    Or(Vec<AnalyzedExpr>),
    /// Negation.
    Not(Box<AnalyzedExpr>),
}

/// Comparison operators referencing catalog identifiers.
#[derive(Clone, Debug)]
pub enum AnalyzedComparison {
    Eq {
        var: VarId,
        prop: PropRef,
        value: Value,
    },
    Ne {
        var: VarId,
        prop: PropRef,
        value: Value,
    },
    Lt {
        var: VarId,
        prop: PropRef,
        value: Value,
    },
    Le {
        var: VarId,
        prop: PropRef,
        value: Value,
    },
    Gt {
        var: VarId,
        prop: PropRef,
        value: Value,
    },
    Ge {
        var: VarId,
        prop: PropRef,
        value: Value,
    },
    Between {
        var: VarId,
        prop: PropRef,
        low: Bound<Value>,
        high: Bound<Value>,
    },
    In {
        var: VarId,
        prop: PropRef,
        values: Vec<Value>,
    },
    Exists {
        var: VarId,
        prop: PropRef,
    },
    IsNull {
        var: VarId,
        prop: PropRef,
    },
    IsNotNull {
        var: VarId,
        prop: PropRef,
    },
}

/// Fully analyzed query passed into the planner.
#[derive(Clone, Debug)]
pub struct AnalyzedQuery {
    /// Schema version supplied in the payload.
    pub schema_version: u32,
    /// Optional client-specified identifier.
    pub request_id: Option<String>,
    vars: Vec<VarBinding>,
    var_index: HashMap<String, VarId>,
    /// Match edges after variable/type resolution.
    pub edges: Vec<AnalyzedEdge>,
    /// Normalized predicate referencing property identifiers.
    pub predicate: Option<AnalyzedExpr>,
    /// Distinct flag forwarded from the AST.
    pub distinct: bool,
    /// Projection list referencing analyzed bindings.
    pub projections: Vec<AnalyzedProjection>,
}

impl AnalyzedQuery {
    /// Returns the ordered list of variable bindings.
    pub fn vars(&self) -> &[VarBinding] {
        &self.vars
    }

    /// Looks up the binding metadata for the provided identifier.
    pub fn var_binding(&self, id: VarId) -> Option<&VarBinding> {
        self.vars.iter().find(|binding| binding.id == id)
    }

    /// Looks up binding metadata using the original variable symbol.
    pub fn binding_by_name(&self, var: &Var) -> Option<&VarBinding> {
        self.var_index
            .get(&var.0)
            .and_then(|id| self.var_binding(*id))
    }

    /// Returns the optional request identifier, if present.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Returns the schema version declared in the request.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

/// Runs normalization + semantic analysis, producing an [`AnalyzedQuery`].
pub fn analyze(ast: &QueryAst, metadata: &dyn MetadataProvider) -> AnalyzeResult<AnalyzedQuery> {
    let normalized = normalize(ast)?;
    Analyzer::new(metadata).run(normalized)
}

/// Normalizes projections and predicates to their canonical encodings.
pub fn normalize(ast: &QueryAst) -> AnalyzeResult<QueryAst> {
    let mut normalized = ast.clone();
    normalized.predicate = match normalized.predicate.take() {
        Some(expr) => normalize_expr(expr)?.or_else(|| Some(BoolExpr::And(Vec::new()))),
        None => None,
    };
    normalize_projections(&mut normalized.projections)?;
    Ok(normalized)
}

fn normalize_projections(projections: &mut [Projection]) -> AnalyzeResult<()> {
    for proj in projections {
        if let Projection::Prop { alias, .. } = proj {
            if let Some(alias) = alias {
                if alias.trim().is_empty() {
                    return Err(AnalyzerError::EmptyProjectionAlias);
                }
            }
        }
    }
    Ok(())
}

fn normalize_expr(expr: BoolExpr) -> AnalyzeResult<Option<BoolExpr>> {
    match simplify(expr)? {
        Simplified::True => Ok(None),
        Simplified::False => Ok(Some(BoolExpr::Or(Vec::new()))),
        Simplified::Expr(expr) => Ok(Some(expr)),
    }
}

enum Simplified {
    True,
    False,
    Expr(BoolExpr),
}

fn simplify(expr: BoolExpr) -> AnalyzeResult<Simplified> {
    match expr {
        BoolExpr::Cmp(cmp) => {
            let cmp = canonicalize_comparison(cmp)?;
            Ok(Simplified::Expr(BoolExpr::Cmp(cmp)))
        }
        BoolExpr::Not(child) => match simplify(*child)? {
            Simplified::True => Ok(Simplified::False),
            Simplified::False => Ok(Simplified::True),
            Simplified::Expr(expr) => Ok(Simplified::Expr(negate_expr(expr))),
        },
        BoolExpr::And(children) => {
            let mut flattened = Vec::new();
            for child in children {
                match simplify(child)? {
                    Simplified::True => {}
                    Simplified::False => return Ok(Simplified::False),
                    Simplified::Expr(expr) => match expr {
                        BoolExpr::And(grand) => flattened.extend(grand),
                        other => flattened.push(other),
                    },
                }
            }
            dedup_exprs(&mut flattened);
            match flattened.len() {
                0 => Ok(Simplified::True),
                1 => Ok(Simplified::Expr(flattened.into_iter().next().unwrap())),
                _ => Ok(Simplified::Expr(BoolExpr::And(flattened))),
            }
        }
        BoolExpr::Or(children) => {
            let mut flattened = Vec::new();
            for child in children {
                match simplify(child)? {
                    Simplified::False => {}
                    Simplified::True => return Ok(Simplified::True),
                    Simplified::Expr(expr) => match expr {
                        BoolExpr::Or(grand) => flattened.extend(grand),
                        other => flattened.push(other),
                    },
                }
            }
            dedup_exprs(&mut flattened);
            match flattened.len() {
                0 => Ok(Simplified::False),
                1 => Ok(Simplified::Expr(flattened.into_iter().next().unwrap())),
                _ => Ok(Simplified::Expr(BoolExpr::Or(flattened))),
            }
        }
    }
}

fn canonicalize_comparison(cmp: Comparison) -> AnalyzeResult<Comparison> {
    match cmp {
        Comparison::Between {
            var,
            prop,
            low,
            high,
        } => {
            validate_between_bounds(&low, &high)?;
            Ok(Comparison::Between {
                var,
                prop,
                low,
                high,
            })
        }
        Comparison::In {
            var,
            prop,
            mut values,
        } => {
            canonicalize_in_values(&mut values)?;
            Ok(Comparison::In { var, prop, values })
        }
        other => Ok(other),
    }
}

fn canonicalize_in_values(values: &mut Vec<Value>) -> AnalyzeResult<()> {
    values.retain(|v| !matches!(v, Value::Null));
    if values.is_empty() {
        return Err(AnalyzerError::InListEmpty);
    }
    let mut seen = HashSet::with_capacity(values.len());
    values.retain(|value| seen.insert(value_sort_key(value)));
    values.sort_by(compare_values);
    Ok(())
}

fn dedup_exprs(exprs: &mut Vec<BoolExpr>) {
    if exprs.is_empty() {
        return;
    }
    let mut keyed: Vec<(String, BoolExpr)> = exprs
        .drain(..)
        .map(|expr| (expr_sort_key(&expr), expr))
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    keyed.dedup_by(|a, b| a.0 == b.0);
    *exprs = keyed.into_iter().map(|(_, expr)| expr).collect();
}

fn negate_expr(expr: BoolExpr) -> BoolExpr {
    match expr {
        BoolExpr::Cmp(cmp) => {
            if let Some(negated) = negate_comparison(&cmp) {
                BoolExpr::Cmp(negated)
            } else {
                BoolExpr::Not(Box::new(BoolExpr::Cmp(cmp)))
            }
        }
        BoolExpr::Not(inner) => *inner,
        other => BoolExpr::Not(Box::new(other)),
    }
}

fn negate_comparison(cmp: &Comparison) -> Option<Comparison> {
    Some(match cmp {
        Comparison::Eq { var, prop, value } => Comparison::Ne {
            var: var.clone(),
            prop: prop.clone(),
            value: value.clone(),
        },
        Comparison::Ne { var, prop, value } => Comparison::Eq {
            var: var.clone(),
            prop: prop.clone(),
            value: value.clone(),
        },
        Comparison::Lt { var, prop, value } => Comparison::Ge {
            var: var.clone(),
            prop: prop.clone(),
            value: value.clone(),
        },
        Comparison::Le { var, prop, value } => Comparison::Gt {
            var: var.clone(),
            prop: prop.clone(),
            value: value.clone(),
        },
        Comparison::Gt { var, prop, value } => Comparison::Le {
            var: var.clone(),
            prop: prop.clone(),
            value: value.clone(),
        },
        Comparison::Ge { var, prop, value } => Comparison::Lt {
            var: var.clone(),
            prop: prop.clone(),
            value: value.clone(),
        },
        Comparison::IsNull { var, prop } => Comparison::IsNotNull {
            var: var.clone(),
            prop: prop.clone(),
        },
        Comparison::IsNotNull { var, prop } => Comparison::IsNull {
            var: var.clone(),
            prop: prop.clone(),
        },
        _ => return None,
    })
}

fn expr_sort_key(expr: &BoolExpr) -> String {
    match expr {
        BoolExpr::Cmp(cmp) => format!("cmp:{}", comparison_sort_key(cmp)),
        BoolExpr::Not(child) => format!("not:{}", expr_sort_key(child)),
        BoolExpr::And(children) => {
            let mut child_keys: Vec<String> = children.iter().map(expr_sort_key).collect();
            child_keys.sort();
            format!("and:{}", child_keys.join("|"))
        }
        BoolExpr::Or(children) => {
            let mut child_keys: Vec<String> = children.iter().map(expr_sort_key).collect();
            child_keys.sort();
            format!("or:{}", child_keys.join("|"))
        }
    }
}

fn comparison_sort_key(cmp: &Comparison) -> String {
    match cmp {
        Comparison::Eq { var, prop, value } => {
            format!("eq:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Ne { var, prop, value } => {
            format!("ne:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Lt { var, prop, value } => {
            format!("lt:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Le { var, prop, value } => {
            format!("le:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Gt { var, prop, value } => {
            format!("gt:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Ge { var, prop, value } => {
            format!("ge:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Between {
            var,
            prop,
            low,
            high,
        } => format!(
            "between:{}:{}:{}:{}",
            var.0,
            prop,
            bound_sort_key(low),
            bound_sort_key(high)
        ),
        Comparison::In { var, prop, values } => {
            let mut value_keys: Vec<String> = values.iter().map(value_sort_key).collect();
            value_keys.sort();
            format!("in:{}:{}:{}", var.0, prop, value_keys.join(","))
        }
        Comparison::Exists { var, prop } => format!("exists:{}:{}", var.0, prop),
        Comparison::IsNull { var, prop } => format!("isnull:{}:{}", var.0, prop),
        Comparison::IsNotNull { var, prop } => format!("isnotnull:{}:{}", var.0, prop),
    }
}

fn bound_sort_key(bound: &Bound<Value>) -> String {
    match bound {
        Bound::Included(v) => format!("inc:{}", value_sort_key(v)),
        Bound::Excluded(v) => format!("exc:{}", value_sort_key(v)),
        Bound::Unbounded => "unbounded".into(),
    }
}

fn value_sort_key(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(v) => format!("bool:{v}"),
        Value::Int(v) => format!("int:{v}"),
        Value::Float(v) => format!("float:{v}"),
        Value::String(v) => format!("str:{v}"),
        Value::Bytes(v) => format!("bytes:{}", BASE64_ENGINE.encode(v)),
        Value::DateTime(v) => format!("datetime:{v}"),
    }
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a
            .partial_cmp(b)
            .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan())),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
        (_left, _right) => type_rank(left).cmp(&type_rank(right)),
    }
}

fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Float(_) => 3,
        Value::String(_) => 4,
        Value::Bytes(_) => 5,
        Value::DateTime(_) => 6,
    }
}

fn sort_values_by_collation(values: &mut [Value], collation: &Collation) {
    match collation {
        Collation::Binary => values.sort_by(compare_values),
    }
}

fn validate_between_bounds(low: &Bound<Value>, high: &Bound<Value>) -> AnalyzeResult<()> {
    match (extract_bound_value(low), extract_bound_value(high)) {
        (Some(a), Some(b)) => {
            if compare_values(a, b) == Ordering::Greater {
                return Err(AnalyzerError::InvalidBounds);
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn extract_bound_value(bound: &Bound<Value>) -> Option<&Value> {
    match bound {
        Bound::Included(v) | Bound::Excluded(v) => Some(v),
        Bound::Unbounded => None,
    }
}

/// Analyzer state that resolves variables and properties to catalog ids.
struct Analyzer<'m> {
    metadata: &'m dyn MetadataProvider,
    vars: Vec<VarBinding>,
    var_index: HashMap<String, VarId>,
    prop_cache: HashMap<String, PropRef>,
}

impl<'m> Analyzer<'m> {
    fn new(metadata: &'m dyn MetadataProvider) -> Self {
        Self {
            metadata,
            vars: Vec::new(),
            var_index: HashMap::new(),
            prop_cache: HashMap::new(),
        }
    }

    fn run(mut self, ast: QueryAst) -> AnalyzeResult<AnalyzedQuery> {
        let request_id = ast.request_id.clone();
        let schema_version = ast.schema_version;
        self.process_matches(&ast.matches)?;
        let edges = self.process_edges(&ast.edges)?;
        let predicate = match ast.predicate {
            Some(expr) => {
                self.validate_predicate_limits(&expr)?;
                Some(self.analyze_expr(expr)?)
            }
            None => None,
        };
        let projections = self.process_projections(&ast.projections)?;
        let Analyzer {
            vars, var_index, ..
        } = self;
        Ok(AnalyzedQuery {
            schema_version,
            request_id,
            vars,
            var_index,
            edges,
            predicate,
            distinct: ast.distinct,
            projections,
        })
    }

    fn process_matches(&mut self, matches: &[MatchClause]) -> AnalyzeResult<()> {
        if matches.is_empty() {
            return Err(AnalyzerError::EmptyMatches);
        }
        if matches.len() > MAX_MATCHES {
            return Err(AnalyzerError::TooManyMatches {
                count: matches.len(),
                max: MAX_MATCHES,
            });
        }
        for clause in matches {
            let name = clause.var.0.clone();
            if self.var_index.contains_key(&name) {
                return Err(AnalyzerError::DuplicateVariable { var: name });
            }
            let label = clause
                .label
                .clone()
                .ok_or_else(|| AnalyzerError::MatchMissingLabel {
                    var: clause.var.0.clone(),
                })?;
            let label_id =
                self.metadata
                    .resolve_label(&label)
                    .map_err(|_| AnalyzerError::UnknownLabel {
                        label: label.clone(),
                    })?;
            let id = VarId(self.vars.len() as u32);
            let binding = VarBinding {
                id,
                var: clause.var.clone(),
                label: Some(label),
                label_id,
            };
            self.var_index.insert(name, id);
            self.vars.push(binding);
        }
        Ok(())
    }

    fn process_edges(&self, edges: &[EdgeClause]) -> AnalyzeResult<Vec<AnalyzedEdge>> {
        let mut out = Vec::with_capacity(edges.len());
        for edge in edges {
            let from = self.require_var(&edge.from, "edge")?;
            let to = self.require_var(&edge.to, "edge")?;
            if from == to {
                return Err(AnalyzerError::EdgeReflexiveNotAllowed {
                    var: edge.from.0.clone(),
                });
            }
            let edge_type = match &edge.edge_type {
                Some(name) => EdgeTypeRef {
                    name: Some(name.clone()),
                    id: Some(self.metadata.resolve_edge_type(name).map_err(|_| {
                        AnalyzerError::UnknownEdgeType {
                            edge_type: name.clone(),
                        }
                    })?),
                },
                None => EdgeTypeRef::default(),
            };
            out.push(AnalyzedEdge {
                from,
                to,
                direction: edge.direction,
                edge_type,
            });
        }
        Ok(out)
    }

    fn process_projections(
        &mut self,
        projections: &[Projection],
    ) -> AnalyzeResult<Vec<AnalyzedProjection>> {
        let mut out = Vec::with_capacity(projections.len());
        for projection in projections {
            match projection {
                Projection::Var { var, alias } => {
                    let var_id = self.require_var(var, "projection")?;
                    out.push(AnalyzedProjection::Var {
                        var: var_id,
                        alias: alias.clone(),
                    });
                }
                Projection::Prop { var, prop, alias } => {
                    let var_id = self.require_var(var, "projection")?;
                    let prop_ref = self.property(prop)?;
                    out.push(AnalyzedProjection::Prop {
                        var: var_id,
                        prop: prop_ref,
                        alias: alias.clone(),
                    });
                }
            }
        }
        Ok(out)
    }

    fn analyze_expr(&mut self, expr: BoolExpr) -> AnalyzeResult<AnalyzedExpr> {
        Ok(match expr {
            BoolExpr::Cmp(cmp) => AnalyzedExpr::Cmp(self.analyze_comparison(cmp)?),
            BoolExpr::And(children) => {
                let mut analyzed = Vec::with_capacity(children.len());
                for child in children {
                    analyzed.push(self.analyze_expr(child)?);
                }
                AnalyzedExpr::And(analyzed)
            }
            BoolExpr::Or(children) => {
                let mut analyzed = Vec::with_capacity(children.len());
                for child in children {
                    analyzed.push(self.analyze_expr(child)?);
                }
                AnalyzedExpr::Or(analyzed)
            }
            BoolExpr::Not(child) => AnalyzedExpr::Not(Box::new(self.analyze_expr(*child)?)),
        })
    }

    fn analyze_comparison(&mut self, cmp: Comparison) -> AnalyzeResult<AnalyzedComparison> {
        match cmp {
            Comparison::Eq { var, prop, value } => {
                self.validate_scalar(&value)?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Eq {
                    var: var_id,
                    prop: prop_ref,
                    value,
                })
            }
            Comparison::Ne { var, prop, value } => {
                self.validate_scalar(&value)?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Ne {
                    var: var_id,
                    prop: prop_ref,
                    value,
                })
            }
            Comparison::Lt { var, prop, value } => {
                self.validate_orderable(&value, "lt()")?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Lt {
                    var: var_id,
                    prop: prop_ref,
                    value,
                })
            }
            Comparison::Le { var, prop, value } => {
                self.validate_orderable(&value, "le()")?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Le {
                    var: var_id,
                    prop: prop_ref,
                    value,
                })
            }
            Comparison::Gt { var, prop, value } => {
                self.validate_orderable(&value, "gt()")?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Gt {
                    var: var_id,
                    prop: prop_ref,
                    value,
                })
            }
            Comparison::Ge { var, prop, value } => {
                self.validate_orderable(&value, "ge()")?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Ge {
                    var: var_id,
                    prop: prop_ref,
                    value,
                })
            }
            Comparison::Between {
                var,
                prop,
                low,
                high,
            } => {
                self.validate_bound(&low, "between()")?;
                self.validate_bound(&high, "between()")?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Between {
                    var: var_id,
                    prop: prop_ref,
                    low,
                    high,
                })
            }
            Comparison::In {
                var,
                prop,
                mut values,
            } => {
                self.validate_in_literals(&values)?;
                canonicalize_in_values(&mut values)?;
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                sort_values_by_collation(&mut values, &prop_ref.collation);
                Ok(AnalyzedComparison::In {
                    var: var_id,
                    prop: prop_ref,
                    values,
                })
            }
            Comparison::Exists { var, prop } => {
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::Exists {
                    var: var_id,
                    prop: prop_ref,
                })
            }
            Comparison::IsNull { var, prop } => {
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::IsNull {
                    var: var_id,
                    prop: prop_ref,
                })
            }
            Comparison::IsNotNull { var, prop } => {
                let (var_id, prop_ref) = self.resolve_var_prop(&var, &prop, "predicate")?;
                Ok(AnalyzedComparison::IsNotNull {
                    var: var_id,
                    prop: prop_ref,
                })
            }
        }
    }

    fn validate_predicate_limits(&self, expr: &BoolExpr) -> AnalyzeResult<()> {
        let stats = predicate_stats(expr);
        if stats.nodes > MAX_PREDICATE_NODES {
            return Err(AnalyzerError::PredicateTooLarge {
                nodes: stats.nodes,
                max: MAX_PREDICATE_NODES,
            });
        }
        if stats.depth > MAX_PREDICATE_DEPTH {
            return Err(AnalyzerError::PredicateTooDeep {
                depth: stats.depth,
                max: MAX_PREDICATE_DEPTH,
            });
        }
        Ok(())
    }

    fn validate_scalar(&self, value: &Value) -> AnalyzeResult<()> {
        validate_scalar_value(value)
    }

    fn validate_orderable(&self, value: &Value, ctx: &'static str) -> AnalyzeResult<()> {
        validate_scalar_value(value)?;
        ensure_orderable(value, ctx)
    }

    fn validate_bound(&self, bound: &Bound<Value>, ctx: &'static str) -> AnalyzeResult<()> {
        if let Some(value) = extract_bound_value(bound) {
            self.validate_orderable(value, ctx)?;
        }
        Ok(())
    }

    fn validate_in_literals(&self, values: &[Value]) -> AnalyzeResult<()> {
        if values.is_empty() {
            return Err(AnalyzerError::InListEmpty);
        }
        if values.len() > MAX_IN_VALUES {
            return Err(AnalyzerError::InListTooLarge { max: MAX_IN_VALUES });
        }
        let mut total_bytes = 0usize;
        for value in values {
            self.validate_scalar(value)?;
            if let Value::Bytes(bytes) = value {
                total_bytes = total_bytes.checked_add(bytes.len()).ok_or(
                    AnalyzerError::BytesLiteralTooLarge {
                        max: MAX_BYTES_LITERAL,
                    },
                )?;
            }
            if total_bytes > MAX_BYTES_LITERAL {
                return Err(AnalyzerError::BytesLiteralTooLarge {
                    max: MAX_BYTES_LITERAL,
                });
            }
        }
        Ok(())
    }

    fn require_var(&self, var: &Var, context: &'static str) -> AnalyzeResult<VarId> {
        self.var_index
            .get(&var.0)
            .copied()
            .ok_or_else(|| AnalyzerError::var_not_matched(var.0.clone(), context))
    }

    fn property(&mut self, name: &str) -> AnalyzeResult<PropRef> {
        if let Some(prop) = self.prop_cache.get(name) {
            return Ok(prop.clone());
        }
        let id =
            self.metadata
                .resolve_property(name)
                .map_err(|_| AnalyzerError::UnknownProperty {
                    prop: name.to_owned(),
                })?;
        let type_hint = self.metadata.property_type_hint(id).unwrap_or(None);
        let prop = PropRef {
            name: name.to_owned(),
            id,
            type_hint,
            collation: Collation::Binary,
        };
        self.prop_cache.insert(name.to_owned(), prop.clone());
        Ok(prop)
    }

    fn binding_for_id(&self, id: VarId) -> Option<&VarBinding> {
        self.vars.iter().find(|binding| binding.id == id)
    }

    fn ensure_property_visible(&self, var_id: VarId, prop: &PropRef) -> AnalyzeResult<()> {
        let binding = self
            .binding_for_id(var_id)
            .expect("variable registered before property usage");
        let allowed = self
            .metadata
            .label_has_property(binding.label_id, prop.id)
            .map_err(|_| AnalyzerError::UnknownProperty {
                prop: prop.name.clone(),
            })?;
        if !allowed {
            return Err(AnalyzerError::PropertyNotInLabel {
                label: binding
                    .label
                    .clone()
                    .unwrap_or_else(|| binding.var.0.clone()),
                prop: prop.name.clone(),
            });
        }
        Ok(())
    }

    fn resolve_var_prop(
        &mut self,
        var: &Var,
        prop: &str,
        context: &'static str,
    ) -> AnalyzeResult<(VarId, PropRef)> {
        let var_id = self.require_var(var, context)?;
        let prop_ref = self.property(prop)?;
        self.ensure_property_visible(var_id, &prop_ref)?;
        Ok((var_id, prop_ref))
    }
}

struct PredicateStats {
    nodes: usize,
    depth: usize,
}

fn predicate_stats(expr: &BoolExpr) -> PredicateStats {
    match expr {
        BoolExpr::Cmp(_) => PredicateStats { nodes: 1, depth: 1 },
        BoolExpr::And(children) | BoolExpr::Or(children) => {
            let mut nodes = 1;
            let mut max_depth = 0;
            for child in children {
                let stats = predicate_stats(child);
                nodes += stats.nodes;
                max_depth = max_depth.max(stats.depth);
            }
            PredicateStats {
                nodes,
                depth: max_depth + 1,
            }
        }
        BoolExpr::Not(child) => {
            let stats = predicate_stats(child);
            PredicateStats {
                nodes: stats.nodes + 1,
                depth: stats.depth + 1,
            }
        }
    }
}

fn validate_scalar_value(value: &Value) -> AnalyzeResult<()> {
    match value {
        Value::Float(v) if !v.is_finite() => Err(AnalyzerError::NonFiniteFloat),
        Value::Bytes(bytes) if bytes.len() > MAX_BYTES_LITERAL => {
            Err(AnalyzerError::BytesLiteralTooLarge {
                max: MAX_BYTES_LITERAL,
            })
        }
        Value::DateTime(ts) if *ts < i64::MIN as i128 || *ts > i64::MAX as i128 => {
            Err(AnalyzerError::DateTimeOutOfRange)
        }
        _ => Ok(()),
    }
}

fn ensure_orderable(value: &Value, ctx: &'static str) -> AnalyzeResult<()> {
    match value {
        Value::Int(_) | Value::Float(_) | Value::String(_) | Value::DateTime(_) => Ok(()),
        Value::Bytes(_) => Err(AnalyzerError::BytesRangeUnsupported { context: ctx }),
        Value::Null => Err(AnalyzerError::NullNotAllowed { context: ctx }),
        Value::Bool(_) => Err(AnalyzerError::RangeTypeMismatch { context: ctx }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{BoolExpr, Comparison, EdgeClause, MatchClause, Projection, Var};
    use crate::query::metadata::InMemoryMetadata;
    use crate::types::{LabelId, PropId, TypeId};

    fn var(name: &str) -> Var {
        Var(name.to_string())
    }

    fn metadata() -> InMemoryMetadata {
        InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_label("Movie", LabelId(2))
            .with_property("name", PropId(3))
            .with_property("age", PropId(4))
            .with_edge_type("FOLLOWS", TypeId(5))
    }

    #[test]
    fn removes_duplicate_and_children() {
        let expr = BoolExpr::And(vec![
            BoolExpr::Cmp(Comparison::Exists {
                var: var("a"),
                prop: "name".into(),
            }),
            BoolExpr::Cmp(Comparison::Exists {
                var: var("a"),
                prop: "name".into(),
            }),
        ]);
        let ast = QueryAst {
            schema_version: 1,
            request_id: None,
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        let normalized = normalize(&ast).expect("normalize succeeds");
        assert!(matches!(
            normalized.predicate,
            Some(BoolExpr::Cmp(Comparison::Exists { .. }))
        ));
    }

    #[test]
    fn canonicalizes_in_values() {
        let expr = BoolExpr::Cmp(Comparison::In {
            var: var("a"),
            prop: "name".into(),
            values: vec![
                Value::String("b".into()),
                Value::Null,
                Value::String("a".into()),
                Value::String("a".into()),
            ],
        });
        let ast = QueryAst {
            schema_version: 1,
            request_id: None,
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        let normalized = normalize(&ast).expect("normalize succeeds");
        match normalized.predicate.unwrap() {
            BoolExpr::Cmp(Comparison::In { values, .. }) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Value::String("a".into()));
                assert_eq!(values[1], Value::String("b".into()));
            }
            other => panic!("unexpected predicate {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_in_after_nulls_removed() {
        let expr = BoolExpr::Cmp(Comparison::In {
            var: var("a"),
            prop: "name".into(),
            values: vec![Value::Null],
        });
        let ast = QueryAst {
            schema_version: 1,
            request_id: None,
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        let err = normalize(&ast).expect_err("normalize should fail");
        assert!(matches!(err, AnalyzerError::InListEmpty));
    }

    #[test]
    fn enforces_between_ordering() {
        let expr = BoolExpr::Cmp(Comparison::Between {
            var: var("a"),
            prop: "age".into(),
            low: Bound::Included(Value::Int(10)),
            high: Bound::Included(Value::Int(5)),
        });
        let ast = QueryAst {
            schema_version: 1,
            request_id: None,
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        assert!(matches!(
            normalize(&ast).unwrap_err(),
            AnalyzerError::InvalidBounds
        ));
    }

    #[test]
    fn empty_and_normalizes_to_true() {
        let ast = QueryAst {
            predicate: Some(BoolExpr::And(vec![])),
            ..Default::default()
        };
        let normalized = normalize(&ast).expect("normalize succeeds");
        assert!(matches!(
            normalized.predicate,
            Some(BoolExpr::And(children)) if children.is_empty()
        ));
    }

    #[test]
    fn empty_or_represents_false() {
        let ast = QueryAst {
            predicate: Some(BoolExpr::Or(vec![])),
            ..Default::default()
        };
        let normalized = normalize(&ast).expect("normalize succeeds");
        assert!(matches!(
            normalized.predicate,
            Some(BoolExpr::Or(children)) if children.is_empty()
        ));
    }

    #[test]
    fn analyze_attaches_ids() {
        let mut ast = QueryAst::default();
        ast.matches.push(MatchClause {
            var: var("a"),
            label: Some("User".into()),
        });
        ast.predicate = Some(BoolExpr::Cmp(Comparison::Eq {
            var: var("a"),
            prop: "age".into(),
            value: Value::Int(42),
        }));
        ast.projections.push(Projection::Prop {
            var: var("a"),
            prop: "name".into(),
            alias: None,
        });
        let analyzed = analyze(&ast, &metadata()).expect("analysis succeeds");
        assert_eq!(analyzed.vars().len(), 1);
        let binding = &analyzed.vars()[0];
        assert_eq!(binding.id, VarId(0));
        assert_eq!(binding.label_id, LabelId(1));
        match analyzed.predicate {
            Some(AnalyzedExpr::Cmp(AnalyzedComparison::Eq { ref prop, .. })) => {
                assert_eq!(prop.id, PropId(4));
                assert!(prop.type_hint.is_none());
            }
            _ => panic!("predicate mismatch"),
        }
        match analyzed.projections.as_slice() {
            [AnalyzedProjection::Prop { prop, .. }] => assert_eq!(prop.id, PropId(3)),
            other => panic!("unexpected projections {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_variables() {
        let mut ast = QueryAst::default();
        ast.matches.push(MatchClause {
            var: var("a"),
            label: Some("User".into()),
        });
        ast.predicate = Some(BoolExpr::Cmp(Comparison::Eq {
            var: var("b"),
            prop: "age".into(),
            value: Value::Int(1),
        }));
        let err = analyze(&ast, &metadata()).expect_err("analysis should fail");
        assert!(matches!(err, AnalyzerError::VarNotMatched { .. }));
    }

    #[test]
    fn rejects_property_not_in_label() {
        let metadata = InMemoryMetadata::new()
            .with_label("User", LabelId(1))
            .with_property("name", PropId(3))
            .with_label_props(LabelId(1), Vec::<PropId>::new());
        let mut ast = QueryAst::default();
        ast.matches.push(MatchClause {
            var: var("a"),
            label: Some("User".into()),
        });
        ast.predicate = Some(BoolExpr::Cmp(Comparison::Eq {
            var: var("a"),
            prop: "name".into(),
            value: Value::String("Ada".into()),
        }));
        let err = analyze(&ast, &metadata).expect_err("analysis should fail");
        assert!(matches!(err, AnalyzerError::PropertyNotInLabel { .. }));
    }

    #[test]
    fn rejects_reflexive_edges() {
        let mut ast = QueryAst::default();
        ast.matches.push(MatchClause {
            var: var("a"),
            label: Some("User".into()),
        });
        ast.edges.push(EdgeClause {
            from: var("a"),
            to: var("a"),
            edge_type: None,
            direction: EdgeDirection::Out,
        });
        let err = analyze(&ast, &metadata()).expect_err("analysis should fail");
        assert!(matches!(err, AnalyzerError::EdgeReflexiveNotAllowed { .. }));
    }

    #[test]
    fn rejects_bytes_range_predicate() {
        let mut ast = QueryAst::default();
        ast.matches.push(MatchClause {
            var: var("a"),
            label: Some("User".into()),
        });
        ast.predicate = Some(BoolExpr::Cmp(Comparison::Gt {
            var: var("a"),
            prop: "avatar".into(),
            value: Value::Bytes(vec![1, 2, 3]),
        }));
        let err = analyze(&ast, &metadata()).expect_err("analysis should fail");
        assert!(matches!(
            err,
            AnalyzerError::BytesRangeUnsupported { context: "gt()" }
        ));
    }

    #[test]
    fn not_eq_normalizes_to_ne() {
        let mut ast = QueryAst::default();
        ast.predicate = Some(BoolExpr::Not(Box::new(BoolExpr::Cmp(Comparison::Eq {
            var: var("a"),
            prop: "name".into(),
            value: Value::String("Ada".into()),
        }))));
        let normalized = normalize(&ast).expect("normalize succeeds");
        match normalized.predicate {
            Some(BoolExpr::Cmp(Comparison::Ne { var, prop, value })) => {
                assert_eq!(var.0, "a");
                assert_eq!(prop, "name");
                assert_eq!(value, Value::String("Ada".into()));
            }
            other => panic!("expected Ne comparison, got {other:?}"),
        }
    }

    #[test]
    fn not_is_null_normalizes_to_is_not_null() {
        let mut ast = QueryAst::default();
        ast.predicate = Some(BoolExpr::Not(Box::new(BoolExpr::Cmp(Comparison::IsNull {
            var: var("a"),
            prop: "name".into(),
        }))));
        let normalized = normalize(&ast).expect("normalize succeeds");
        match normalized.predicate {
            Some(BoolExpr::Cmp(Comparison::IsNotNull { var, prop })) => {
                assert_eq!(var.0, "a");
                assert_eq!(prop, "name");
            }
            other => panic!("expected IsNotNull comparison, got {other:?}"),
        }
    }
}
