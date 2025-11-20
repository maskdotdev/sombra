//! Fluent query builder scaffolding.

use crate::query::{
    ast::{
        BoolExpr, Comparison, EdgeClause, EdgeDirection, MatchClause, Projection, QueryAst, Var,
    },
    executor::{Executor, QueryResult},
    planner::{PlanExplain, Planner, PlannerOutput},
    Value,
};
use crate::types::{Result, SombraError};
use std::{mem, ops::Bound};

/// Fluent builder matching the Stage 8 ergonomics.
#[derive(Default)]
pub struct QueryBuilder {
    ast: QueryAst,
    last_var: Option<Var>,
    next_var_idx: usize,
    pending_direction: EdgeDirection,
    error: Option<SombraError>,
}

impl QueryBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self {
            ast: QueryAst::default(),
            last_var: None,
            next_var_idx: 0,
            pending_direction: EdgeDirection::Out,
            error: None,
        }
    }

    /// Entry-point for constructing the builder.
    pub fn start() -> Self {
        Self::new()
    }

    /// Adds a node match clause.
    pub fn r#match<T>(mut self, target: T) -> Self
    where
        T: Into<MatchTarget>,
    {
        if self.error.is_some() {
            return self;
        }
        let (var, label) = target.into().into_parts(self.next_auto_var());
        self.ast.matches.push(MatchClause {
            var: var.clone(),
            label,
        });
        self.last_var = Some(var);
        self
    }

    /// Adds an edge clause pointing to the supplied target.
    pub fn where_edge<E, T>(mut self, edge: E, target: T) -> Self
    where
        E: Into<EdgeSpec>,
        T: Into<MatchTarget>,
    {
        if self.error.is_some() {
            return self;
        }
        let from = self.last_var.clone().or_else(|| {
            self.error = Some(SombraError::Invalid(
                "where_edge requires an existing left variable",
            ));
            None
        });
        let Some(from) = from else {
            return self;
        };
        let target = target.into();
        let (to, label) = target.into_parts(self.next_auto_var());
        let edge_spec: EdgeSpec = edge.into();

        // Ensure the destination node exists in the AST.
        if !self.ast.matches.iter().any(|m| m.var == to) {
            self.ast.matches.push(MatchClause {
                var: to.clone(),
                label,
            });
        }

        self.ast.edges.push(EdgeClause {
            from,
            to: to.clone(),
            edge_type: edge_spec.edge_type,
            direction: self.pending_direction,
        });

        self.last_var = Some(to);
        self.pending_direction = EdgeDirection::Out;
        self
    }

    /// Adds predicates for a specific variable using the supplied builder.
    pub fn where_var<S, F>(mut self, var: S, build: F) -> Self
    where
        S: Into<String>,
        F: FnOnce(&mut PredicateBuilder),
    {
        if self.error.is_some() {
            return self;
        }
        let var = Var(var.into());
        let mut builder = PredicateBuilder::new(var);
        build(&mut builder);
        if let Some(err) = builder.error {
            self.error = Some(err);
            return self;
        }
        let expr = match builder.finish() {
            Some(expr) => expr,
            None => {
                self.error = Some(SombraError::Invalid(
                    "where_var requires at least one predicate",
                ));
                return self;
            }
        };
        self.append_bool_expr(expr);
        self
    }

    /// Sets the direction for the next edge clause.
    pub fn direction(mut self, dir: EdgeDirection) -> Self {
        self.pending_direction = dir;
        self
    }

    /// Convenience helper for bidirectional expansions.
    pub fn bidirectional(self) -> Self {
        self.direction(EdgeDirection::Both)
    }

    /// Marks the query as distinct.
    pub fn distinct(mut self) -> Self {
        self.ast.distinct = true;
        self
    }

    /// Configures the projection list.
    pub fn select<I, P>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<ProjectionSpec>,
    {
        if self.error.is_some() {
            return self;
        }
        self.ast.projections = fields
            .into_iter()
            .map(|p| p.into().into_projection())
            .collect();
        self
    }

    /// Builds the AST without planning.
    pub fn build(self) -> Result<QueryAst> {
        if let Some(err) = self.error {
            return Err(err);
        }
        Ok(self.ast)
    }

    /// Requests a plan from the supplied planner.
    pub fn plan(self, planner: &Planner) -> Result<PlannerOutput> {
        let ast = self.build()?;
        planner.plan(&ast)
    }

    /// Explains the plan using the supplied planner.
    pub fn explain(self, planner: &Planner) -> Result<PlanExplain> {
        Ok(self.plan(planner)?.explain)
    }

    /// Executes the query using the supplied planner and executor.
    pub fn execute(self, planner: &Planner, executor: &Executor) -> Result<QueryResult> {
        let output = self.plan(planner)?;
        executor.execute(&output.plan, None)
    }

    fn next_auto_var(&mut self) -> Var {
        let idx = self.next_var_idx;
        self.next_var_idx += 1;
        Var(auto_var_name(idx))
    }

    fn append_bool_expr(&mut self, expr: BoolExpr) {
        self.ast.predicate = Some(match self.ast.predicate.take() {
            Some(existing) => match existing {
                BoolExpr::And(mut args) => {
                    args.push(expr);
                    BoolExpr::And(args)
                }
                other => BoolExpr::And(vec![other, expr]),
            },
            None => expr,
        });
    }
}

/// Specifies the target node for a match or edge clause.
pub enum MatchTarget {
    /// Match by label only
    Label(String),
    /// Match by variable name and optional label
    Var {
        /// Variable name
        name: String,
        /// Optional label constraint
        label: Option<String>,
    },
}

impl MatchTarget {
    fn into_parts(self, fallback: Var) -> (Var, Option<String>) {
        match self {
            MatchTarget::Label(label) => (fallback, Some(label)),
            MatchTarget::Var { name, label } => (Var(name), label),
        }
    }
}

impl From<&str> for MatchTarget {
    fn from(label: &str) -> Self {
        MatchTarget::Label(label.to_owned())
    }
}

impl From<String> for MatchTarget {
    fn from(label: String) -> Self {
        MatchTarget::Label(label)
    }
}

impl From<(&str, &str)> for MatchTarget {
    fn from((var, label): (&str, &str)) -> Self {
        MatchTarget::Var {
            name: var.to_owned(),
            label: Some(label.to_owned()),
        }
    }
}

impl From<(&str, Option<&str>)> for MatchTarget {
    fn from((var, label): (&str, Option<&str>)) -> Self {
        MatchTarget::Var {
            name: var.to_owned(),
            label: label.map(|l| l.to_owned()),
        }
    }
}

/// Edge specification used by the builder.
pub struct EdgeSpec {
    edge_type: Option<String>,
}

impl EdgeSpec {
    /// Creates a new edge specification with an optional edge type constraint.
    pub fn new(edge_type: Option<String>) -> Self {
        Self { edge_type }
    }
}

impl From<&str> for EdgeSpec {
    fn from(edge_type: &str) -> Self {
        Self::new(Some(edge_type.to_owned()))
    }
}

impl From<Option<&str>> for EdgeSpec {
    fn from(edge_type: Option<&str>) -> Self {
        Self::new(edge_type.map(|e| e.to_owned()))
    }
}

#[derive(Clone, Copy, Debug)]
enum PredicateMode {
    And,
    Or,
}

/// Builder used to construct predicates bound to a single variable.
pub struct PredicateBuilder {
    var: Var,
    mode: PredicateMode,
    exprs: Vec<BoolExpr>,
    error: Option<SombraError>,
}

impl PredicateBuilder {
    fn new(var: Var) -> Self {
        Self::with_mode(var, PredicateMode::And)
    }

    fn with_mode(var: Var, mode: PredicateMode) -> Self {
        Self {
            var,
            mode,
            exprs: Vec::new(),
            error: None,
        }
    }

    fn push_cmp(&mut self, cmp: Comparison) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        self.exprs.push(BoolExpr::Cmp(cmp));
        self
    }

    fn push_expr(&mut self, expr: BoolExpr) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        self.exprs.push(expr);
        self
    }

    fn finish(self) -> Option<BoolExpr> {
        match self.error {
            Some(_) => None,
            None => match self.exprs.len() {
                0 => None,
                1 => self.exprs.into_iter().next(),
                _ => Some(match self.mode {
                    PredicateMode::And => BoolExpr::And(self.exprs),
                    PredicateMode::Or => BoolExpr::Or(self.exprs),
                }),
            },
        }
    }

    fn build_group_expr<F>(var: Var, mode: PredicateMode, build: F) -> Result<BoolExpr>
    where
        F: FnOnce(&mut PredicateBuilder),
    {
        let mut nested = PredicateBuilder::with_mode(var, mode);
        build(&mut nested);
        if let Some(err) = nested.error {
            return Err(err);
        }
        nested.finish().ok_or(SombraError::Invalid(
            "predicate group must emit at least one predicate",
        ))
    }
    fn record_error(&mut self, err: SombraError) {
        if self.error.is_none() {
            self.error = Some(err);
        }
    }

    /// Adds an equality predicate comparing the property to a literal.
    pub fn eq<P, V>(&mut self, prop: P, value: V) -> &mut Self
    where
        P: Into<String>,
        V: Into<Value>,
    {
        self.push_cmp(Comparison::Eq {
            var: self.var.clone(),
            prop: prop.into(),
            value: value.into(),
        })
    }

    /// Adds an inequality predicate comparing the property to a literal.
    pub fn ne<P, V>(&mut self, prop: P, value: V) -> &mut Self
    where
        P: Into<String>,
        V: Into<Value>,
    {
        self.push_cmp(Comparison::Ne {
            var: self.var.clone(),
            prop: prop.into(),
            value: value.into(),
        })
    }

    /// Adds a strict less-than predicate.
    pub fn lt<P, V>(&mut self, prop: P, value: V) -> &mut Self
    where
        P: Into<String>,
        V: Into<Value>,
    {
        self.push_cmp(Comparison::Lt {
            var: self.var.clone(),
            prop: prop.into(),
            value: value.into(),
        })
    }

    /// Adds a less-than-or-equal predicate.
    pub fn le<P, V>(&mut self, prop: P, value: V) -> &mut Self
    where
        P: Into<String>,
        V: Into<Value>,
    {
        self.push_cmp(Comparison::Le {
            var: self.var.clone(),
            prop: prop.into(),
            value: value.into(),
        })
    }

    /// Adds a strict greater-than predicate.
    pub fn gt<P, V>(&mut self, prop: P, value: V) -> &mut Self
    where
        P: Into<String>,
        V: Into<Value>,
    {
        self.push_cmp(Comparison::Gt {
            var: self.var.clone(),
            prop: prop.into(),
            value: value.into(),
        })
    }

    /// Adds a greater-than-or-equal predicate.
    pub fn ge<P, V>(&mut self, prop: P, value: V) -> &mut Self
    where
        P: Into<String>,
        V: Into<Value>,
    {
        self.push_cmp(Comparison::Ge {
            var: self.var.clone(),
            prop: prop.into(),
            value: value.into(),
        })
    }

    /// Adds an inclusive between predicate with both bounds included.
    pub fn between<P, L, H>(&mut self, prop: P, low: L, high: H) -> &mut Self
    where
        P: Into<String>,
        L: Into<Value>,
        H: Into<Value>,
    {
        self.between_bounds(
            prop,
            Bound::Included(low.into()),
            Bound::Included(high.into()),
        )
    }

    /// Adds a between predicate with explicit bound inclusivity.
    pub fn between_bounds<P>(&mut self, prop: P, low: Bound<Value>, high: Bound<Value>) -> &mut Self
    where
        P: Into<String>,
    {
        self.push_cmp(Comparison::Between {
            var: self.var.clone(),
            prop: prop.into(),
            low,
            high,
        })
    }

    /// Adds an `IN` predicate matching a finite homogeneous literal set.
    pub fn in_list<P, I, V>(&mut self, prop: P, values: I) -> &mut Self
    where
        P: Into<String>,
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        if self.error.is_some() {
            return self;
        }
        let collected: Vec<Value> = values.into_iter().map(Into::into).collect();
        if collected.is_empty() {
            self.record_error(SombraError::Invalid("in_list requires at least one value"));
            return self;
        }
        let first_tag = mem::discriminant(&collected[0]);
        if !collected
            .iter()
            .all(|value| mem::discriminant(value) == first_tag)
        {
            self.record_error(SombraError::Invalid(
                "in_list requires all values to share the same type",
            ));
            return self;
        }
        self.push_cmp(Comparison::In {
            var: self.var.clone(),
            prop: prop.into(),
            values: collected,
        })
    }

    /// Asserts that the property key is present on the entity.
    pub fn exists<P>(&mut self, prop: P) -> &mut Self
    where
        P: Into<String>,
    {
        self.push_cmp(Comparison::Exists {
            var: self.var.clone(),
            prop: prop.into(),
        })
    }

    /// Tests whether the property is null or missing.
    pub fn is_null<P>(&mut self, prop: P) -> &mut Self
    where
        P: Into<String>,
    {
        self.push_cmp(Comparison::IsNull {
            var: self.var.clone(),
            prop: prop.into(),
        })
    }

    /// Tests whether the property exists and is not null.
    pub fn is_not_null<P>(&mut self, prop: P) -> &mut Self
    where
        P: Into<String>,
    {
        self.push_cmp(Comparison::IsNotNull {
            var: self.var.clone(),
            prop: prop.into(),
        })
    }

    /// Nests a group of predicates combined with logical AND.
    pub fn and_group<F>(&mut self, build: F) -> &mut Self
    where
        F: FnOnce(&mut PredicateBuilder),
    {
        match PredicateBuilder::build_group_expr(self.var.clone(), PredicateMode::And, build) {
            Ok(expr) => self.push_expr(expr),
            Err(err) => {
                self.record_error(err);
                self
            }
        }
    }

    /// Nests a group of predicates combined with logical OR.
    pub fn or_group<F>(&mut self, build: F) -> &mut Self
    where
        F: FnOnce(&mut PredicateBuilder),
    {
        match PredicateBuilder::build_group_expr(self.var.clone(), PredicateMode::Or, build) {
            Ok(expr) => self.push_expr(expr),
            Err(err) => {
                self.record_error(err);
                self
            }
        }
    }

    /// Nests a group of predicates and negates the result.
    pub fn not_group<F>(&mut self, build: F) -> &mut Self
    where
        F: FnOnce(&mut PredicateBuilder),
    {
        match PredicateBuilder::build_group_expr(self.var.clone(), PredicateMode::And, build) {
            Ok(expr) => self.push_expr(BoolExpr::Not(Box::new(expr))),
            Err(err) => {
                self.record_error(err);
                self
            }
        }
    }
}

/// Projection helper used by the builder API.
pub struct ProjectionSpec {
    projection: Projection,
}

impl ProjectionSpec {
    fn into_projection(self) -> Projection {
        self.projection
    }
}

impl From<&str> for ProjectionSpec {
    fn from(var: &str) -> Self {
        Self {
            projection: Projection::Var {
                var: Var(var.to_owned()),
                alias: None,
            },
        }
    }
}

impl From<String> for ProjectionSpec {
    fn from(var: String) -> Self {
        Self {
            projection: Projection::Var {
                var: Var(var),
                alias: None,
            },
        }
    }
}

impl From<(&str, &str)> for ProjectionSpec {
    fn from((var, alias): (&str, &str)) -> Self {
        Self {
            projection: Projection::Var {
                var: Var(var.to_owned()),
                alias: Some(alias.to_owned()),
            },
        }
    }
}

impl From<Projection> for ProjectionSpec {
    fn from(projection: Projection) -> Self {
        Self { projection }
    }
}

fn auto_var_name(idx: usize) -> String {
    const FIRST: u8 = b'a';
    let letter = (FIRST + (idx as u8 % 26)) as char;
    if idx < 26 {
        letter.to_string()
    } else {
        format!("{}{}", letter, idx / 26)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_adds_match_and_edge() {
        let builder = QueryBuilder::new()
            .r#match("User")
            .where_edge("FOLLOWS", "User")
            .select(["a", "b"]);

        let ast = builder.build().expect("builder should succeed");
        assert_eq!(ast.matches.len(), 2);
        assert_eq!(ast.edges.len(), 1);
        assert_eq!(ast.projections.len(), 2);
    }

    #[test]
    fn builder_parses_property_predicates() {
        let ast = QueryBuilder::new()
            .r#match("User")
            .where_var("a", |pred| {
                pred.ge("age", Value::Int(21));
            })
            .build()
            .expect("builder should succeed");
        assert!(ast.predicate.is_some());
    }
}
