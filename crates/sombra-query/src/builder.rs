//! Fluent query builder scaffolding.

use crate::{
    ast::{
        EdgeClause, EdgeDirection, Literal, MatchClause, Projection, PropPredicate, QueryAst, Var,
    },
    executor::{Executor, QueryResult},
    planner::{PlanExplain, Planner, PlannerOutput},
};
use sombra_types::Result;
use std::ops::Bound;

/// Fluent builder matching the Stage 8 ergonomics.
#[derive(Default)]
pub struct QueryBuilder {
    ast: QueryAst,
    last_var: Option<Var>,
    next_var_idx: usize,
    pending_direction: EdgeDirection,
}

impl QueryBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self {
            ast: QueryAst::default(),
            last_var: None,
            next_var_idx: 0,
            pending_direction: EdgeDirection::Out,
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
        let from = self
            .last_var
            .clone()
            .expect("where_edge requires an existing left variable");
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

    /// Adds a property predicate.
    pub fn where_prop<V, P, L1, L2>(
        mut self,
        var: V,
        prop: P,
        op: PropOp,
        value: L1,
        value2: Option<L2>,
    ) -> Self
    where
        V: Into<String>,
        P: Into<String>,
        L1: Into<Literal>,
        L2: Into<Literal>,
    {
        let var = Var(var.into());
        let prop = prop.into();
        let value = value.into();
        let value2 = value2.map(|v| v.into());
        let predicate = match op {
            PropOp::Eq => PropPredicate::Eq { var, prop, value },
            PropOp::Between => {
                let (lower, upper) =
                    value2.map_or_else(|| panic!("between requires two values"), |v2| (value, v2));
                PropPredicate::Range {
                    var,
                    prop,
                    lower: Bound::Included(lower),
                    upper: Bound::Included(upper),
                }
            }
            PropOp::Gt => PropPredicate::Range {
                var,
                prop,
                lower: Bound::Excluded(value),
                upper: Bound::Unbounded,
            },
            PropOp::Ge => PropPredicate::Range {
                var,
                prop,
                lower: Bound::Included(value),
                upper: Bound::Unbounded,
            },
            PropOp::Lt => PropPredicate::Range {
                var,
                prop,
                lower: Bound::Unbounded,
                upper: Bound::Excluded(value),
            },
            PropOp::Le => PropPredicate::Range {
                var,
                prop,
                lower: Bound::Unbounded,
                upper: Bound::Included(value),
            },
        };
        self.ast.predicates.push(predicate);
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
        self.ast.projections = fields
            .into_iter()
            .map(|p| p.into().into_projection())
            .collect();
        self
    }

    /// Builds the AST without planning.
    pub fn build(self) -> QueryAst {
        self.ast
    }

    /// Requests a plan from the supplied planner.
    pub fn plan(self, planner: &Planner) -> Result<PlannerOutput> {
        planner.plan(&self.ast)
    }

    /// Explains the plan using the supplied planner.
    pub fn explain(self, planner: &Planner) -> Result<PlanExplain> {
        Ok(self.plan(planner)?.explain)
    }

    /// Executes the query using the supplied planner and executor.
    pub fn execute(self, planner: &Planner, executor: &Executor) -> Result<QueryResult> {
        let output = self.plan(planner)?;
        executor.execute(&output.plan)
    }

    fn next_auto_var(&mut self) -> Var {
        let idx = self.next_var_idx;
        self.next_var_idx += 1;
        Var(auto_var_name(idx))
    }
}

/// Specifies the target node for a match or edge clause.
pub enum MatchTarget {
    Label(String),
    Var { name: String, label: Option<String> },
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

/// Property comparison operators supported by the builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
    Between,
}

impl PropOp {
    pub fn from_str(op: &str) -> Option<Self> {
        match op {
            "=" => Some(Self::Eq),
            "<" => Some(Self::Lt),
            "<=" => Some(Self::Le),
            ">" => Some(Self::Gt),
            ">=" => Some(Self::Ge),
            "between" => Some(Self::Between),
            _ => None,
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

        let ast = builder.build();
        assert_eq!(ast.matches.len(), 2);
        assert_eq!(ast.edges.len(), 1);
        assert_eq!(ast.projections.len(), 2);
    }

    #[test]
    fn builder_parses_property_predicates() {
        let ast = QueryBuilder::new()
            .r#match("User")
            .where_prop("a", "age", PropOp::Ge, Literal::Int(21), None::<Literal>)
            .build();
        assert_eq!(ast.predicates.len(), 1);
    }
}
