//! High-level abstract syntax tree representing the user-facing query DSL.
//!
//! The structures defined here are intentionally ergonomic and closer to the
//! bindings exposed in Stage 8. They are later lowered into logical plan
//! operators before any physical planning decisions are made.

use std::ops::Bound;

/// Identifier assigned to a binding (node or edge) within the query.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Var(pub String);

/// A match clause describing the starting variable and optional label.
#[derive(Clone, Debug)]
pub struct MatchClause {
    /// The variable binding for this match clause.
    pub var: Var,
    /// Optional label to filter nodes by type.
    pub label: Option<String>,
}

/// Direction selector for edge traversals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EdgeDirection {
    /// Outgoing edges from the source node.
    Out,
    /// Incoming edges to the target node.
    In,
    /// Edges in both directions.
    Both,
}

impl Default for EdgeDirection {
    fn default() -> Self {
        EdgeDirection::Out
    }
}

/// Edge expansion captured in the AST.
#[derive(Clone, Debug)]
pub struct EdgeClause {
    /// Source variable for the edge traversal.
    pub from: Var,
    /// Destination variable for the edge traversal.
    pub to: Var,
    /// Optional edge type filter.
    pub edge_type: Option<String>,
    /// Direction of the edge traversal.
    pub direction: EdgeDirection,
}

/// Supported property predicate kinds.
#[derive(Clone, Debug)]
pub enum PropPredicate {
    /// Equality predicate for exact property value matching.
    Eq {
        /// Variable to test the property on.
        var: Var,
        /// Property name to check.
        prop: String,
        /// Expected value for the property.
        value: Literal,
    },
    /// Range predicate for property values within bounds.
    Range {
        /// Variable to test the property on.
        var: Var,
        /// Property name to check.
        prop: String,
        /// Lower bound for the range (inclusive or exclusive).
        lower: Bound<Literal>,
        /// Upper bound for the range (inclusive or exclusive).
        upper: Bound<Literal>,
    },
    /// Placeholder for future custom predicate expressions.
    Custom {
        /// Custom expression string.
        expr: String,
    },
}

/// Literal values surfaced by the bindings layer.
#[derive(Clone, Debug)]
pub enum Literal {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Signed 64-bit integer value.
    Int(i64),
    /// 64-bit floating point value.
    Float(f64),
    /// String value.
    String(String),
}

impl From<&str> for Literal {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for Literal {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for Literal {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for Literal {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for Literal {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

/// Projection item included in the final result.
#[derive(Clone, Debug)]
pub enum Projection {
    /// Projection of a variable binding.
    Var {
        /// Variable to project.
        var: Var,
        /// Optional alias for the projected variable.
        alias: Option<String>,
    },
    /// Projection of a computed expression.
    Expr {
        /// Expression string to evaluate.
        expr: String,
        /// Alias for the expression result.
        alias: String,
    },
}

/// Top-level AST produced by the query builder.
#[derive(Clone, Debug, Default)]
pub struct QueryAst {
    /// Match clauses defining initial variable bindings.
    pub matches: Vec<MatchClause>,
    /// Edge traversal clauses connecting variables.
    pub edges: Vec<EdgeClause>,
    /// Property predicates for filtering results.
    pub predicates: Vec<PropPredicate>,
    /// Whether to deduplicate results.
    pub distinct: bool,
    /// Projection items defining the output columns.
    pub projections: Vec<Projection>,
}
