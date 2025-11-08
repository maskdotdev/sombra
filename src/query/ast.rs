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
    pub var: Var,
    pub label: Option<String>,
}

/// Direction selector for edge traversals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EdgeDirection {
    Out,
    In,
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
    pub from: Var,
    pub to: Var,
    pub edge_type: Option<String>,
    pub direction: EdgeDirection,
}

/// Supported property predicate kinds.
#[derive(Clone, Debug)]
pub enum PropPredicate {
    Eq {
        var: Var,
        prop: String,
        value: Literal,
    },
    Range {
        var: Var,
        prop: String,
        lower: Bound<Literal>,
        upper: Bound<Literal>,
    },
    /// Placeholder for future custom predicate expressions.
    Custom { expr: String },
}

/// Literal values surfaced by the bindings layer.
#[derive(Clone, Debug)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
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
    Var { var: Var, alias: Option<String> },
    Expr { expr: String, alias: String },
}

/// Top-level AST produced by the query builder.
#[derive(Clone, Debug, Default)]
pub struct QueryAst {
    pub matches: Vec<MatchClause>,
    pub edges: Vec<EdgeClause>,
    pub predicates: Vec<PropPredicate>,
    pub distinct: bool,
    pub projections: Vec<Projection>,
}
