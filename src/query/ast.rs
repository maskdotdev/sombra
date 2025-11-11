//! High-level abstract syntax tree representing the user-facing query DSL.
//!
//! The structures defined here are intentionally ergonomic and closer to the
//! bindings exposed in Stage 8. They are later lowered into logical plan
//! operators before any physical planning decisions are made.

use crate::query::value::Value;
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

/// Boolean predicate tree for typed comparisons.
#[derive(Clone, Debug)]
pub enum BoolExpr {
    /// Comparison leaf node.
    Cmp(Comparison),
    /// Conjunction of child expressions.
    And(Vec<BoolExpr>),
    /// Disjunction of child expressions.
    Or(Vec<BoolExpr>),
    /// Negation of a child expression.
    Not(Box<BoolExpr>),
}

/// Comparison operators that can appear as leaves within the predicate tree.
#[derive(Clone, Debug)]
pub enum Comparison {
    /// Equality comparison.
    Eq {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Literal value to compare against.
        value: Value,
    },
    /// Inequality comparison.
    Ne {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Literal value to compare against.
        value: Value,
    },
    /// Less-than comparison.
    Lt {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Literal value to compare against.
        value: Value,
    },
    /// Less-than-or-equal comparison.
    Le {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Literal value to compare against.
        value: Value,
    },
    /// Greater-than comparison.
    Gt {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Literal value to compare against.
        value: Value,
    },
    /// Greater-than-or-equal comparison.
    Ge {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Literal value to compare against.
        value: Value,
    },
    /// Between comparison with optional bound inclusivity.
    Between {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Lower bound for the predicate.
        low: Bound<Value>,
        /// Upper bound for the predicate.
        high: Bound<Value>,
    },
    /// Inclusion comparison against a finite literal set.
    In {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
        /// Literal values to test membership against.
        values: Vec<Value>,
    },
    /// Checks if a property key exists (value may still be null).
    Exists {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
    },
    /// Property is null or missing.
    IsNull {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
    },
    /// Property is present and not null.
    IsNotNull {
        /// Variable binding referenced by the predicate.
        var: Var,
        /// Property name on the variable.
        prop: String,
    },
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
    /// Projection of a property from a bound variable.
    Prop {
        /// Variable exposing the property.
        var: Var,
        /// Property name to project.
        prop: String,
        /// Optional alias for the projected column.
        alias: Option<String>,
    },
}

/// Top-level AST produced by the query builder.
#[derive(Clone, Debug, Default)]
pub struct QueryAst {
    /// Optional client-specified identifier for correlating requests.
    pub request_id: Option<String>,
    /// Match clauses defining initial variable bindings.
    pub matches: Vec<MatchClause>,
    /// Edge traversal clauses connecting variables.
    pub edges: Vec<EdgeClause>,
    /// Canonical boolean predicate tree.
    pub predicate: Option<BoolExpr>,
    /// Whether to deduplicate results.
    pub distinct: bool,
    /// Projection items defining the output columns.
    pub projections: Vec<Projection>,
}
