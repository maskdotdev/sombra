//! Logical query plan structures built from the AST before physical
//! optimisation.

use crate::query::{
    ast::{BoolExpr, EdgeDirection, Projection, Var},
    value::Value,
};
use std::ops::Bound;

/// Logical operator tree for a query.
#[derive(Clone, Debug)]
pub struct LogicalPlan {
    /// The root node of the logical plan tree.
    pub root: PlanNode,
}

impl LogicalPlan {
    /// Creates a new logical plan with the supplied root node.
    pub fn new(root: PlanNode) -> Self {
        Self { root }
    }
}

/// Node within the logical plan tree.
#[derive(Clone, Debug)]
pub struct PlanNode {
    /// The logical operator at this node.
    pub op: LogicalOp,
    /// Child nodes that provide input to this operator.
    pub inputs: Vec<PlanNode>,
}

impl PlanNode {
    /// Creates a new plan node with no inputs.
    pub fn new(op: LogicalOp) -> Self {
        Self {
            op,
            inputs: Vec::new(),
        }
    }

    /// Creates a new plan node with the given inputs.
    pub fn with_inputs(op: LogicalOp, inputs: Vec<PlanNode>) -> Self {
        Self { op, inputs }
    }
}

/// Logical operators available prior to physical selection.
#[derive(Clone, Debug)]
pub enum LogicalOp {
    /// Scans all nodes with an optional label filter.
    LabelScan {
        /// Optional label name to filter by.
        label: Option<String>,
        /// Variable name to bind matched nodes.
        as_var: Var,
    },
    /// Scans nodes using a property index.
    PropIndexScan {
        /// Optional label to scan within.
        label: Option<String>,
        /// Property name to filter on.
        prop: String,
        /// Predicate to apply on the property.
        predicate: PropPredicate,
        /// Estimated predicate selectivity.
        selectivity: f64,
        /// Variable name to bind matched nodes.
        as_var: Var,
    },
    /// Expands from nodes to their neighbors.
    Expand {
        /// Variable representing source nodes.
        from: Var,
        /// Variable to bind target nodes.
        to: Var,
        /// Direction of edge traversal.
        direction: EdgeDirection,
        /// Optional edge type filter.
        edge_type: Option<String>,
        /// Whether to ensure distinct target nodes.
        distinct_nodes: bool,
    },
    /// Filters rows based on a predicate.
    Filter {
        /// The predicate to apply for filtering.
        predicate: PropPredicate,
        /// Estimated predicate selectivity.
        selectivity: f64,
    },
    /// Intersects multiple node ID streams.
    Intersect {
        /// Variables whose values should be intersected.
        vars: Vec<Var>,
    },
    /// Performs a hash join between two streams.
    HashJoin {
        /// Variable from the left stream to join on.
        left: Var,
        /// Variable from the right stream to join on.
        right: Var,
    },
    /// Projects specific fields into the output.
    Project {
        /// Fields to include in the projection.
        fields: Vec<Projection>,
    },
    /// Removes duplicate rows from the result stream.
    Distinct,
    /// Filters rows using a boolean predicate tree.
    BoolFilter {
        /// Predicate to evaluate.
        expr: BoolExpr,
    },
}
/// Simple property predicate used for pushdown planning decisions.
#[derive(Clone, Debug)]
pub enum PropPredicate {
    /// Equality predicate for exact property value matching.
    Eq {
        /// Variable to test the property on.
        var: Var,
        /// Property name to check.
        prop: String,
        /// Expected value for the property.
        value: Value,
    },
    /// Range predicate for property values within bounds.
    Range {
        /// Variable to test the property on.
        var: Var,
        /// Property name to check.
        prop: String,
        /// Lower bound for the range (inclusive or exclusive).
        lower: Bound<Value>,
        /// Upper bound for the range (inclusive or exclusive).
        upper: Bound<Value>,
    },
}
