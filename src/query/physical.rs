//! Physical operator tree selected by the rule-based planner.

use crate::query::ast::{Literal, Var};
use crate::types::{LabelId, PropId, TypeId};
use std::ops::Bound;

/// Physical plan produced by the planner.
#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    /// The root node of the physical plan tree.
    pub root: PhysicalNode,
}

impl PhysicalPlan {
    /// Creates a new physical plan with the given root node.
    pub fn new(root: PhysicalNode) -> Self {
        Self { root }
    }
}

/// Node within the physical plan tree.
#[derive(Clone, Debug)]
pub struct PhysicalNode {
    /// The physical operator at this node.
    pub op: PhysicalOp,
    /// Child nodes that provide input to this operator.
    pub inputs: Vec<PhysicalNode>,
}

impl PhysicalNode {
    /// Creates a new physical node with no inputs.
    pub fn new(op: PhysicalOp) -> Self {
        Self {
            op,
            inputs: Vec::new(),
        }
    }

    /// Creates a new physical node with the given inputs.
    pub fn with_inputs(op: PhysicalOp, inputs: Vec<PhysicalNode>) -> Self {
        Self { op, inputs }
    }
}

/// Physical operators supported by Stage 8.
#[derive(Clone, Debug)]
pub enum PhysicalOp {
    /// Scans all nodes with a specific label.
    LabelScan {
        /// The label ID to scan for.
        label: LabelId,
        /// Variable name to bind matched nodes.
        as_var: Var,
    },
    /// Scans nodes using a property index with a predicate.
    PropIndexScan {
        /// The label of nodes to scan.
        label: LabelId,
        /// The property to filter on.
        prop: PropId,
        /// The predicate to apply.
        pred: PropPredicate,
        /// Variable name to bind matched nodes.
        as_var: Var,
    },
    /// Expands from one node to its neighbors via edges.
    Expand {
        /// Variable representing the source node.
        from: Var,
        /// Variable to bind target nodes.
        to: Var,
        /// Direction of edge traversal.
        dir: Dir,
        /// Optional edge type filter.
        ty: Option<TypeId>,
        /// Whether to ensure distinct target nodes.
        distinct_nodes: bool,
    },
    /// Filters rows based on a property predicate.
    Filter {
        /// The predicate to apply for filtering.
        pred: PropPredicate,
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
    /// Removes duplicate rows from the result stream.
    Distinct,
    /// Projects specific fields into the output.
    Project {
        /// Fields to include in the projection.
        fields: Vec<ProjectField>,
    },
}

/// Edge traversal direction for physical expansion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dir {
    /// Traverse outgoing edges from source to target.
    Out,
    /// Traverse incoming edges from target to source.
    In,
    /// Traverse edges in both directions.
    Both,
}

/// Property predicate lowered to the physical layer.
#[derive(Clone, Debug)]
pub enum PropPredicate {
    /// Equality predicate on a property.
    Eq {
        /// Variable whose property to check.
        var: Var,
        /// Property ID to check.
        prop: PropId,
        /// Value to compare against.
        value: LiteralValue,
    },
    /// Range predicate on a property.
    Range {
        /// Variable whose property to check.
        var: Var,
        /// Property ID to check.
        prop: PropId,
        /// Lower bound of the range (inclusive or exclusive).
        lower: Bound<LiteralValue>,
        /// Upper bound of the range (inclusive or exclusive).
        upper: Bound<LiteralValue>,
    },
    /// Custom predicate expression (for future extension).
    Custom {
        /// String representation of the custom expression.
        expr: String,
    },
}

/// Projected field in the output stream.
#[derive(Clone, Debug)]
pub enum ProjectField {
    /// Projects a variable value.
    Var {
        /// The variable to project.
        var: Var,
        /// Optional alias for the output field.
        alias: Option<String>,
    },
    /// Projects a computed expression.
    Expr {
        /// String representation of the expression.
        expr: String,
        /// Alias for the computed field.
        alias: String,
    },
}

/// Literal surfaced in the physical plan.
#[derive(Clone, Debug)]
pub enum LiteralValue {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point number.
    Float(f64),
    /// String value.
    String(String),
}

impl From<&Literal> for LiteralValue {
    fn from(value: &Literal) -> Self {
        match value {
            Literal::Null => LiteralValue::Null,
            Literal::Bool(v) => LiteralValue::Bool(*v),
            Literal::Int(v) => LiteralValue::Int(*v),
            Literal::Float(v) => LiteralValue::Float(*v),
            Literal::String(v) => LiteralValue::String(v.clone()),
        }
    }
}
