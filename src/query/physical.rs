//! Physical operator tree selected by the rule-based planner.

use crate::query::ast::Var;
use crate::query::Value;
use crate::types::{LabelId, PropId, TypeId};
use std::convert::TryInto;
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
        /// Estimated predicate selectivity.
        selectivity: f64,
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
    /// Removes duplicate rows from the result stream.
    Distinct,
    /// Projects specific fields into the output.
    Project {
        /// Fields to include in the projection.
        fields: Vec<ProjectField>,
    },
    /// Filters rows using a boolean predicate tree.
    BoolFilter {
        /// Predicate to evaluate.
        expr: PhysicalBoolExpr,
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
}

/// Boolean predicate tree resolved to physical identifiers.
#[derive(Clone, Debug)]
pub enum PhysicalBoolExpr {
    /// Comparison leaf.
    Cmp(PhysicalComparison),
    /// Logical AND.
    And(Vec<PhysicalBoolExpr>),
    /// Logical OR.
    Or(Vec<PhysicalBoolExpr>),
    /// Logical NOT.
    Not(Box<PhysicalBoolExpr>),
}

/// Comparison operator referencing resolved property identifiers.
#[derive(Clone, Debug)]
pub enum PhysicalComparison {
    /// Equality predicate on a resolved property id.
    Eq {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Literal value to compare against.
        value: LiteralValue,
    },
    /// Inequality predicate on a resolved property id.
    Ne {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Literal value to compare against.
        value: LiteralValue,
    },
    /// Less-than predicate on a resolved property id.
    Lt {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Literal value to compare against.
        value: LiteralValue,
    },
    /// Less-than-or-equal predicate on a resolved property id.
    Le {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Literal value to compare against.
        value: LiteralValue,
    },
    /// Greater-than predicate on a resolved property id.
    Gt {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Literal value to compare against.
        value: LiteralValue,
    },
    /// Greater-than-or-equal predicate on a resolved property id.
    Ge {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Literal value to compare against.
        value: LiteralValue,
    },
    /// Between predicate with explicit bounds.
    Between {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Lower bound literal (inclusive/exclusive).
        low: Bound<LiteralValue>,
        /// Upper bound literal (inclusive/exclusive).
        high: Bound<LiteralValue>,
    },
    /// Inclusion predicate with a literal set.
    In {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Literal set to test membership against.
        values: Vec<LiteralValue>,
    },
    /// Checks whether the property key exists on the node.
    Exists {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
    },
    /// Checks whether the property value is null or missing.
    IsNull {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
    },
    /// Checks whether the property is present and not null.
    IsNotNull {
        /// Variable whose property is inspected.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
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
    /// Projects a property from a bound variable.
    Prop {
        /// Variable exposing the property.
        var: Var,
        /// Resolved property identifier.
        prop: PropId,
        /// Property name preserved for explain output / default aliasing.
        prop_name: String,
        /// Optional alias for the output field.
        alias: Option<String>,
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
    /// Binary value stored inline (base64 encoded over the wire).
    Bytes(Vec<u8>),
    /// DateTime literal represented as nanoseconds since Unix epoch.
    DateTime(i64),
}

impl From<&Value> for LiteralValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => LiteralValue::Null,
            Value::Bool(v) => LiteralValue::Bool(*v),
            Value::Int(v) => LiteralValue::Int(*v),
            Value::Float(v) => LiteralValue::Float(*v),
            Value::String(v) => LiteralValue::String(v.clone()),
            Value::Bytes(bytes) => LiteralValue::Bytes(bytes.clone()),
            Value::DateTime(v) => {
                let nanos: i64 = (*v)
                    .try_into()
                    .expect("datetime literal exceeds i64 range after validation");
                LiteralValue::DateTime(nanos)
            }
        }
    }
}
