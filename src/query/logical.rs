//! Logical query plan structures built from the AST before physical
//! optimisation.

use crate::query::ast::{EdgeDirection, Projection, PropPredicate, Var};

/// Logical operator tree for a query.
#[derive(Clone, Debug)]
pub struct LogicalPlan {
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
    pub op: LogicalOp,
    pub inputs: Vec<PlanNode>,
}

impl PlanNode {
    pub fn new(op: LogicalOp) -> Self {
        Self {
            op,
            inputs: Vec::new(),
        }
    }

    pub fn with_inputs(op: LogicalOp, inputs: Vec<PlanNode>) -> Self {
        Self { op, inputs }
    }
}

/// Logical operators available prior to physical selection.
#[derive(Clone, Debug)]
pub enum LogicalOp {
    LabelScan {
        label: Option<String>,
        as_var: Var,
    },
    PropIndexScan {
        label: Option<String>,
        prop: String,
        predicate: PropPredicate,
        as_var: Var,
    },
    Expand {
        from: Var,
        to: Var,
        direction: EdgeDirection,
        edge_type: Option<String>,
        distinct_nodes: bool,
    },
    Filter {
        predicate: PropPredicate,
    },
    Intersect {
        vars: Vec<Var>,
    },
    HashJoin {
        left: Var,
        right: Var,
    },
    Project {
        fields: Vec<Projection>,
    },
    Distinct,
}
