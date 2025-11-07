//! Physical operator tree selected by the rule-based planner.

use crate::ast::{Literal, Var};
use sombra_types::{LabelId, PropId, TypeId};
use std::ops::Bound;

/// Physical plan produced by the planner.
#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    pub root: PhysicalNode,
}

impl PhysicalPlan {
    pub fn new(root: PhysicalNode) -> Self {
        Self { root }
    }
}

/// Node within the physical plan tree.
#[derive(Clone, Debug)]
pub struct PhysicalNode {
    pub op: PhysicalOp,
    pub inputs: Vec<PhysicalNode>,
}

impl PhysicalNode {
    pub fn new(op: PhysicalOp) -> Self {
        Self {
            op,
            inputs: Vec::new(),
        }
    }

    pub fn with_inputs(op: PhysicalOp, inputs: Vec<PhysicalNode>) -> Self {
        Self { op, inputs }
    }
}

/// Physical operators supported by Stage 8.
#[derive(Clone, Debug)]
pub enum PhysicalOp {
    LabelScan {
        label: LabelId,
        as_var: Var,
    },
    PropIndexScan {
        label: LabelId,
        prop: PropId,
        pred: PropPredicate,
        as_var: Var,
    },
    Expand {
        from: Var,
        to: Var,
        dir: Dir,
        ty: Option<TypeId>,
        distinct_nodes: bool,
    },
    Filter {
        pred: PropPredicate,
    },
    Intersect {
        vars: Vec<Var>,
    },
    HashJoin {
        left: Var,
        right: Var,
    },
    Distinct,
    Project {
        fields: Vec<ProjectField>,
    },
}

/// Edge traversal direction for physical expansion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dir {
    Out,
    In,
    Both,
}

/// Property predicate lowered to the physical layer.
#[derive(Clone, Debug)]
pub enum PropPredicate {
    Eq {
        var: Var,
        prop: PropId,
        value: LiteralValue,
    },
    Range {
        var: Var,
        prop: PropId,
        lower: Bound<LiteralValue>,
        upper: Bound<LiteralValue>,
    },
    Custom {
        expr: String,
    },
}

/// Projected field in the output stream.
#[derive(Clone, Debug)]
pub enum ProjectField {
    Var { var: Var, alias: Option<String> },
    Expr { expr: String, alias: String },
}

/// Literal surfaced in the physical plan.
#[derive(Clone, Debug)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
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
