//! Query normalization utilities used by the planner/FFI pipeline (Phase 3).
//!
//! The analyzer canonicalizes boolean predicates so equivalent logical trees
//! produce identical plan fingerprints. It also enforces guardrails such as
//! sorted `IN` lists, non-empty predicates, and ordered `BETWEEN` bounds.

use crate::query::ast::{BoolExpr, Comparison, Projection, QueryAst};
use crate::query::Value;
use crate::types::{Result, SombraError};
use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::Bound;

/// Normalizes a query AST by canonicalizing boolean predicates and projections.
pub fn normalize(ast: &QueryAst) -> Result<QueryAst> {
    let mut normalized = ast.clone();
    normalized.predicate = match normalized.predicate.take() {
        Some(expr) => normalize_expr(expr)?.or_else(|| Some(BoolExpr::And(Vec::new()))),
        None => None,
    };
    normalize_projections(&mut normalized.projections)?;
    Ok(normalized)
}

fn normalize_projections(projections: &mut [Projection]) -> Result<()> {
    for proj in projections {
        if let Projection::Prop { alias, .. } = proj {
            if let Some(alias) = alias {
                if alias.trim().is_empty() {
                    return Err(SombraError::Invalid("projection alias cannot be empty"));
                }
            }
        }
    }
    Ok(())
}

fn normalize_expr(expr: BoolExpr) -> Result<Option<BoolExpr>> {
    match simplify(expr)? {
        Simplified::True => Ok(None),
        Simplified::False => Ok(Some(BoolExpr::Or(Vec::new()))),
        Simplified::Expr(expr) => Ok(Some(expr)),
    }
}

enum Simplified {
    True,
    False,
    Expr(BoolExpr),
}

fn simplify(expr: BoolExpr) -> Result<Simplified> {
    match expr {
        BoolExpr::Cmp(cmp) => {
            let cmp = canonicalize_comparison(cmp)?;
            Ok(Simplified::Expr(BoolExpr::Cmp(cmp)))
        }
        BoolExpr::Not(child) => match simplify(*child)? {
            Simplified::True => Ok(Simplified::False),
            Simplified::False => Ok(Simplified::True),
            Simplified::Expr(expr) => Ok(Simplified::Expr(BoolExpr::Not(Box::new(expr)))),
        },
        BoolExpr::And(children) => {
            let mut flattened = Vec::new();
            for child in children {
                match simplify(child)? {
                    Simplified::True => {}
                    Simplified::False => return Ok(Simplified::False),
                    Simplified::Expr(expr) => match expr {
                        BoolExpr::And(grand) => flattened.extend(grand),
                        other => flattened.push(other),
                    },
                }
            }
            dedup_exprs(&mut flattened);
            match flattened.len() {
                0 => Ok(Simplified::True),
                1 => Ok(Simplified::Expr(flattened.into_iter().next().unwrap())),
                _ => Ok(Simplified::Expr(BoolExpr::And(flattened))),
            }
        }
        BoolExpr::Or(children) => {
            let mut flattened = Vec::new();
            for child in children {
                match simplify(child)? {
                    Simplified::False => {}
                    Simplified::True => return Ok(Simplified::True),
                    Simplified::Expr(expr) => match expr {
                        BoolExpr::Or(grand) => flattened.extend(grand),
                        other => flattened.push(other),
                    },
                }
            }
            dedup_exprs(&mut flattened);
            match flattened.len() {
                0 => Ok(Simplified::False),
                1 => Ok(Simplified::Expr(flattened.into_iter().next().unwrap())),
                _ => Ok(Simplified::Expr(BoolExpr::Or(flattened))),
            }
        }
    }
}

fn canonicalize_comparison(cmp: Comparison) -> Result<Comparison> {
    match cmp {
        Comparison::Between {
            var,
            prop,
            low,
            high,
        } => {
            validate_between_bounds(&low, &high)?;
            Ok(Comparison::Between {
                var,
                prop,
                low,
                high,
            })
        }
        Comparison::In {
            var,
            prop,
            mut values,
        } => {
            canonicalize_in_values(&mut values)?;
            Ok(Comparison::In { var, prop, values })
        }
        other => Ok(other),
    }
}

fn canonicalize_in_values(values: &mut Vec<Value>) -> Result<()> {
    // Drop nulls (Null is never matched via IN).
    values.retain(|v| !matches!(v, Value::Null));
    if values.is_empty() {
        return Err(SombraError::Invalid(
            "in() requires at least one non-null literal",
        ));
    }
    let mut seen = HashSet::with_capacity(values.len());
    values.retain(|value| seen.insert(value_sort_key(value)));
    values.sort_by(|a, b| compare_values(a, b));
    Ok(())
}

fn dedup_exprs(exprs: &mut Vec<BoolExpr>) {
    if exprs.is_empty() {
        return;
    }
    let mut keyed: Vec<(String, BoolExpr)> = exprs
        .drain(..)
        .map(|expr| (expr_sort_key(&expr), expr))
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    keyed.dedup_by(|a, b| a.0 == b.0);
    *exprs = keyed.into_iter().map(|(_, expr)| expr).collect();
}

fn expr_sort_key(expr: &BoolExpr) -> String {
    match expr {
        BoolExpr::Cmp(cmp) => format!("cmp:{}", comparison_sort_key(cmp)),
        BoolExpr::Not(child) => format!("not:{}", expr_sort_key(child)),
        BoolExpr::And(children) => {
            let mut child_keys: Vec<String> = children.iter().map(expr_sort_key).collect();
            child_keys.sort();
            format!("and:{}", child_keys.join("|"))
        }
        BoolExpr::Or(children) => {
            let mut child_keys: Vec<String> = children.iter().map(expr_sort_key).collect();
            child_keys.sort();
            format!("or:{}", child_keys.join("|"))
        }
    }
}

fn comparison_sort_key(cmp: &Comparison) -> String {
    match cmp {
        Comparison::Eq { var, prop, value } => {
            format!("eq:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Ne { var, prop, value } => {
            format!("ne:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Lt { var, prop, value } => {
            format!("lt:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Le { var, prop, value } => {
            format!("le:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Gt { var, prop, value } => {
            format!("gt:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Ge { var, prop, value } => {
            format!("ge:{}:{}:{}", var.0, prop, value_sort_key(value))
        }
        Comparison::Between {
            var,
            prop,
            low,
            high,
        } => format!(
            "between:{}:{}:{}:{}",
            var.0,
            prop,
            bound_sort_key(low),
            bound_sort_key(high)
        ),
        Comparison::In { var, prop, values } => {
            let mut value_keys: Vec<String> = values.iter().map(value_sort_key).collect();
            value_keys.sort();
            format!("in:{}:{}:{}", var.0, prop, value_keys.join(","))
        }
        Comparison::Exists { var, prop } => format!("exists:{}:{}", var.0, prop),
        Comparison::IsNull { var, prop } => format!("isnull:{}:{}", var.0, prop),
        Comparison::IsNotNull { var, prop } => format!("isnotnull:{}:{}", var.0, prop),
    }
}

fn bound_sort_key(bound: &Bound<Value>) -> String {
    match bound {
        Bound::Included(v) => format!("inc:{}", value_sort_key(v)),
        Bound::Excluded(v) => format!("exc:{}", value_sort_key(v)),
        Bound::Unbounded => "unbounded".into(),
    }
}

fn value_sort_key(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(v) => format!("bool:{v}"),
        Value::Int(v) => format!("int:{v}"),
        Value::Float(v) => format!("float:{v}"),
        Value::String(v) => format!("str:{v}"),
        Value::Bytes(v) => format!("bytes:{}", BASE64_ENGINE.encode(v)),
        Value::DateTime(v) => format!("datetime:{v}"),
    }
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a
            .partial_cmp(b)
            .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan())),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
        // Ensure deterministic ordering across mixed types.
        (_left, _right) => type_rank(left).cmp(&type_rank(right)),
    }
}

fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Float(_) => 3,
        Value::String(_) => 4,
        Value::Bytes(_) => 5,
        Value::DateTime(_) => 6,
    }
}

fn validate_between_bounds(low: &Bound<Value>, high: &Bound<Value>) -> Result<()> {
    match (extract_bound_value(low), extract_bound_value(high)) {
        (Some(a), Some(b)) => {
            if compare_values(a, b) == Ordering::Greater {
                return Err(SombraError::Invalid(
                    "between() lower bound must be <= upper bound",
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn extract_bound_value(bound: &Bound<Value>) -> Option<&Value> {
    match bound {
        Bound::Included(v) | Bound::Excluded(v) => Some(v),
        Bound::Unbounded => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{BoolExpr, Comparison, Var};
    use crate::query::Value;
    use std::ops::Bound;

    fn var(name: &str) -> Var {
        Var(name.to_string())
    }

    #[test]
    fn removes_duplicate_and_children() {
        let expr = BoolExpr::And(vec![
            BoolExpr::Cmp(Comparison::Exists {
                var: var("a"),
                prop: "name".into(),
            }),
            BoolExpr::Cmp(Comparison::Exists {
                var: var("a"),
                prop: "name".into(),
            }),
        ]);
        let ast = QueryAst {
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        let normalized = normalize(&ast).expect("normalize succeeds");
        assert!(matches!(
            normalized.predicate,
            Some(BoolExpr::Cmp(Comparison::Exists { .. }))
        ));
    }

    #[test]
    fn canonicalizes_in_values() {
        let expr = BoolExpr::Cmp(Comparison::In {
            var: var("a"),
            prop: "name".into(),
            values: vec![
                Value::String("b".into()),
                Value::Null,
                Value::String("a".into()),
                Value::String("a".into()),
            ],
        });
        let ast = QueryAst {
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        let normalized = normalize(&ast).expect("normalize succeeds");
        match normalized.predicate.unwrap() {
            BoolExpr::Cmp(Comparison::In { values, .. }) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Value::String("a".into()));
                assert_eq!(values[1], Value::String("b".into()));
            }
            other => panic!("unexpected predicate {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_in_after_nulls_removed() {
        let expr = BoolExpr::Cmp(Comparison::In {
            var: var("a"),
            prop: "name".into(),
            values: vec![Value::Null],
        });
        let ast = QueryAst {
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        let err = normalize(&ast).expect_err("normalize should fail");
        assert!(matches!(err, SombraError::Invalid(_)));
    }

    #[test]
    fn enforces_between_ordering() {
        let expr = BoolExpr::Cmp(Comparison::Between {
            var: var("a"),
            prop: "age".into(),
            low: Bound::Included(Value::Int(10)),
            high: Bound::Included(Value::Int(5)),
        });
        let ast = QueryAst {
            matches: vec![],
            edges: vec![],
            predicate: Some(expr),
            distinct: false,
            projections: vec![],
        };
        assert!(normalize(&ast).is_err());
    }
}
