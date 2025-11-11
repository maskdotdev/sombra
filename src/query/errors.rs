#![forbid(unsafe_code)]
#![allow(missing_docs)]

use std::fmt;

use thiserror::Error;

/// Structured errors emitted by the query analyzer.
///
/// These errors bubble up through FFI so callers can distinguish between
/// catalog resolution failures, invalid predicates, and resource limits.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AnalyzerError {
    /// No match clauses were supplied.
    #[error("query requires at least one MATCH clause")]
    EmptyMatches,
    /// The number of match clauses exceeds the configured limit.
    #[error("query exceeds {max} MATCH clauses (got {count})")]
    TooManyMatches { count: usize, max: usize },
    /// A match variable was declared more than once.
    #[error("duplicate match variable '{var}'")]
    DuplicateVariable { var: String },
    /// A match variable omitted its label constraint.
    #[error("match variable '{var}' is missing a label")]
    MatchMissingLabel { var: String },
    /// Referenced label name does not exist in the catalog.
    #[error("unknown label '{label}'")]
    UnknownLabel { label: String },
    /// Referenced variable was never declared.
    #[error("unknown variable '{var}' referenced in {context}")]
    UnknownVariable { var: String, context: &'static str },
    /// Property name is absent from the catalog.
    #[error("unknown property '{prop}'")]
    UnknownProperty { prop: String },
    /// Edge type name is absent from the catalog.
    #[error("unknown edge type '{edge_type}'")]
    UnknownEdgeType { edge_type: String },
    /// Reflexive edges are disabled unless explicitly configured.
    #[error("reflexive edge on '{var}' not allowed")]
    EdgeReflexiveNotAllowed { var: String },
    /// Projection alias cannot be blank or whitespace-only.
    #[error("projection alias cannot be empty")]
    EmptyProjectionAlias,
    /// Predicate tree exceeds the node budget.
    #[error("predicate tree exceeds {max} nodes (got {nodes})")]
    PredicateTooLarge { nodes: usize, max: usize },
    /// Predicate tree nesting exceeds the allowed depth.
    #[error("predicate tree exceeds depth {max} (got {depth})")]
    PredicateTooDeep { depth: usize, max: usize },
    /// IN list normalized to zero entries.
    #[error("in() requires at least one non-null literal")]
    InListEmpty,
    /// IN list exceeded the element budget.
    #[error("in() list exceeds maximum of {max} literals")]
    InListTooLarge { max: usize },
    /// Total binary literal payload exceeded the configured budget.
    #[error("binary literal exceeds {max} bytes")]
    BytesLiteralTooLarge { max: usize },
    /// Float literal is NaN/∞ and cannot round-trip.
    #[error("float literal must be finite")]
    NonFiniteFloat,
    /// Datetime literal falls outside the supported 64-bit nanosecond range.
    #[error("datetime literal must fit within signed 64-bit range")]
    DateTimeOutOfRange,
    /// Null literal supplied for an operator that disallows nulls.
    #[error("{context} does not accept null literals")]
    NullNotAllowed { context: &'static str },
    /// Literal type incompatible with the operator's ordering requirements.
    #[error("{context} requires numeric, datetime, or string literal")]
    RangeTypeMismatch { context: &'static str },
    /// Bytes literal used with a range operator that doesn't support it.
    #[error("bytes literals only supported with eq()/ne(), not {context}")]
    BytesRangeUnsupported { context: &'static str },
    /// Between bounds are inverted after normalization.
    #[error("between() lower bound must be <= upper bound")]
    InvalidBounds,
}

impl AnalyzerError {
    /// Builds an [`AnalyzerError::UnknownVariable`] for a specific context.
    pub fn unknown_var(var: impl Into<String>, context: &'static str) -> Self {
        AnalyzerError::UnknownVariable {
            var: var.into(),
            context,
        }
    }
}

/// Convenience wrapper that formats analyzer errors with their codes.
pub struct AnalyzerErrorWithCode<'a>(pub &'a AnalyzerError);

impl fmt::Display for AnalyzerErrorWithCode<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.0.code(), self.0)
    }
}

impl AnalyzerError {
    /// Returns a machine-readable code for the error variant.
    pub fn code(&self) -> &'static str {
        match self {
            AnalyzerError::EmptyMatches => "EmptyMatches",
            AnalyzerError::TooManyMatches { .. } => "TooManyMatches",
            AnalyzerError::DuplicateVariable { .. } => "DuplicateVariable",
            AnalyzerError::MatchMissingLabel { .. } => "MatchMissingLabel",
            AnalyzerError::UnknownLabel { .. } => "UnknownLabel",
            AnalyzerError::UnknownVariable { .. } => "UnknownVariable",
            AnalyzerError::UnknownProperty { .. } => "UnknownProperty",
            AnalyzerError::UnknownEdgeType { .. } => "UnknownEdgeType",
            AnalyzerError::EdgeReflexiveNotAllowed { .. } => "EdgeReflexiveNotAllowed",
            AnalyzerError::EmptyProjectionAlias => "EmptyProjectionAlias",
            AnalyzerError::PredicateTooLarge { .. } => "PredicateTooLarge",
            AnalyzerError::PredicateTooDeep { .. } => "PredicateTooDeep",
            AnalyzerError::InListEmpty => "InListEmpty",
            AnalyzerError::InListTooLarge { .. } => "InListTooLarge",
            AnalyzerError::BytesLiteralTooLarge { .. } => "BytesLiteralTooLarge",
            AnalyzerError::NonFiniteFloat => "NonFiniteFloat",
            AnalyzerError::DateTimeOutOfRange => "DateTimeInvalid",
            AnalyzerError::NullNotAllowed { .. } => "NullNotAllowed",
            AnalyzerError::RangeTypeMismatch { .. } => "TypeMismatch",
            AnalyzerError::BytesRangeUnsupported { .. } => "TypeMismatch",
            AnalyzerError::InvalidBounds => "InvalidBounds",
        }
    }
}
