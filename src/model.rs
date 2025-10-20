//! Data models for graph entities.
//!
//! This module defines the core data structures used to represent
//! nodes, edges, and their properties in the Sombra graph database.
//!
//! # Key Types
//!
//! - [`Node`] - Represents a graph node with labels and properties
//! - [`Edge`] - Represents a directed edge between nodes
//! - [`PropertyValue`] - Enum for different property value types
//! - [`NodeId`] / [`EdgeId`] - Unique identifiers for nodes and edges
//!
//! # Examples
//!
//! ```rust
//! use sombra::model::{Node, Edge, PropertyValue};
//! use std::collections::BTreeMap;
//!
//! // Create a node with properties
//! let mut properties = BTreeMap::new();
//! properties.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
//!
//! let mut node = Node::new(1);
//! node.labels.push("Person".to_string());
//! node.properties = properties;
//!
//! // Create an edge
//! let edge = Edge::new(1, 1, 2, "KNOWS");
//! ```

use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Unique identifier for nodes in the graph.
pub type NodeId = u64;

/// Unique identifier for edges in the graph.
pub type EdgeId = u64;

/// Special value indicating no edge (null edge ID).
pub const NULL_EDGE_ID: EdgeId = 0;

/// Special value indicating no node (null node ID).
pub const NULL_NODE_ID: NodeId = 0;

/// Represents a property value that can be stored on nodes and edges.
///
/// Property values support various data types commonly used in graph databases.
/// Only Bool, Int, and String values can be indexed for fast lookups.
///
/// # Examples
///
/// ```rust
/// use sombra::model::PropertyValue;
///
/// let name = PropertyValue::String("Alice".to_string());
/// let age = PropertyValue::Int(30);
/// let active = PropertyValue::Bool(true);
/// let score = PropertyValue::Float(95.5);
/// let data = PropertyValue::Bytes(vec![1, 2, 3]);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    /// Boolean value (true/false)
    Bool(bool),
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string
    String(String),
    /// Arbitrary byte array
    Bytes(Vec<u8>),
}

impl PropertyValue {
    pub fn partial_cmp_value(&self, other: &PropertyValue) -> Option<Ordering> {
        match (self, other) {
            (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a.partial_cmp(b),
            (PropertyValue::Int(a), PropertyValue::Int(b)) => a.partial_cmp(b),
            (PropertyValue::Float(a), PropertyValue::Float(b)) => a.partial_cmp(b),
            (PropertyValue::String(a), PropertyValue::String(b)) => a.partial_cmp(b),
            (PropertyValue::Bytes(a), PropertyValue::Bytes(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

/// Represents a node in the graph.
///
/// Nodes are the primary entities in a graph database and can have
/// multiple labels and properties. Nodes are connected by edges.
///
/// # Fields
///
/// * `id` - Unique identifier for the node
/// * `labels` - List of labels categorizing the node
/// * `properties` - Key-value pairs storing node attributes
/// * `first_outgoing_edge_id` - Head of the outgoing edge linked list
/// * `first_incoming_edge_id` - Head of the incoming edge linked list
///
/// # Examples
///
/// ```rust
/// use sombra::model::{Node, PropertyValue};
/// use std::collections::BTreeMap;
///
/// let mut properties = BTreeMap::new();
/// properties.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
/// properties.insert("age".to_string(), PropertyValue::Int(30));
///
/// let mut node = Node::new(1);
/// node.labels.push("Person".to_string());
/// node.properties = properties;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// Unique identifier for this node
    pub id: NodeId,
    /// Labels that categorize this node
    pub labels: Vec<String>,
    /// Properties as key-value pairs
    pub properties: BTreeMap<String, PropertyValue>,
    /// First edge in the outgoing edge list
    pub first_outgoing_edge_id: EdgeId,
    /// First edge in the incoming edge list
    pub first_incoming_edge_id: EdgeId,
}

impl Node {
    /// Creates a new node with the given ID.
    ///
    /// The node starts with no labels, no properties, and no edges.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for the node
    ///
    /// # Returns
    /// A new `Node` instance.
    ///
    /// # Example
    /// ```rust
    /// use sombra::model::Node;
    ///
    /// let node = Node::new(1);
    /// assert_eq!(node.id, 1);
    /// assert!(node.labels.is_empty());
    /// assert!(node.properties.is_empty());
    /// ```
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            labels: Vec::new(),
            properties: BTreeMap::new(),
            first_outgoing_edge_id: NULL_EDGE_ID,
            first_incoming_edge_id: NULL_EDGE_ID,
        }
    }
}

/// Represents a directed edge connecting two nodes in the graph.
///
/// Edges represent relationships between nodes and can have a type
/// and properties. Edges are directed from a source node to a target node.
///
/// # Fields
///
/// * `id` - Unique identifier for the edge
/// * `source_node_id` - ID of the source (origin) node
/// * `target_node_id` - ID of the target (destination) node
/// * `type_name` - Type/name of the relationship
/// * `properties` - Key-value pairs storing edge attributes
/// * `next_outgoing_edge_id` - Next edge from source node
/// * `next_incoming_edge_id` - Next edge to target node
///
/// # Examples
///
/// ```rust
/// use sombra::model::{Edge, PropertyValue};
/// use std::collections::BTreeMap;
///
/// let mut properties = BTreeMap::new();
/// properties.insert("since".to_string(), PropertyValue::Int(2020));
///
/// let edge = Edge::new(1, 1, 2, "KNOWS");
/// // Or with properties:
/// let mut edge_with_props = Edge::new(2, 1, 2, "WORKS_WITH");
/// edge_with_props.properties = properties;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    /// Unique identifier for this edge
    pub id: EdgeId,
    /// ID of the source node
    pub source_node_id: NodeId,
    /// ID of the target node
    pub target_node_id: NodeId,
    /// Type/name of the relationship
    pub type_name: String,
    /// Properties as key-value pairs
    pub properties: BTreeMap<String, PropertyValue>,
    /// Next edge from the source node
    pub next_outgoing_edge_id: EdgeId,
    /// Next edge to the target node
    pub next_incoming_edge_id: EdgeId,
}

impl Edge {
    /// Creates a new edge with the given parameters.
    ///
    /// The edge starts with no properties and no linked edges.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for the edge
    /// * `source_node_id` - ID of the source node
    /// * `target_node_id` - ID of the target node
    /// * `type_name` - Type/name of the relationship
    ///
    /// # Returns
    /// A new `Edge` instance.
    ///
    /// # Example
    /// ```rust
    /// use sombra::model::Edge;
    ///
    /// let edge = Edge::new(1, 1, 2, "KNOWS");
    /// assert_eq!(edge.id, 1);
    /// assert_eq!(edge.source_node_id, 1);
    /// assert_eq!(edge.target_node_id, 2);
    /// assert_eq!(edge.type_name, "KNOWS");
    /// assert!(edge.properties.is_empty());
    /// ```
    pub fn new(
        id: EdgeId,
        source_node_id: NodeId,
        target_node_id: NodeId,
        type_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            source_node_id,
            target_node_id,
            type_name: type_name.into(),
            properties: BTreeMap::new(),
            next_outgoing_edge_id: NULL_EDGE_ID,
            next_incoming_edge_id: NULL_EDGE_ID,
        }
    }
}
