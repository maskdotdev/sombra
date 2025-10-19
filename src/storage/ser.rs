use std::convert::TryInto;

use crate::error::{GraphError, Result};
use crate::model::{Edge, Node, PropertyValue, NULL_EDGE_ID, NULL_NODE_ID};

const TAG_BOOL: u8 = 0x01;
const TAG_INT: u8 = 0x02;
const TAG_FLOAT: u8 = 0x03;
const TAG_STRING: u8 = 0x04;
const TAG_BYTES: u8 = 0x05;

pub fn serialize_node(node: &Node) -> Result<Vec<u8>> {
    if node.id == NULL_NODE_ID {
        return Err(GraphError::InvalidArgument("node id 0 is reserved".into()));
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(&node.id.to_le_bytes());
    buf.extend_from_slice(&node.first_outgoing_edge_id.to_le_bytes());
    buf.extend_from_slice(&node.first_incoming_edge_id.to_le_bytes());

    let label_count: u32 = node
        .labels
        .len()
        .try_into()
        .map_err(|_| GraphError::InvalidArgument("too many labels to serialize".into()))?;
    buf.extend_from_slice(&label_count.to_le_bytes());

    for label in &node.labels {
        if label.is_empty() {
            return Err(GraphError::InvalidArgument(
                "labels must be non-empty".into(),
            ));
        }
        let label_bytes = label.as_bytes();
        let len: u32 = label_bytes
            .len()
            .try_into()
            .map_err(|_| GraphError::InvalidArgument("label length exceeds u32::MAX".into()))?;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(label_bytes);
    }

    let property_count: u32 = node
        .properties
        .len()
        .try_into()
        .map_err(|_| GraphError::InvalidArgument("too many properties to serialize".into()))?;
    buf.extend_from_slice(&property_count.to_le_bytes());

    for (key, value) in &node.properties {
        if key.is_empty() {
            return Err(GraphError::InvalidArgument(
                "property keys must be non-empty".into(),
            ));
        }
        let key_bytes = key.as_bytes();
        let key_len: u32 = key_bytes.len().try_into().map_err(|_| {
            GraphError::InvalidArgument("property key length exceeds u32::MAX".into())
        })?;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key_bytes);
        write_property_value(&mut buf, value)?;
    }

    Ok(buf)
}

pub fn serialize_edge(edge: &Edge) -> Result<Vec<u8>> {
    if edge.id == NULL_EDGE_ID {
        return Err(GraphError::InvalidArgument("edge id 0 is reserved".into()));
    }
    if edge.source_node_id == NULL_NODE_ID {
        return Err(GraphError::InvalidArgument(
            "edge source node id 0 is invalid".into(),
        ));
    }
    if edge.target_node_id == NULL_NODE_ID {
        return Err(GraphError::InvalidArgument(
            "edge target node id 0 is invalid".into(),
        ));
    }
    if edge.type_name.is_empty() {
        return Err(GraphError::InvalidArgument(
            "edge type must be non-empty".into(),
        ));
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(&edge.id.to_le_bytes());
    buf.extend_from_slice(&edge.source_node_id.to_le_bytes());
    buf.extend_from_slice(&edge.target_node_id.to_le_bytes());
    buf.extend_from_slice(&edge.next_outgoing_edge_id.to_le_bytes());
    buf.extend_from_slice(&edge.next_incoming_edge_id.to_le_bytes());

    write_string(&mut buf, &edge.type_name)?;

    let property_count: u32 = edge
        .properties
        .len()
        .try_into()
        .map_err(|_| GraphError::InvalidArgument("too many properties to serialize".into()))?;
    buf.extend_from_slice(&property_count.to_le_bytes());

    for (key, value) in &edge.properties {
        if key.is_empty() {
            return Err(GraphError::InvalidArgument(
                "property keys must be non-empty".into(),
            ));
        }
        let key_bytes = key.as_bytes();
        let key_len: u32 = key_bytes.len().try_into().map_err(|_| {
            GraphError::InvalidArgument("property key length exceeds u32::MAX".into())
        })?;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key_bytes);
        write_property_value(&mut buf, value)?;
    }

    Ok(buf)
}

pub fn deserialize_node(bytes: &[u8]) -> Result<Node> {
    let mut cursor = Cursor::new(bytes);

    let id = cursor.read_u64()?;
    if id == NULL_NODE_ID {
        return Err(GraphError::Corruption("node id 0 encountered".into()));
    }

    let first_outgoing_edge_id = cursor.read_u64()?;
    let first_incoming_edge_id = cursor.read_u64()?;

    let label_count = cursor.read_u32()? as usize;
    let mut labels = Vec::with_capacity(label_count);
    for _ in 0..label_count {
        let label = cursor.read_string()?;
        if label.is_empty() {
            return Err(GraphError::Corruption(
                "empty label encountered in serialization".into(),
            ));
        }
        labels.push(label);
    }

    let property_count = cursor.read_u32()? as usize;
    let mut properties = std::collections::BTreeMap::new();
    for _ in 0..property_count {
        let key = cursor.read_string()?;
        if key.is_empty() {
            return Err(GraphError::Corruption(
                "empty property key encountered".into(),
            ));
        }
        let value = cursor.read_property_value()?;
        if properties.insert(key.clone(), value).is_some() {
            return Err(GraphError::Corruption(
                "duplicate property key encountered".into(),
            ));
        }
    }

    cursor.ensure_consumed()?;

    Ok(Node {
        id,
        labels,
        properties,
        first_outgoing_edge_id,
        first_incoming_edge_id,
    })
}

pub fn deserialize_edge(bytes: &[u8]) -> Result<Edge> {
    let mut cursor = Cursor::new(bytes);

    let id = cursor.read_u64()?;
    if id == NULL_EDGE_ID {
        return Err(GraphError::Corruption("edge id 0 encountered".into()));
    }
    let source_node_id = cursor.read_u64()?;
    let target_node_id = cursor.read_u64()?;
    if source_node_id == NULL_NODE_ID || target_node_id == NULL_NODE_ID {
        return Err(GraphError::Corruption("edge references node id 0".into()));
    }

    let next_outgoing_edge_id = cursor.read_u64()?;
    let next_incoming_edge_id = cursor.read_u64()?;
    let type_name = cursor.read_string()?;
    if type_name.is_empty() {
        return Err(GraphError::Corruption("edge type must be non-empty".into()));
    }

    let property_count = cursor.read_u32()? as usize;
    let mut properties = std::collections::BTreeMap::new();
    for _ in 0..property_count {
        let key = cursor.read_string()?;
        if key.is_empty() {
            return Err(GraphError::Corruption(
                "empty property key encountered".into(),
            ));
        }
        let value = cursor.read_property_value()?;
        if properties.insert(key.clone(), value).is_some() {
            return Err(GraphError::Corruption(
                "duplicate property key encountered".into(),
            ));
        }
    }

    cursor.ensure_consumed()?;

    Ok(Edge {
        id,
        source_node_id,
        target_node_id,
        type_name,
        properties,
        next_outgoing_edge_id,
        next_incoming_edge_id,
    })
}

fn write_property_value(buf: &mut Vec<u8>, value: &PropertyValue) -> Result<()> {
    match value {
        PropertyValue::Bool(v) => {
            buf.push(TAG_BOOL);
            buf.push(if *v { 1 } else { 0 });
        }
        PropertyValue::Int(v) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::Float(v) => {
            buf.push(TAG_FLOAT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::String(s) => {
            buf.push(TAG_STRING);
            write_string(buf, s)?;
        }
        PropertyValue::Bytes(b) => {
            buf.push(TAG_BYTES);
            write_bytes(buf, b)?;
        }
    }
    Ok(())
}

fn write_string(buf: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len: u32 = bytes
        .len()
        .try_into()
        .map_err(|_| GraphError::InvalidArgument("string length exceeds u32::MAX".into()))?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len: u32 = bytes
        .len()
        .try_into()
        .map_err(|_| GraphError::InvalidArgument("byte array length exceeds u32::MAX".into()))?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

struct Cursor<'a> {
    data: &'a [u8],
    index: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, index: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.index + len > self.data.len() {
            return Err(GraphError::Corruption("unexpected end of payload".into()));
        }
        let start = self.index;
        self.index += len;
        Ok(&self.data[start..start + len])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .expect("slice has exactly 4 bytes");
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .expect("slice has exactly 8 bytes");
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| GraphError::Corruption("invalid UTF-8 string".into()))
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        Ok(bytes.to_vec())
    }

    fn read_property_value(&mut self) -> Result<PropertyValue> {
        let tag = self.read_exact(1)?[0];
        match tag {
            TAG_BOOL => {
                let value = self.read_exact(1)?[0];
                match value {
                    0 => Ok(PropertyValue::Bool(false)),
                    1 => Ok(PropertyValue::Bool(true)),
                    other => Err(GraphError::Corruption(format!(
                        "invalid boolean encoding: {other}"
                    ))),
                }
            }
            TAG_INT => {
                let bytes: [u8; 8] = self
                    .read_exact(8)?
                    .try_into()
                    .expect("slice has exactly 8 bytes");
                Ok(PropertyValue::Int(i64::from_le_bytes(bytes)))
            }
            TAG_FLOAT => {
                let bytes: [u8; 8] = self
                    .read_exact(8)?
                    .try_into()
                    .expect("slice has exactly 8 bytes");
                Ok(PropertyValue::Float(f64::from_le_bytes(bytes)))
            }
            TAG_STRING => Ok(PropertyValue::String(self.read_string()?)),
            TAG_BYTES => Ok(PropertyValue::Bytes(self.read_bytes()?)),
            other => Err(GraphError::Corruption(format!(
                "unknown property value tag: 0x{other:02X}"
            ))),
        }
    }

    fn ensure_consumed(&self) -> Result<()> {
        if self.index != self.data.len() {
            return Err(GraphError::Corruption(
                "unexpected trailing bytes in payload".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn node_round_trip() {
        let mut node = Node::new(1);
        node.labels = vec!["File".into(), "Rust".into()];
        node.first_outgoing_edge_id = 10;
        node.first_incoming_edge_id = 20;
        node.properties = BTreeMap::from([
            ("path".into(), PropertyValue::String("main.rs".into())),
            ("size".into(), PropertyValue::Int(1337)),
            ("active".into(), PropertyValue::Bool(true)),
        ]);

        let serialized = serialize_node(&node).expect("serialize");
        let decoded = deserialize_node(&serialized).expect("deserialize");
        assert_eq!(node, decoded);
    }

    #[test]
    fn edge_round_trip() {
        let mut edge = Edge::new(2, 1, 3, "CALLS");
        edge.next_outgoing_edge_id = 42;
        edge.next_incoming_edge_id = 99;
        edge.properties = BTreeMap::from([
            ("weight".into(), PropertyValue::Float(0.75)),
            ("bytes".into(), PropertyValue::Bytes(vec![1, 2, 3])),
        ]);

        let serialized = serialize_edge(&edge).expect("serialize");
        let decoded = deserialize_edge(&serialized).expect("deserialize");
        assert_eq!(edge, decoded);
    }

    #[test]
    fn invalid_boolean_tag() {
        let invalid = vec![
            TAG_BOOL, 2, // invalid boolean byte
        ];
        let mut cursor = Cursor::new(&invalid);
        assert!(cursor.read_property_value().is_err());
    }

    #[test]
    fn trailing_bytes_error() {
        let mut node = Node::new(5);
        node.labels.push("Solo".into());
        let mut serialized = serialize_node(&node).expect("serialize");
        serialized.extend_from_slice(&[0, 1, 2]); // extra bytes
        let err = deserialize_node(&serialized).unwrap_err();
        assert!(matches!(err, GraphError::Corruption(_)));
    }
}
