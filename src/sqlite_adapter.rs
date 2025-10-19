use crate::{Edge, Node, PropertyValue};
use base64::Engine;
use rusqlite::{params, Connection, Result as SqliteResult};

pub struct SqliteGraphDB {
    conn: Connection,
}

impl SqliteGraphDB {
    pub fn new(path: &str) -> SqliteResult<Self> {
        let conn = Connection::open(path)?;

        // Ensure full durability for fair comparison
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;

        // Create tables for graph representation
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY,
                labels TEXT,
                properties TEXT
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS edges (
                id INTEGER PRIMARY KEY,
                source_id INTEGER,
                target_id INTEGER,
                type_name TEXT,
                properties TEXT,
                FOREIGN KEY (source_id) REFERENCES nodes (id),
                FOREIGN KEY (target_id) REFERENCES nodes (id)
            )",
            [],
        )?;

        // Create indexes for performance
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_source ON edges (source_id)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_target ON edges (target_id)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_type ON edges (type_name)",
            [],
        )?;

        Ok(Self { conn })
    }

    pub fn add_node(&mut self, node: Node) -> SqliteResult<u64> {
        let labels_json = serde_json::to_string(&node.labels).unwrap();
        let properties_json = self.properties_to_json(&node.properties);

        self.conn.execute(
            "INSERT INTO nodes (id, labels, properties) VALUES (?1, ?2, ?3)",
            params![node.id as i64, labels_json, properties_json],
        )?;

        Ok(node.id)
    }

    pub fn add_edge(&mut self, edge: Edge) -> SqliteResult<u64> {
        let properties_json = self.properties_to_json(&edge.properties);

        self.conn.execute(
            "INSERT INTO edges (id, source_id, target_id, type_name, properties) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                edge.id as i64,
                edge.source_node_id as i64,
                edge.target_node_id as i64,
                edge.type_name,
                properties_json
            ],
        )?;

        Ok(edge.id)
    }

    pub fn get_node(&mut self, node_id: u64) -> SqliteResult<Option<Node>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, labels, properties FROM nodes WHERE id = ?1")?;

        let node_iter = stmt.query_map([node_id as i64], |row| {
            let id: i64 = row.get(0)?;
            let labels_json: String = row.get(1)?;
            let properties_json: String = row.get(2)?;

            let labels: Vec<String> = serde_json::from_str(&labels_json).unwrap_or_default();
            let properties = self.json_to_properties(&properties_json);

            Ok(Node {
                id: id as u64,
                labels,
                properties,
                first_outgoing_edge_id: 0,
                first_incoming_edge_id: 0,
            })
        })?;

        for node in node_iter {
            return Ok(Some(node?));
        }

        Ok(None)
    }

    pub fn get_neighbors(&mut self, node_id: u64) -> SqliteResult<Vec<u64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT target_id FROM edges WHERE source_id = ?1")?;

        let neighbor_iter = stmt.query_map([node_id as i64], |row| {
            let target_id: i64 = row.get(0)?;
            Ok(target_id as u64)
        })?;

        let mut neighbors = Vec::new();
        for neighbor in neighbor_iter {
            neighbors.push(neighbor?);
        }

        Ok(neighbors)
    }

    pub fn get_incoming_neighbors(&mut self, node_id: u64) -> SqliteResult<Vec<u64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT source_id FROM edges WHERE target_id = ?1")?;

        let neighbor_iter = stmt.query_map([node_id as i64], |row| {
            let source_id: i64 = row.get(0)?;
            Ok(source_id as u64)
        })?;

        let mut neighbors = Vec::new();
        for neighbor in neighbor_iter {
            neighbors.push(neighbor?);
        }

        Ok(neighbors)
    }

    pub fn get_neighbors_two_hops(&mut self, node_id: u64) -> SqliteResult<Vec<u64>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT e2.target_id 
             FROM edges e1
             JOIN edges e2 ON e1.target_id = e2.source_id
             WHERE e1.source_id = ?1 AND e2.target_id != ?1",
        )?;

        let neighbor_iter = stmt.query_map([node_id as i64], |row| {
            let target_id: i64 = row.get(0)?;
            Ok(target_id as u64)
        })?;

        let mut neighbors = Vec::new();
        for neighbor in neighbor_iter {
            neighbors.push(neighbor?);
        }

        Ok(neighbors)
    }

    pub fn get_neighbors_three_hops(&mut self, node_id: u64) -> SqliteResult<Vec<u64>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT e3.target_id 
             FROM edges e1
             JOIN edges e2 ON e1.target_id = e2.source_id
             JOIN edges e3 ON e2.target_id = e3.source_id
             WHERE e1.source_id = ?1 AND e3.target_id != ?1",
        )?;

        let neighbor_iter = stmt.query_map([node_id as i64], |row| {
            let target_id: i64 = row.get(0)?;
            Ok(target_id as u64)
        })?;

        let mut neighbors = Vec::new();
        for neighbor in neighbor_iter {
            neighbors.push(neighbor?);
        }

        Ok(neighbors)
    }

    pub fn bfs_traversal(
        &mut self,
        start_node_id: u64,
        max_depth: usize,
    ) -> SqliteResult<Vec<(u64, usize)>> {
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        let mut result = Vec::new();

        queue.push_back((start_node_id, 0));
        visited.insert(start_node_id);

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            result.push((node_id, depth));

            let neighbors = self.get_neighbors(node_id)?;
            for neighbor_id in neighbors {
                if visited.insert(neighbor_id) {
                    queue.push_back((neighbor_id, depth + 1));
                }
            }
        }

        Ok(result)
    }

    pub fn get_nodes_by_label(&mut self, label: &str) -> SqliteResult<Vec<u64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id FROM nodes WHERE labels LIKE ?")?;

        let search_pattern = format!("%\"{}\"", label);
        let node_iter = stmt.query_map([search_pattern], |row| {
            let id: i64 = row.get(0)?;
            Ok(id as u64)
        })?;

        let mut nodes = Vec::new();
        for node in node_iter {
            nodes.push(node?);
        }

        Ok(nodes)
    }

    pub fn count_outgoing_edges(&mut self, node_id: u64) -> SqliteResult<usize> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM edges WHERE source_id = ?1")?;
        let count: i64 = stmt.query_row([node_id as i64], |row| row.get(0))?;
        Ok(count as usize)
    }

    pub fn count_incoming_edges(&mut self, node_id: u64) -> SqliteResult<usize> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM edges WHERE target_id = ?1")?;
        let count: i64 = stmt.query_row([node_id as i64], |row| row.get(0))?;
        Ok(count as usize)
    }

    pub fn begin_transaction(&mut self) -> SqliteResult<()> {
        self.conn.execute("BEGIN TRANSACTION", [])?;
        Ok(())
    }

    pub fn commit(&mut self) -> SqliteResult<()> {
        self.conn.execute("COMMIT", [])?;
        Ok(())
    }

    pub fn rollback(&mut self) -> SqliteResult<()> {
        self.conn.execute("ROLLBACK", [])?;
        Ok(())
    }

    pub fn bulk_insert_nodes(&mut self, nodes: &[Node]) -> SqliteResult<()> {
        let tx = self.conn.transaction()?;

        for node in nodes {
            let labels_json = serde_json::to_string(&node.labels).unwrap();
            let properties_json = {
                // Convert properties to JSON without borrowing self
                let mut map = serde_json::Map::new();
                for (key, value) in &node.properties {
                    let json_value = match value {
                        PropertyValue::Bool(b) => serde_json::Value::Bool(*b),
                        PropertyValue::Int(i) => {
                            serde_json::Value::Number(serde_json::Number::from(*i))
                        }
                        PropertyValue::Float(f) => serde_json::Value::Number(
                            serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
                        ),
                        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
                        PropertyValue::Bytes(bytes) => serde_json::Value::String(
                            base64::engine::general_purpose::STANDARD.encode(bytes),
                        ),
                    };
                    map.insert(key.clone(), json_value);
                }
                serde_json::Value::Object(map)
            };

            tx.execute(
                "INSERT INTO nodes (id, labels, properties) VALUES (?1, ?2, ?3)",
                params![node.id as i64, labels_json, properties_json.to_string()],
            )?;
        }

        tx.commit()
    }

    pub fn bulk_insert_edges(&mut self, edges: &[Edge]) -> SqliteResult<()> {
        let tx = self.conn.transaction()?;

        for edge in edges {
            let properties_json = {
                // Convert properties to JSON without borrowing self
                let mut map = serde_json::Map::new();
                for (key, value) in &edge.properties {
                    let json_value = match value {
                        PropertyValue::Bool(b) => serde_json::Value::Bool(*b),
                        PropertyValue::Int(i) => {
                            serde_json::Value::Number(serde_json::Number::from(*i))
                        }
                        PropertyValue::Float(f) => serde_json::Value::Number(
                            serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
                        ),
                        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
                        PropertyValue::Bytes(bytes) => serde_json::Value::String(
                            base64::engine::general_purpose::STANDARD.encode(bytes),
                        ),
                    };
                    map.insert(key.clone(), json_value);
                }
                serde_json::Value::Object(map)
            };

            tx.execute(
                "INSERT INTO edges (id, source_id, target_id, type_name, properties) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    edge.id as i64,
                    edge.source_node_id as i64,
                    edge.target_node_id as i64,
                    edge.type_name,
                    properties_json.to_string()
                ],
            )?;
        }

        tx.commit()
    }

    pub fn count_nodes(&mut self) -> SqliteResult<u64> {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM nodes")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count as u64)
    }

    pub fn count_edges(&mut self) -> SqliteResult<u64> {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM edges")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count as u64)
    }

    fn properties_to_json(
        &self,
        properties: &std::collections::BTreeMap<String, PropertyValue>,
    ) -> String {
        let mut map = serde_json::Map::new();
        for (key, value) in properties {
            map.insert(key.clone(), self.property_value_to_json(value));
        }
        serde_json::to_string(&map).unwrap_or_default()
    }

    fn property_value_to_json(&self, value: &PropertyValue) -> serde_json::Value {
        match value {
            PropertyValue::Bool(b) => serde_json::Value::Bool(*b),
            PropertyValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
            PropertyValue::Float(f) => serde_json::Value::Number(
                serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)),
            ),
            PropertyValue::String(s) => serde_json::Value::String(s.clone()),
            PropertyValue::Bytes(bytes) => {
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
            }
        }
    }

    fn json_to_properties(&self, json: &str) -> std::collections::BTreeMap<String, PropertyValue> {
        let mut properties = std::collections::BTreeMap::new();

        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json) {
            if let serde_json::Value::Object(map) = parsed {
                for (key, value) in map {
                    properties.insert(key, self.json_to_property_value(&value));
                }
            }
        }

        properties
    }

    fn json_to_property_value(&self, value: &serde_json::Value) -> PropertyValue {
        match value {
            serde_json::Value::Bool(b) => PropertyValue::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    PropertyValue::Int(i)
                } else if let Some(f) = n.as_f64() {
                    PropertyValue::Float(f)
                } else {
                    PropertyValue::Int(0)
                }
            }
            serde_json::Value::String(s) => {
                // Try to detect if this is base64 encoded bytes
                if s.len() % 4 == 0
                    && s.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
                {
                    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s) {
                        if bytes.len() > 32 {
                            // Heuristic: if it's long, probably bytes
                            return PropertyValue::Bytes(bytes);
                        }
                    }
                }
                PropertyValue::String(s.clone())
            }
            _ => PropertyValue::String(value.to_string()),
        }
    }
}
