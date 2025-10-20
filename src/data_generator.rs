use crate::{Edge, Node, PropertyValue};
use rand::Rng;

pub struct DataGenerator {
    rng: rand::rngs::ThreadRng,
}

impl DataGenerator {
    pub fn new() -> Self {
        Self {
            rng: rand::thread_rng(),
        }
    }

    pub fn generate_social_network(
        &mut self,
        num_users: usize,
        avg_connections: usize,
    ) -> (Vec<Node>, Vec<Edge>) {
        let mut nodes = Vec::with_capacity(num_users);
        let mut edges = Vec::new();
        let mut edge_id_counter = 1u64;

        // Generate user nodes
        for i in 0..num_users {
            let mut node = Node::new((i + 1) as u64);
            node.labels.push("User".to_string());

            // Add realistic user properties
            node.properties.insert(
                "name".to_string(),
                PropertyValue::String(format!("User{}", i + 1)),
            );
            node.properties.insert(
                "age".to_string(),
                PropertyValue::Int(self.rng.gen_range(18..65)),
            );
            node.properties.insert(
                "active".to_string(),
                PropertyValue::Bool(self.rng.gen_bool(0.8)),
            );
            node.properties.insert(
                "join_date".to_string(),
                PropertyValue::Int(1609459200 + self.rng.gen_range(0..86400 * 365)),
            ); // Unix timestamp
            node.properties.insert(
                "score".to_string(),
                PropertyValue::Float(self.rng.gen_range(0.0..1000.0)),
            );

            nodes.push(node);
        }

        // Generate friendship connections (undirected graph)
        for i in 0..num_users {
            let num_connections = self.rng.gen_range(0..avg_connections * 2);
            for _ in 0..num_connections {
                let target = self.rng.gen_range(0..num_users);
                if target != i {
                    let source_id = (i + 1) as u64;
                    let target_id = (target + 1) as u64;

                    // Avoid duplicate edges
                    if !edges.iter().any(|e: &Edge| {
                        (e.source_node_id == source_id && e.target_node_id == target_id)
                            || (e.source_node_id == target_id && e.target_node_id == source_id)
                    }) {
                        let mut edge =
                            Edge::new(edge_id_counter, source_id, target_id, "FRIENDS_WITH");
                        edge_id_counter += 1;
                        edge.properties.insert(
                            "since".to_string(),
                            PropertyValue::Int(1609459200 + self.rng.gen_range(0..86400 * 365)),
                        );
                        edge.properties.insert(
                            "strength".to_string(),
                            PropertyValue::Float(self.rng.gen_range(0.1..1.0)),
                        );

                        edges.push(edge);
                    }
                }
            }
        }

        (nodes, edges)
    }

    pub fn generate_product_catalog(
        &mut self,
        num_products: usize,
        num_categories: usize,
    ) -> (Vec<Node>, Vec<Edge>) {
        let mut nodes = Vec::with_capacity(num_products + num_categories);
        let mut edges = Vec::new();
        let mut edge_id_counter = 1u64;

        // Generate category nodes
        for i in 0..num_categories {
            let mut node = Node::new((i + 1) as u64);
            node.labels.push("Category".to_string());

            node.properties.insert(
                "name".to_string(),
                PropertyValue::String(format!("Category{}", i + 1)),
            );
            node.properties.insert(
                "description".to_string(),
                PropertyValue::String(format!("Description for category {}", i + 1)),
            );
            node.properties.insert(
                "level".to_string(),
                PropertyValue::Int(self.rng.gen_range(1..4)),
            );

            nodes.push(node);
        }

        // Generate product nodes
        for i in 0..num_products {
            let mut node = Node::new((num_categories + i + 1) as u64);
            node.labels.push("Product".to_string());

            node.properties.insert(
                "name".to_string(),
                PropertyValue::String(format!("Product{}", i + 1)),
            );
            node.properties.insert(
                "price".to_string(),
                PropertyValue::Float(self.rng.gen_range(1.0..1000.0)),
            );
            node.properties.insert(
                "stock".to_string(),
                PropertyValue::Int(self.rng.gen_range(0..1000)),
            );
            node.properties.insert(
                "rating".to_string(),
                PropertyValue::Float(self.rng.gen_range(1.0..5.0)),
            );
            node.properties.insert(
                "available".to_string(),
                PropertyValue::Bool(self.rng.gen_bool(0.9)),
            );

            nodes.push(node);

            // Connect to random categories
            let num_categories_for_product = self.rng.gen_range(1..=3);
            for _ in 0..num_categories_for_product {
                let category_id = (self.rng.gen_range(1..=num_categories)) as u64;
                let product_id = (num_categories + i + 1) as u64;

                let mut edge = Edge::new(edge_id_counter, product_id, category_id, "BELONGS_TO");
                edge_id_counter += 1;
                edge.properties.insert(
                    "relevance".to_string(),
                    PropertyValue::Float(self.rng.gen_range(0.5..1.0)),
                );

                edges.push(edge);
            }
        }

        (nodes, edges)
    }

    pub fn generate_knowledge_graph(
        &mut self,
        num_entities: usize,
        num_relationships: usize,
    ) -> (Vec<Node>, Vec<Edge>) {
        let mut nodes = Vec::with_capacity(num_entities);
        let mut edges = Vec::new();
        let mut edge_id_counter = 1u64;

        let entity_types = ["Person", "Organization", "Location", "Event", "Concept"];
        let relationship_types = [
            "WORKS_FOR",
            "LOCATED_IN",
            "PARTICIPATES_IN",
            "RELATED_TO",
            "KNOWS",
        ];

        // Generate entity nodes
        for i in 0..num_entities {
            let mut node = Node::new((i + 1) as u64);
            let entity_type = entity_types[self.rng.gen_range(0..entity_types.len())];
            node.labels.push(entity_type.to_string());

            node.properties.insert(
                "name".to_string(),
                PropertyValue::String(format!("Entity{}", i + 1)),
            );
            node.properties.insert(
                "description".to_string(),
                PropertyValue::String(format!("Description for entity {}", i + 1)),
            );
            node.properties.insert(
                "confidence".to_string(),
                PropertyValue::Float(self.rng.gen_range(0.5..1.0)),
            );
            node.properties.insert(
                "created_at".to_string(),
                PropertyValue::Int(1609459200 + self.rng.gen_range(0..86400 * 730)),
            ); // 2 years

            nodes.push(node);
        }

        // Generate relationships
        for _ in 0..num_relationships {
            let source_id = (self.rng.gen_range(1..=num_entities)) as u64;
            let target_id = (self.rng.gen_range(1..=num_entities)) as u64;

            if source_id != target_id {
                let relationship_type =
                    relationship_types[self.rng.gen_range(0..relationship_types.len())];

                let mut edge = Edge::new(edge_id_counter, source_id, target_id, relationship_type);
                edge_id_counter += 1;
                edge.properties.insert(
                    "weight".to_string(),
                    PropertyValue::Float(self.rng.gen_range(0.1..1.0)),
                );
                edge.properties.insert(
                    "verified".to_string(),
                    PropertyValue::Bool(self.rng.gen_bool(0.7)),
                );

                edges.push(edge);
            }
        }

        (nodes, edges)
    }

    pub fn generate_small_dataset(&mut self) -> (Vec<Node>, Vec<Edge>) {
        self.generate_social_network(100, 10)
    }

    pub fn generate_medium_dataset(&mut self) -> (Vec<Node>, Vec<Edge>) {
        self.generate_social_network(1000, 25)
    }

    pub fn generate_large_dataset(&mut self) -> (Vec<Node>, Vec<Edge>) {
        self.generate_social_network(5000, 50)
    }

    pub fn generate_xlarge_dataset(&mut self) -> (Vec<Node>, Vec<Edge>) {
        self.generate_social_network(50000, 100)
    }

    pub fn generate_xxlarge_dataset(&mut self) -> (Vec<Node>, Vec<Edge>) {
        self.generate_social_network(100000, 100)
    }

    pub fn generate_xxxlarge_dataset(&mut self) -> (Vec<Node>, Vec<Edge>) {
        self.generate_social_network(500000, 50)
    }
}

impl Default for DataGenerator {
    fn default() -> Self {
        Self::new()
    }
}
