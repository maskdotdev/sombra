//! Knowledge Graph Example
//!
//! This example demonstrates building a knowledge graph with Sombra,
//! representing entities, concepts, relationships, and semantic information.

use serde::{Deserialize, Serialize};
use sombra::{Edge, GraphDB, Node, PropertyValue, Result};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entity {
    id: Option<u64>,
    name: String,
    entity_type: String,
    description: Option<String>,
    properties: HashMap<String, String>,
    confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Relationship {
    id: Option<u64>,
    from_entity: String,
    to_entity: String,
    relation_type: String,
    properties: HashMap<String, String>,
    confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Concept {
    id: Option<u64>,
    name: String,
    category: String,
    definition: Option<String>,
    synonyms: Vec<String>,
    related_concepts: Vec<String>,
}

struct KnowledgeGraph {
    db: GraphDB,
}

impl KnowledgeGraph {
    fn new(db_path: &str) -> Result<Self> {
        let db = GraphDB::open(db_path)?;
        Ok(KnowledgeGraph { db })
    }

    /// Add an entity to the knowledge graph
    fn add_entity(&mut self, entity: Entity) -> Result<Node> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = vec![
            ("name".into(), PropertyValue::String(entity.name.clone())),
            (
                "type".into(),
                PropertyValue::String(entity.entity_type.clone()),
            ),
            ("confidence".into(), PropertyValue::Float(entity.confidence)),
        ];

        if let Some(description) = entity.description {
            properties.push(("description".into(), PropertyValue::String(description)));
        }

        // Add custom properties
        for (key, value) in entity.properties {
            properties.push((key.into(), PropertyValue::String(value)));
        }

        let entity_node = tx.create_node("Entity", properties)?;
        tx.commit()?;
        Ok(entity_node)
    }

    /// Add a concept to the knowledge graph
    fn add_concept(&mut self, concept: Concept) -> Result<Node> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = vec![
            ("name".into(), PropertyValue::String(concept.name.clone())),
            (
                "category".into(),
                PropertyValue::String(concept.category.clone()),
            ),
        ];

        if let Some(definition) = concept.definition {
            properties.push(("definition".into(), PropertyValue::String(definition)));
        }

        // Add synonyms as a comma-separated string
        if !concept.synonyms.is_empty() {
            properties.push((
                "synonyms".into(),
                PropertyValue::String(concept.synonyms.join(", ")),
            ));
        }

        let concept_node = tx.create_node("Concept", properties)?;
        tx.commit()?;
        Ok(concept_node)
    }

    /// Create a relationship between two entities
    fn create_relationship(&mut self, relationship: Relationship) -> Result<Edge> {
        let mut tx = self.db.begin_transaction()?;

        // Find the entities by name
        let from_entities = tx.find_nodes_by_property(
            "Entity",
            "name",
            &PropertyValue::String(relationship.from_entity.clone()),
        )?;
        let to_entities = tx.find_nodes_by_property(
            "Entity",
            "name",
            &PropertyValue::String(relationship.to_entity.clone()),
        )?;

        if from_entities.is_empty() || to_entities.is_empty() {
            return Err(sombra::GraphError::InvalidArgument(
                "One or both entities not found".into(),
            ));
        }

        let mut properties = vec![
            (
                "type".into(),
                PropertyValue::String(relationship.relation_type.clone()),
            ),
            (
                "confidence".into(),
                PropertyValue::Float(relationship.confidence),
            ),
        ];

        // Add custom properties
        for (key, value) in relationship.properties {
            properties.push((key.into(), PropertyValue::String(value)));
        }

        let edge = tx.create_edge(
            from_entities[0].id,
            to_entities[0].id,
            relationship.relation_type,
            properties,
        )?;

        tx.commit()?;
        Ok(edge)
    }

    /// Link entity to concept
    fn link_entity_to_concept(&mut self, entity_name: &str, concept_name: &str) -> Result<Edge> {
        let mut tx = self.db.begin_transaction()?;

        // Find entity and concept
        let entities = tx.find_nodes_by_property(
            "Entity",
            "name",
            &PropertyValue::String(entity_name.to_string()),
        )?;
        let concepts = tx.find_nodes_by_property(
            "Concept",
            "name",
            &PropertyValue::String(concept_name.to_string()),
        )?;

        if entities.is_empty() || concepts.is_empty() {
            return Err(sombra::GraphError::InvalidArgument(
                "Entity or concept not found".into(),
            ));
        }

        let edge = tx.create_edge(
            entities[0].id,
            concepts[0].id,
            "INSTANCE_OF",
            vec![(
                "linked_at".into(),
                PropertyValue::Integer(chrono::Utc::now().timestamp()),
            )],
        )?;

        tx.commit()?;
        Ok(edge)
    }

    /// Find entities by type
    fn find_entities_by_type(&self, entity_type: &str) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;
        let entities = tx.find_nodes_by_property(
            "Entity",
            "type",
            &PropertyValue::String(entity_type.to_string()),
        )?;
        Ok(entities)
    }

    /// Find concepts by category
    fn find_concepts_by_category(&self, category: &str) -> Result<Vec<Node>> {
        let tx = self.db.begin_transaction()?;
        let concepts = tx.find_nodes_by_property(
            "Concept",
            "category",
            &PropertyValue::String(category.to_string()),
        )?;
        Ok(concepts)
    }

    /// Get relationships for an entity
    fn get_entity_relationships(&self, entity_name: &str) -> Result<Vec<(String, String, String)>> {
        let tx = self.db.begin_transaction()?;

        // Find the entity
        let entities = tx.find_nodes_by_property(
            "Entity",
            "name",
            &PropertyValue::String(entity_name.to_string()),
        )?;
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let entity_id = entities[0].id;
        let mut relationships = Vec::new();

        // Get outgoing relationships
        let outgoing = tx.get_outgoing_edges(entity_id)?;
        for edge in outgoing {
            let target_node = tx.get_node(edge.to_node)?;
            let target_props = tx.get_node_properties(edge.to_node)?;
            let target_name = target_props
                .get("name")
                .and_then(|v| v.as_string())
                .unwrap_or("Unknown");
            relationships.push((entity_name.to_string(), edge.label.clone(), target_name));
        }

        // Get incoming relationships
        let incoming = tx.get_incoming_edges(entity_id)?;
        for edge in incoming {
            let source_node = tx.get_node(edge.from_node)?;
            let source_props = tx.get_node_properties(edge.from_node)?;
            let source_name = source_props
                .get("name")
                .and_then(|v| v.as_string())
                .unwrap_or("Unknown");
            relationships.push((source_name, edge.label.clone(), entity_name.to_string()));
        }

        Ok(relationships)
    }

    /// Find related entities through concept connections
    fn find_related_entities(
        &self,
        entity_name: &str,
        max_depth: usize,
    ) -> Result<Vec<(String, String, usize)>> {
        let tx = self.db.begin_transaction()?;

        // Find the entity
        let entities = tx.find_nodes_by_property(
            "Entity",
            "name",
            &PropertyValue::String(entity_name.to_string()),
        )?;
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let mut related = Vec::new();
        let mut visited = std::collections::HashSet::new();
        visited.insert(entities[0].id);

        // BFS traversal to find related entities
        let mut queue = vec![(entities[0].id, 0)];

        while let Some((current_id, depth)) = queue.pop() {
            if depth >= max_depth {
                continue;
            }

            // Get connected entities
            let outgoing = tx.get_outgoing_edges(current_id)?;
            for edge in outgoing {
                if !visited.contains(&edge.to_node) {
                    visited.insert(edge.to_node);
                    queue.push((edge.to_node, depth + 1));

                    let target_node = tx.get_node(edge.to_node)?;
                    if target_node.label == "Entity" {
                        let target_props = tx.get_node_properties(edge.to_node)?;
                        if let Some(name) = target_props.get("name").and_then(|v| v.as_string()) {
                            related.push((name, edge.label.clone(), depth + 1));
                        }
                    }
                }
            }

            let incoming = tx.get_incoming_edges(current_id)?;
            for edge in incoming {
                if !visited.contains(&edge.from_node) {
                    visited.insert(edge.from_node);
                    queue.push((edge.from_node, depth + 1));

                    let source_node = tx.get_node(edge.from_node)?;
                    if source_node.label == "Entity" {
                        let source_props = tx.get_node_properties(edge.from_node)?;
                        if let Some(name) = source_props.get("name").and_then(|v| v.as_string()) {
                            related.push((name, format!("{}_INVERSE", edge.label), depth + 1));
                        }
                    }
                }
            }
        }

        Ok(related)
    }

    /// Search entities and concepts by text
    fn search(&self, query: &str) -> Result<Vec<(String, String, String)>> {
        let tx = self.db.begin_transaction()?;
        let mut results = Vec::new();

        // Search entities
        let entities = tx.find_nodes_by_label("Entity")?;
        for entity in entities {
            let props = tx.get_node_properties(entity.id)?;
            if let Some(name) = props.get("name").and_then(|v| v.as_string()) {
                if name.to_lowercase().contains(&query.to_lowercase()) {
                    let entity_type = props
                        .get("type")
                        .and_then(|v| v.as_string())
                        .unwrap_or("Unknown");
                    results.push((name.clone(), "Entity".to_string(), entity_type.to_string()));
                }
            }
        }

        // Search concepts
        let concepts = tx.find_nodes_by_label("Concept")?;
        for concept in concepts {
            let props = tx.get_node_properties(concept.id)?;
            if let Some(name) = props.get("name").and_then(|v| v.as_string()) {
                if name.to_lowercase().contains(&query.to_lowercase()) {
                    let category = props
                        .get("category")
                        .and_then(|v| v.as_string())
                        .unwrap_or("Unknown");
                    results.push((name.clone(), "Concept".to_string(), category.to_string()));
                }
            }
        }

        Ok(results)
    }

    /// Get knowledge graph statistics
    fn get_statistics(&self) -> Result<KnowledgeGraphStats> {
        let tx = self.db.begin_transaction()?;

        let entities = tx.find_nodes_by_label("Entity")?;
        let concepts = tx.find_nodes_by_label("Concept")?;

        let mut entity_types = HashMap::new();
        let mut concept_categories = HashMap::new();
        let mut relationship_types = HashMap::new();

        // Count entity types
        for entity in &entities {
            let props = tx.get_node_properties(entity.id)?;
            if let Some(entity_type) = props.get("type").and_then(|v| v.as_string()) {
                *entity_types.entry(entity_type.to_string()).or_insert(0) += 1;
            }
        }

        // Count concept categories
        for concept in &concepts {
            let props = tx.get_node_properties(concept.id)?;
            if let Some(category) = props.get("category").and_then(|v| v.as_string()) {
                *concept_categories.entry(category.to_string()).or_insert(0) += 1;
            }
        }

        // Count relationship types
        for entity in &entities {
            let outgoing = tx.get_outgoing_edges(entity.id)?;
            for edge in outgoing {
                *relationship_types.entry(edge.label.clone()).or_insert(0) += 1;
            }
        }

        Ok(KnowledgeGraphStats {
            total_entities: entities.len(),
            total_concepts: concepts.len(),
            entity_types,
            concept_categories,
            relationship_types,
        })
    }

    /// Export knowledge graph to JSON
    fn export_to_json(&self) -> Result<String> {
        let tx = self.db.begin_transaction()?;

        let mut export = serde_json::json!({
            "entities": [],
            "concepts": [],
            "relationships": []
        });

        // Export entities
        let entities = tx.find_nodes_by_label("Entity")?;
        for entity in entities {
            let props = tx.get_node_properties(entity.id)?;
            let mut entity_data = serde_json::Map::new();

            for (key, value) in props {
                entity_data.insert(key, serde_json::Value::String(value.to_string()));
            }

            export["entities"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::Value::Object(entity_data));
        }

        // Export concepts
        let concepts = tx.find_nodes_by_label("Concept")?;
        for concept in concepts {
            let props = tx.get_node_properties(concept.id)?;
            let mut concept_data = serde_json::Map::new();

            for (key, value) in props {
                concept_data.insert(key, serde_json::Value::String(value.to_string()));
            }

            export["concepts"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::Value::Object(concept_data));
        }

        // Export relationships
        for entity in &entities {
            let outgoing = tx.get_outgoing_edges(entity.id)?;
            for edge in outgoing {
                let from_props = tx.get_node_properties(edge.from_node)?;
                let to_props = tx.get_node_properties(edge.to_node)?;

                let from_name = from_props
                    .get("name")
                    .and_then(|v| v.as_string())
                    .unwrap_or("Unknown");
                let to_name = to_props
                    .get("name")
                    .and_then(|v| v.as_string())
                    .unwrap_or("Unknown");

                let relationship = serde_json::json!({
                    "from": from_name,
                    "to": to_name,
                    "type": edge.label
                });

                export["relationships"]
                    .as_array_mut()
                    .unwrap()
                    .push(relationship);
            }
        }

        Ok(serde_json::to_string_pretty(&export)?)
    }
}

#[derive(Debug)]
struct KnowledgeGraphStats {
    total_entities: usize,
    total_concepts: usize,
    entity_types: HashMap<String, usize>,
    concept_categories: HashMap<String, usize>,
    relationship_types: HashMap<String, usize>,
}

fn main() -> Result<()> {
    println!("🧠 Knowledge Graph Example with Sombra");

    // Initialize knowledge graph
    let mut kg = KnowledgeGraph::new("knowledge_graph.db")?;

    // Add concepts
    let person_concept = kg.add_concept(Concept {
        id: None,
        name: "Person".to_string(),
        category: "Entity".to_string(),
        definition: Some("A human being regarded as an individual.".to_string()),
        synonyms: vec![
            "human".to_string(),
            "individual".to_string(),
            "personage".to_string(),
        ],
        related_concepts: vec!["Organization".to_string(), "Location".to_string()],
    })?;

    let company_concept = kg.add_concept(Concept {
        id: None,
        name: "Company".to_string(),
        category: "Organization".to_string(),
        definition: Some("A commercial business.".to_string()),
        synonyms: vec![
            "corporation".to_string(),
            "firm".to_string(),
            "enterprise".to_string(),
        ],
        related_concepts: vec!["Person".to_string(), "Product".to_string()],
    })?;

    let technology_concept = kg.add_concept(Concept {
        id: None,
        name: "Technology".to_string(),
        category: "Concept".to_string(),
        definition: Some(
            "The application of scientific knowledge for practical purposes.".to_string(),
        ),
        synonyms: vec![
            "tech".to_string(),
            "innovation".to_string(),
            "advancement".to_string(),
        ],
        related_concepts: vec!["Company".to_string(), "Product".to_string()],
    })?;

    let ai_concept = kg.add_concept(Concept {
        id: None,
        name: "Artificial Intelligence".to_string(),
        category: "Technology".to_string(),
        definition: Some("The simulation of human intelligence in machines.".to_string()),
        synonyms: vec!["AI".to_string(), "machine intelligence".to_string()],
        related_concepts: vec!["Technology".to_string(), "Computer Science".to_string()],
    })?;

    println!("✅ Added concepts");

    // Add entities
    let mut elon_properties = HashMap::new();
    elon_properties.insert("birth_date".to_string(), "1971-06-28".to_string());
    elon_properties.insert("nationality".to_string(), "South African".to_string());

    let elon = kg.add_entity(Entity {
        id: None,
        name: "Elon Musk".to_string(),
        entity_type: "Person".to_string(),
        description: Some("CEO of Tesla and SpaceX".to_string()),
        properties: elon_properties,
        confidence: 1.0,
    })?;

    let mut tesla_properties = HashMap::new();
    tesla_properties.insert("founded".to_string(), "2003".to_string());
    tesla_properties.insert("industry".to_string(), "Automotive".to_string());

    let tesla = kg.add_entity(Entity {
        id: None,
        name: "Tesla".to_string(),
        entity_type: "Company".to_string(),
        description: Some("Electric vehicle manufacturer".to_string()),
        properties: tesla_properties,
        confidence: 1.0,
    })?;

    let mut spacex_properties = HashMap::new();
    spacex_properties.insert("founded".to_string(), "2002".to_string());
    spacex_properties.insert("industry".to_string(), "Aerospace".to_string());

    let spacex = kg.add_entity(Entity {
        id: None,
        name: "SpaceX".to_string(),
        entity_type: "Company".to_string(),
        description: Some("Space exploration company".to_string()),
        properties: spacex_properties,
        confidence: 1.0,
    })?;

    let mut openai_properties = HashMap::new();
    openai_properties.insert("founded".to_string(), "2015".to_string());
    openai_properties.insert("industry".to_string(), "AI Research".to_string());

    let openai = kg.add_entity(Entity {
        id: None,
        name: "OpenAI".to_string(),
        entity_type: "Company".to_string(),
        description: Some("AI research laboratory".to_string()),
        properties: openai_properties,
        confidence: 1.0,
    })?;

    let mut chatgpt_properties = HashMap::new();
    chatgpt_properties.insert("released".to_string(), "2022".to_string());
    chatgpt_properties.insert("type".to_string(), "Language Model".to_string());

    let chatgpt = kg.add_entity(Entity {
        id: None,
        name: "ChatGPT".to_string(),
        entity_type: "Product".to_string(),
        description: Some("AI-powered conversational agent".to_string()),
        properties: chatgpt_properties,
        confidence: 0.95,
    })?;

    println!("✅ Added entities");

    // Link entities to concepts
    kg.link_entity_to_concept("Elon Musk", "Person")?;
    kg.link_entity_to_concept("Tesla", "Company")?;
    kg.link_entity_to_concept("SpaceX", "Company")?;
    kg.link_entity_to_concept("OpenAI", "Company")?;
    kg.link_entity_to_concept("ChatGPT", "Artificial Intelligence")?;

    println!("✅ Linked entities to concepts");

    // Create relationships
    let mut ceo_properties = HashMap::new();
    ceo_properties.insert("since".to_string(), "2008".to_string());

    kg.create_relationship(Relationship {
        id: None,
        from_entity: "Elon Musk".to_string(),
        to_entity: "Tesla".to_string(),
        relation_type: "CEO_OF".to_string(),
        properties: ceo_properties,
        confidence: 1.0,
    })?;

    let mut founder_properties = HashMap::new();
    founder_properties.insert("role".to_string(), "Founder".to_string());

    kg.create_relationship(Relationship {
        id: None,
        from_entity: "Elon Musk".to_string(),
        to_entity: "SpaceX".to_string(),
        relation_type: "FOUNDER_OF".to_string(),
        properties: founder_properties,
        confidence: 1.0,
    })?;

    let mut investor_properties = HashMap::new();
    investor_properties.insert("amount".to_string(), "Significant".to_string());

    kg.create_relationship(Relationship {
        id: None,
        from_entity: "Elon Musk".to_string(),
        to_entity: "OpenAI".to_string(),
        relation_type: "INVESTOR_IN".to_string(),
        properties: investor_properties,
        confidence: 0.9,
    })?;

    let mut developed_properties = HashMap::new();
    developed_properties.insert("relationship".to_string(), "Developer".to_string());

    kg.create_relationship(Relationship {
        id: None,
        from_entity: "OpenAI".to_string(),
        to_entity: "ChatGPT".to_string(),
        relation_type: "DEVELOPED".to_string(),
        properties: developed_properties,
        confidence: 1.0,
    })?;

    println!("✅ Created relationships");

    // Demonstrate knowledge graph features
    println!("\n🔍 Knowledge Graph Queries:");

    // Find entities by type
    let companies = kg.find_entities_by_type("Company")?;
    println!("Companies in knowledge graph: {}", companies.len());
    for company in &companies {
        let tx = kg.db.begin_transaction()?;
        let props = tx.get_node_properties(company.id)?;
        let name = props
            .get("name")
            .and_then(|v| v.as_string())
            .unwrap_or("Unknown");
        println!("  - {}", name);
    }

    // Find concepts by category
    let tech_concepts = kg.find_concepts_by_category("Technology")?;
    println!("Technology concepts: {}", tech_concepts.len());
    for concept in &tech_concepts {
        let tx = kg.db.begin_transaction()?;
        let props = tx.get_node_properties(concept.id)?;
        let name = props
            .get("name")
            .and_then(|v| v.as_string())
            .unwrap_or("Unknown");
        println!("  - {}", name);
    }

    // Get relationships for Elon Musk
    let elon_relationships = kg.get_entity_relationships("Elon Musk")?;
    println!("\nElon Musk's relationships:");
    for (from, relation, to) in elon_relationships {
        println!("  {} -> {} -> {}", from, relation, to);
    }

    // Find related entities
    let related_to_tesla = kg.find_related_entities("Tesla", 2)?;
    println!("\nEntities related to Tesla (within 2 hops):");
    for (name, relation, depth) in related_to_tesla {
        println!("  {} ({} at depth {})", name, relation, depth);
    }

    // Search functionality
    let search_results = kg.search("AI")?;
    println!("\nSearch results for 'AI':");
    for (name, node_type, category) in search_results {
        println!("  {} ({}, {})", name, node_type, category);
    }

    // Statistics
    let stats = kg.get_statistics()?;
    println!("\n📊 Knowledge Graph Statistics:");
    println!("Total entities: {}", stats.total_entities);
    println!("Total concepts: {}", stats.total_concepts);
    println!("Entity types: {:?}", stats.entity_types);
    println!("Concept categories: {:?}", stats.concept_categories);
    println!("Relationship types: {:?}", stats.relationship_types);

    // Export to JSON
    let json_export = kg.export_to_json()?;
    std::fs::write("knowledge_graph_export.json", json_export)?;
    println!("\n💾 Knowledge graph exported to: knowledge_graph_export.json");

    println!("\n🎉 Knowledge graph example completed successfully!");
    println!("Database saved to: knowledge_graph.db");

    Ok(())
}
