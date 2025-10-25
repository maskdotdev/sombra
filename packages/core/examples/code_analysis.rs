#![allow(clippy::uninlined_format_args)]

//! Code Structure Analysis Example
//!
//! This example demonstrates using Sombra to analyze and query code structure,
//! modeling classes, functions, imports, and their relationships.

use sombra::{Edge, EdgeId, GraphDB, GraphError, Node, NodeId, PropertyValue};
use std::collections::BTreeMap;

struct CodeGraph {
    db: GraphDB,
}

impl CodeGraph {
    fn new(db_path: &str) -> Result<Self, GraphError> {
        let db = GraphDB::open(db_path)?;
        Ok(CodeGraph { db })
    }

    fn add_file(&mut self, path: &str, language: &str, lines: i64) -> Result<NodeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = BTreeMap::new();
        properties.insert("path".to_string(), PropertyValue::String(path.to_string()));
        properties.insert(
            "language".to_string(),
            PropertyValue::String(language.to_string()),
        );
        properties.insert("lines".to_string(), PropertyValue::Int(lines));

        let mut node = Node::new(0);
        node.properties = properties;
        node.labels.push("File".to_string());

        let node_id = tx.add_node(node)?;
        tx.commit()?;
        Ok(node_id)
    }

    fn add_class(&mut self, name: &str, file_id: NodeId, line: i64) -> Result<NodeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = BTreeMap::new();
        properties.insert("name".to_string(), PropertyValue::String(name.to_string()));
        properties.insert("line".to_string(), PropertyValue::Int(line));

        let mut node = Node::new(0);
        node.properties = properties;
        node.labels.push("Class".to_string());

        let class_id = tx.add_node(node)?;

        let edge = Edge::new(0, file_id, class_id, "CONTAINS");
        tx.add_edge(edge)?;

        tx.commit()?;
        Ok(class_id)
    }

    fn add_function(
        &mut self,
        name: &str,
        parent_id: NodeId,
        line: i64,
        complexity: i64,
    ) -> Result<NodeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let mut properties = BTreeMap::new();
        properties.insert("name".to_string(), PropertyValue::String(name.to_string()));
        properties.insert("line".to_string(), PropertyValue::Int(line));
        properties.insert("complexity".to_string(), PropertyValue::Int(complexity));

        let mut node = Node::new(0);
        node.properties = properties;
        node.labels.push("Function".to_string());

        let func_id = tx.add_node(node)?;

        let edge = Edge::new(0, parent_id, func_id, "DEFINES");
        tx.add_edge(edge)?;

        tx.commit()?;
        Ok(func_id)
    }

    fn add_call(
        &mut self,
        caller_id: NodeId,
        callee_id: NodeId,
        count: i64,
    ) -> Result<EdgeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let mut edge = Edge::new(0, caller_id, callee_id, "CALLS");
        edge.properties
            .insert("count".to_string(), PropertyValue::Int(count));

        let edge_id = tx.add_edge(edge)?;
        tx.commit()?;
        Ok(edge_id)
    }

    fn add_import(
        &mut self,
        from_file_id: NodeId,
        to_file_id: NodeId,
    ) -> Result<EdgeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let edge = Edge::new(0, from_file_id, to_file_id, "IMPORTS");
        let edge_id = tx.add_edge(edge)?;

        tx.commit()?;
        Ok(edge_id)
    }

    #[allow(dead_code)]
    fn add_inherits(
        &mut self,
        subclass_id: NodeId,
        superclass_id: NodeId,
    ) -> Result<EdgeId, GraphError> {
        let mut tx = self.db.begin_transaction()?;

        let edge = Edge::new(0, subclass_id, superclass_id, "INHERITS");
        let edge_id = tx.add_edge(edge)?;

        tx.commit()?;
        Ok(edge_id)
    }

    fn find_all_classes(&mut self) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let class_ids = tx.get_nodes_by_label("Class")?;
        tx.commit()?;
        Ok(class_ids)
    }

    fn find_all_functions(&mut self) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let func_ids = tx.get_nodes_by_label("Function")?;
        tx.commit()?;
        Ok(func_ids)
    }

    fn get_class_methods(&mut self, class_id: NodeId) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let neighbors = tx.get_neighbors(class_id)?;

        let mut methods = Vec::new();
        for neighbor_id in neighbors {
            if let Some(node) = tx.get_node(neighbor_id)? {
                if node.labels.contains(&"Function".to_string()) {
                    methods.push(neighbor_id);
                }
            }
        }

        tx.commit()?;
        Ok(methods)
    }

    fn get_function_calls(&mut self, func_id: NodeId) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let neighbors = tx.get_neighbors(func_id)?;
        tx.commit()?;
        Ok(neighbors)
    }

    fn calculate_file_complexity(&mut self, file_id: NodeId) -> Result<i64, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let neighbors = tx.get_neighbors(file_id)?;

        let mut total_complexity = 0;
        for neighbor_id in neighbors {
            if let Some(node) = tx.get_node(neighbor_id)? {
                if node.labels.contains(&"Class".to_string()) {
                    let class_neighbors = tx.get_neighbors(neighbor_id)?;
                    for method_id in class_neighbors {
                        if let Some(method_node) = tx.get_node(method_id)? {
                            if method_node.labels.contains(&"Function".to_string()) {
                                if let Some(PropertyValue::Int(complexity)) =
                                    method_node.properties.get("complexity")
                                {
                                    total_complexity += complexity;
                                }
                            }
                        }
                    }
                } else if node.labels.contains(&"Function".to_string()) {
                    if let Some(PropertyValue::Int(complexity)) = node.properties.get("complexity")
                    {
                        total_complexity += complexity;
                    }
                }
            }
        }

        tx.commit()?;
        Ok(total_complexity)
    }

    fn find_file_dependencies(&mut self, file_id: NodeId) -> Result<Vec<NodeId>, GraphError> {
        let mut tx = self.db.begin_transaction()?;
        let neighbors = tx.get_neighbors(file_id)?;

        let mut dependencies = Vec::new();
        for neighbor_id in neighbors {
            if let Some(node) = tx.get_node(neighbor_id)? {
                if node.labels.contains(&"File".to_string()) && neighbor_id != file_id {
                    dependencies.push(neighbor_id);
                }
            }
        }

        tx.commit()?;
        Ok(dependencies)
    }
}

fn main() -> Result<(), GraphError> {
    println!("🔍 Code Structure Analysis Example with Sombra\n");

    let mut code_graph = CodeGraph::new("code_analysis.db")?;

    println!("📁 Creating file nodes...");
    let user_service = code_graph.add_file("src/services/user_service.rs", "rust", 250)?;
    let database = code_graph.add_file("src/db/database.rs", "rust", 180)?;
    let api_handler = code_graph.add_file("src/api/handler.rs", "rust", 320)?;
    println!("✅ Created 3 files\n");

    println!("🏗️  Creating class/module nodes...");
    let user_service_class = code_graph.add_class("UserService", user_service, 15)?;
    let db_connection_class = code_graph.add_class("DbConnection", database, 10)?;
    let api_handler_class = code_graph.add_class("ApiHandler", api_handler, 20)?;
    println!("✅ Created 3 classes\n");

    println!("⚙️  Creating function nodes...");
    let create_user = code_graph.add_function("create_user", user_service_class, 25, 8)?;
    let find_user = code_graph.add_function("find_user", user_service_class, 45, 5)?;
    let update_user = code_graph.add_function("update_user", user_service_class, 65, 12)?;

    let execute_query = code_graph.add_function("execute_query", db_connection_class, 20, 6)?;
    let begin_transaction =
        code_graph.add_function("begin_transaction", db_connection_class, 50, 4)?;

    let handle_post = code_graph.add_function("handle_post", api_handler_class, 30, 10)?;
    let handle_get = code_graph.add_function("handle_get", api_handler_class, 60, 7)?;
    println!("✅ Created 7 functions\n");

    println!("🔗 Creating relationships...");
    code_graph.add_import(user_service, database)?;
    code_graph.add_import(api_handler, user_service)?;

    code_graph.add_call(create_user, execute_query, 2)?;
    code_graph.add_call(find_user, execute_query, 1)?;
    code_graph.add_call(update_user, begin_transaction, 1)?;
    code_graph.add_call(update_user, execute_query, 3)?;

    code_graph.add_call(handle_post, create_user, 1)?;
    code_graph.add_call(handle_get, find_user, 1)?;
    println!("✅ Created imports and function calls\n");

    println!("═══════════════════════════════════════════════════");
    println!("📊 ANALYSIS RESULTS");
    println!("═══════════════════════════════════════════════════\n");

    let all_classes = code_graph.find_all_classes()?;
    println!("📦 Total Classes: {}", all_classes.len());

    let all_functions = code_graph.find_all_functions()?;
    println!("⚙️  Total Functions: {}", all_functions.len());
    println!();

    let user_service_methods = code_graph.get_class_methods(user_service_class)?;
    println!("🔧 UserService methods: {}", user_service_methods.len());

    let create_user_calls = code_graph.get_function_calls(create_user)?;
    println!(
        "📞 create_user() calls {} other functions",
        create_user_calls.len()
    );
    println!();

    let user_service_complexity = code_graph.calculate_file_complexity(user_service)?;
    println!(
        "📈 user_service.rs cyclomatic complexity: {} (expected: 25 from 8+5+12)",
        user_service_complexity
    );

    let api_handler_complexity = code_graph.calculate_file_complexity(api_handler)?;
    println!(
        "📈 handler.rs cyclomatic complexity: {} (expected: 17 from 10+7)",
        api_handler_complexity
    );
    println!();

    let api_dependencies = code_graph.find_file_dependencies(api_handler)?;
    println!(
        "🔗 handler.rs imports {} files (expected: 1)",
        api_dependencies.len()
    );

    let user_service_dependencies = code_graph.find_file_dependencies(user_service)?;
    println!(
        "🔗 user_service.rs imports {} files (expected: 1)",
        user_service_dependencies.len()
    );
    println!();

    println!("═══════════════════════════════════════════════════");
    println!("✓ VERIFICATION");
    println!("═══════════════════════════════════════════════════");

    assert_eq!(all_classes.len(), 3, "Expected 3 classes");
    assert_eq!(all_functions.len(), 7, "Expected 7 functions");
    assert_eq!(
        user_service_methods.len(),
        3,
        "Expected 3 methods in UserService"
    );
    assert_eq!(
        create_user_calls.len(),
        1,
        "Expected create_user to call 1 function"
    );
    assert_eq!(
        user_service_complexity, 25,
        "Expected user_service complexity = 25"
    );
    assert_eq!(
        api_handler_complexity, 17,
        "Expected api_handler complexity = 17"
    );
    assert_eq!(
        api_dependencies.len(),
        1,
        "Expected handler to import 1 file"
    );
    assert_eq!(
        user_service_dependencies.len(),
        1,
        "Expected user_service to import 1 file"
    );

    println!("✅ All assertions passed - metrics are accurate!");
    println!();

    println!("═══════════════════════════════════════════════════");
    println!("💡 USE CASES");
    println!("═══════════════════════════════════════════════════");
    println!("✓ Identify highly complex functions (refactoring targets)");
    println!("✓ Find circular dependencies between modules");
    println!("✓ Track function call chains for debugging");
    println!("✓ Analyze class hierarchies and inheritance patterns");
    println!("✓ Calculate metrics: cyclomatic complexity, fan-in/fan-out");
    println!("✓ Generate dependency graphs for documentation");
    println!("✓ Find dead code by analyzing call relationships");
    println!("✓ Impact analysis: what breaks if I change this function?");
    println!("\n🎉 Code analysis example completed!");
    println!("Database saved to: code_analysis.db");

    Ok(())
}
