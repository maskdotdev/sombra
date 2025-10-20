use crate::{
    data_generator::DataGenerator, performance_utils::BenchmarkSuite,
    sqlite_adapter::SqliteGraphDB, Edge, GraphDB, Node, PropertyValue,
};
use std::cell::RefCell;
use tempfile::TempDir;

macro_rules! log_or_return {
    ($expr:expr) => {{
        match $expr {
            Ok(value) => value,
            Err(err) => {
                eprintln!("{} failed: {}", stringify!($expr), err);
                return;
            }
        }
    }};
}

pub struct BenchmarkRunner {
    pub suite: BenchmarkSuite,
    data_generator: DataGenerator,
}

impl BenchmarkRunner {
    pub fn new() -> Self {
        Self {
            suite: BenchmarkSuite::new(),
            data_generator: DataGenerator::new(),
        }
    }

    pub fn run_all_benchmarks(&mut self) {
        println!("Running comprehensive benchmarks...\n");

        // Small dataset benchmarks
        self.run_small_dataset_benchmarks();

        // Medium dataset benchmarks
        self.run_medium_dataset_benchmarks();

        // Large dataset benchmarks (if feasible)
        self.run_large_dataset_benchmarks();

        // Print results
        self.suite.print_summary();
        self.suite.print_detailed();

        // Export to CSV
        if let Err(e) = self.suite.export_csv("benchmark_results.csv") {
            eprintln!("Failed to export CSV: {}", e);
        }
    }

    pub fn run_small_dataset_benchmarks(&mut self) {
        self.run_small_dataset_benchmarks_with_modes();
    }

    fn run_small_dataset_benchmarks_with_modes(&mut self) {
        println!("=== Small Dataset Benchmarks (100 nodes, ~1000 edges) ===");

        let (nodes, edges) = self.data_generator.generate_small_dataset();

        println!("\n--- Benchmark Mode (No Sync) ---");
        self.benchmark_sombra_insert_with_config(
            "sombra_small_benchmark",
            &nodes,
            &edges,
            crate::db::Config::benchmark(),
        );

        println!("\n--- Balanced Mode (Sync every 100 tx) ---");
        self.benchmark_sombra_insert_with_config(
            "sombra_small_balanced",
            &nodes,
            &edges,
            crate::db::Config::balanced(),
        );

        println!("\n--- Production Mode (Full Sync) ---");
        self.benchmark_sombra_insert_with_config(
            "sombra_small_production",
            &nodes,
            &edges,
            crate::db::Config::production(),
        );

        println!("\n--- SQLite Benchmarks ---");
        self.benchmark_sqlite_insert("sqlite_small_insert", &nodes, &edges);
        self.benchmark_sqlite_query("sqlite_small_query", &nodes, &edges);
    }

    pub fn run_medium_dataset_benchmarks(&mut self) {
        println!("\n=== Medium Dataset Benchmarks (1000 nodes, ~25000 edges) ===");

        let (nodes, edges) = self.data_generator.generate_medium_dataset();

        // Sombra benchmarks
        self.benchmark_sombra_insert("sombra_medium_insert", &nodes, &edges);
        self.benchmark_sombra_query("sombra_medium_query", &nodes, &edges);

        // SQLite benchmarks
        self.benchmark_sqlite_insert("sqlite_medium_insert", &nodes, &edges);
        self.benchmark_sqlite_query("sqlite_medium_query", &nodes, &edges);
    }

    pub fn run_large_dataset_benchmarks(&mut self) {
        println!("\n=== Large Dataset Benchmarks (10000 nodes, ~500000 edges) ===");

        let (nodes, edges) = self.data_generator.generate_large_dataset();

        // Sombra benchmarks
        self.benchmark_sombra_insert("sombra_large_insert", &nodes, &edges);
        self.benchmark_sombra_query("sombra_large_query", &nodes, &edges);

        // SQLite benchmarks
        self.benchmark_sqlite_insert("sqlite_large_insert", &nodes, &edges);
        self.benchmark_sqlite_query("sqlite_large_query", &nodes, &edges);
    }

    pub fn run_scalability_benchmarks(&mut self) {
        println!("\n=== Scalability Benchmarks (100K+ nodes) ===\n");

        self.run_xlarge_dataset_benchmarks();
        self.run_xxlarge_dataset_benchmarks();

        self.suite.print_summary();
        self.suite.print_detailed();

        if let Err(e) = self.suite.export_csv("scalability_benchmark_results.csv") {
            eprintln!("Failed to export CSV: {}", e);
        }
    }

    fn run_xlarge_dataset_benchmarks(&mut self) {
        println!("=== XLarge Dataset (50K nodes, ~5M edges) ===\n");
        let (nodes, edges) = self.data_generator.generate_xlarge_dataset();

        self.benchmark_sombra_scalability("xlarge", &nodes, &edges);
    }

    fn run_xxlarge_dataset_benchmarks(&mut self) {
        println!("\n=== XXLarge Dataset (100K nodes, ~10M edges) ===\n");
        let (nodes, edges) = self.data_generator.generate_xxlarge_dataset();

        self.benchmark_sombra_scalability("xxlarge", &nodes, &edges);
    }

    fn benchmark_sombra_scalability(&mut self, size: &str, nodes: &[Node], edges: &[Edge]) {
        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("sombra_scalability.db");

        println!("--- Phase 1: Bulk Insert ---");
        let _result = self.suite.run_benchmark(
            format!("sombra_{}_bulk_insert_nodes", size),
            nodes.len() as u64,
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::benchmark()
                ));
                let mut tx = log_or_return!(db.begin_transaction());

                for node in nodes {
                    let _ = log_or_return!(tx.add_node(node.clone()));
                }

                let _ = log_or_return!(tx.commit());
            },
        );

        let _result = self.suite.run_benchmark(
            format!("sombra_{}_bulk_insert_edges", size),
            edges.len() as u64,
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::benchmark()
                ));
                let mut tx = log_or_return!(db.begin_transaction());

                for edge in edges {
                    let _ = log_or_return!(tx.add_edge(edge.clone()));
                }

                let _ = log_or_return!(tx.commit());
            },
        );

        println!("\n--- Phase 2: Read Performance with Metrics ---");

        let sample_ids: Vec<u64> = (1..=nodes.len().min(1000))
            .step_by((nodes.len() / 100).max(1))
            .map(|i| i as u64)
            .collect();

        let _result = self.suite.run_latency_benchmark(
            format!("sombra_{}_random_node_reads", size),
            sample_ids.len() as u64,
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::benchmark()
                ));
                for &node_id in &sample_ids {
                    let _node = log_or_return!(db.get_node(node_id));
                }
            },
        );

        let _result = self.suite.run_latency_benchmark(
            format!("sombra_{}_repeated_node_reads", size),
            sample_ids.len() as u64 * 10,
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::benchmark()
                ));
                for _ in 0..10 {
                    for &node_id in sample_ids.iter().take(100) {
                        let _node = log_or_return!(db.get_node(node_id));
                    }
                }
            },
        );

        println!("\n--- Phase 3: Label Index Performance ---");

        let _result =
            self.suite
                .run_latency_benchmark(format!("sombra_{}_label_queries", size), 10, || {
                    let mut db = log_or_return!(GraphDB::open_with_config(
                        &db_path,
                        crate::db::Config::benchmark()
                    ));
                    for _ in 0..10 {
                        let _nodes = log_or_return!(db.get_nodes_by_label("User"));
                    }
                });

        println!("\n--- Phase 4: Graph Traversal Performance ---");

        let _result = self.suite.run_latency_benchmark(
            format!("sombra_{}_neighbor_traversal", size),
            sample_ids.len().min(100) as u64,
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::benchmark()
                ));
                for &node_id in sample_ids.iter().take(100) {
                    let _neighbors = log_or_return!(db.get_neighbors(node_id));
                }
            },
        );

        let _result = self.suite.run_latency_benchmark(
            format!("sombra_{}_two_hop_traversal", size),
            sample_ids.len().min(10) as u64,
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::benchmark()
                ));
                for &node_id in sample_ids.iter().take(10) {
                    let _neighbors = log_or_return!(db.get_neighbors_two_hops(node_id));
                }
            },
        );

        println!("\n--- Phase 5: Performance Metrics Report ---");
        let db = log_or_return!(GraphDB::open_with_config(
            &db_path,
            crate::db::Config::benchmark()
        ));
        db.metrics.print_report();
    }

    fn benchmark_sombra_insert(&mut self, name: &str, nodes: &[Node], edges: &[Edge]) {
        self.benchmark_sombra_insert_with_config(name, nodes, edges, crate::db::Config::benchmark())
    }

    fn benchmark_sombra_insert_with_config(
        &mut self,
        name: &str,
        nodes: &[Node],
        edges: &[Edge],
        config: crate::db::Config,
    ) {
        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("sombra_test.db");

        let _result =
            self.suite
                .run_benchmark(format!("{}_nodes", name), nodes.len() as u64, || {
                    let mut db =
                        log_or_return!(GraphDB::open_with_config(&db_path, config.clone()));
                    let mut tx = log_or_return!(db.begin_transaction());

                    for node in nodes {
                        let _ = log_or_return!(tx.add_node(node.clone()));
                    }

                    let _ = log_or_return!(tx.commit());
                });

        let _result =
            self.suite
                .run_benchmark(format!("{}_edges", name), edges.len() as u64, || {
                    let mut db =
                        log_or_return!(GraphDB::open_with_config(&db_path, config.clone()));
                    let mut tx = log_or_return!(db.begin_transaction());

                    for edge in edges {
                        let _ = log_or_return!(tx.add_edge(edge.clone()));
                    }

                    let _ = log_or_return!(tx.commit());
                });
    }

    fn benchmark_sombra_query(&mut self, name: &str, nodes: &[Node], _edges: &[Edge]) {
        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("sombra_test.db");

        // First, populate the database
        {
            let mut db = log_or_return!(GraphDB::open(&db_path));
            let mut tx = log_or_return!(db.begin_transaction());

            for node in nodes {
                let _ = log_or_return!(tx.add_node(node.clone()));
            }

            let _ = log_or_return!(tx.commit());
        }

        // Now benchmark queries
        let sample_node_ids: Vec<u64> = (1..=nodes.len().min(100)).map(|i| i as u64).collect();

        let _result = self.suite.run_benchmark(
            format!("{}_get_node", name),
            sample_node_ids.len() as u64 * 10, // 10 iterations per node
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::balanced()
                ));
                for _ in 0..10 {
                    for &node_id in &sample_node_ids {
                        let _node = log_or_return!(db.get_node(node_id));
                    }
                }
            },
        );

        let _result = self.suite.run_benchmark(
            format!("{}_get_neighbors", name),
            sample_node_ids.len() as u64 * 10,
            || {
                let mut db = log_or_return!(GraphDB::open_with_config(
                    &db_path,
                    crate::db::Config::balanced()
                ));
                for _ in 0..10 {
                    for &node_id in &sample_node_ids {
                        let _neighbors = log_or_return!(db.get_neighbors(node_id));
                    }
                }
            },
        );
    }

    fn benchmark_sqlite_insert(&mut self, name: &str, nodes: &[Node], edges: &[Edge]) {
        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("sqlite_test.db");

        let _result =
            self.suite
                .run_benchmark(format!("{}_nodes", name), nodes.len() as u64, || {
                    let mut db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
                        Some(db) => db,
                        None => return,
                    };
                    let _ = log_or_return!(db.bulk_insert_nodes(nodes));
                });

        let _result =
            self.suite
                .run_benchmark(format!("{}_edges", name), edges.len() as u64, || {
                    let mut db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
                        Some(db) => db,
                        None => return,
                    };
                    let _ = log_or_return!(db.bulk_insert_edges(edges));
                });
    }

    fn benchmark_sqlite_query(&mut self, name: &str, nodes: &[Node], _edges: &[Edge]) {
        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("sqlite_test.db");

        // First, populate the database
        {
            let mut db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
                Some(db) => db,
                None => return,
            };
            let _ = log_or_return!(db.bulk_insert_nodes(nodes));
        }

        // Now benchmark queries
        let sample_node_ids: Vec<u64> = (1..=nodes.len().min(100)).map(|i| i as u64).collect();

        let _result = self.suite.run_benchmark(
            format!("{}_get_node", name),
            sample_node_ids.len() as u64 * 10,
            || {
                let mut db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
                    Some(db) => db,
                    None => return,
                };
                for _ in 0..10 {
                    for &node_id in &sample_node_ids {
                        let _node = log_or_return!(db.get_node(node_id));
                    }
                }
            },
        );

        let _result = self.suite.run_benchmark(
            format!("{}_get_neighbors", name),
            sample_node_ids.len() as u64 * 10,
            || {
                let mut db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
                    Some(db) => db,
                    None => return,
                };
                for _ in 0..10 {
                    for &node_id in &sample_node_ids {
                        let _neighbors = log_or_return!(db.get_neighbors(node_id));
                    }
                }
            },
        );
    }

    pub fn run_stress_test(&mut self, duration_secs: u64) {
        println!("\n=== Stress Test ({} seconds) ===", duration_secs);

        let temp_dir = log_or_return!(TempDir::new());

        // Test 1: Sombra with fully durable settings (fair comparison)
        let db_path = temp_dir.path().join("stress_test.sombra");
        let node_counter = std::sync::atomic::AtomicU64::new(1);

        let config = crate::db::Config::fully_durable();
        let db = RefCell::new(log_or_return!(GraphDB::open_with_config(&db_path, config)));

        self.suite
            .run_timed_benchmark("sombra_fully_durable".to_string(), duration_secs, || {
                let mut db_ref = db.borrow_mut();
                let mut tx = log_or_return!(db_ref.begin_transaction());

                let node_id = node_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut node = Node::new(node_id);
                node.labels.push("StressTest".to_string());
                let timestamp = match current_timestamp() {
                    Some(value) => value,
                    None => return,
                };
                node.properties
                    .insert("timestamp".to_string(), PropertyValue::Int(timestamp));

                let _ = log_or_return!(tx.add_node(node));
                let _ = log_or_return!(tx.commit());
            });

        // Test 2: SQLite with fully durable settings (default)
        let sqlite_path = temp_dir.path().join("stress_test.sqlite");
        let sqlite_node_counter = std::sync::atomic::AtomicU64::new(1);
        let sqlite_db = match open_sqlite_db(&sqlite_path, "Failed to open SQLite database") {
            Some(db) => RefCell::new(db),
            None => return,
        };

        self.suite
            .run_timed_benchmark("sqlite_fully_durable".to_string(), duration_secs, || {
                let mut db = sqlite_db.borrow_mut();

                let node_id = sqlite_node_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut node = Node::new(node_id);
                node.labels.push("StressTest".to_string());
                let timestamp = match current_timestamp() {
                    Some(value) => value,
                    None => return,
                };
                node.properties
                    .insert("timestamp".to_string(), PropertyValue::Int(timestamp));

                let _ = log_or_return!(db.add_node(node));
            });

        // Test 3: Sombra with benchmark settings (for comparison)
        let db_path2 = temp_dir.path().join("stress_test_benchmark.sombra");
        let node_counter2 = std::sync::atomic::AtomicU64::new(1);

        let config_benchmark = crate::db::Config::benchmark();
        let db2 = match GraphDB::open_with_config(&db_path2, config_benchmark) {
            Ok(db) => RefCell::new(db),
            Err(err) => {
                eprintln!("GraphDB::open_with_config failed: {}", err);
                return;
            }
        };

        self.suite
            .run_timed_benchmark("sombra_benchmark_mode".to_string(), duration_secs, || {
                let mut db_ref = db2.borrow_mut();
                let mut tx = log_or_return!(db_ref.begin_transaction());

                let node_id = node_counter2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut node = Node::new(node_id);
                node.labels.push("StressTest".to_string());
                let timestamp = match current_timestamp() {
                    Some(value) => value,
                    None => return,
                };
                node.properties
                    .insert("timestamp".to_string(), PropertyValue::Int(timestamp));

                let _ = log_or_return!(tx.add_node(node));
                let _ = log_or_return!(tx.commit());
            });

        // Print results after stress test
        self.suite.print_summary();
    }

    fn benchmark_sombra_bulk_insert(&mut self, name: &str, nodes: &[Node], edges: &[Edge]) {
        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("test_sombra.db");

        let _result = self.suite.run_benchmark(
            format!("{}_bulk_insert", name),
            (nodes.len() + edges.len()) as u64,
            || {
                let mut db = match open_graphdb_default(&db_path, "Failed to open Sombra database")
                {
                    Some(db) => db,
                    None => return,
                };
                let mut tx = log_or_return!(db.begin_transaction());

                // Bulk insert nodes
                for node in nodes {
                    let _ = log_or_return!(tx.add_node(node.clone()));
                }

                // Bulk insert edges
                for edge in edges {
                    let _ = log_or_return!(tx.add_edge(edge.clone()));
                }

                let _ = log_or_return!(tx.commit());
            },
        );
    }

    fn benchmark_sqlite_bulk_insert(&mut self, name: &str, nodes: &[Node], edges: &[Edge]) {
        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("test_sqlite.db");

        let _result = self.suite.run_benchmark(
            format!("{}_bulk_insert", name),
            (nodes.len() + edges.len()) as u64,
            || {
                let mut db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
                    Some(db) => db,
                    None => return,
                };
                let _ = log_or_return!(db.bulk_insert_nodes(nodes));
                let _ = log_or_return!(db.bulk_insert_edges(edges));
            },
        );
    }

    // New methods for selective benchmarking
    pub fn run_insert_benchmarks(&mut self) {
        println!("Running insert benchmarks...\n");

        // Test all dataset sizes for inserts
        self.run_small_dataset_inserts();
        self.run_medium_dataset_inserts();
        self.run_large_dataset_inserts();
    }

    pub fn run_query_benchmarks(&mut self) {
        println!("Running query benchmarks...\n");

        self.run_small_dataset_queries();
        self.run_medium_dataset_queries();
        self.run_large_dataset_queries();
    }

    pub fn run_read_benchmarks(&mut self) {
        println!("\n=== Comprehensive Read Benchmarks ===\n");

        self.run_small_dataset_reads();
        self.run_medium_dataset_reads();
        self.run_large_dataset_reads();

        self.suite.print_summary();
        self.suite.print_detailed();

        if let Err(e) = self.suite.export_csv("read_benchmark_results.csv") {
            eprintln!("Failed to export CSV: {}", e);
        }
    }

    pub fn run_small_dataset_reads(&mut self) {
        println!("=== Small Dataset Read Benchmarks (100 nodes, ~1000 edges) ===\n");
        let (nodes, edges) = self.data_generator.generate_small_dataset();

        self.benchmark_reads("small", &nodes, &edges);
    }

    pub fn run_medium_dataset_reads(&mut self) {
        println!("\n=== Medium Dataset Read Benchmarks (1000 nodes, ~25000 edges) ===\n");
        let (nodes, edges) = self.data_generator.generate_medium_dataset();

        self.benchmark_reads("medium", &nodes, &edges);
    }

    pub fn run_large_dataset_reads(&mut self) {
        println!("\n=== Large Dataset Read Benchmarks (10000 nodes, ~500000 edges) ===\n");
        let (nodes, edges) = self.data_generator.generate_large_dataset();

        self.benchmark_reads("large", &nodes, &edges);
    }

    fn benchmark_reads(&mut self, size: &str, nodes: &[Node], edges: &[Edge]) {
        println!("--- Sombra Read Benchmarks ---");
        self.benchmark_sombra_reads(size, nodes, edges);

        println!("\n--- SQLite Read Benchmarks ---");
        self.benchmark_sqlite_reads(size, nodes, edges);
    }

    fn benchmark_sombra_reads(&mut self, size: &str, nodes: &[Node], edges: &[Edge]) {
        use std::cell::RefCell;

        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("sombra_read_test.db");

        {
            let mut db = log_or_return!(GraphDB::open_with_config(
                &db_path,
                crate::db::Config::balanced()
            ));
            let mut tx = log_or_return!(db.begin_transaction());
            for node in nodes {
                let _ = log_or_return!(tx.add_node(node.clone()));
            }
            for edge in edges {
                let _ = log_or_return!(tx.add_edge(edge.clone()));
            }
            let _ = log_or_return!(tx.commit());
        }

        let sample_ids: Vec<u64> = (1..=nodes.len().min(1000))
            .step_by(nodes.len().max(10) / 100)
            .map(|i| i as u64)
            .collect();

        let db = RefCell::new(log_or_return!(GraphDB::open_with_config(
            &db_path,
            crate::db::Config::balanced()
        )));

        let _result = self.suite.run_benchmark(
            format!("sombra_{}_get_node", size),
            sample_ids.len() as u64,
            || {
                let mut db_ref = db.borrow_mut();
                for &node_id in &sample_ids {
                    let _node = log_or_return!(db_ref.get_node(node_id));
                }
            },
        );

        let _result = self.suite.run_benchmark(
            format!("sombra_{}_get_neighbors", size),
            sample_ids.len() as u64,
            || {
                let mut db_ref = db.borrow_mut();
                for &node_id in &sample_ids {
                    let _neighbors = log_or_return!(db_ref.get_neighbors(node_id));
                }
            },
        );

        let _result =
            self.suite
                .run_benchmark(format!("sombra_{}_two_hop_neighbors", size), 10, || {
                    let mut db_ref = db.borrow_mut();
                    for &node_id in sample_ids.iter().take(10) {
                        let _neighbors = log_or_return!(db_ref.get_neighbors_two_hops(node_id));
                    }
                });

        let _result =
            self.suite
                .run_benchmark(format!("sombra_{}_bfs_traversal_depth3", size), 10, || {
                    let mut db_ref = db.borrow_mut();
                    for &node_id in sample_ids.iter().take(10) {
                        let _ = log_or_return!(db_ref.bfs_traversal(node_id, 3));
                    }
                });
    }

    fn benchmark_sqlite_reads(&mut self, size: &str, nodes: &[Node], edges: &[Edge]) {
        use std::cell::RefCell;

        let temp_dir = log_or_return!(TempDir::new());
        let db_path = temp_dir.path().join("sqlite_read_test.db");

        {
            let mut db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
                Some(db) => db,
                None => return,
            };
            let _ = log_or_return!(db.bulk_insert_nodes(nodes));
            let _ = log_or_return!(db.bulk_insert_edges(edges));
        }

        let sample_ids: Vec<u64> = (1..=nodes.len().min(1000))
            .step_by(nodes.len().max(10) / 100)
            .map(|i| i as u64)
            .collect();

        let db = match open_sqlite_db(&db_path, "Failed to open SQLite database") {
            Some(db) => RefCell::new(db),
            None => return,
        };

        let _result = self.suite.run_benchmark(
            format!("sqlite_{}_get_node", size),
            sample_ids.len() as u64,
            || {
                let mut db_ref = db.borrow_mut();
                for &node_id in &sample_ids {
                    let _node = log_or_return!(db_ref.get_node(node_id));
                }
            },
        );

        let _result = self.suite.run_benchmark(
            format!("sqlite_{}_get_neighbors", size),
            sample_ids.len() as u64,
            || {
                let mut db_ref = db.borrow_mut();
                for &node_id in &sample_ids {
                    let _neighbors = log_or_return!(db_ref.get_neighbors(node_id));
                }
            },
        );

        let _result =
            self.suite
                .run_benchmark(format!("sqlite_{}_two_hop_neighbors", size), 10, || {
                    let mut db_ref = db.borrow_mut();
                    for &node_id in sample_ids.iter().take(10) {
                        let _neighbors = log_or_return!(db_ref.get_neighbors_two_hops(node_id));
                    }
                });

        let _result =
            self.suite
                .run_benchmark(format!("sqlite_{}_bfs_traversal_depth3", size), 10, || {
                    let mut db_ref = db.borrow_mut();
                    for &node_id in sample_ids.iter().take(10) {
                        let _ = log_or_return!(db_ref.bfs_traversal(node_id, 3));
                    }
                });
    }

    pub fn run_bulk_benchmarks(&mut self) {
        println!("Running bulk insert benchmarks...\n");

        // Test bulk operations
        self.run_small_dataset_bulk();
        self.run_medium_dataset_bulk();
        self.run_large_dataset_bulk();
    }

    fn run_small_dataset_inserts(&mut self) {
        println!("=== Small Dataset Insert Benchmarks (100 nodes, ~1000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_small_dataset();

        self.benchmark_sombra_insert("sombra_small_insert", &nodes, &edges);
        self.benchmark_sqlite_insert("sqlite_small_insert", &nodes, &edges);
    }

    fn run_medium_dataset_inserts(&mut self) {
        println!("=== Medium Dataset Insert Benchmarks (1000 nodes, ~25000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_medium_dataset();

        self.benchmark_sombra_insert("sombra_medium_insert", &nodes, &edges);
        self.benchmark_sqlite_insert("sqlite_medium_insert", &nodes, &edges);
    }

    fn run_large_dataset_inserts(&mut self) {
        println!("=== Large Dataset Insert Benchmarks (10000 nodes, ~500000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_large_dataset();

        self.benchmark_sombra_insert("sombra_large_insert", &nodes, &edges);
        self.benchmark_sqlite_insert("sqlite_large_insert", &nodes, &edges);
    }

    fn run_small_dataset_queries(&mut self) {
        println!("=== Small Dataset Query Benchmarks (100 nodes, ~1000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_small_dataset();

        self.benchmark_sombra_query("sombra_small_query", &nodes, &edges);
        self.benchmark_sqlite_query("sqlite_small_query", &nodes, &edges);
    }

    fn run_medium_dataset_queries(&mut self) {
        println!("=== Medium Dataset Query Benchmarks (1000 nodes, ~25000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_medium_dataset();

        self.benchmark_sombra_query("sombra_medium_query", &nodes, &edges);
        self.benchmark_sqlite_query("sqlite_medium_query", &nodes, &edges);
    }

    fn run_large_dataset_queries(&mut self) {
        println!("=== Large Dataset Query Benchmarks (10000 nodes, ~500000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_large_dataset();

        self.benchmark_sombra_query("sombra_large_query", &nodes, &edges);
        self.benchmark_sqlite_query("sqlite_large_query", &nodes, &edges);
    }

    fn run_small_dataset_bulk(&mut self) {
        println!("=== Small Dataset Bulk Benchmarks (100 nodes, ~1000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_small_dataset();

        self.benchmark_sombra_bulk_insert("sombra_small_bulk", &nodes, &edges);
        self.benchmark_sqlite_bulk_insert("sqlite_small_bulk", &nodes, &edges);
    }

    fn run_medium_dataset_bulk(&mut self) {
        println!("=== Medium Dataset Bulk Benchmarks (1000 nodes, ~25000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_medium_dataset();

        self.benchmark_sombra_bulk_insert("sombra_medium_bulk", &nodes, &edges);
        self.benchmark_sqlite_bulk_insert("sqlite_medium_bulk", &nodes, &edges);
    }

    fn run_large_dataset_bulk(&mut self) {
        println!("=== Large Dataset Bulk Benchmarks (10000 nodes, ~500000 edges) ===");
        let (nodes, edges) = self.data_generator.generate_large_dataset();

        self.benchmark_sombra_bulk_insert("sombra_large_bulk", &nodes, &edges);
        self.benchmark_sqlite_bulk_insert("sqlite_medium_bulk", &nodes, &edges);
    }

    pub fn print_results(&self) {
        self.suite.print_summary();
        self.suite.print_detailed();
    }

    pub fn export_results(&self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.suite.export_csv(filename)?;
        Ok(())
    }
}

fn path_to_string(path: &std::path::Path) -> Option<String> {
    match path.to_str() {
        Some(s) => Some(s.to_owned()),
        None => {
            eprintln!("Path contains invalid UTF-8: {:?}", path);
            None
        }
    }
}

fn open_sqlite_db(path: &std::path::Path, context: &str) -> Option<SqliteGraphDB> {
    let path_string = path_to_string(path)?;
    match SqliteGraphDB::new(&path_string) {
        Ok(db) => Some(db),
        Err(err) => {
            eprintln!("{}: {}", context, err);
            None
        }
    }
}

fn open_graphdb_default(path: &std::path::Path, context: &str) -> Option<GraphDB> {
    let path_string = path_to_string(path)?;
    match GraphDB::open(&path_string) {
        Ok(db) => Some(db),
        Err(err) => {
            eprintln!("{}: {}", context, err);
            None
        }
    }
}

fn current_timestamp() -> Option<i64> {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => Some(duration.as_secs() as i64),
        Err(err) => {
            eprintln!("Failed to get system time: {}", err);
            None
        }
    }
}

impl Default for BenchmarkRunner {
    fn default() -> Self {
        Self::new()
    }
}
