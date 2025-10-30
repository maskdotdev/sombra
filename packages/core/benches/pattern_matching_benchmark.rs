//! Pattern Matching Performance Benchmarks using Criterion
//!
//! Benchmarks for pattern matching performance across various scenarios.
//!
//! Run with: cargo bench --bench pattern_matching_benchmark --features benchmarks
//! View results: open target/criterion/report/index.html

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use sombra::db::query::pattern::{
    EdgePattern, NodePattern, Pattern, PropertyBound, PropertyFilters, PropertyRangeFilter,
};
use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, EdgeDirection, Node, PropertyValue};
use tempfile::TempDir;

// ============================================================================
// Setup Helpers
// ============================================================================

fn create_test_db(path: &str) -> GraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut config = Config::benchmark();

    GraphDB::open_with_config(path, config).unwrap()
}

/// Creates a code graph: Functions -> Classes -> Modules
fn create_code_graph(path: &str, module_count: usize, classes_per_module: usize, funcs_per_class: usize) -> GraphDB {
    let db = create_test_db(path);
    let mut tx = db.begin_transaction().unwrap();

    for m in 0..module_count {
        let mut module = Node::new(0);
        module.labels.push("Module".to_string());
        module
            .properties
            .insert("name".to_string(), PropertyValue::String(format!("Module{}", m)));
        module
            .properties
            .insert("index".to_string(), PropertyValue::Int(m as i64));
        let module_id = tx.add_node(module).unwrap();

        for c in 0..classes_per_module {
            let mut class = Node::new(0);
            class.labels.push("Class".to_string());
            class
                .properties
                .insert("name".to_string(), PropertyValue::String(format!("Class{}", c)));
            class
                .properties
                .insert("lines".to_string(), PropertyValue::Int((c * 100) as i64));
            let class_id = tx.add_node(class).unwrap();

            tx.add_edge(Edge::new(0, module_id, class_id, "CONTAINS"))
                .unwrap();

            for f in 0..funcs_per_class {
                let mut func = Node::new(0);
                func.labels.push("Function".to_string());
                func.properties.insert(
                    "name".to_string(),
                    PropertyValue::String(format!("func{}", f)),
                );
                func.properties
                    .insert("complexity".to_string(), PropertyValue::Int((f % 10) as i64));
                let func_id = tx.add_node(func).unwrap();

                tx.add_edge(Edge::new(0, class_id, func_id, "DEFINES"))
                    .unwrap();
            }
        }
    }

    tx.commit().unwrap();
    db
}

/// Creates a social network: Users with FOLLOWS and LIKES relationships
fn create_social_graph(path: &str, user_count: usize, avg_follows: usize) -> GraphDB {
    let db = create_test_db(path);
    let mut tx = db.begin_transaction().unwrap();

    let mut user_ids = Vec::new();
    for i in 0..user_count {
        let mut user = Node::new(0);
        user.labels.push("User".to_string());
        user.properties.insert(
            "username".to_string(),
            PropertyValue::String(format!("user{}", i)),
        );
        user.properties
            .insert("age".to_string(), PropertyValue::Int((20 + (i % 50)) as i64));
        user.properties
            .insert("posts".to_string(), PropertyValue::Int((i * 10) as i64));
        let user_id = tx.add_node(user).unwrap();
        user_ids.push(user_id);
    }

    // Create FOLLOWS edges
    for i in 0..user_count {
        for j in 0..avg_follows.min(user_count) {
            let target = (i + j + 1) % user_count;
            if i != target {
                let mut edge = Edge::new(0, user_ids[i], user_ids[target], "FOLLOWS");
                edge.properties.insert(
                    "since".to_string(),
                    PropertyValue::Int((2020 + (i % 4)) as i64),
                );
                tx.add_edge(edge).unwrap();
            }
        }
    }

    tx.commit().unwrap();
    db
}

/// Creates a dependency graph: Packages with DEPENDS_ON relationships
fn create_dependency_graph(path: &str, pkg_count: usize, avg_deps: usize) -> GraphDB {
    let db = create_test_db(path);
    let mut tx = db.begin_transaction().unwrap();

    let mut pkg_ids = Vec::new();
    for i in 0..pkg_count {
        let mut pkg = Node::new(0);
        pkg.labels.push("Package".to_string());
        pkg.properties.insert(
            "name".to_string(),
            PropertyValue::String(format!("package-{}", i)),
        );
        pkg.properties.insert(
            "version".to_string(),
            PropertyValue::String(format!("{}.0.0", i % 10)),
        );
        let pkg_id = tx.add_node(pkg).unwrap();
        pkg_ids.push(pkg_id);
    }

    // Create DEPENDS_ON edges
    for i in 0..pkg_count {
        for j in 0..avg_deps.min(pkg_count) {
            let target = (i + j + 1) % pkg_count;
            if i != target {
                tx.add_edge(Edge::new(0, pkg_ids[i], pkg_ids[target], "DEPENDS_ON"))
                    .unwrap();
            }
        }
    }

    tx.commit().unwrap();
    db
}

// ============================================================================
// Benchmark 1: Simple 2-Node Pattern with Label Filter
// ============================================================================

fn bench_simple_two_node_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_two_node_pattern");

    for size in [100, 500, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("pattern.db");
                    let db = create_code_graph(path.to_str().unwrap(), 10, size / 10, 1);
                    (db, temp_dir)
                },
                |(mut db, _temp_dir)| {
                    let pattern = Pattern {
                        nodes: vec![
                            NodePattern {
                                var_name: "module".into(),
                                labels: vec!["Module".into()],
                                properties: PropertyFilters::default(),
                            },
                            NodePattern {
                                var_name: "class".into(),
                                labels: vec!["Class".into()],
                                properties: PropertyFilters::default(),
                            },
                        ],
                        edges: vec![EdgePattern {
                            from_var: "module".into(),
                            to_var: "class".into(),
                            types: vec!["CONTAINS".into()],
                            properties: PropertyFilters::default(),
                            direction: EdgeDirection::Outgoing,
                        }],
                    };
                    
                    let matches = db.match_pattern(&pattern).unwrap();
                    black_box(matches);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark 2: Three-Hop Pattern (Multi-hop Path)
// ============================================================================

fn bench_three_hop_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("three_hop_pattern");

    for size in [50, 100, 200] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("pattern.db");
                    let db = create_code_graph(path.to_str().unwrap(), 5, size / 5, 2);
                    (db, temp_dir)
                },
                |(mut db, _temp_dir)| {
                    let pattern = Pattern {
                        nodes: vec![
                            NodePattern {
                                var_name: "module".into(),
                                labels: vec!["Module".into()],
                                properties: PropertyFilters::default(),
                            },
                            NodePattern {
                                var_name: "class".into(),
                                labels: vec!["Class".into()],
                                properties: PropertyFilters::default(),
                            },
                            NodePattern {
                                var_name: "func".into(),
                                labels: vec!["Function".into()],
                                properties: PropertyFilters::default(),
                            },
                        ],
                        edges: vec![
                            EdgePattern {
                                from_var: "module".into(),
                                to_var: "class".into(),
                                types: vec!["CONTAINS".into()],
                                properties: PropertyFilters::default(),
                                direction: EdgeDirection::Outgoing,
                            },
                            EdgePattern {
                                from_var: "class".into(),
                                to_var: "func".into(),
                                types: vec!["DEFINES".into()],
                                properties: PropertyFilters::default(),
                                direction: EdgeDirection::Outgoing,
                            },
                        ],
                    };
                    
                    let matches = db.match_pattern(&pattern).unwrap();
                    black_box(matches);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark 3: Pattern with Property Equality Filter
// ============================================================================

fn bench_pattern_with_property_equality(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_property_equality");

    for size in [100, 500, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("pattern.db");
                    let db = create_social_graph(path.to_str().unwrap(), size, 5);
                    (db, temp_dir)
                },
                |(mut db, _temp_dir)| {
                    let mut user1_filters = PropertyFilters::default();
                    user1_filters
                        .equals
                        .insert("age".to_string(), PropertyValue::Int(25));

                    let pattern = Pattern {
                        nodes: vec![
                            NodePattern {
                                var_name: "u1".into(),
                                labels: vec!["User".into()],
                                properties: user1_filters,
                            },
                            NodePattern {
                                var_name: "u2".into(),
                                labels: vec!["User".into()],
                                properties: PropertyFilters::default(),
                            },
                        ],
                        edges: vec![EdgePattern {
                            from_var: "u1".into(),
                            to_var: "u2".into(),
                            types: vec!["FOLLOWS".into()],
                            properties: PropertyFilters::default(),
                            direction: EdgeDirection::Outgoing,
                        }],
                    };
                    
                    let matches = db.match_pattern(&pattern).unwrap();
                    black_box(matches);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark 4: Pattern with Property Range Filter
// ============================================================================

fn bench_pattern_with_property_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_property_range");

    for size in [100, 500, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("pattern.db");
                    let db = create_social_graph(path.to_str().unwrap(), size, 5);
                    (db, temp_dir)
                },
                |(mut db, _temp_dir)| {
                    let mut user1_filters = PropertyFilters::default();
                    user1_filters.ranges.push(PropertyRangeFilter {
                        key: "age".into(),
                        min: Some(PropertyBound {
                            value: PropertyValue::Int(25),
                            inclusive: true,
                        }),
                        max: Some(PropertyBound {
                            value: PropertyValue::Int(35),
                            inclusive: true,
                        }),
                    });

                    let pattern = Pattern {
                        nodes: vec![
                            NodePattern {
                                var_name: "u1".into(),
                                labels: vec!["User".into()],
                                properties: user1_filters,
                            },
                            NodePattern {
                                var_name: "u2".into(),
                                labels: vec!["User".into()],
                                properties: PropertyFilters::default(),
                            },
                        ],
                        edges: vec![EdgePattern {
                            from_var: "u1".into(),
                            to_var: "u2".into(),
                            types: vec!["FOLLOWS".into()],
                            properties: PropertyFilters::default(),
                            direction: EdgeDirection::Outgoing,
                        }],
                    };
                    
                    let matches = db.match_pattern(&pattern).unwrap();
                    black_box(matches);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark 5: Pattern with Edge Property Filter
// ============================================================================

fn bench_pattern_with_edge_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_edge_filter");

    for size in [100, 500, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("pattern.db");
                    let db = create_social_graph(path.to_str().unwrap(), size, 5);
                    (db, temp_dir)
                },
                |(mut db, _temp_dir)| {
                    let mut edge_filters = PropertyFilters::default();
                    edge_filters
                        .equals
                        .insert("since".to_string(), PropertyValue::Int(2022));

                    let pattern = Pattern {
                        nodes: vec![
                            NodePattern {
                                var_name: "u1".into(),
                                labels: vec!["User".into()],
                                properties: PropertyFilters::default(),
                            },
                            NodePattern {
                                var_name: "u2".into(),
                                labels: vec!["User".into()],
                                properties: PropertyFilters::default(),
                            },
                        ],
                        edges: vec![EdgePattern {
                            from_var: "u1".into(),
                            to_var: "u2".into(),
                            types: vec!["FOLLOWS".into()],
                            properties: edge_filters,
                            direction: EdgeDirection::Outgoing,
                        }],
                    };
                    
                    let matches = db.match_pattern(&pattern).unwrap();
                    black_box(matches);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark 6: Pattern with Incoming Direction
// ============================================================================

fn bench_pattern_incoming_direction(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_incoming_direction");

    for size in [100, 500, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("pattern.db");
                    let db = create_dependency_graph(path.to_str().unwrap(), size, 3);
                    (db, temp_dir)
                },
                |(mut db, _temp_dir)| {
                    let pattern = Pattern {
                        nodes: vec![
                            NodePattern {
                                var_name: "dependent".into(),
                                labels: vec!["Package".into()],
                                properties: PropertyFilters::default(),
                            },
                            NodePattern {
                                var_name: "dependency".into(),
                                labels: vec!["Package".into()],
                                properties: PropertyFilters::default(),
                            },
                        ],
                        edges: vec![EdgePattern {
                            from_var: "dependent".into(),
                            to_var: "dependency".into(),
                            types: vec!["DEPENDS_ON".into()],
                            properties: PropertyFilters::default(),
                            direction: EdgeDirection::Incoming,
                        }],
                    };
                    
                    let matches = db.match_pattern(&pattern).unwrap();
                    black_box(matches);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark 7: Sparse vs Dense Graph Pattern Matching
// ============================================================================

fn bench_pattern_sparse_vs_dense(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_sparse_vs_dense");

    // Sparse: 1000 nodes, avg 2 edges per node
    group.bench_function("sparse_1000_nodes_2_avg_edges", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("pattern.db");
                let db = create_social_graph(path.to_str().unwrap(), 1000, 2);
                (db, temp_dir)
            },
            |(mut db, _temp_dir)| {
                let pattern = Pattern {
                    nodes: vec![
                        NodePattern {
                            var_name: "u1".into(),
                            labels: vec!["User".into()],
                            properties: PropertyFilters::default(),
                        },
                        NodePattern {
                            var_name: "u2".into(),
                            labels: vec!["User".into()],
                            properties: PropertyFilters::default(),
                        },
                    ],
                    edges: vec![EdgePattern {
                        from_var: "u1".into(),
                        to_var: "u2".into(),
                        types: vec!["FOLLOWS".into()],
                        properties: PropertyFilters::default(),
                        direction: EdgeDirection::Outgoing,
                    }],
                };
                
                let matches = db.match_pattern(&pattern).unwrap();
                black_box(matches);
            },
            BatchSize::SmallInput,
        );
    });

    // Dense: 500 nodes, avg 10 edges per node
    group.bench_function("dense_500_nodes_10_avg_edges", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("pattern.db");
                let db = create_social_graph(path.to_str().unwrap(), 500, 10);
                (db, temp_dir)
            },
            |(mut db, _temp_dir)| {
                let pattern = Pattern {
                    nodes: vec![
                        NodePattern {
                            var_name: "u1".into(),
                            labels: vec!["User".into()],
                            properties: PropertyFilters::default(),
                        },
                        NodePattern {
                            var_name: "u2".into(),
                            labels: vec!["User".into()],
                            properties: PropertyFilters::default(),
                        },
                    ],
                    edges: vec![EdgePattern {
                        from_var: "u1".into(),
                        to_var: "u2".into(),
                        types: vec!["FOLLOWS".into()],
                        properties: PropertyFilters::default(),
                        direction: EdgeDirection::Outgoing,
                    }],
                };
                
                let matches = db.match_pattern(&pattern).unwrap();
                black_box(matches);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ============================================================================
// Benchmark 8: Complex Multi-Filter Pattern
// ============================================================================

fn bench_pattern_complex_multi_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_complex_multi_filter");

    for size in [100, 300, 500] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("pattern.db");
                    let db = create_code_graph(path.to_str().unwrap(), 10, size / 10, 3);
                    (db, temp_dir)
                },
                |(mut db, _temp_dir)| {
                    let mut module_filters = PropertyFilters::default();
                    module_filters.ranges.push(PropertyRangeFilter {
                        key: "index".into(),
                        min: Some(PropertyBound {
                            value: PropertyValue::Int(0),
                            inclusive: true,
                        }),
                        max: Some(PropertyBound {
                            value: PropertyValue::Int(5),
                            inclusive: false,
                        }),
                    });

                    let mut class_filters = PropertyFilters::default();
                    class_filters.ranges.push(PropertyRangeFilter {
                        key: "lines".into(),
                        min: Some(PropertyBound {
                            value: PropertyValue::Int(100),
                            inclusive: true,
                        }),
                        max: Some(PropertyBound {
                            value: PropertyValue::Int(500),
                            inclusive: true,
                        }),
                    });

                    let mut func_filters = PropertyFilters::default();
                    func_filters.ranges.push(PropertyRangeFilter {
                        key: "complexity".into(),
                        min: Some(PropertyBound {
                            value: PropertyValue::Int(3),
                            inclusive: true,
                        }),
                        max: Some(PropertyBound {
                            value: PropertyValue::Int(7),
                            inclusive: true,
                        }),
                    });

                    let pattern = Pattern {
                        nodes: vec![
                            NodePattern {
                                var_name: "module".into(),
                                labels: vec!["Module".into()],
                                properties: module_filters,
                            },
                            NodePattern {
                                var_name: "class".into(),
                                labels: vec!["Class".into()],
                                properties: class_filters,
                            },
                            NodePattern {
                                var_name: "func".into(),
                                labels: vec!["Function".into()],
                                properties: func_filters,
                            },
                        ],
                        edges: vec![
                            EdgePattern {
                                from_var: "module".into(),
                                to_var: "class".into(),
                                types: vec!["CONTAINS".into()],
                                properties: PropertyFilters::default(),
                                direction: EdgeDirection::Outgoing,
                            },
                            EdgePattern {
                                from_var: "class".into(),
                                to_var: "func".into(),
                                types: vec!["DEFINES".into()],
                                properties: PropertyFilters::default(),
                                direction: EdgeDirection::Outgoing,
                            },
                        ],
                    };
                    
                    let matches = db.match_pattern(&pattern).unwrap();
                    black_box(matches);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    benches,
    bench_simple_two_node_pattern,
    bench_three_hop_pattern,
    bench_pattern_with_property_equality,
    bench_pattern_with_property_range,
    bench_pattern_with_edge_filter,
    bench_pattern_incoming_direction,
    bench_pattern_sparse_vs_dense,
    bench_pattern_complex_multi_filter,
);

criterion_main!(benches);
