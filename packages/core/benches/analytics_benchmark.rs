use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, BatchSize};
use sombra::db::GraphDB;
use sombra::model::{Node, Edge, EdgeDirection};
use sombra::db::query::analytics::DegreeType;
use tempfile::NamedTempFile;

/// Helper: Create a database with a specified number of nodes and edges
/// Returns (db, temp_file, node_ids)
fn create_graph(
    num_nodes: usize,
    edges_per_node: usize,
    num_labels: usize,
    num_edge_types: usize,
) -> (GraphDB, NamedTempFile, Vec<u64>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    let labels: Vec<String> = (0..num_labels)
        .map(|i| format!("Label{}", i % num_labels))
        .collect();

    let edge_types: Vec<String> = (0..num_edge_types)
        .map(|i| format!("TYPE{}", i % num_edge_types))
        .collect();

    // Create nodes with distributed labels
    let mut node_ids = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        let mut node = Node::new(0);
        node.labels.push(labels[i % num_labels].clone());
        let id = db.add_node(node).unwrap();
        node_ids.push(id);
    }

    // Create edges with distributed types
    let mut edge_count = 0;
    for i in 0..num_nodes {
        for j in 0..edges_per_node.min(num_nodes - 1) {
            let target = (i + j + 1) % num_nodes;
            let edge_type = &edge_types[edge_count % num_edge_types];
            db.add_edge(Edge::new(0, node_ids[i], node_ids[target], edge_type))
                .unwrap();
            edge_count += 1;
        }
    }

    (db, temp_file, node_ids)
}

/// Helper: Create a scale-free graph (hub-and-spoke pattern)
/// Some nodes have many connections (hubs), most have few
fn create_scale_free_graph(
    num_hubs: usize,
    nodes_per_hub: usize,
    num_labels: usize,
) -> (GraphDB, NamedTempFile, Vec<u64>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    let labels: Vec<String> = (0..num_labels)
        .map(|i| format!("Label{}", i))
        .collect();

    let mut node_ids = Vec::new();

    // Create hub nodes
    for i in 0..num_hubs {
        let mut node = Node::new(0);
        node.labels.push(labels[i % num_labels].clone());
        let id = db.add_node(node).unwrap();
        node_ids.push(id);
    }

    // Create spoke nodes and connect to hubs
    for hub_idx in 0..num_hubs {
        let hub_id = node_ids[hub_idx];
        for _ in 0..nodes_per_hub {
            let mut node = Node::new(0);
            node.labels.push(labels[(hub_idx + 1) % num_labels].clone());
            let spoke_id = db.add_node(node).unwrap();
            node_ids.push(spoke_id);

            // Connect spoke to hub
            db.add_edge(Edge::new(0, spoke_id, hub_id, "CONNECTS_TO"))
                .unwrap();
        }
    }

    (db, temp_file, node_ids)
}

/// Helper: Create a graph with many isolated nodes
fn create_sparse_graph(
    num_connected: usize,
    num_isolated: usize,
) -> (GraphDB, NamedTempFile, Vec<u64>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    let mut node_ids = Vec::new();

    // Create connected nodes (chain)
    for i in 0..num_connected {
        let mut node = Node::new(0);
        node.labels.push("Connected".to_string());
        let id = db.add_node(node).unwrap();
        node_ids.push(id);

        if i > 0 {
            db.add_edge(Edge::new(0, node_ids[i - 1], id, "NEXT"))
                .unwrap();
        }
    }

    // Create isolated nodes
    for _ in 0..num_isolated {
        let mut node = Node::new(0);
        node.labels.push("Isolated".to_string());
        let id = db.add_node(node).unwrap();
        node_ids.push(id);
    }

    (db, temp_file, node_ids)
}

fn bench_degree_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/degree_distribution");

    for &size in &[100, 500, 1000, 2000] {
        group.bench_with_input(
            BenchmarkId::new("uniform", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 5, 3),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.degree_distribution().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("scale_free", size),
            &size,
            |b, &size| {
                let num_hubs = size / 10;
                let nodes_per_hub = 9;
                b.iter_batched(
                    || create_scale_free_graph(num_hubs, nodes_per_hub, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.degree_distribution().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_find_hubs(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/find_hubs");

    for &size in &[100, 500, 1000, 2000] {
        let num_hubs = size / 10;
        let nodes_per_hub = 9;

        for &min_degree in &[5, 10, 20] {
            group.bench_with_input(
                BenchmarkId::new(format!("min_{}", min_degree), size),
                &(size, min_degree),
                |b, &(_size, min_degree)| {
                    b.iter_batched(
                        || create_scale_free_graph(num_hubs, nodes_per_hub, 5),
                        |(mut db, _temp, _nodes)| {
                            black_box(db.find_hubs(min_degree, DegreeType::Total).unwrap())
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_find_isolated_nodes(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/find_isolated_nodes");

    for &total_size in &[100, 500, 1000, 2000] {
        for &isolated_pct in &[10, 50, 90] {
            let num_isolated = (total_size * isolated_pct) / 100;
            let num_connected = total_size - num_isolated;

            group.bench_with_input(
                BenchmarkId::new(format!("{}pct_isolated", isolated_pct), total_size),
                &(num_connected, num_isolated),
                |b, &(connected, isolated)| {
                    b.iter_batched(
                        || create_sparse_graph(connected, isolated),
                        |(mut db, _temp, _nodes)| {
                            black_box(db.find_isolated_nodes().unwrap())
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_find_leaf_nodes(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/find_leaf_nodes");

    for &size in &[100, 500, 1000, 2000] {
        for direction in &[EdgeDirection::Outgoing, EdgeDirection::Incoming, EdgeDirection::Both] {
            let dir_name = match direction {
                EdgeDirection::Outgoing => "out",
                EdgeDirection::Incoming => "in",
                EdgeDirection::Both => "both",
            };

            group.bench_with_input(
                BenchmarkId::new(dir_name, size),
                &(size, *direction),
                |b, &(size, direction)| {
                    b.iter_batched(
                        || create_graph(size, 3, 5, 3),
                        |(mut db, _temp, _nodes)| {
                            black_box(db.find_leaf_nodes(direction).unwrap())
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_count_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/count_operations");

    for &size in &[100, 500, 1000, 2000] {
        // Benchmark count_nodes_by_label
        group.bench_with_input(
            BenchmarkId::new("nodes_by_label", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(db, _temp, _nodes)| {
                        black_box(db.count_nodes_by_label())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark count_edges_by_type
        group.bench_with_input(
            BenchmarkId::new("edges_by_type", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.count_edges_by_type().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark get_total_node_count
        group.bench_with_input(
            BenchmarkId::new("total_node_count", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 5, 3),
                    |(db, _temp, _nodes)| {
                        black_box(db.get_total_node_count())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark get_total_edge_count
        group.bench_with_input(
            BenchmarkId::new("total_edge_count", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 5, 3),
                    |(db, _temp, _nodes)| {
                        black_box(db.get_total_edge_count())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark count_nodes_with_label (specific label)
        group.bench_with_input(
            BenchmarkId::new("nodes_with_label", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(db, _temp, _nodes)| {
                        black_box(db.count_nodes_with_label("Label0"))
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark count_edges_with_type (specific type)
        group.bench_with_input(
            BenchmarkId::new("edges_with_type", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.count_edges_with_type("TYPE0").unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_statistics_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/statistics");

    for &size in &[100, 500, 1000, 2000] {
        // Benchmark get_label_statistics
        group.bench_with_input(
            BenchmarkId::new("label_statistics", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(db, _temp, _nodes)| {
                        black_box(db.get_label_statistics())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark get_edge_type_statistics
        group.bench_with_input(
            BenchmarkId::new("edge_type_statistics", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.get_edge_type_statistics().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark get_degree_statistics
        group.bench_with_input(
            BenchmarkId::new("degree_statistics", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.get_degree_statistics().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark get_average_degree
        group.bench_with_input(
            BenchmarkId::new("average_degree", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.get_average_degree().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Benchmark get_density
        group.bench_with_input(
            BenchmarkId::new("density", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || create_graph(size, 3, 10, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.get_density().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_label_cardinality_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/label_cardinality");
    let size = 1000;

    for &num_labels in &[2, 10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("count_by_label", num_labels),
            &num_labels,
            |b, &num_labels| {
                b.iter_batched(
                    || create_graph(size, 3, num_labels, 5),
                    |(db, _temp, _nodes)| {
                        black_box(db.count_nodes_by_label())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("label_statistics", num_labels),
            &num_labels,
            |b, &num_labels| {
                b.iter_batched(
                    || create_graph(size, 3, num_labels, 5),
                    |(db, _temp, _nodes)| {
                        black_box(db.get_label_statistics())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_edge_type_cardinality_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/edge_type_cardinality");
    let size = 1000;

    for &num_types in &[2, 10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("count_by_type", num_types),
            &num_types,
            |b, &num_types| {
                b.iter_batched(
                    || create_graph(size, 3, 5, num_types),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.count_edges_by_type().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("edge_type_statistics", num_types),
            &num_types,
            |b, &num_types| {
                b.iter_batched(
                    || create_graph(size, 3, 5, num_types),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.get_edge_type_statistics().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_graph_density_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("analytics/density_impact");
    let size = 500;

    for &edges_per_node in &[1, 3, 10, 20] {
        group.bench_with_input(
            BenchmarkId::new("degree_distribution", edges_per_node),
            &edges_per_node,
            |b, &edges_per_node| {
                b.iter_batched(
                    || create_graph(size, edges_per_node, 5, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.degree_distribution().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("density_calculation", edges_per_node),
            &edges_per_node,
            |b, &edges_per_node| {
                b.iter_batched(
                    || create_graph(size, edges_per_node, 5, 5),
                    |(mut db, _temp, _nodes)| {
                        black_box(db.get_density().unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_degree_distribution,
    bench_find_hubs,
    bench_find_isolated_nodes,
    bench_find_leaf_nodes,
    bench_count_operations,
    bench_statistics_operations,
    bench_label_cardinality_impact,
    bench_edge_type_cardinality_impact,
    bench_graph_density_impact,
);

criterion_main!(benches);
