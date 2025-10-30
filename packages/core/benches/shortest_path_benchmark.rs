//! Shortest Path and Path Finding Performance Benchmarks using Criterion
//!
//! Benchmarks for shortest path algorithms and multi-path finding across various graph topologies.
//!
//! Run with: cargo bench --bench shortest_path_benchmark --features benchmarks
//! View results: open target/criterion/report/index.html

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, BatchSize};
use sombra::db::GraphDB;
use sombra::model::{Node, Edge};
use tempfile::NamedTempFile;

/// Helper: Create a chain graph (A -> B -> C -> D -> ...)
/// Perfect for testing path length scaling
fn create_chain_graph(chain_length: usize) -> (GraphDB, NamedTempFile, Vec<u64>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    let mut node_ids = Vec::with_capacity(chain_length);
    
    // Create first node
    let first_id = db.add_node(Node::new(0)).unwrap();
    node_ids.push(first_id);

    // Create chain
    for i in 1..chain_length {
        let node_id = db.add_node(Node::new(i as u64)).unwrap();
        db.add_edge(Edge::new(0, node_ids[i - 1], node_id, "NEXT"))
            .unwrap();
        node_ids.push(node_id);
    }

    (db, temp_file, node_ids)
}

/// Helper: Create a grid graph (NxN grid with connections to adjacent cells)
/// Good for testing path finding in 2D spaces
fn create_grid_graph(grid_size: usize) -> (GraphDB, NamedTempFile, Vec<Vec<u64>>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    let mut grid = vec![vec![0; grid_size]; grid_size];

    // Create nodes
    for i in 0..grid_size {
        for j in 0..grid_size {
            let node_id = db.add_node(Node::new((i * grid_size + j) as u64)).unwrap();
            grid[i][j] = node_id;
        }
    }

    // Create edges (4-way connectivity: up, down, left, right)
    for i in 0..grid_size {
        for j in 0..grid_size {
            // Right
            if j < grid_size - 1 {
                db.add_edge(Edge::new(0, grid[i][j], grid[i][j + 1], "RIGHT"))
                    .unwrap();
            }
            // Down
            if i < grid_size - 1 {
                db.add_edge(Edge::new(0, grid[i][j], grid[i + 1][j], "DOWN"))
                    .unwrap();
            }
            // Left
            if j > 0 {
                db.add_edge(Edge::new(0, grid[i][j], grid[i][j - 1], "LEFT"))
                    .unwrap();
            }
            // Up
            if i > 0 {
                db.add_edge(Edge::new(0, grid[i][j], grid[i - 1][j], "UP"))
                    .unwrap();
            }
        }
    }

    (db, temp_file, grid)
}

/// Helper: Create a star graph (central hub with many spokes)
/// Good for testing hub-centric path finding
fn create_star_graph(num_spokes: usize) -> (GraphDB, NamedTempFile, u64, Vec<u64>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    // Create center node
    let center_id = db.add_node(Node::new(0)).unwrap();

    // Create spoke nodes
    let mut spoke_ids = Vec::with_capacity(num_spokes);
    for i in 0..num_spokes {
        let spoke_id = db.add_node(Node::new((i + 1) as u64)).unwrap();
        db.add_edge(Edge::new(0, center_id, spoke_id, "CONNECTS"))
            .unwrap();
        spoke_ids.push(spoke_id);
    }

    (db, temp_file, center_id, spoke_ids)
}

/// Helper: Create a binary tree graph
/// Good for testing hierarchical path finding
fn create_binary_tree(depth: usize) -> (GraphDB, NamedTempFile, u64, Vec<u64>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    let mut node_ids = Vec::new();
    let root_id = db.add_node(Node::new(0)).unwrap();
    node_ids.push(root_id);

    let mut current_level = vec![root_id];
    let mut node_counter = 1u64;

    for _ in 0..depth {
        let mut next_level = Vec::new();
        for parent_id in &current_level {
            // Left child
            let left_id = db.add_node(Node::new(node_counter)).unwrap();
            db.add_edge(Edge::new(0, *parent_id, left_id, "LEFT"))
                .unwrap();
            node_ids.push(left_id);
            next_level.push(left_id);
            node_counter += 1;

            // Right child
            let right_id = db.add_node(Node::new(node_counter)).unwrap();
            db.add_edge(Edge::new(0, *parent_id, right_id, "RIGHT"))
                .unwrap();
            node_ids.push(right_id);
            next_level.push(right_id);
            node_counter += 1;
        }
        current_level = next_level;
    }

    (db, temp_file, root_id, node_ids)
}

/// Helper: Create a social network graph (random connections)
/// Good for testing realistic path finding scenarios
fn create_social_graph(num_users: usize, avg_friends: usize) -> (GraphDB, NamedTempFile, Vec<u64>) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp_file.path()).unwrap();

    let mut user_ids = Vec::with_capacity(num_users);
    
    // Create users
    for i in 0..num_users {
        let user_id = db.add_node(Node::new(i as u64)).unwrap();
        user_ids.push(user_id);
    }

    // Create friendships (using simple modulo-based connections for determinism)
    for i in 0..num_users {
        for j in 1..=avg_friends {
            let friend_idx = (i + j * 7) % num_users; // Pseudo-random but deterministic
            if friend_idx != i {
                db.add_edge(Edge::new(0, user_ids[i], user_ids[friend_idx], "FRIEND"))
                    .unwrap();
            }
        }
    }

    (db, temp_file, user_ids)
}

fn bench_shortest_path_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path/chain");

    for &chain_length in &[50, 100, 500, 1000] {
        for &distance in &[10, 25, 50] {
            if distance >= chain_length {
                continue;
            }

            group.bench_with_input(
                BenchmarkId::new(format!("len_{}", chain_length), distance),
                &(chain_length, distance),
                |b, &(chain_length, distance)| {
                    b.iter_batched(
                        || create_chain_graph(chain_length),
                        |(mut db, _temp, nodes)| {
                            let start = nodes[0];
                            let end = nodes[distance];
                            black_box(db.shortest_path(start, end, None).unwrap())
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_shortest_path_grid(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path/grid");

    for &grid_size in &[10, 20, 30] {
        group.bench_with_input(
            BenchmarkId::new("corner_to_corner", grid_size),
            &grid_size,
            |b, &grid_size| {
                b.iter_batched(
                    || create_grid_graph(grid_size),
                    |(mut db, _temp, grid)| {
                        let start = grid[0][0];
                        let end = grid[grid_size - 1][grid_size - 1];
                        black_box(db.shortest_path(start, end, None).unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("adjacent", grid_size),
            &grid_size,
            |b, &grid_size| {
                b.iter_batched(
                    || create_grid_graph(grid_size),
                    |(mut db, _temp, grid)| {
                        let start = grid[grid_size / 2][grid_size / 2];
                        let end = grid[grid_size / 2][grid_size / 2 + 1];
                        black_box(db.shortest_path(start, end, None).unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_shortest_path_star(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path/star");

    for &num_spokes in &[100, 500, 1000, 2000] {
        group.bench_with_input(
            BenchmarkId::new("spoke_to_spoke", num_spokes),
            &num_spokes,
            |b, &num_spokes| {
                b.iter_batched(
                    || create_star_graph(num_spokes),
                    |(mut db, _temp, _center, spokes)| {
                        let start = spokes[0];
                        let end = spokes[num_spokes - 1];
                        black_box(db.shortest_path(start, end, None).unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("center_to_spoke", num_spokes),
            &num_spokes,
            |b, &num_spokes| {
                b.iter_batched(
                    || create_star_graph(num_spokes),
                    |(mut db, _temp, center, spokes)| {
                        let end = spokes[num_spokes / 2];
                        black_box(db.shortest_path(center, end, None).unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_shortest_path_tree(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path/binary_tree");

    for &depth in &[5, 7, 9, 10] {
        group.bench_with_input(
            BenchmarkId::new("root_to_leaf", depth),
            &depth,
            |b, &depth| {
                b.iter_batched(
                    || create_binary_tree(depth),
                    |(mut db, _temp, root, nodes)| {
                        let end = nodes[nodes.len() - 1]; // Rightmost leaf
                        black_box(db.shortest_path(root, end, None).unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("leaf_to_leaf", depth),
            &depth,
            |b, &depth| {
                b.iter_batched(
                    || create_binary_tree(depth),
                    |(mut db, _temp, _root, nodes)| {
                        // Path between leftmost and rightmost leaves
                        let total_nodes = nodes.len();
                        let first_leaf_idx = (total_nodes + 1) / 2 - 1;
                        let start = nodes[first_leaf_idx];
                        let end = nodes[total_nodes - 1];
                        black_box(db.shortest_path(start, end, None).unwrap())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_shortest_path_social(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path/social_network");

    for &num_users in &[100, 500, 1000] {
        for &avg_friends in &[5, 10, 20] {
            group.bench_with_input(
                BenchmarkId::new(format!("friends_{}", avg_friends), num_users),
                &(num_users, avg_friends),
                |b, &(num_users, avg_friends)| {
                    b.iter_batched(
                        || create_social_graph(num_users, avg_friends),
                        |(mut db, _temp, users)| {
                            let start = users[0];
                            let end = users[num_users - 1];
                            black_box(db.shortest_path(start, end, None).unwrap())
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_shortest_path_with_edge_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path/edge_type_filter");

    let grid_size = 20;

    group.bench_function("no_filter", |b| {
        b.iter_batched(
            || create_grid_graph(grid_size),
            |(mut db, _temp, grid)| {
                let start = grid[0][0];
                let end = grid[grid_size - 1][grid_size - 1];
                black_box(db.shortest_path(start, end, None).unwrap())
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("right_down_only", |b| {
        b.iter_batched(
            || create_grid_graph(grid_size),
            |(mut db, _temp, grid)| {
                let start = grid[0][0];
                let end = grid[grid_size - 1][grid_size - 1];
                black_box(db.shortest_path(start, end, Some(&["RIGHT", "DOWN"])).unwrap())
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("single_type", |b| {
        b.iter_batched(
            || create_grid_graph(grid_size),
            |(mut db, _temp, grid)| {
                let start = grid[0][0];
                let end = grid[0][grid_size - 1];
                black_box(db.shortest_path(start, end, Some(&["RIGHT"])).unwrap())
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_find_paths_multiple(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_paths/multiple_paths");

    // Grid graphs have many alternate paths
    for &grid_size in &[5, 10, 15] {
        for &max_depth in &[5, 10, 15] {
            group.bench_with_input(
                BenchmarkId::new(format!("grid_{}", grid_size), max_depth),
                &(grid_size, max_depth),
                |b, &(grid_size, max_depth)| {
                    b.iter_batched(
                        || create_grid_graph(grid_size),
                        |(mut db, _temp, grid)| {
                            let start = grid[0][0];
                            let end = grid[grid_size.min(3) - 1][grid_size.min(3) - 1];
                            black_box(db.find_paths(start, end, 1, max_depth, None).unwrap())
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_find_paths_depth_variation(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_paths/depth_variation");

    let grid_size = 10;

    for &min_depth in &[1, 3, 5] {
        for &max_depth in &[5, 10, 15] {
            if min_depth > max_depth {
                continue;
            }

            group.bench_with_input(
                BenchmarkId::new(format!("min_{}", min_depth), max_depth),
                &(min_depth, max_depth),
                |b, &(min_depth, max_depth)| {
                    b.iter_batched(
                        || create_grid_graph(grid_size),
                        |(mut db, _temp, grid)| {
                            let start = grid[0][0];
                            let end = grid[3][3];
                            black_box(db.find_paths(start, end, min_depth, max_depth, None).unwrap())
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_shortest_path_no_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path/no_path_exists");

    for &size in &[100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("disconnected_nodes", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let temp_file = NamedTempFile::new().unwrap();
                        let mut db = GraphDB::open(temp_file.path()).unwrap();
                        
                        // Create two disconnected components
                        let component1_start = db.add_node(Node::new(0)).unwrap();
                        let mut prev = component1_start;
                        for i in 1..size/2 {
                            let node = db.add_node(Node::new(i as u64)).unwrap();
                            db.add_edge(Edge::new(0, prev, node, "NEXT")).unwrap();
                            prev = node;
                        }
                        
                        let component2_start = db.add_node(Node::new(1000)).unwrap();
                        let mut prev = component2_start;
                        for i in 1..size/2 {
                            let node = db.add_node(Node::new((1000 + i) as u64)).unwrap();
                            db.add_edge(Edge::new(0, prev, node, "NEXT")).unwrap();
                            prev = node;
                        }
                        
                        (db, temp_file, component1_start, component2_start)
                    },
                    |(mut db, _temp, start, end)| {
                        black_box(db.shortest_path(start, end, None).unwrap())
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
    bench_shortest_path_chain,
    bench_shortest_path_grid,
    bench_shortest_path_star,
    bench_shortest_path_tree,
    bench_shortest_path_social,
    bench_shortest_path_with_edge_filter,
    bench_find_paths_multiple,
    bench_find_paths_depth_variation,
    bench_shortest_path_no_path,
);

criterion_main!(benches);
