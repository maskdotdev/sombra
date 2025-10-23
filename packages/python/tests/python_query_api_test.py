#!/usr/bin/env python3
"""Test script for Python query API bindings"""

import os
import tempfile
from sombra import SombraDB

def test_analytics_apis():
    """Test all analytics API methods"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        db = SombraDB(db_path)
        
        # Create test graph: users and posts with various edges
        user1 = db.add_node(["User"], {"name": "Alice"})
        user2 = db.add_node(["User"], {"name": "Bob"})
        user3 = db.add_node(["User"], {"name": "Charlie"})
        post1 = db.add_node(["Post"], {"title": "Hello World"})
        post2 = db.add_node(["Post"], {"title": "Python Rocks"})
        
        # Create edges
        db.add_edge(user1, user2, "follows", None)
        db.add_edge(user2, user3, "follows", None)
        db.add_edge(user1, post1, "wrote", None)
        db.add_edge(user2, post2, "wrote", None)
        db.add_edge(user1, post2, "likes", None)
        
        print("✓ Created test graph")
        
        # Test count_nodes_by_label
        label_counts = db.count_nodes_by_label()
        assert label_counts["User"] == 3
        assert label_counts["Post"] == 2
        print(f"✓ count_nodes_by_label: {label_counts}")
        
        # Test count_edges_by_type
        edge_counts = db.count_edges_by_type()
        assert edge_counts["follows"] == 2
        assert edge_counts["wrote"] == 2
        assert edge_counts["likes"] == 1
        print(f"✓ count_edges_by_type: {edge_counts}")
        
        # Test get_total_node_count
        total_nodes = db.get_total_node_count()
        assert total_nodes == 5
        print(f"✓ get_total_node_count: {total_nodes}")
        
        # Test get_total_edge_count
        total_edges = db.get_total_edge_count()
        assert total_edges == 5
        print(f"✓ get_total_edge_count: {total_edges}")
        
        # Test degree_distribution
        dist = db.degree_distribution()
        assert len(dist.in_degree) > 0
        assert len(dist.out_degree) > 0
        assert len(dist.total_degree) > 0
        print(f"✓ degree_distribution: {len(dist.in_degree)} in, {len(dist.out_degree)} out, {len(dist.total_degree)} total")
        
        # Test find_hubs
        hubs_out = db.find_hubs(1, "out")
        assert len(hubs_out) > 0
        print(f"✓ find_hubs (out, min=1): {hubs_out}")
        
        hubs_in = db.find_hubs(1, "in")
        print(f"✓ find_hubs (in, min=1): {hubs_in}")
        
        hubs_total = db.find_hubs(2, "total")
        print(f"✓ find_hubs (total, min=2): {hubs_total}")
        
        # Test find_isolated_nodes
        isolated = db.find_isolated_nodes()
        assert len(isolated) == 0  # All nodes have edges
        print(f"✓ find_isolated_nodes: {isolated}")
        
        # Test find_leaf_nodes
        leaves_out = db.find_leaf_nodes("outgoing")
        print(f"✓ find_leaf_nodes (outgoing): {leaves_out}")
        
        leaves_in = db.find_leaf_nodes("incoming")
        print(f"✓ find_leaf_nodes (incoming): {leaves_in}")
        
        leaves_both = db.find_leaf_nodes("both")
        print(f"✓ find_leaf_nodes (both): {leaves_both}")
        
        # Test get_average_degree
        avg_degree = db.get_average_degree()
        assert avg_degree > 0
        print(f"✓ get_average_degree: {avg_degree:.2f}")
        
        # Test get_density
        density = db.get_density()
        assert 0 <= density <= 1
        print(f"✓ get_density: {density:.3f}")
        
        # Test count_nodes_with_label
        user_count = db.count_nodes_with_label("User")
        assert user_count == 3
        post_count = db.count_nodes_with_label("Post")
        assert post_count == 2
        print(f"✓ count_nodes_with_label: User={user_count}, Post={post_count}")
        
        # Test count_edges_with_type
        follows_count = db.count_edges_with_type("follows")
        assert follows_count == 2
        wrote_count = db.count_edges_with_type("wrote")
        assert wrote_count == 2
        print(f"✓ count_edges_with_type: follows={follows_count}, wrote={wrote_count}")
        
        print("\n✅ All analytics API tests passed!")

def test_subgraph_apis():
    """Test subgraph extraction APIs"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        db = SombraDB(db_path)
        
        # Create test graph
        n1 = db.add_node(["Node"], {"name": "n1"})
        n2 = db.add_node(["Node"], {"name": "n2"})
        n3 = db.add_node(["Node"], {"name": "n3"})
        n4 = db.add_node(["Node"], {"name": "n4"})
        
        db.add_edge(n1, n2, "link", None)
        db.add_edge(n2, n3, "link", None)
        db.add_edge(n3, n4, "other", None)
        
        print("✓ Created test graph for subgraph extraction")
        
        # Test extract_subgraph (no filter)
        subgraph1 = db.extract_subgraph([n1], 2, None, None)
        assert len(subgraph1.nodes) >= 3
        print(f"✓ extract_subgraph (depth=2, no filter): {len(subgraph1.nodes)} nodes, {len(subgraph1.edges)} edges")
        
        # Test extract_subgraph (with edge type filter)
        subgraph2 = db.extract_subgraph([n1], 2, ["link"], "outgoing")
        assert len(subgraph2.nodes) >= 2
        print(f"✓ extract_subgraph (depth=2, type='link'): {len(subgraph2.nodes)} nodes, {len(subgraph2.edges)} edges")
        
        # Test extract_induced_subgraph
        subgraph3 = db.extract_induced_subgraph([n1, n2, n3])
        assert len(subgraph3.nodes) == 3
        print(f"✓ extract_induced_subgraph ([n1,n2,n3]): {len(subgraph3.nodes)} nodes, {len(subgraph3.edges)} edges")
        
        print("\n✅ All subgraph API tests passed!")

if __name__ == "__main__":
    print("Testing Python Query API Bindings\n")
    print("=" * 50)
    test_analytics_apis()
    print("\n" + "=" * 50)
    test_subgraph_apis()
    print("\n" + "=" * 50)
    print("\n🎉 All Python query API tests passed!")
