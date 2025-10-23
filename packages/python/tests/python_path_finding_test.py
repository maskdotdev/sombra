#!/usr/bin/env python3
"""Test script for Python path finding API bindings"""

import os
import tempfile
from sombra import SombraDB

def test_path_finding():
    """Test path finding methods"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        db = SombraDB(db_path)
        
        # Create a simple graph: A -> B -> C -> D
        #                        A -> E -> D
        node_a = db.add_node(["Node"], {"name": "A"})
        node_b = db.add_node(["Node"], {"name": "B"})
        node_c = db.add_node(["Node"], {"name": "C"})
        node_d = db.add_node(["Node"], {"name": "D"})
        node_e = db.add_node(["Node"], {"name": "E"})
        
        db.add_edge(node_a, node_b, "link", None)
        db.add_edge(node_b, node_c, "link", None)
        db.add_edge(node_c, node_d, "link", None)
        db.add_edge(node_a, node_e, "shortcut", None)
        db.add_edge(node_e, node_d, "shortcut", None)
        
        print("✓ Created test graph")
        
        # Test shortest_path - should find A -> E -> D (2 hops)
        path = db.shortest_path(node_a, node_d, None)
        assert path is not None
        assert len(path) == 3
        assert path[0] == node_a
        assert path[2] == node_d
        print(f"✓ shortest_path (any edge): {path}")
        
        # Test shortest_path with edge type filter - should find A -> B -> C -> D (3 hops)
        path_link = db.shortest_path(node_a, node_d, ["link"])
        assert path_link is not None
        assert len(path_link) == 4
        assert path_link == [node_a, node_b, node_c, node_d]
        print(f"✓ shortest_path (link edges only): {path_link}")
        
        # Test shortest_path with no path
        isolated = db.add_node(["Node"], {"name": "Isolated"})
        no_path = db.shortest_path(node_a, isolated, None)
        assert no_path is None
        print("✓ shortest_path returns None when no path exists")
        
        # Test find_paths - should find multiple paths
        paths = db.find_paths(node_a, node_d, 2, 4, None)
        assert len(paths) >= 2
        print(f"✓ find_paths found {len(paths)} paths:")
        for i, p in enumerate(paths):
            print(f"  Path {i+1}: {p}")
        
        # Test find_paths with depth constraints (depth = number of nodes in path)
        shallow_paths = db.find_paths(node_a, node_d, 3, 3, None)
        for path in shallow_paths:
            assert len(path) == 3  # depth 3 = 3 nodes
        print(f"✓ find_paths with depth 3-3: {len(shallow_paths)} paths")
        
        # Test find_paths with edge type filter (A->B->C->D = 4 nodes)
        link_paths = db.find_paths(node_a, node_d, 4, 4, ["link"])
        assert len(link_paths) >= 1
        assert [node_a, node_b, node_c, node_d] in link_paths
        print(f"✓ find_paths (link edges only): {len(link_paths)} paths")
        
        # Test self-loop case
        self_path = db.shortest_path(node_a, node_a, None)
        assert self_path == [node_a]
        print("✓ shortest_path handles self-loops correctly")
        
        print("\n✅ All path finding tests passed!")

if __name__ == "__main__":
    test_path_finding()
