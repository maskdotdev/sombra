#!/usr/bin/env python3

import os
import tempfile
import sombra

def test_query_builder():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
        db_path = tmp.name

    try:
        db = sombra.SombraDB(db_path)
        
        func1 = db.add_node(["Function"], {"name": "foo", "lines": 10})
        func2 = db.add_node(["Function"], {"name": "bar", "lines": 20})
        func3 = db.add_node(["Function"], {"name": "baz", "lines": 30})
        file1 = db.add_node(["File"], {"path": "main.py"})
        
        db.add_edge(func1, func2, "CALLS", {})
        db.add_edge(func2, func3, "CALLS", {})
        db.add_edge(file1, func1, "CONTAINS", {})
        
        result = db.query() \
            .start_from_label("Function") \
            .traverse(["CALLS"], "outgoing", 2) \
            .limit(10) \
            .execute()
        
        print(f"✓ Query executed successfully")
        print(f"  Start nodes: {result.start_nodes}")
        print(f"  Node IDs: {result.node_ids}")
        print(f"  Nodes: {len(result.nodes)}")
        print(f"  Edges: {len(result.edges)}")
        print(f"  Limited: {result.limited}")
        
        assert len(result.nodes) > 0, "Expected nodes in result"
        assert len(result.node_ids) > 0, "Expected node IDs in result"
        
        result2 = db.query() \
            .start_from([func1]) \
            .traverse(["CALLS"], "outgoing", 1) \
            .execute()
        
        print(f"✓ Query from explicit node executed")
        print(f"  Node IDs: {result2.node_ids}")
        
        result3 = db.query() \
            .start_from_property("Function", "name", "foo") \
            .traverse(["CALLS"], "outgoing", 2) \
            .execute()
        
        print(f"✓ Query from property executed")
        print(f"  Node IDs: {result3.node_ids}")
        
        print("\n✓ All tests passed!")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    test_query_builder()
