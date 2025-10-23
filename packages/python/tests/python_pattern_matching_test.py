import sys
import os
import tempfile

try:
    import sombra
except ImportError:
    print("Error: sombra module not found. Build the Python extension first:")
    print("  maturin develop --features python")
    sys.exit(1)

class TestPatternMatchingAPI:
    def test_basic_call_pattern(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            target_id = db.add_node(["Function"], {"name": "foo"})
            call_id = db.add_node(["CallExpr"], {"callee": "foo"})
            other_func_id = db.add_node(["Function"], {"name": "bar"})
            other_call_id = db.add_node(["CallExpr"], {"callee": "bar"})
            
            call_edge_id = db.add_edge(call_id, target_id, "CALLS", {})
            db.add_edge(other_call_id, other_func_id, "CALLS", {})
            
            db.flush()
            db.checkpoint()
            
            pattern = sombra.Pattern(
                nodes=[
                    sombra.NodePattern(
                        var_name="call",
                        labels=["CallExpr"],
                        properties=sombra.PropertyFilters(
                            equals={"callee": "foo"},
                            not_equals={},
                            ranges=[]
                        )
                    ),
                    sombra.NodePattern(
                        var_name="func",
                        labels=["Function"],
                        properties=sombra.PropertyFilters(
                            equals={"name": "foo"},
                            not_equals={},
                            ranges=[]
                        )
                    )
                ],
                edges=[
                    sombra.EdgePattern(
                        from_var="call",
                        to_var="func",
                        types=["CALLS"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        ),
                        direction="outgoing"
                    )
                ]
            )
            
            matches = db.match_pattern(pattern)
            
            assert len(matches) == 1
            assert matches[0].node_bindings["call"] == call_id
            assert matches[0].node_bindings["func"] == target_id
            assert matches[0].edge_ids == [call_edge_id]
            
            os.unlink(tf.name)
    
    def test_incoming_edge_pattern(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            parent_id = db.add_node(["Module"], {"name": "core"})
            child_id = db.add_node(["File"], {"path": "src/lib.rs"})
            sibling_id = db.add_node(["File"], {"path": "src/mod.rs"})
            
            contains_edge_id = db.add_edge(parent_id, child_id, "CONTAINS", {})
            db.add_edge(parent_id, sibling_id, "CONTAINS", {})
            
            db.flush()
            db.checkpoint()
            
            pattern = sombra.Pattern(
                nodes=[
                    sombra.NodePattern(
                        var_name="child",
                        labels=["File"],
                        properties=sombra.PropertyFilters(
                            equals={"path": "src/lib.rs"},
                            not_equals={},
                            ranges=[]
                        )
                    ),
                    sombra.NodePattern(
                        var_name="parent",
                        labels=["Module"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        )
                    )
                ],
                edges=[
                    sombra.EdgePattern(
                        from_var="child",
                        to_var="parent",
                        types=["CONTAINS"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        ),
                        direction="incoming"
                    )
                ]
            )
            
            matches = db.match_pattern(pattern)
            
            assert len(matches) == 1
            assert matches[0].node_bindings["child"] == child_id
            assert matches[0].node_bindings["parent"] == parent_id
            assert matches[0].edge_ids == [contains_edge_id]
            
            os.unlink(tf.name)
    
    def test_property_range_filter(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            user1_id = db.add_node(["User"], {"name": "Alice", "age": 25})
            user2_id = db.add_node(["User"], {"name": "Bob", "age": 35})
            user3_id = db.add_node(["User"], {"name": "Charlie", "age": 45})
            
            post1_id = db.add_node(["Post"], {"title": "Post1"})
            post2_id = db.add_node(["Post"], {"title": "Post2"})
            post3_id = db.add_node(["Post"], {"title": "Post3"})
            
            db.add_edge(user1_id, post1_id, "AUTHORED", {})
            edge2_id = db.add_edge(user2_id, post2_id, "AUTHORED", {})
            db.add_edge(user3_id, post3_id, "AUTHORED", {})
            
            db.flush()
            db.checkpoint()
            
            pattern = sombra.Pattern(
                nodes=[
                    sombra.NodePattern(
                        var_name="user",
                        labels=["User"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[
                                sombra.PropertyRangeFilter(
                                    "age",
                                    sombra.PropertyBound(30, True),
                                    sombra.PropertyBound(40, True)
                                )
                            ]
                        )
                    ),
                    sombra.NodePattern(
                        var_name="post",
                        labels=["Post"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        )
                    )
                ],
                edges=[
                    sombra.EdgePattern(
                        from_var="user",
                        to_var="post",
                        types=["AUTHORED"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        ),
                        direction="outgoing"
                    )
                ]
            )
            
            matches = db.match_pattern(pattern)
            
            assert len(matches) == 1
            assert matches[0].node_bindings["user"] == user2_id
            assert matches[0].node_bindings["post"] == post2_id
            assert matches[0].edge_ids == [edge2_id]
            
            os.unlink(tf.name)
    
    def test_multi_hop_pattern(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            alice_id = db.add_node(["Person"], {"name": "Alice"})
            bob_id = db.add_node(["Person"], {"name": "Bob"})
            charlie_id = db.add_node(["Person"], {"name": "Charlie"})
            
            edge1_id = db.add_edge(alice_id, bob_id, "KNOWS", {})
            edge2_id = db.add_edge(bob_id, charlie_id, "KNOWS", {})
            
            db.flush()
            db.checkpoint()
            
            pattern = sombra.Pattern(
                nodes=[
                    sombra.NodePattern(
                        var_name="a",
                        labels=["Person"],
                        properties=sombra.PropertyFilters(
                            equals={"name": "Alice"},
                            not_equals={},
                            ranges=[]
                        )
                    ),
                    sombra.NodePattern(
                        var_name="b",
                        labels=["Person"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        )
                    ),
                    sombra.NodePattern(
                        var_name="c",
                        labels=["Person"],
                        properties=sombra.PropertyFilters(
                            equals={"name": "Charlie"},
                            not_equals={},
                            ranges=[]
                        )
                    )
                ],
                edges=[
                    sombra.EdgePattern(
                        from_var="a",
                        to_var="b",
                        types=["KNOWS"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        ),
                        direction="outgoing"
                    ),
                    sombra.EdgePattern(
                        from_var="b",
                        to_var="c",
                        types=["KNOWS"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={},
                            ranges=[]
                        ),
                        direction="outgoing"
                    )
                ]
            )
            
            matches = db.match_pattern(pattern)
            
            assert len(matches) == 1
            assert matches[0].node_bindings["a"] == alice_id
            assert matches[0].node_bindings["b"] == bob_id
            assert matches[0].node_bindings["c"] == charlie_id
            assert matches[0].edge_ids == [edge1_id, edge2_id]
            
            os.unlink(tf.name)
    
    def test_not_equals_filter(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            foo_id = db.add_node(["Function"], {"name": "foo", "visibility": "public"})
            db.add_node(["Function"], {"name": "bar", "visibility": "private"})
            
            db.flush()
            db.checkpoint()
            
            pattern = sombra.Pattern(
                nodes=[
                    sombra.NodePattern(
                        var_name="func",
                        labels=["Function"],
                        properties=sombra.PropertyFilters(
                            equals={},
                            not_equals={"visibility": "private"},
                            ranges=[]
                        )
                    )
                ],
                edges=[]
            )
            
            matches = db.match_pattern(pattern)
            
            assert len(matches) == 1
            assert matches[0].node_bindings["func"] == foo_id
            
            os.unlink(tf.name)

if __name__ == "__main__":
    test = TestPatternMatchingAPI()
    
    print("Running test_basic_call_pattern...")
    test.test_basic_call_pattern()
    print("✓ test_basic_call_pattern passed")
    
    print("Running test_incoming_edge_pattern...")
    test.test_incoming_edge_pattern()
    print("✓ test_incoming_edge_pattern passed")
    
    print("Running test_property_range_filter...")
    test.test_property_range_filter()
    print("✓ test_property_range_filter passed")
    
    print("Running test_multi_hop_pattern...")
    test.test_multi_hop_pattern()
    print("✓ test_multi_hop_pattern passed")
    
    print("Running test_not_equals_filter...")
    test.test_not_equals_filter()
    print("✓ test_not_equals_filter passed")
    
    print("\nAll tests passed!")
