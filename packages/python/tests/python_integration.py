import sys
import os
import tempfile
import pytest
import time
import threading

try:
    import sombra
except ImportError:
    print("Error: sombra module not found. Build the Python extension first:")
    print("  maturin develop --features python")
    sys.exit(1)

class TestBasicOperations:
    def test_create_and_get_node(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node_id = db.add_node(["Person"], {"name": "Alice", "age": 30})
            assert node_id == 1
            
            node = db.get_node(node_id)
            assert node.id == node_id
            assert node.labels == ["Person"]
            assert node.properties["name"] == "Alice"
            assert node.properties["age"] == 30
    
    def test_create_and_get_edge(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node1 = db.add_node(["Person"], {"name": "Alice"})
            node2 = db.add_node(["Person"], {"name": "Bob"})
            
            edge_id = db.add_edge(node1, node2, "KNOWS", {"since": 2020})
            assert edge_id == 1
            
            edge = db.get_edge(edge_id)
            assert edge.id == edge_id
            assert edge.source_node_id == node1
            assert edge.target_node_id == node2
            assert edge.type_name == "KNOWS"
            assert edge.properties["since"] == 2020

class TestTransactions:
    def test_transaction_commit(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            tx = db.begin_transaction()
            node_id = tx.add_node(["Test"], {"value": 42})
            tx.commit()
            
            node = db.get_node(node_id)
            assert node.properties["value"] == 42
    
    def test_transaction_rollback(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            tx1 = db.begin_transaction()
            node_id = tx1.add_node(["Committed"], {"status": "committed"})
            tx1.commit()
            
            tx2 = db.begin_transaction()
            rollback_node_id = tx2.add_node(["RolledBack"], {"status": "rollback"})
            tx2.rollback()
            
            committed_node = db.get_node(node_id)
            assert committed_node is not None
            assert committed_node.properties["status"] == "committed"
            
            try:
                rollback_node = db.get_node(rollback_node_id)
                assert False, "Rolled back node should not exist"
            except:
                pass

class TestGraphTraversal:
    def test_get_outgoing_edges(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node1 = db.add_node(["Node"], {})
            node2 = db.add_node(["Node"], {})
            node3 = db.add_node(["Node"], {})
            
            db.add_edge(node1, node2, "CONNECTS", {})
            db.add_edge(node1, node3, "CONNECTS", {})
            
            outgoing = db.get_outgoing_edges(node1)
            assert len(outgoing) == 2
    
    def test_get_neighbors(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            center = db.add_node(["Center"], {})
            n1 = db.add_node(["Neighbor"], {"id": 1})
            n2 = db.add_node(["Neighbor"], {"id": 2})
            
            db.add_edge(center, n1, "LINKS", {})
            db.add_edge(center, n2, "LINKS", {})
            
            neighbors = db.get_neighbors(center)
            assert len(neighbors) == 2
            assert n1 in neighbors
            assert n2 in neighbors

class TestPropertyTypes:
    def test_integer_properties(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node_id = db.add_node(["Test"], {"count": 100, "negative": -50})
            node = db.get_node(node_id)
            
            assert node.properties["count"] == 100
            assert node.properties["negative"] == -50
    
    def test_float_properties(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node_id = db.add_node(["Test"], {"value": 3.14, "negative": -2.5})
            node = db.get_node(node_id)
            
            assert isinstance(node.properties["value"], float)
            assert isinstance(node.properties["negative"], float)
    
    def test_boolean_properties(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node_id = db.add_node(["Test"], {"active": True, "deleted": False})
            node = db.get_node(node_id)
            
            assert node.properties["active"] == True
            assert node.properties["deleted"] == False
    
    def test_string_properties(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node_id = db.add_node(["Test"], {
                "name": "Test Node",
                "description": "A test node with multiple properties"
            })
            node = db.get_node(node_id)
            
            assert node.properties["name"] == "Test Node"
            assert node.properties["description"] == "A test node with multiple properties"
    
    def test_mixed_properties(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node_id = db.add_node(["Test"], {
                "name": "Mixed",
                "count": 42,
                "ratio": 0.75,
                "active": True
            })
            node = db.get_node(node_id)
            
            assert node.properties["name"] == "Mixed"
            assert node.properties["count"] == 42
            assert isinstance(node.properties["ratio"], float)
            assert node.properties["active"] == True

class TestConcurrency:
    def test_sequential_transactions(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            for i in range(10):
                tx = db.begin_transaction()
                tx.add_node(["Sequential"], {"index": i})
                tx.commit()
            
            count = 0
            for i in range(1, 20):
                try:
                    node = db.get_node(i)
                    if node:
                        count += 1
                except:
                    pass
            
            assert count == 10

class TestBulkOperations:
    def test_bulk_node_insertion(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            node_count = 100
            for i in range(node_count):
                db.add_node(["Bulk"], {"index": i})
            
            count = 0
            for i in range(1, node_count + 10):
                try:
                    node = db.get_node(i)
                    if node:
                        count += 1
                except:
                    pass
            
            assert count == node_count
    
    def test_bulk_edge_insertion(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            nodes = []
            for i in range(20):
                node_id = db.add_node(["Node"], {"id": i})
                nodes.append(node_id)
            
            edge_count = 0
            for i in range(len(nodes) - 1):
                db.add_edge(nodes[i], nodes[i + 1], "NEXT", {})
                edge_count += 1
            
            first_node_edges = db.get_outgoing_edges(nodes[0])
            assert len(first_node_edges) == 1

class TestPersistence:
    def test_database_persistence(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            path = tf.name
            
            db1 = sombra.SombraDB(path)
            node_id = db1.add_node(["Persistent"], {"value": "preserved"})
            db1.checkpoint()
            del db1
            
            db2 = sombra.SombraDB(path)
            node = db2.get_node(node_id)
            assert node is not None
            assert node.properties["value"] == "preserved"

class TestLargeProperties:
    def test_large_string_property(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            large_text = "x" * 10000
            node_id = db.add_node(["Large"], {"text": large_text})
            
            node = db.get_node(node_id)
            text_prop = node.properties["text"]
            assert isinstance(text_prop, str)
            assert len(text_prop) == 10000

class TestBFSTraversal:
    def test_bfs_simple_chain(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            nodes = []
            for i in range(5):
                node_id = db.add_node(["Chain"], {"index": i})
                nodes.append(node_id)
            
            for i in range(len(nodes) - 1):
                db.add_edge(nodes[i], nodes[i + 1], "NEXT", {})
            
            results = db.bfs_traversal(nodes[0], 10)
            assert len(results) >= 1

class TestNodesByLabel:
    def test_get_nodes_by_label(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            for i in range(5):
                db.add_node(["Person"], {"id": i})
            
            for i in range(3):
                db.add_node(["Company"], {"id": i})
            
            persons = db.get_nodes_by_label("Person")
            assert len(persons) == 5
            
            companies = db.get_nodes_by_label("Company")
            assert len(companies) == 3

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
