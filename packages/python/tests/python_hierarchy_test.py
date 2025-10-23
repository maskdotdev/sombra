import sys
import os
import tempfile
import pytest

try:
    import sombra
except ImportError:
    print("Error: sombra module not found. Build the Python extension first:")
    print("  maturin develop --features python")
    sys.exit(1)

class TestHierarchyAPI:
    def test_find_ancestor_by_label(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            file = db.add_node(["File"], {"name": "main.js"})
            func = db.add_node(["Function"], {"name": "processData"})
            block = db.add_node(["Block"], {"name": "if-block"})
            stmt = db.add_node(["Statement"], {"name": "return"})
            
            db.add_edge(func, file, "PARENT")
            db.add_edge(block, func, "PARENT")
            db.add_edge(stmt, block, "PARENT")
            
            db.flush()
            db.checkpoint()
            
            found_func = db.find_ancestor_by_label(stmt, "Function", "PARENT")
            assert found_func == func
            
            found_file = db.find_ancestor_by_label(stmt, "File", "PARENT")
            assert found_file == file
            
            not_found = db.find_ancestor_by_label(stmt, "NonExistent", "PARENT")
            assert not_found is None
            
            os.unlink(tf.name)
    
    def test_get_ancestors(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            file = db.add_node(["File"], {"name": "main.js"})
            func = db.add_node(["Function"], {"name": "processData"})
            block = db.add_node(["Block"], {"name": "if-block"})
            stmt = db.add_node(["Statement"], {"name": "return"})
            
            db.add_edge(func, file, "PARENT")
            db.add_edge(block, func, "PARENT")
            db.add_edge(stmt, block, "PARENT")
            
            db.flush()
            db.checkpoint()
            
            ancestors = db.get_ancestors(stmt, "PARENT")
            assert len(ancestors) == 3
            assert block in ancestors
            assert func in ancestors
            assert file in ancestors
            
            ancestors_depth_2 = db.get_ancestors(stmt, "PARENT", 2)
            assert len(ancestors_depth_2) == 2
            assert block in ancestors_depth_2
            assert func in ancestors_depth_2
            
            os.unlink(tf.name)
    
    def test_get_descendants(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            file = db.add_node(["File"], {"name": "main.js"})
            func = db.add_node(["Function"], {"name": "processData"})
            block1 = db.add_node(["Block"], {"name": "if-block"})
            block2 = db.add_node(["Block"], {"name": "loop-block"})
            stmt1 = db.add_node(["Statement"], {"name": "return"})
            stmt2 = db.add_node(["Statement"], {"name": "call"})
            
            db.add_edge(func, file, "PARENT")
            db.add_edge(block1, func, "PARENT")
            db.add_edge(block2, func, "PARENT")
            db.add_edge(stmt1, block1, "PARENT")
            db.add_edge(stmt2, block2, "PARENT")
            
            db.flush()
            db.checkpoint()
            
            descendants = db.get_descendants(func, "PARENT")
            assert len(descendants) == 4
            assert block1 in descendants
            assert block2 in descendants
            assert stmt1 in descendants
            assert stmt2 in descendants
            
            descendants_depth_1 = db.get_descendants(func, "PARENT", 1)
            assert len(descendants_depth_1) == 2
            assert block1 in descendants_depth_1
            assert block2 in descendants_depth_1
            
            os.unlink(tf.name)
    
    def test_get_containing_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            db = sombra.SombraDB(tf.name)
            
            file = db.add_node(["File"], {"name": "main.js"})
            func = db.add_node(["Function"], {"name": "processData"})
            block = db.add_node(["Block"], {"name": "if-block"})
            stmt = db.add_node(["Statement"], {"name": "return"})
            
            db.add_edge(func, file, "PARENT")
            db.add_edge(block, func, "PARENT")
            db.add_edge(stmt, block, "PARENT")
            
            db.flush()
            db.checkpoint()
            
            containing_file = db.get_containing_file(stmt)
            assert containing_file == file
            
            containing_file_from_func = db.get_containing_file(func)
            assert containing_file_from_func == file
            
            os.unlink(tf.name)
