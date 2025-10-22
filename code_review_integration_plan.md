# Sombra Graph Database Integration for Code Review Tools

## Overview

This document outlines a comprehensive plan for integrating Sombra graph database with Tree-sitter-based code analysis tools to create a powerful knowledge graph system for code review and analysis.

## Architecture

```
Tree-sitter Parser → AST → Graph Builder → Sombra DB
                                              ↓
                                    Query Helper Layer
                                              ↓
                                    LLM Tool Interface
                                              ↓
                                    Code Review Agent
```

## Graph Schema Design

### Node Labels

```
File           - Source files
Module         - Logical modules/packages
Function       - Functions/methods
Class          - Classes/interfaces/types
Variable       - Variables/constants
Parameter      - Function parameters
Import         - Import statements
Identifier     - Symbol references
ASTNode        - Generic AST nodes (fallback)
```

### Node Properties

**File:**
- `path: str` - Relative file path
- `language: str` - Programming language
- `hash: str` - Content hash for change detection
- `lines: int` - Total lines

**Function:**
- `name: str` - Function name
- `start_line: int` - Starting line number
- `end_line: int` - Ending line number
- `is_async: bool` - Async/sync
- `is_exported: bool` - Public/private
- `signature: str` - Full signature

**Class:**
- `name: str`
- `start_line: int`
- `end_line: int`
- `is_abstract: bool`
- `is_exported: bool`

### Edge Types

```
CONTAINS         - Parent-child AST relationship (File->Function, Function->Variable)
CALLS            - Function invocation
IMPORTS          - Module dependency
REFERENCES       - Symbol usage
DEFINES          - Declaration/definition
INHERITS         - Class inheritance
IMPLEMENTS       - Interface implementation
PARAMETER_OF     - Parameter belongs to function
RETURNS          - Return type relationship
THROWS           - Exception relationship
```

## Core Query Patterns

### 1. Find Function Definition
```python
def find_function_by_name(db, function_name: str) -> Optional[Node]:
    function_nodes = db.get_nodes_by_label("Function")
    for node_id in function_nodes:
        node = db.get_node(node_id)
        if node.properties.get("name") == function_name:
            return node
    return None
```

### 2. Get Function Calls (Direct Callees)
```python
def get_function_calls(db, function_id: int) -> List[Dict]:
    edges = db.get_outgoing_edges(function_id)
    calls = []
    for edge_id in edges:
        edge = db.get_edge(edge_id)
        if edge.edge_type == "CALLS":
            callee = db.get_node(edge.target_id)
            calls.append({
                "name": callee.properties.get("name"),
                "id": edge.target_id,
                "file": get_containing_file(db, edge.target_id)
            })
    return calls
```

### 3. Impact Analysis
```python
def analyze_impact(db, function_id: int, max_depth: int = 3) -> Dict:
    result = bfs_traversal(db, function_id, max_depth, 
                          edge_filter=lambda e: e.edge_type == "CALLS",
                          direction="incoming")
    
    affected_functions = []
    affected_files = set()
    
    for node_id, depth in result:
        if node_id != function_id:
            node = db.get_node(node_id)
            affected_functions.append({
                "name": node.properties.get("name"),
                "depth": depth,
                "file": get_containing_file(db, node_id)
            })
            affected_files.add(get_containing_file(db, node_id))
    
    return {
        "affected_functions": affected_functions,
        "affected_files": list(affected_files),
        "impact_score": len(affected_functions)
    }
```

### 4. File Dependency Analysis
```python
def get_file_dependencies(db, file_path: str) -> Dict:
    file_node_id = find_file_by_path(db, file_path)
    if not file_node_id:
        return {"dependencies": [], "dependents": []}
    
    # Find all IMPORT edges from this file's children
    import_nodes = []
    file_contents = db.get_outgoing_edges(file_node_id)
    for edge_id in file_contents:
        edge = db.get_edge(edge_id)
        if edge.edge_type == "CONTAINS":
            child = db.get_node(edge.target_id)
            if child.labels and "Import" in child.labels:
                import_nodes.append(edge.target_id)
    
    dependencies = set()
    for import_id in import_nodes:
        edges = db.get_outgoing_edges(import_id)
        for edge_id in edges:
            edge = db.get_edge(edge_id)
            if edge.edge_type == "IMPORTS":
                target_file = get_containing_file(db, edge.target_id)
                dependencies.add(target_file)
    
    return {
        "dependencies": list(dependencies),
        "dependents": find_files_importing(db, file_path)
    }
```

### 5. Unused Code Detection
```python
def find_unused_functions(db, entry_points: List[str]) -> List[Dict]:
    all_functions = db.get_nodes_by_label("Function")
    reachable = set()
    
    for entry_name in entry_points:
        entry_node = find_function_by_name(db, entry_name)
        if entry_node:
            visited = bfs_call_graph(db, entry_node.id, max_depth=100)
            reachable.update(visited)
    
    unused = []
    for func_id in all_functions:
        if func_id not in reachable:
            func = db.get_node(func_id)
            if not func.properties.get("is_exported", False):
                unused.append({
                    "name": func.properties.get("name"),
                    "file": get_containing_file(db, func_id),
                    "line": func.properties.get("start_line")
                })
    
    return unused
```

## Helper Functions

### Core Utilities
```python
def get_containing_file(db, node_id: int) -> Optional[str]:
    """Walk up CONTAINS edges to find the File node"""
    visited = set()
    current = node_id
    
    while current and current not in visited:
        visited.add(current)
        node = db.get_node(current)
        
        if node.labels and "File" in node.labels:
            return node.properties.get("path")
        
        # Walk up via incoming CONTAINS edges
        edges = db.get_incoming_edges(current)
        parent_found = False
        for edge_id in edges:
            edge = db.get_edge(edge_id)
            if edge.edge_type == "CONTAINS":
                current = edge.source_id
                parent_found = True
                break
        
        if not parent_found:
            break
    
    return None

def find_file_by_path(db, file_path: str) -> Optional[int]:
    """Find a File node by its path"""
    file_nodes = db.get_nodes_by_label("File")
    for node_id in file_nodes:
        node = db.get_node(node_id)
        if node.properties.get("path") == file_path:
            return node_id
    return None

def get_parent_function(db, node_id: int) -> Optional[Dict]:
    """Find the function containing this node"""
    visited = set()
    current = node_id
    
    while current and current not in visited:
        visited.add(current)
        node = db.get_node(current)
        
        if node.labels and "Function" in node.labels:
            return {
                "name": node.properties.get("name"),
                "id": current
            }
        
        # Walk up via incoming CONTAINS edges
        edges = db.get_incoming_edges(current)
        parent_found = False
        for edge_id in edges:
            edge = db.get_edge(edge_id)
            if edge.edge_type == "CONTAINS":
                current = edge.source_id
                parent_found = True
                break
        
        if not parent_found:
            break
    
    return None
```

### BFS with Custom Filtering
```python
def bfs_with_filter(db, start_id: int, max_depth: int,
                    edge_filter: Callable, direction: str = "outgoing") -> List[Tuple[int, int]]:
    """BFS traversal with custom edge filtering"""
    visited = set()
    queue = [(start_id, 0)]
    result = []
    
    while queue:
        node_id, depth = queue.pop(0)
        
        if node_id in visited or depth > max_depth:
            continue
        
        visited.add(node_id)
        result.append((node_id, depth))
        
        # Get edges based on direction
        if direction == "outgoing":
            edges = db.get_outgoing_edges(node_id)
        elif direction == "incoming":
            edges = db.get_incoming_edges(node_id)
        else:
            edges = db.get_outgoing_edges(node_id) + db.get_incoming_edges(node_id)
        
        # Apply filter and add to queue
        for edge_id in edges:
            edge = db.get_edge(edge_id)
            if edge_filter(edge):
                next_id = edge.target_id if direction == "outgoing" else edge.source_id
                if next_id not in visited:
                    queue.append((next_id, depth + 1))
    
    return result
```

## LLM Tool Interface

### Tool Definitions for LLM Integration

```python
class CodeAnalysisTools:
    """Tools exposed to LLM for code review and analysis"""
    
    def __init__(self, db):
        self.db = db
        self.node_cache = cache_nodes_by_label(db)
    
    def find_function(self, function_name: str) -> Dict:
        """Find a function by name and return its details"""
        function_nodes = self.node_cache.get("Function", [])
        for node_id in function_nodes:
            node = self.db.get_node(node_id)
            if node.properties.get("name") == function_name:
                return {
                    "name": function_name,
                    "file": get_containing_file(self.db, node_id),
                    "start_line": node.properties.get("start_line"),
                    "end_line": node.properties.get("end_line"),
                    "signature": node.properties.get("signature"),
                    "is_async": node.properties.get("is_async", False),
                    "id": node_id
                }
        return {"error": f"Function '{function_name}' not found"}
    
    def get_function_calls(self, function_name: str) -> List[Dict]:
        """Get all functions called by a given function"""
        func_info = self.find_function(function_name)
        if "error" in func_info:
            return []
        
        function_id = func_info["id"]
        edges = self.db.get_outgoing_edges(function_id)
        calls = []
        
        for edge_id in edges:
            edge = self.db.get_edge(edge_id)
            if edge.edge_type == "CALLS":
                callee = self.db.get_node(edge.target_id)
                calls.append({
                    "name": callee.properties.get("name"),
                    "file": get_containing_file(self.db, edge.target_id)
                })
        
        return calls
    
    def analyze_change_impact(self, function_name: str, max_depth: int = 3) -> Dict:
        """Analyze the impact of changing a function"""
        func_info = self.find_function(function_name)
        if "error" in func_info:
            return {"error": func_info["error"]}
        
        function_id = func_info["id"]
        
        # BFS up the call chain
        result = bfs_with_filter(
            self.db, 
            function_id, 
            max_depth,
            edge_filter=lambda e: e.edge_type == "CALLS",
            direction="incoming"
        )
        
        affected_functions = []
        affected_files = set()
        
        for node_id, depth in result:
            if node_id != function_id:
                node = self.db.get_node(node_id)
                file_path = get_containing_file(self.db, node_id)
                affected_functions.append({
                    "name": node.properties.get("name"),
                    "file": file_path,
                    "distance": depth
                })
                if file_path:
                    affected_files.add(file_path)
        
        return {
            "changed_function": function_name,
            "affected_functions": affected_functions,
            "affected_files": list(affected_files),
            "impact_score": len(affected_functions),
            "risk_level": "high" if len(affected_functions) > 10 else "medium" if len(affected_functions) > 3 else "low"
        }
    
    def find_unused_functions(self, entry_points: List[str] = None) -> List[Dict]:
        """Find functions that are never called (potential dead code)"""
        if not entry_points:
            entry_points = ["main", "handler", "index"]
        
        all_functions = self.node_cache.get("Function", [])
        reachable = set()
        
        # BFS from each entry point
        for entry_name in entry_points:
            func_info = self.find_function(entry_name)
            if "id" in func_info:
                result = bfs_with_filter(
                    self.db,
                    func_info["id"],
                    max_depth=100,
                    edge_filter=lambda e: e.edge_type == "CALLS",
                    direction="outgoing"
                )
                reachable.update([node_id for node_id, _ in result])
        
        # Find unreachable functions
        unused = []
        for func_id in all_functions:
            if func_id not in reachable:
                func = self.db.get_node(func_id)
                # Skip exported functions
                if not func.properties.get("is_exported", False):
                    unused.append({
                        "name": func.properties.get("name"),
                        "file": get_containing_file(self.db, func_id),
                        "line": func.properties.get("start_line")
                    })
        
        return unused
```

### MCP Tool Schema
```json
[
  {
    "name": "find_function",
    "description": "Find a function by name and get its details (file, line numbers, signature)",
    "parameters": {
      "type": "object",
      "properties": {
        "function_name": {"type": "string", "description": "Name of the function"}
      },
      "required": ["function_name"]
    }
  },
  {
    "name": "get_function_calls",
    "description": "Get all functions called by a given function (direct dependencies)",
    "parameters": {
      "type": "object",
      "properties": {
        "function_name": {"type": "string", "description": "Name of the function"}
      },
      "required": ["function_name"]
    }
  },
  {
    "name": "analyze_change_impact",
    "description": "Analyze the impact of changing a function - finds all code that depends on it",
    "parameters": {
      "type": "object",
      "properties": {
        "function_name": {"type": "string", "description": "Name of the function"},
        "max_depth": {"type": "integer", "description": "How deep to traverse (default 3)", "default": 3}
      },
      "required": ["function_name"]
    }
  },
  {
    "name": "find_unused_functions",
    "description": "Find functions that are never called (potential dead code)",
    "parameters": {
      "type": "object",
      "properties": {
        "entry_points": {"type": "array", "items": {"type": "string"}, "description": "Entry point functions"}
      }
    }
  }
]
```

## Performance Optimization

### Caching Strategy
```python
class OptimizedCodeAnalysis:
    def __init__(self, db):
        self.db = db
        
        # Cache node IDs by label
        self.label_cache = cache_nodes_by_label(db)
        
        # Cache frequently accessed nodes
        self.node_cache = {}  # node_id -> node
        
        # Cache file paths
        self.file_path_cache = {}  # node_id -> file_path
        
        # Cache call graph adjacency for hot paths
        self.call_graph_cache = {}  # func_id -> [callee_ids]
    
    def get_node_cached(self, node_id: int):
        if node_id not in self.node_cache:
            self.node_cache[node_id] = self.db.get_node(node_id)
        return self.node_cache[node_id]
    
    def invalidate_cache(self):
        """Call after database updates"""
        self.node_cache.clear()
        self.file_path_cache.clear()
        self.call_graph_cache.clear()
```

### Batch Operations
```python
def batch_analyze_functions(db, function_names: List[str]) -> Dict:
    """Analyze multiple functions in a single pass"""
    # Build node cache
    all_functions = db.get_nodes_by_label("Function")
    name_to_id = {}
    
    for func_id in all_functions:
        node = db.get_node(func_id)
        name = node.properties.get("name")
        if name in function_names:
            name_to_id[name] = func_id
    
    # Batch fetch edges
    results = {}
    for name, func_id in name_to_id.items():
        edges = db.get_outgoing_edges(func_id)
        calls = []
        for edge_id in edges:
            edge = db.get_edge(edge_id)
            if edge.edge_type == "CALLS":
                calls.append(edge.target_id)
        results[name] = calls
    
    return results
```

### Incremental Updates
```python
def incremental_ast_update(db, changed_files: List[str]):
    """Only update changed files instead of rebuilding entire graph"""
    txn = db.begin_transaction()
    
    try:
        for file_path in changed_files:
            # Find existing file node
            file_id = find_file_by_path(db, file_path)
            
            if file_id:
                # Delete old AST subtree
                children = get_all_children(db, file_id, recursive=True)
                for child_id in children:
                    db.delete_node(child_id)
            else:
                # Create new file node
                file_id = db.add_node(["File"], {"path": file_path})
            
            # Parse and add new AST
            ast = parse_file_with_treesitter(file_path)
            build_graph_from_ast(db, file_id, ast)
        
        txn.commit()
    except Exception as e:
        txn.rollback()
        raise
```

## Implementation Roadmap

### Phase 1: Core Infrastructure
- [ ] Set up Sombra database with Python bindings
- [ ] Implement graph schema (node labels, edge types)
- [ ] Build Tree-sitter → Sombra converter
- [ ] Create helper function library

### Phase 2: Query Patterns
- [ ] Implement 8 core query patterns
- [ ] Add caching layer for performance
- [ ] Test with sample codebases
- [ ] Benchmark query performance

### Phase 3: LLM Integration
- [ ] Create `CodeAnalysisTools` class
- [ ] Define MCP tool schemas
- [ ] Implement tool router/dispatcher
- [ ] Add error handling and validation

### Phase 4: Optimization
- [ ] Add incremental update support
- [ ] Implement batch operations
- [ ] Build node/path caching
- [ ] Add parallel processing for large codebases

## Key Benefits

1. **Performance**: 18-23x faster than SQLite for graph operations
2. **Scalability**: Handles 100M+ relationships efficiently
3. **ACID Compliance**: Reliable data consistency
4. **Multi-language Support**: Python, Node.js, Rust APIs
5. **Memory Efficient**: Bounded cache usage (~90MB steady state)
6. **Incremental Updates**: Only process changed files

## Usage Examples

### Basic Function Analysis
```python
tools = CodeAnalysisTools(db)

# Find function details
func_info = tools.find_function("calculate_total")
print(f"Function in {func_info['file']} at line {func_info['start_line']}")

# Get function calls
calls = tools.get_function_calls("calculate_total")
print(f"Calls {len(calls)} other functions")

# Analyze impact
impact = tools.analyze_change_impact("calculate_total", max_depth=3)
print(f"Impact score: {impact['impact_score']} (risk: {impact['risk_level']})")
```

### Code Quality Analysis
```python
# Find dead code
unused = tools.find_unused_functions(["main", "handler"])
print(f"Found {len(unused)} potentially unused functions")

# Check circular dependencies
cycles = tools.find_circular_dependencies("src/utils.py")
print(f"Found {len(cycles)} circular dependency chains")

# Get class hierarchy
hierarchy = tools.get_class_hierarchy("BaseController")
print(f"Class has {len(hierarchy['parents'])} parents and {len(hierarchy['children'])} children")
```

### LLM Integration
```python
# LLM can use these tools to understand code structure
llm_tools = get_mcp_tool_definitions()

# Example LLM query:
# "What functions does the main function call?"
# LLM calls: get_function_calls("main")

# Example LLM analysis:
# "If I change the validate_input function, what might break?"
# LLM calls: analyze_change_impact("validate_input", max_depth=3)
```

## Performance Considerations

1. **Use label indexes**: Always query by label first (`get_nodes_by_label("Function")`)
2. **Cache frequently accessed nodes**: Implement node and path caching
3. **Batch operations**: Use transactions for bulk imports
4. **Incremental updates**: Only rebuild changed files
5. **Memory management**: Sombra has bounded memory usage (~90MB)
6. **Parallel processing**: Use for large codebases with many files

## Next Steps

1. **Set up Sombra**: Install and configure Sombra with Python bindings
2. **Build AST converter**: Create Tree-sitter → Sombra pipeline
3. **Implement core tools**: Start with `find_function`, `get_function_calls`, `analyze_change_impact`
4. **Test on real code**: Validate with your actual codebase
5. **Add LLM integration**: Connect tools to your LLM system
6. **Optimize performance**: Add caching and batch operations

This integration provides a powerful foundation for building sophisticated code analysis and review tools that can understand complex relationships in your codebase at scale.