#!/usr/bin/env python3
"""
Fix old API calls to use new API in test and example files.
Old: tx.create_node(vec!["Label"], props) -> NodeId
New: tx.add_node(Node::new(0) with labels/props) -> Result<NodeId>

Old: tx.create_edge(from, to, "TYPE", props) -> EdgeId  
New: tx.add_edge(Edge::new(0, from, to, "TYPE") with props) -> Result<EdgeId>

Old: tx.get_node(id) -> Result<Option<Node>>
New: tx.get_node(id) -> Result<Node>
"""

import re
import sys
from pathlib import Path

def fix_imports(content):
    """Fix imports to include Node, Edge and use BTreeMap"""
    # Add Node and Edge to imports
    content = re.sub(
        r'use sombra::\{([^}]*?GraphDB[^}]*?PropertyValue[^}]*?)\};',
        r'use sombra::{GraphDB, PropertyValue, Node, Edge};',
        content
    )
    
    # Change HashMap to BTreeMap
    content = content.replace('use std::collections::HashMap', 'use std::collections::BTreeMap')
    content = content.replace('HashMap::', 'BTreeMap::')
    
    return content

def fix_create_node_simple(content):
    """Fix simple create_node patterns"""
    # Pattern: tx.create_node(vec!["Label"], props)
    pattern = r'(\w+)\.create_node\(vec!\[(.*?)\], (\w+)\)'
    
    def replace(m):
        tx, labels, props = m.groups()
        # Extract first label if it's a simple string
        label_match = re.search(r'"([^"]+)"', labels)
        if label_match:
            label = label_match.group(1)
            return f'''{{
            let mut node = Node::new(0);
            node.labels.push("{label}".to_string());
            node.properties = {props};
            {tx}.add_node(node)
        }}'''
        return m.group(0)
    
    content = re.sub(pattern, replace, content)
    return content

def fix_node_option_checks(content):
    """Fix node.is_some() / is_none() / unwrap() patterns"""
    # node.is_some() -> node.is_ok()
    content = re.sub(r'(\w+)\.is_some\(\)', r'\1.is_ok()', content)
    # node.is_none() -> node.is_err()
    content = re.sub(r'(\w+)\.is_none\(\)', r'\1.is_err()', content)
    
    # Remove unwrap after get_node when used directly
    # let node = tx.get_node(id).unwrap(); assert!(node.is_some())
    # -> let node = tx.get_node(id).unwrap(); (node is now Node not Option<Node>)
    
    return content

def process_file(filepath):
    """Process a single file"""
    print(f"Processing {filepath}")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    
    content = fix_imports(content)
    # content = fix_create_node_simple(content)
    content = fix_node_option_checks(content)
    
    if content != original:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"  Modified {filepath}")
        return True
    return False

def main():
    test_dir = Path('tests')
    example_dir = Path('examples')
    
    count = 0
    for pattern in ['*.rs']:
        for directory in [test_dir, example_dir]:
            if directory.exists():
                for filepath in directory.glob(pattern):
                    if process_file(filepath):
                        count += 1
    
    print(f"\nModified {count} files")

if __name__ == '__main__':
    main()
