#!/bin/bash

# This script fixes the test and example files to use the new API

# Fix imports - add Node and Edge, change HashMap to BTreeMap
find tests examples -name "*.rs" -type f -exec sed -i '' \
  's/use sombra::{GraphDB, PropertyValue};/use sombra::{GraphDB, PropertyValue, Node, Edge};/' {} +

find tests examples -name "*.rs" -type f -exec sed -i '' \
  's/use std::collections::HashMap;/use std::collections::BTreeMap;/' {} +

# Also handle case where both are imported
find tests examples -name "*.rs" -type f -exec sed -i '' \
  's/use std::collections::{HashMap, /use std::collections::{BTreeMap, /' {} +

find tests examples -name "*.rs" -type f -exec sed -i '' \
  '/use std::collections::HashMap/d' {} +

echo "Fixed imports"
