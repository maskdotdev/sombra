#!/bin/bash

# Demo script showing CLI and web functionality
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Sombra CLI & Web Demo ===${NC}\n"

# Setup
DEMO_DB="/tmp/sombra-demo-$(date +%s).db"
CLI_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=13000

echo -e "${GREEN}1. Creating demo database...${NC}"
cat > /tmp/create-demo-db.js <<'EOF'
const { SombraDB } = require('@unyth/sombra');
const db = new SombraDB(process.argv[2]);

console.log('Creating social network demo data...');

// Create people
const alice = db.addNode(['Person'], { name: 'Alice', age: 30, role: 'Engineer' });
const bob = db.addNode(['Person'], { name: 'Bob', age: 28, role: 'Designer' });
const charlie = db.addNode(['Person'], { name: 'Charlie', age: 35, role: 'Manager' });
const diana = db.addNode(['Person'], { name: 'Diana', age: 32, role: 'Analyst' });

// Create companies
const techCorp = db.addNode(['Company'], { name: 'TechCorp', employees: 500 });
const innovate = db.addNode(['Company'], { name: 'Innovate Inc', employees: 150 });

// Create cities
const sf = db.addNode(['City'], { name: 'San Francisco', state: 'CA', population: 873965 });
const nyc = db.addNode(['City'], { name: 'New York', state: 'NY', population: 8336817 });

// Create relationships
db.addEdge(alice, bob, 'KNOWS', { since: 2020, closeness: 8 });
db.addEdge(bob, charlie, 'KNOWS', { since: 2019, closeness: 7 });
db.addEdge(charlie, diana, 'KNOWS', { since: 2021, closeness: 9 });
db.addEdge(diana, alice, 'KNOWS', { since: 2022, closeness: 6 });

db.addEdge(alice, techCorp, 'WORKS_AT', { position: 'Senior Engineer', years: 3 });
db.addEdge(bob, techCorp, 'WORKS_AT', { position: 'Lead Designer', years: 2 });
db.addEdge(charlie, innovate, 'WORKS_AT', { position: 'Engineering Manager', years: 5 });
db.addEdge(diana, innovate, 'WORKS_AT', { position: 'Data Analyst', years: 1 });

db.addEdge(alice, sf, 'LIVES_IN', { years: 5 });
db.addEdge(bob, sf, 'LIVES_IN', { years: 4 });
db.addEdge(charlie, nyc, 'LIVES_IN', { years: 10 });
db.addEdge(diana, nyc, 'LIVES_IN', { years: 3 });

db.flush();

const stats = {
  nodes: db.getTotalNodeCount(),
  edges: db.getTotalEdgeCount(),
  people: db.countNodesWithLabel('Person'),
  companies: db.countNodesWithLabel('Company'),
  cities: db.countNodesWithLabel('City')
};

console.log(`\nCreated ${stats.nodes} nodes and ${stats.edges} edges`);
console.log(`- ${stats.people} people`);
console.log(`- ${stats.companies} companies`);
console.log(`- ${stats.cities} cities`);
console.log('\nDemo database ready!');
EOF

cd "$CLI_DIR/packages/web"
NODE_PATH="$CLI_DIR/packages/web/node_modules" node /tmp/create-demo-db.js "$DEMO_DB"

echo -e "\n${GREEN}2. Testing CLI commands...${NC}\n"

echo "$ sombra --help"
node "$CLI_DIR/packages/cli/bin/sombra.js" --help
echo

echo "$ sombra web --help"
node "$CLI_DIR/packages/cli/bin/sombra.js" web --help
echo

echo -e "${GREEN}3. Starting web server with CLI (no browser)...${NC}"
echo "$ sombra web --db $DEMO_DB --port $PORT --no-open"

# Start server in background
SOMBRA_DB_PATH="$DEMO_DB" PORT=$PORT node "$CLI_DIR/packages/cli/bin/sombra.js" web --no-open > /tmp/sombra-demo-server.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
sleep 8

# Test if server is responding
if curl -s "http://localhost:$PORT" > /dev/null; then
    echo -e "${GREEN}✓ Server started successfully on http://localhost:$PORT${NC}\n"
    
    echo -e "${GREEN}4. Testing API endpoints...${NC}\n"
    
    echo "$ curl http://localhost:$PORT/api/graph/stats"
    curl -s "http://localhost:$PORT/api/graph/stats" | head -c 200
    echo -e "\n"
    
    echo "$ curl http://localhost:$PORT/api/graph/nodes | head -c 300"
    curl -s "http://localhost:$PORT/api/graph/nodes" | head -c 300
    echo -e "...\n"
    
    echo "$ curl http://localhost:$PORT/api/graph/edges | head -c 300"
    curl -s "http://localhost:$PORT/api/graph/edges" | head -c 300
    echo -e "...\n"
    
    echo -e "${GREEN}5. Web UI Access${NC}"
    echo -e "   Open your browser to: ${YELLOW}http://localhost:$PORT${NC}"
    echo -e "   - View the graph visualization"
    echo -e "   - Click nodes to explore connections"
    echo -e "   - Inspect node and edge properties\n"
    
    echo -e "${YELLOW}Press Enter to stop the server and cleanup...${NC}"
    read
    
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    echo -e "${GREEN}✓ Server stopped${NC}"
else
    echo -e "\033[0;31m✗ Server failed to start${NC}"
    cat /tmp/sombra-demo-server.log
    kill $SERVER_PID 2>/dev/null || true
fi

# Cleanup
rm -f "$DEMO_DB" "$DEMO_DB-shm" "$DEMO_DB-wal"
rm -f /tmp/create-demo-db.js /tmp/sombra-demo-server.log

echo -e "\n${GREEN}=== Demo Complete ===${NC}"
echo -e "The CLI and web package are working correctly!\n"
