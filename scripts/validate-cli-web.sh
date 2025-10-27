#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== SombraDB CLI & Web Validation ===${NC}\n"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
CLI_DIR="$ROOT_DIR/packages/cli"
WEB_DIR="$ROOT_DIR/packages/web"
TEST_DB="$ROOT_DIR/test-cli-validation.db"

# Track test results
PASSED=0
FAILED=0

test_step() {
    echo -e "${YELLOW}Testing: $1${NC}"
}

test_pass() {
    echo -e "${GREEN}✓ PASSED: $1${NC}\n"
    PASSED=$((PASSED + 1))
}

test_fail() {
    echo -e "${RED}✗ FAILED: $1${NC}\n"
    FAILED=$((FAILED + 1))
}

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    # Kill any running node processes from our tests on specific ports
    lsof -ti:13579 2>/dev/null | xargs kill -9 2>/dev/null || true
    lsof -ti:13580 2>/dev/null | xargs kill -9 2>/dev/null || true
    pkill -f "dist-npm/start.js" 2>/dev/null || true
    pkill -f "sombra-web" 2>/dev/null || true
    pkill -f "next start" 2>/dev/null || true
    sleep 1
    # Remove test database
    rm -f "$TEST_DB" "$TEST_DB-shm" "$TEST_DB-wal"
}

trap cleanup EXIT

# Test 1: CLI package structure
test_step "CLI package structure"
if [ -f "$CLI_DIR/package.json" ] && [ -f "$CLI_DIR/bin/sombra.js" ]; then
    test_pass "CLI package structure is correct"
else
    test_fail "CLI package structure is missing files"
fi

# Test 2: CLI executable permissions
test_step "CLI executable permissions"
if [ -x "$CLI_DIR/bin/sombra.js" ]; then
    test_pass "CLI script is executable"
else
    chmod +x "$CLI_DIR/bin/sombra.js"
    test_pass "CLI script made executable"
fi

# Test 3: CLI help command
test_step "CLI help command"
if node "$CLI_DIR/bin/sombra.js" --help | grep -q "Sombra CLI"; then
    test_pass "CLI help command works"
else
    test_fail "CLI help command failed"
fi

# Test 4: CLI web help command
test_step "CLI web help command"
if node "$CLI_DIR/bin/sombra.js" web --help | grep -q "sombra web"; then
    test_pass "CLI web help command works"
else
    test_fail "CLI web help command failed"
fi

# Test 5: Web package structure
test_step "Web package structure"
if [ -f "$WEB_DIR/package.json" ] && [ -d "$WEB_DIR/app" ] && [ -d "$WEB_DIR/components" ]; then
    test_pass "Web package structure is correct"
else
    test_fail "Web package structure is missing files"
fi

# Test 6: Web package dependencies
test_step "Web package dependencies"
cd "$WEB_DIR"
if [ -d "node_modules" ] && [ -f "node_modules/next/package.json" ]; then
    test_pass "Web package dependencies are installed"
else
    echo -e "${YELLOW}Installing web dependencies...${NC}"
    npm install
    if [ -d "node_modules" ]; then
        test_pass "Web package dependencies installed successfully"
    else
        test_fail "Failed to install web package dependencies"
    fi
fi

# Test 7: Web build process
test_step "Web build process"
cd "$WEB_DIR"
if npm run build > /tmp/web-build.log 2>&1; then
    test_pass "Web package builds successfully"
else
    echo -e "${RED}Build output:${NC}"
    cat /tmp/web-build.log
    test_fail "Web package build failed"
fi

# Test 8: Web dist-npm structure
test_step "Web dist-npm structure"
if [ -f "$WEB_DIR/dist-npm/start.js" ] && [ -d "$WEB_DIR/dist-npm/.next" ]; then
    test_pass "Web dist-npm structure is correct"
else
    test_fail "Web dist-npm structure is missing files"
fi

# Test 9: Web standalone server script
test_step "Web standalone server script"
if [ -x "$WEB_DIR/dist-npm/start.js" ]; then
    test_pass "Web start script is executable"
else
    chmod +x "$WEB_DIR/dist-npm/start.js"
    test_pass "Web start script made executable"
fi

# Test 10: Create test database
test_step "Creating test database"
cat > /tmp/create-test-db.js <<'EOF'
const { SombraDB } = require('@unyth/sombra');
const db = new SombraDB(process.argv[2]);
// Create some test data using typed API (without schema)
const n1 = db.addNode(['Person'], { name: 'Alice', age: 30 });
const n2 = db.addNode(['Person'], { name: 'Bob', age: 25 });
const n3 = db.addNode(['City'], { name: 'New York', population: 8000000 });
db.addEdge(n1, n2, 'KNOWS', { since: 2020 });
db.addEdge(n1, n3, 'LIVES_IN', { years: 5 });
db.flush();
console.log('Test database created');
EOF

cd "$WEB_DIR"
if NODE_PATH="$WEB_DIR/node_modules" node /tmp/create-test-db.js "$TEST_DB" > /tmp/db-create.log 2>&1; then
    test_pass "Test database created successfully"
else
    echo -e "${RED}Database creation output:${NC}"
    cat /tmp/db-create.log
    test_fail "Failed to create test database"
fi
cd "$ROOT_DIR"

# Test 11: Web server can start (quick test)
test_step "Web server startup"
cd "$WEB_DIR"
PORT=13579 SOMBRA_DB_PATH="$TEST_DB" timeout 10s node dist-npm/start.js > /tmp/web-server.log 2>&1 &
SERVER_PID=$!
sleep 5

if ps -p $SERVER_PID > /dev/null; then
    test_pass "Web server started successfully"
    kill $SERVER_PID 2>/dev/null || true
else
    echo -e "${RED}Server output:${NC}"
    cat /tmp/web-server.log
    test_fail "Web server failed to start"
fi

# Test 12: API endpoint tests (if server started)
test_step "API endpoints availability"
cd "$WEB_DIR"
PORT=13580 SOMBRA_DB_PATH="$TEST_DB" node dist-npm/start.js > /tmp/web-api-test.log 2>&1 &
API_SERVER_PID=$!
sleep 8

# Check if server is running and responding
SERVER_READY=false
for i in {1..10}; do
    if curl -s "http://localhost:13580" > /dev/null 2>&1; then
        SERVER_READY=true
        break
    fi
    sleep 1
done

if [ "$SERVER_READY" = true ] && ps -p $API_SERVER_PID > /dev/null 2>&1; then
    # Test stats endpoint
    if curl -s "http://localhost:13580/api/graph/stats" | grep -q "nodeCount"; then
        test_pass "API stats endpoint works"
    else
        test_fail "API stats endpoint failed"
    fi
    
    # Test nodes endpoint
    if curl -s "http://localhost:13580/api/graph/nodes" | grep -q "nodes"; then
        test_pass "API nodes endpoint works"
    else
        test_fail "API nodes endpoint failed"
    fi
    
    # Test edges endpoint
    if curl -s "http://localhost:13580/api/graph/edges" | grep -q "edges"; then
        test_pass "API edges endpoint works"
    else
        test_fail "API edges endpoint failed"
    fi
    
    kill $API_SERVER_PID 2>/dev/null || true
    wait $API_SERVER_PID 2>/dev/null || true
else
    echo -e "${RED}API server logs:${NC}"
    cat /tmp/web-api-test.log 2>&1 | tail -30
    test_fail "API server failed to start for endpoint tests"
    kill $API_SERVER_PID 2>/dev/null || true
fi

# Test 13: Web runtime packaging script
test_step "Web runtime packaging script"
if [ -f "$WEB_DIR/scripts/package-web-runtime.js" ]; then
    test_pass "Web runtime packaging script exists"
else
    test_fail "Web runtime packaging script is missing"
fi

# Test 14: Next.js configuration
test_step "Next.js configuration"
if [ -f "$WEB_DIR/next.config.ts" ]; then
    if grep -q "standalone" "$WEB_DIR/next.config.ts"; then
        test_pass "Next.js standalone output is configured"
    else
        test_fail "Next.js standalone output is not configured"
    fi
else
    test_fail "Next.js configuration file is missing"
fi

# Test 15: TypeScript configuration
test_step "TypeScript configuration"
if [ -f "$WEB_DIR/tsconfig.json" ]; then
    test_pass "TypeScript configuration exists"
else
    test_fail "TypeScript configuration is missing"
fi

# Test 16: Essential web components
test_step "Essential web components"
MISSING_COMPONENTS=()
[ ! -f "$WEB_DIR/components/graph-explorer.tsx" ] && MISSING_COMPONENTS+=("graph-explorer.tsx")
[ ! -f "$WEB_DIR/components/database-selector.tsx" ] && MISSING_COMPONENTS+=("database-selector.tsx")
[ ! -f "$WEB_DIR/lib/db.ts" ] && MISSING_COMPONENTS+=("lib/db.ts")

if [ ${#MISSING_COMPONENTS[@]} -eq 0 ]; then
    test_pass "All essential web components exist"
else
    test_fail "Missing web components: ${MISSING_COMPONENTS[*]}"
fi

# Test 17: API routes
test_step "API routes"
MISSING_ROUTES=()
[ ! -f "$WEB_DIR/app/api/graph/nodes/route.ts" ] && MISSING_ROUTES+=("nodes/route.ts")
[ ! -f "$WEB_DIR/app/api/graph/edges/route.ts" ] && MISSING_ROUTES+=("edges/route.ts")
[ ! -f "$WEB_DIR/app/api/graph/stats/route.ts" ] && MISSING_ROUTES+=("stats/route.ts")
[ ! -f "$WEB_DIR/app/api/graph/traverse/route.ts" ] && MISSING_ROUTES+=("traverse/route.ts")

if [ ${#MISSING_ROUTES[@]} -eq 0 ]; then
    test_pass "All API routes exist"
else
    test_fail "Missing API routes: ${MISSING_ROUTES[*]}"
fi

# Test 18: Check SombraDB dependency
test_step "SombraDB dependency"
cd "$WEB_DIR"
if npm list @unyth/sombra > /dev/null 2>&1; then
    test_pass "SombraDB is properly installed as dependency"
else
    test_fail "SombraDB dependency issue"
fi

# Summary
echo -e "\n${YELLOW}=== Validation Summary ===${NC}"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"

if [ $FAILED -eq 0 ]; then
    echo -e "\n${GREEN}✓ All validations passed!${NC}"
    exit 0
else
    echo -e "\n${RED}✗ Some validations failed.${NC}"
    exit 1
fi
