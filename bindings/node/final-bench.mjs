import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url)
const { Database } = require('./main.js')

const NODE_COUNT = Number.parseInt(process.env.NODES ?? "5000", 10);
const EDGE_COUNT = Number.parseInt(process.env.EDGES ?? "20000", 10);
const READ_COUNT = Number.parseInt(process.env.READS ?? "10000", 10);

function generateCodeText() {
  const functions = [
    "function foo() { return 1; }",
    "const bar = () => { console.log('hello'); };",
    "class Baz { constructor() {} }",
    "export function qux() { return true; }",
    "const quux = (x, y) => x + y;",
  ];
  return functions[Math.floor(Math.random() * functions.length)];
}

function generateMetadata() {
  const metadata = [
    '{"type": "function", "exported": true}',
    '{"type": "variable", "exported": false}',
    '{"type": "class", "exported": true}',
  ];
  return metadata[Math.floor(Math.random() * metadata.length)];
}

async function main() {
	const dir = mkdtempSync(join(tmpdir(), "sombra-bench-"));
	const dbPath = join(dir, "bench.sombra");
	console.log(`📂 temp db: ${dbPath}`);

	const db = Database.open(dbPath, {
		synchronous: 'normal',
		commitCoalesceMs: 0,
		commitMaxFrames: 16384,
		cachePages: 16384,
	});

	// Create nodes AND edges in single transaction using handles (like bulk_create.js)
	console.log('Creating nodes and edges in single transaction...');
	const start = performance.now();
	const builder = db.create();
	const handles = [];
	
	// Create nodes
	for (let i = 0; i < NODE_COUNT; i++) {
		const handle = builder.node(['Node'], {
			name: `fn_${i}`,
			filePath: `/tmp/file_${Math.floor(i / 50)}.ts`,
			startLine: i,
			endLine: i + 5,
			codeText: generateCodeText(),
			language: "typescript",
			metadata: generateMetadata(),
		});
		handles.push(handle);
	}
	const nodeTime = performance.now() - start;
	console.log(`  prepared ${NODE_COUNT} nodes: ${nodeTime.toFixed(1)} ms`);
	
	// Create edges using handles
	const edgeStart = performance.now();
	for (let i = 0; i < EDGE_COUNT; i++) {
		const src = handles[i % handles.length];
		const dst = handles[(i * 13 + 7) % handles.length];
		// Skip self loops
		if (src === dst) {
			const next = (i + 1) % handles.length;
			builder.edge(src, 'LINKS', handles[next], {
				weight: (i % 10) / 10,
				kind: i % 2 === 0 ? "call" : "reference",
			});
			continue;
		}
		builder.edge(src, 'LINKS', dst, {
			weight: (i % 10) / 10,
			kind: i % 2 === 0 ? "call" : "reference",
		});
	}
	const edgePrepTime = performance.now() - edgeStart;
	console.log(`  prepared ${EDGE_COUNT} edges: ${edgePrepTime.toFixed(1)} ms`);
	
	// Execute the transaction
	const execStart = performance.now();
	const summary = builder.execute();
	const execTime = performance.now() - execStart;
	console.log(`  executed transaction: ${execTime.toFixed(1)} ms`);
	
	const totalWriteTime = performance.now() - start;
	console.log(`create total: ${totalWriteTime.toFixed(1)} ms`);

	// Random reads - using direct node lookup (getNodeRecord)
	console.log('Running reads...');
	const readStart = performance.now();
	const actualNodeIds = summary.nodes;
	for (let i = 0; i < READ_COUNT; i++) {
		const id = actualNodeIds[(i * 17) % actualNodeIds.length];
		db.getNodeRecord(id);
	}
	const readTime = performance.now() - readStart;
	console.log(`random reads: ${readTime.toFixed(1)} ms`);

	db.close();
	rmSync(dir, { recursive: true, force: true });
	
	// Print summary
	console.log('\n📊 Benchmark Summary:');
	console.log(`- Nodes: ${NODE_COUNT} (${(NODE_COUNT / (totalWriteTime / 1000)).toFixed(0)} nodes/sec)`);
	console.log(`- Edges: ${EDGE_COUNT} (${(EDGE_COUNT / (totalWriteTime / 1000)).toFixed(0)} edges/sec)`);
	console.log(`- Reads: ${READ_COUNT} (${readTime.toFixed(1)}ms, ${(READ_COUNT / (readTime / 1000)).toFixed(0)} reads/sec)`);
	console.log(`- Total write time: ${totalWriteTime.toFixed(1)}ms`);
}

main().catch(console.error);