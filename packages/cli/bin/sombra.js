#!/usr/bin/env node
const { spawnSync, spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');
const readline = require('readline');

function printUsage() {
  console.log(`Sombra CLI

Usage:
  sombra <command> [options]

Commands:
  web           Start the Sombra web UI
  seed          Create a demo database with sample data
  inspect       Inspect database information and statistics
  repair        Perform maintenance and repair operations
  verify        Run comprehensive integrity verification
  version       Show version information
  help          Show this help

Run 'sombra <command> --help' for more information on a command.`);
}

function openBrowser(url) {
  const platform = os.platform();
  let cmd, args;
  if (platform === 'darwin') { cmd = 'open'; args = [url]; }
  else if (platform === 'win32') { cmd = 'cmd'; args = ['/c', 'start', '""', url]; }
  else { cmd = 'xdg-open'; args = [url]; }
  const r = spawn(cmd, args, { stdio: 'ignore', detached: true });
  r.unref();
}

function getCacheDir() {
  const platform = os.platform();
  const home = os.homedir();
  if (platform === 'darwin') return path.join(home, 'Library', 'Caches', 'sombra', 'web');
  if (platform === 'win32') return path.join(process.env.LOCALAPPDATA || path.join(home, 'AppData', 'Local'), 'sombra', 'web');
  return path.join(process.env.XDG_CACHE_HOME || path.join(home, '.cache'), 'sombra', 'web');
}

function resolveLocalSombraWeb() {
  try {
    const p = require.resolve('sombra-web/package.json');
    return path.dirname(p);
  } catch (_) {
    return null;
  }
}

function ensureSombraWebInstalled(version) {
  const local = resolveLocalSombraWeb();
  if (local) return local;
  const cacheDir = getCacheDir();
  
  // Handle file:// or absolute path versions
  const isFilePath = version && (version.startsWith('file:') || version.startsWith('/') || version.startsWith('.'));
  const targetName = isFilePath ? version.replace(/[^a-zA-Z0-9.-]/g, '_') : (version || 'latest');
  const target = path.join(cacheDir, targetName);
  
  const marker = path.join(target, 'node_modules', 'sombra-web', 'package.json');
  if (fs.existsSync(marker)) return path.dirname(marker);
  
  fs.mkdirSync(target, { recursive: true });
  
  // Determine install spec
  const installSpec = isFilePath ? version : `sombra-web@${version || 'latest'}`;
  
  // Install to cache directory
  // Use --force to ensure optional dependencies (native bindings) are properly installed
  // This works around npm bug with optional dependencies: https://github.com/npm/cli/issues/4828
  const r2 = spawnSync('npm', ['i', installSpec, '--force'], { cwd: target, stdio: 'inherit' });
  if (r2.status !== 0) {
    console.error('Failed to install sombra-web');
    process.exit(1);
  }
  
  // Installed under node_modules/sombra-web
  const installedDir = path.join(target, 'node_modules', 'sombra-web');
  if (fs.existsSync(installedDir)) return installedDir;
  return target; // last resort
}

function cmdSeed(argv) {
  const help = argv.includes('--help') || argv.includes('-h');
  if (help) {
    console.log(`Usage: sombra seed [database-path]

Create a demo database with sample data for testing and exploration.

Arguments:
  database-path    Path for the new database (default: ./demo.db)

Example:
  sombra seed demo.db
  sombra web demo.db
`);
    process.exit(0);
  }

  // Ensure sombra-web is installed (needed for seed script)
  let webDir = resolveLocalSombraWeb();
  if (!webDir) {
    console.log('Installing sombra-web (needed for seeding)...');
    webDir = ensureSombraWebInstalled();
  }

  const seedScript = path.join(webDir, 'scripts', 'seed-demo.js');
  if (!fs.existsSync(seedScript)) {
    console.error('Error: seed-demo.js not found in sombra-web package.');
    console.error('Try updating: sombra web --update');
    process.exit(1);
  }

  const dbPath = argv[0] || './demo.db';
  
  console.log(`Creating demo database: ${dbPath}`);
  const result = spawnSync(process.execPath, [seedScript, dbPath], { stdio: 'inherit' });
  
  if (result.error) {
    console.error('Error running seed script:', result.error);
    process.exit(1);
  }
  
  process.exit(result.status || 0);
}

function cmdWeb(argv) {
  const help = argv.includes('--help') || argv.includes('-h');
  if (help) {
    console.log(`Usage: sombra web [--db <path>] [--port <port>] [--open] [--no-open] [--update]\n`);
    process.exit(0);
  }
  const getArg = (name) => { const i = argv.indexOf(name); return i !== -1 ? argv[i + 1] : undefined; };
  const port = getArg('--port') || process.env.PORT || '3000';
  const db = getArg('--db') || process.env.SOMBRA_DB_PATH;
  const shouldOpen = argv.includes('--open') || (!argv.includes('--no-open'));
  const version = getArg('--version-pin');
  const update = argv.includes('--update');
  const preinstall = argv.includes('--install');

  let webDir = resolveLocalSombraWeb();
  if (!webDir || update) {
    webDir = ensureSombraWebInstalled(version);
  }

  if (preinstall) {
    console.log('sombra-web installed to cache.');
    process.exit(0);
  }

  const startJs = path.join(webDir, 'dist-npm', 'start.js');
  const binStart = path.join(webDir, 'dist-npm', 'start.js');
  const entry = fs.existsSync(startJs) ? startJs : (fs.existsSync(binStart) ? binStart : null);
  if (!entry) {
    console.error('Could not locate sombra-web runtime.');
    process.exit(1);
  }

  const env = { ...process.env, PORT: String(port) };
  if (db) env.SOMBRA_DB_PATH = db;
  const child = spawn(process.execPath, [entry, '--port', String(port)].concat(db ? ['--db', db] : []), { stdio: 'inherit', env });
  child.on('spawn', () => {
    if (shouldOpen) {
      const url = `http://localhost:${port}`;
      openBrowser(url);
      console.log(`Sombra web running at ${url}`);
    }
  });
  child.on('exit', (code) => process.exit(code ?? 0));
}

// Utility functions for formatting
function formatBytes(bytes) {
  const KB = 1024;
  const MB = KB * 1024;
  const GB = MB * 1024;

  if (bytes >= GB) {
    return `${(bytes / GB).toFixed(2)} GB`;
  } else if (bytes >= MB) {
    return `${(bytes / MB).toFixed(2)} MB`;
  } else if (bytes >= KB) {
    return `${(bytes / KB).toFixed(2)} KB`;
  } else {
    return `${bytes} B`;
  }
}

function printHeader(title) {
  const width = 60;
  const padding = Math.floor((width - title.length - 2) / 2);
  console.log();
  console.log('╔' + '═'.repeat(width) + '╗');
  console.log('║' + ' '.repeat(padding) + title + ' '.repeat(width - padding - title.length) + '║');
  console.log('╚' + '═'.repeat(width) + '╝');
  console.log();
}

function printSection(title) {
  console.log();
  console.log('─── ' + title + ' ' + '─'.repeat(55 - title.length));
}

function printField(name, value) {
  const dots = '.'.repeat(Math.max(1, 30 - name.length));
  console.log(`  ${name}${dots} ${value}`);
}

// Load sombradb with error handling
function loadSombraDB() {
  try {
    return require('sombradb');
  } catch (err) {
    console.error('Error: sombradb package not found or failed to load.');
    console.error('');
    console.error('To use inspect, repair, and verify commands, install sombradb:');
    console.error('');
    console.error('  npm install sombradb');
    console.error('');
    console.error('Or if using the CLI globally:');
    console.error('');
    console.error('  npm install -g sombra');
    console.error('');
    process.exit(1);
  }
}

function cmdInspectInfo(dbPath) {
  printHeader('DATABASE INFO');

  const { SombraDB, getDefaultPageSize } = loadSombraDB();
  const db = new SombraDB(dbPath);

  const fileSize = fs.statSync(dbPath).size;
  const pageSize = getDefaultPageSize();
  const header = db.getHeader();

  printSection('General');
  printField('Database Path', dbPath);
  printField('File Size', formatBytes(fileSize));
  printField('Page Size', `${pageSize} bytes`);

  printSection('Graph Statistics');
  printField('Total Nodes', header.nextNodeId);
  printField('Total Edges', header.nextEdgeId);

  printSection('Storage');
  if (header.freePageHead !== undefined && header.freePageHead !== null) {
    printField('Free Page List Head', header.freePageHead);
  } else {
    printField('Free Page List Head', 'None');
  }
  if (header.lastRecordPage !== undefined && header.lastRecordPage !== null) {
    printField('Last Record Page', header.lastRecordPage);
  } else {
    printField('Last Record Page', 'None');
  }

  printSection('Index');
  if (header.btreeIndexPage !== undefined && header.btreeIndexPage !== null) {
    printField('BTree Index Root Page', header.btreeIndexPage);
    printField('BTree Index Size', `${header.btreeIndexSize} entries`);
  } else {
    printField('BTree Index', 'Not initialized');
  }

  printSection('Transactions');
  printField('Last Committed TX ID', header.lastCommittedTxId);

  console.log();
  console.log('✓ Database opened successfully');
  console.log();
}

function cmdInspectStats(dbPath) {
  printHeader('DATABASE STATISTICS');

  const { SombraDB } = loadSombraDB();
  const db = new SombraDB(dbPath);
  const metrics = db.getMetrics();

  printSection('Performance Metrics');
  printField('Cache Hits', metrics.cacheHits);
  printField('Cache Misses', metrics.cacheMisses);

  const totalAccesses = metrics.cacheHits + metrics.cacheMisses;
  if (totalAccesses > 0) {
    const hitRate = (metrics.cacheHits / totalAccesses) * 100.0;
    printField('Cache Hit Rate', `${hitRate.toFixed(2)}%`);
  }

  printField('Node Lookups', metrics.nodeLookups);
  printField('Edge Traversals', metrics.edgeTraversals);

  printSection('Write-Ahead Log');
  printField('WAL Bytes Written', formatBytes(metrics.walBytesWritten));
  printField('WAL Syncs', metrics.walSyncs);
  printField('Checkpoints', metrics.checkpointsPerformed);
  printField('Page Evictions', metrics.pageEvictions);

  printSection('Transactions');
  printField('Transactions Committed', metrics.transactionsCommitted);
  printField('Transactions Rolled Back', metrics.transactionsRolledBack);

  console.log();
}

function cmdInspectVerify(dbPath) {
  printHeader('INTEGRITY VERIFICATION');

  const { SombraDB } = loadSombraDB();
  const db = new SombraDB(dbPath);

  console.log('  Running integrity checks...');
  console.log();

  const options = {
    checksumOnly: false,
    maxErrors: 100,
    verifyIndexes: true,
    verifyAdjacency: true,
  };

  const report = db.verifyIntegrity(options);

  printSection('Verification Results');
  printField('Pages Checked', report.checkedPages);
  printField('Checksum Failures', report.checksumFailures);
  printField('Record Errors', report.recordErrors);
  printField('Index Errors', report.indexErrors);
  printField('Adjacency Errors', report.adjacencyErrors);

  const totalErrors = report.checksumFailures + report.recordErrors + report.indexErrors + report.adjacencyErrors;

  console.log();

  if (totalErrors === 0) {
    console.log('  ✓ No issues found - database is healthy!');
    console.log();
    console.log('  Status: PASS');
  } else {
    console.log(`  ✗ Found ${totalErrors} issue(s)`);

    if (report.errors && report.errors.length > 0) {
      printSection('Error Details');
      for (let i = 0; i < report.errors.length; i++) {
        console.log(`  ${i + 1}. ${report.errors[i]}`);
      }
    }

    console.log();
    console.log('  Status: FAIL');
  }

  console.log();
}

function cmdInspectHeader(dbPath) {
  printHeader('RAW HEADER CONTENTS');

  const { SombraDB } = loadSombraDB();
  const db = new SombraDB(dbPath);
  const header = db.getHeader();

  printSection('Header Fields');
  printField('next_node_id', header.nextNodeId);
  printField('next_edge_id', header.nextEdgeId);
  printField('free_page_head', header.freePageHead !== undefined ? header.freePageHead : 'None');
  printField('last_record_page', header.lastRecordPage !== undefined ? header.lastRecordPage : 'None');
  printField('last_committed_tx_id', header.lastCommittedTxId);
  printField('btree_index_page', header.btreeIndexPage !== undefined ? header.btreeIndexPage : 'None');
  printField('btree_index_size', header.btreeIndexSize);

  console.log();
}

function cmdInspectWalInfo(dbPath) {
  printHeader('WAL INFORMATION');

  const walPath = `${dbPath}-wal`;

  try {
    const stats = fs.statSync(walPath);
    const size = stats.size;

    printSection('WAL Status');
    printField('WAL File', walPath);
    printField('WAL Size', formatBytes(size));
    printField('Status', 'Active');

    if (size === 0) {
      console.log();
      console.log('  ℹ WAL file exists but is empty (clean state)');
    } else {
      const frameSize = 4096 + 24;
      const estimatedFrames = Math.floor(size / frameSize);
      printField('Estimated Frames', estimatedFrames);

      console.log();
      console.log('  ⚠ WAL contains uncommitted changes');
      console.log('    Run checkpoint to merge changes into main database');
    }
  } catch (err) {
    printSection('WAL Status');
    printField('WAL File', 'Not found');
    printField('Status', 'No active WAL');

    console.log();
    console.log('  ✓ Database is in clean state (no WAL)');
  }

  console.log();
}

function cmdInspect(argv) {
  if (argv.length < 2 || argv.includes('--help') || argv.includes('-h')) {
    console.log(`┌─────────────────────────────────────────────┐
│         Sombra Database Inspector           │
└─────────────────────────────────────────────┘

USAGE:
    sombra inspect <database> <command>

COMMANDS:
    info         Show database metadata
    stats        Show detailed statistics
    verify       Run integrity check
    header       Show raw header contents
    wal-info     Show WAL status

EXAMPLES:
    sombra inspect graph.db info
    sombra inspect graph.db verify
`);
    process.exit(argv.includes('--help') || argv.includes('-h') ? 0 : 1);
  }

  const dbPath = argv[0];
  const subcommand = argv[1];

  switch (subcommand) {
    case 'info':
      return cmdInspectInfo(dbPath);
    case 'stats':
      return cmdInspectStats(dbPath);
    case 'verify':
      return cmdInspectVerify(dbPath);
    case 'header':
      return cmdInspectHeader(dbPath);
    case 'wal-info':
      return cmdInspectWalInfo(dbPath);
    default:
      console.error(`Error: Unknown inspect command '${subcommand}'`);
      console.error();
      process.exit(1);
  }
}

function cmdRepairCheckpoint(dbPath) {
  printHeader('CHECKPOINT WAL');

  const walPath = `${dbPath}-wal`;

  let walSizeBefore = null;
  try {
    walSizeBefore = fs.statSync(walPath).size;
    console.log('  WAL size before: ' + formatBytes(walSizeBefore));
  } catch (err) {
    console.log('  No WAL file found');
  }

  console.log();
  console.log('  Performing checkpoint...');

  const { SombraDB } = loadSombraDB();
  const db = new SombraDB(dbPath);
  db.checkpoint();

  let walSizeAfter = null;
  try {
    walSizeAfter = fs.statSync(walPath).size;
  } catch (err) {
    // WAL removed
  }

  printSection('Results');

  if (walSizeAfter !== null) {
    console.log('  WAL size after: ' + formatBytes(walSizeAfter));

    if (walSizeBefore !== null) {
      const saved = Math.max(0, walSizeBefore - walSizeAfter);
      if (saved > 0) {
        console.log('  Space reclaimed: ' + formatBytes(saved));
      }
    }
  } else {
    console.log('  WAL file removed (clean state)');
  }

  console.log();
  console.log('  ✓ Checkpoint completed successfully');
  console.log();
}

function cmdRepairVacuum(dbPath) {
  printHeader('VACUUM DATABASE');

  const sizeBefore = fs.statSync(dbPath).size;

  console.log('  Database size before: ' + formatBytes(sizeBefore));
  console.log();
  console.log('  Compacting database...');
  console.log('  (This may take a while for large databases)');
  console.log();

  const { SombraDB } = loadSombraDB();
  const db = new SombraDB(dbPath);
  db.checkpoint();

  printSection('Results');

  const sizeAfter = fs.statSync(dbPath).size;
  console.log('  Database size after: ' + formatBytes(sizeAfter));

  if (sizeBefore > sizeAfter) {
    const saved = sizeBefore - sizeAfter;
    const percent = (saved / sizeBefore) * 100.0;
    console.log(`  Space reclaimed: ${formatBytes(saved)} (${percent.toFixed(1)}%)`);
  } else {
    console.log('  No space reclaimed (database already compact)');
  }

  console.log();
  console.log('  ✓ Vacuum completed successfully');
  console.log();
}

function askConfirmation(callback) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  rl.question('  Continue? [y/N] ', (answer) => {
    rl.close();
    callback(answer.trim().toLowerCase() === 'y');
  });
}

function cmdRepair(argv) {
  if (argv.length < 2 || argv.includes('--help') || argv.includes('-h')) {
    console.log(`┌─────────────────────────────────────────────┐
│          Sombra Database Repair             │
└─────────────────────────────────────────────┘

USAGE:
    sombra repair <database> <command> [--yes]

COMMANDS:
    checkpoint       Force WAL checkpoint
    vacuum           Compact database

OPTIONS:
    --yes            Skip confirmation prompt

EXAMPLES:
    sombra repair graph.db checkpoint
    sombra repair graph.db vacuum

WARNING:
    Always backup your database before repair!
`);
    process.exit(argv.includes('--help') || argv.includes('-h') ? 0 : 1);
  }

  const dbPath = argv[0];
  const subcommand = argv[1];
  const skipConfirm = argv.includes('--yes');

  if (!fs.existsSync(dbPath)) {
    console.log();
    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║                         ERROR                            ║');
    console.log('╚══════════════════════════════════════════════════════════╝');
    console.log();
    console.log(`  Database file not found: ${dbPath}`);
    console.log();
    process.exit(1);
  }

  const executeRepair = () => {
    switch (subcommand) {
      case 'checkpoint':
        return cmdRepairCheckpoint(dbPath);
      case 'vacuum':
        return cmdRepairVacuum(dbPath);
      default:
        console.log();
        console.error(`Error: Unknown repair command '${subcommand}'`);
        console.log();
        process.exit(1);
    }
  };

  if (skipConfirm) {
    executeRepair();
  } else {
    console.log();
    console.log('  ⚠  WARNING: Always backup your database before repair!');
    console.log();
    askConfirmation((confirmed) => {
      if (confirmed) {
        executeRepair();
      } else {
        console.log();
        console.log('  Aborted.');
        console.log();
        process.exit(0);
      }
    });
  }
}

function cmdVerify(argv) {
  let checksumOnly = false;
  let verifyIndexes = true;
  let verifyAdjacency = true;
  let maxErrors = 16;
  let dbPath = null;

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      console.log(`┌─────────────────────────────────────────────┐
│         Sombra Database Verification        │
└─────────────────────────────────────────────┘

USAGE:
    sombra verify [OPTIONS] <database>

OPTIONS:
    --checksum-only       Verify only page checksums
    --skip-indexes        Skip index consistency validation
    --skip-adjacency      Skip adjacency validation
    --max-errors=N        Limit the number of reported issues (default 16)
    -h, --help            Show this help message

EXAMPLES:
    sombra verify graph.db
    sombra verify --checksum-only graph.db
    sombra verify --max-errors=100 graph.db
`);
      process.exit(0);
    }
    if (arg === '--checksum-only') {
      checksumOnly = true;
      continue;
    }
    if (arg === '--skip-indexes') {
      verifyIndexes = false;
      continue;
    }
    if (arg === '--skip-adjacency') {
      verifyAdjacency = false;
      continue;
    }
    if (arg.startsWith('--max-errors=')) {
      maxErrors = parseInt(arg.split('=')[1], 10);
      if (isNaN(maxErrors)) {
        console.error('Error: Invalid max-errors value');
        process.exit(1);
      }
      continue;
    }
    if (!dbPath) {
      dbPath = arg;
    } else {
      console.error(`Error: unexpected extra argument: ${arg}`);
      process.exit(1);
    }
  }

  if (!dbPath) {
    console.error('Error: database path required');
    console.log();
    process.exit(1);
  }

  printHeader('INTEGRITY VERIFICATION');

  const { SombraDB } = loadSombraDB();
  const db = new SombraDB(dbPath);

  console.log('  Running integrity checks...');
  console.log();

  const options = {
    checksumOnly,
    verifyIndexes,
    verifyAdjacency,
    maxErrors,
  };

  const report = db.verifyIntegrity(options);

  printSection('Verification Results');
  printField('Pages Checked', report.checkedPages);
  printField('Checksum Failures', report.checksumFailures);
  printField('Record Errors', report.recordErrors);
  printField('Index Errors', report.indexErrors);
  printField('Adjacency Errors', report.adjacencyErrors);

  if (report.errors && report.errors.length > 0) {
    printSection('Error Details');
    for (let i = 0; i < report.errors.length; i++) {
      console.log(`  ${i + 1}. ${report.errors[i]}`);
    }
  }

  console.log();

  const totalErrors = report.checksumFailures + report.recordErrors + report.indexErrors + report.adjacencyErrors;
  if (totalErrors === 0) {
    console.log('  ✓ No issues found - database is healthy!');
    console.log();
    console.log('  Status: PASS');
    console.log();
    process.exit(0);
  } else {
    console.log('  ✗ Integrity violations detected');
    console.log();
    console.log('  Status: FAIL');
    console.log();
    process.exit(1);
  }
}

function main() {
  const [, , subcmd, ...argv] = process.argv;
  if (!subcmd || subcmd === 'help' || subcmd === '--help' || subcmd === '-h') return printUsage();
  
  switch (subcmd) {
    case 'web':
      return cmdWeb(argv);
    case 'seed':
      return cmdSeed(argv);
    case 'inspect':
      return cmdInspect(argv);
    case 'repair':
      return cmdRepair(argv);
    case 'verify':
      return cmdVerify(argv);
    case 'version':
      // Read version from package.json
      try {
        const pkg = require('../package.json');
        console.log(`sombra ${pkg.version}`);
      } catch (err) {
        console.log('sombra (version unknown)');
      }
      process.exit(0);
    default:
      console.error(`Unknown command: ${subcmd}`);
      printUsage();
      process.exit(1);
  }
}

main();
