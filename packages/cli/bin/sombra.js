#!/usr/bin/env node
const { spawnSync, spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');

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

function findRustBinary() {
  // Try to find the sombra Rust binary
  // Check 1: In PATH
  const inPath = spawnSync(os.platform() === 'win32' ? 'where' : 'which', ['sombra'], { stdio: 'pipe' });
  if (inPath.status === 0) {
    return 'sombra';
  }
  
  // Check 2: In cargo bin directory
  const home = os.homedir();
  const cargoBinPath = path.join(home, '.cargo', 'bin', os.platform() === 'win32' ? 'sombra.exe' : 'sombra');
  if (fs.existsSync(cargoBinPath)) {
    return cargoBinPath;
  }
  
  // Check 3: In current directory (for dev)
  const localBinaryPath = path.join(__dirname, '..', '..', '..', 'target', 'release', os.platform() === 'win32' ? 'sombra.exe' : 'sombra');
  if (fs.existsSync(localBinaryPath)) {
    return localBinaryPath;
  }
  
  return null;
}

function delegateToRustBinary(args) {
  const binaryPath = findRustBinary();
  
  if (!binaryPath) {
    console.error('Error: Sombra CLI binary not found.');
    console.error('');
    console.error('To use inspect, repair, and verify commands, install the Rust binary:');
    console.error('');
    console.error('  cargo install sombra');
    console.error('');
    console.error('Or build from source:');
    console.error('');
    console.error('  cd /path/to/sombra');
    console.error('  cargo build --release');
    console.error('');
    process.exit(1);
  }
  
  const result = spawnSync(binaryPath, args, { stdio: 'inherit' });
  
  if (result.error) {
    console.error('Error executing sombra binary:', result.error);
    process.exit(1);
  }
  
  process.exit(result.status || 0);
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

function main() {
  const [, , subcmd, ...argv] = process.argv;
  if (!subcmd || subcmd === 'help' || subcmd === '--help' || subcmd === '-h') return printUsage();
  
  switch (subcmd) {
    case 'web':
      return cmdWeb(argv);
    case 'seed':
      return cmdSeed(argv);
    case 'inspect':
    case 'repair':
    case 'verify':
    case 'version':
      // Delegate to Rust binary for these commands
      return delegateToRustBinary([subcmd, ...argv]);
    default:
      console.error(`Unknown command: ${subcmd}`);
      printUsage();
      process.exit(1);
  }
}

main();


