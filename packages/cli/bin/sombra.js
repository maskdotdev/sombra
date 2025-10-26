#!/usr/bin/env node
const { spawnSync, spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');

function printUsage() {
  console.log(`Sombra CLI\n\nUsage:\n  sombra <command> [options]\n\nCommands:\n  web           Start the Sombra web UI\n  help          Show this help\n\nRun 'sombra <command> --help' for more information on a command.`);
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
    const p = require.resolve('@sombra/web/package.json');
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
  
  const marker = path.join(target, 'node_modules', '@sombra', 'web', 'package.json');
  if (fs.existsSync(marker)) return path.dirname(marker);
  
  fs.mkdirSync(target, { recursive: true });
  
  // Determine install spec
  const installSpec = isFilePath ? version : `@sombra/web@${version || 'latest'}`;
  
  // Install to cache directory
  const r2 = spawnSync('npm', ['i', installSpec], { cwd: target, stdio: 'inherit' });
  if (r2.status !== 0) {
    console.error('Failed to install @sombra/web');
    process.exit(1);
  }
  
  // Installed under node_modules/@sombra/web
  const installedDir = path.join(target, 'node_modules', '@sombra', 'web');
  if (fs.existsSync(installedDir)) return installedDir;
  return target; // last resort
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
    console.log('@sombra/web installed to cache.');
    process.exit(0);
  }

  const startJs = path.join(webDir, 'dist-npm', 'start.js');
  const binStart = path.join(webDir, 'dist-npm', 'start.js');
  const entry = fs.existsSync(startJs) ? startJs : (fs.existsSync(binStart) ? binStart : null);
  if (!entry) {
    console.error('Could not locate @sombra/web runtime.');
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
    case 'web': return cmdWeb(argv);
    default:
      console.error(`Unknown command: ${subcmd}`);
      printUsage();
      process.exit(1);
  }
}

main();


