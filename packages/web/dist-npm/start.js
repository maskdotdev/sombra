#!/usr/bin/env node
const { spawn } = require('child_process');
const path = require('path');

function getArg(name) {
  const i = process.argv.indexOf(name);
  return i !== -1 ? process.argv[i + 1] : undefined;
}

const port = getArg('--port') || process.env.PORT || 3000;
const db = getArg('--db') || process.env.SOMBRA_DB_PATH;

const env = { ...process.env, PORT: String(port) };
if (db) env.SOMBRA_DB_PATH = db;

const standaloneDir = path.join(__dirname, '.next', 'standalone');
const serverJs = path.join(standaloneDir, 'server.js');
const child = spawn(process.execPath, [serverJs], { 
  stdio: 'inherit', 
  env,
  cwd: standaloneDir
});
child.on('exit', (code) => process.exit(code ?? 0));
