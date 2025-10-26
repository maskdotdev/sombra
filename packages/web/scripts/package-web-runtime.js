#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function copyRecursive(src, dest) {
  if (!fs.existsSync(src)) return;
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    ensureDir(dest);
    for (const entry of fs.readdirSync(src)) {
      copyRecursive(path.join(src, entry), path.join(dest, entry));
    }
  } else {
    ensureDir(path.dirname(dest));
    fs.copyFileSync(src, dest);
  }
}

function writeStartScript(destDir) {
  const startJs = `#!/usr/bin/env node
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
`;
  fs.writeFileSync(path.join(destDir, 'start.js'), startJs);
}

function main() {
  const root = path.join(__dirname, '..');
  const dist = path.join(root, 'dist-npm');
  const nextDir = path.join(root, '.next');
  const standalone = path.join(nextDir, 'standalone');
  const staticDir = path.join(nextDir, 'static');
  const publicDir = path.join(root, 'public');

  ensureDir(dist);
  writeStartScript(dist);

  // Copy Next standalone server
  const standaloneOut = path.join(dist, '.next', 'standalone');
  copyRecursive(standalone, standaloneOut);
  
  // Copy static assets into standalone directory where Next.js expects them
  copyRecursive(staticDir, path.join(standaloneOut, '.next', 'static'));
  copyRecursive(publicDir, path.join(standaloneOut, 'public'));

  // Copy README and LICENSE if present
  for (const file of ['README.md', 'LICENSE']) {
    const src = path.join(root, file);
    if (fs.existsSync(src)) {
      copyRecursive(src, path.join(dist, file));
    }
  }
}

main();


