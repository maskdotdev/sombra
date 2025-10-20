#!/usr/bin/env node

const { spawnSync } = require('child_process');
const path = require('path');
const os = require('os');

function getBinaryPath() {
  const platform = os.platform();
  
  let binaryName = 'sombra';
  if (platform === 'win32') {
    binaryName = 'sombra.exe';
  }
  
  return path.join(__dirname, binaryName);
}

function main() {
  const binaryPath = getBinaryPath();
  const args = process.argv.slice(2);
  
  const result = spawnSync(binaryPath, args, {
    stdio: 'inherit',
    shell: false
  });
  
  if (result.error) {
    if (result.error.code === 'ENOENT') {
      console.error('Error: Sombra CLI binary not found.');
      console.error('Please install the CLI tools with: cargo install sombra');
      console.error('Or build from source with: cargo build --release');
      process.exit(1);
    }
    console.error('Error executing sombra:', result.error);
    process.exit(1);
  }
  
  process.exit(result.status || 0);
}

main();
