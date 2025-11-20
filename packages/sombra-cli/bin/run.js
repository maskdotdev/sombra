#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { spawn } = require('node:child_process')

const binaryName = process.platform === 'win32' ? 'sombra.exe' : 'sombra'
const binaryPath = path.resolve(__dirname, '..', 'dist', binaryName)

if (!fs.existsSync(binaryPath)) {
  console.error('sombra-cli binary is missing. Reinstall the package to download the correct build.')
  process.exit(1)
}

const child = spawn(binaryPath, process.argv.slice(2), {
  stdio: 'inherit',
  env: process.env,
  windowsHide: false
})

child.on('exit', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal)
    return
  }
  process.exit(code ?? 0)
})

child.on('error', (error) => {
  console.error('failed to launch sombra-cli binary')
  console.error(error instanceof Error ? error.message : error)
  process.exit(1)
})
