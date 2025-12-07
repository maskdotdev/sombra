#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const fsp = require('node:fs/promises')
const os = require('node:os')
const path = require('node:path')
const { Readable } = require('node:stream')
const { pipeline } = require('node:stream/promises')
const tar = require('tar')

const fetchFn = globalThis.fetch

const pkg = require('../package.json')

const MATRIX = {
  'darwin-arm64': { triple: 'aarch64-apple-darwin', binaryName: 'sombra' },
  'darwin-x64': { triple: 'x86_64-apple-darwin', binaryName: 'sombra' },
  'linux-x64': { triple: 'x86_64-unknown-linux-gnu', binaryName: 'sombra' },
  'linux-arm64': { triple: 'aarch64-unknown-linux-gnu', binaryName: 'sombra' },
  'win32-x64': { triple: 'x86_64-pc-windows-msvc', binaryName: 'sombra.exe' }
}

async function main() {
  if (process.env.SOMBRA_CLI_SKIP_DOWNLOAD) {
    console.log('[sombra-cli] SOMBRA_CLI_SKIP_DOWNLOAD set, skipping binary download')
    return
  }

  // Check if binary is already bundled in dist/
  const target = resolveTarget()
  const distDir = path.resolve(__dirname, '..', 'dist')
  const bundledBinary = path.join(distDir, target.binaryName)
  if (fs.existsSync(bundledBinary)) {
    console.log(`[sombra-cli] binary already bundled at ${bundledBinary}, skipping download`)
    return
  }

  const version = process.env.SOMBRA_CLI_VERSION || pkg.version
  const url = buildDownloadUrl(version, target)

  const workDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'sombra-cli-'))
  const archivePath = path.join(workDir, 'sombra-cli.tar.gz')
  const extractDir = path.join(workDir, 'extract')

  try {
    console.log(`[sombra-cli] downloading ${url}`)
    await download(url, archivePath)

    await fsp.mkdir(extractDir)
    await tar.x({ file: archivePath, cwd: extractDir })

    const binaryPath = await findBinary(extractDir, target.binaryName)
    if (!binaryPath) {
      throw new Error(`unable to locate ${target.binaryName} inside extracted archive`)
    }

    const distDir = path.resolve(__dirname, '..', 'dist')
    await fsp.mkdir(distDir, { recursive: true })
    const destination = path.join(distDir, target.binaryName)

    await fsp.copyFile(binaryPath, destination)
    if (process.platform !== 'win32') {
      await fsp.chmod(destination, 0o755)
    }

    console.log(`[sombra-cli] installed ${target.triple} binary -> ${destination}`)
  } catch (error) {
    console.error('[sombra-cli] failed to install binary')
    console.error(error instanceof Error ? error.message : error)
    process.exitCode = 1
  } finally {
    await cleanup(workDir)
  }
}

function resolveTarget() {
  const key = `${process.platform}-${process.arch}`
  const target = MATRIX[key]
  if (!target) {
    const supported = Object.keys(MATRIX).join(', ')
    throw new Error(`unsupported platform (${key}). Supported targets: ${supported}`)
  }
  return target
}

function buildDownloadUrl(version, target) {
  const repo = process.env.SOMBRA_CLI_REPO || 'maskdotdev/sombra-db'
  const tag = process.env.SOMBRA_CLI_TAG || `v${version}`
  const assetName = process.env.SOMBRA_CLI_ASSET || `sombra-cli-v${version}-${target.triple}.tar.gz`
  const base = process.env.SOMBRA_CLI_BASE_URL || `https://github.com/${repo}/releases/download/${tag}`
  return process.env.SOMBRA_CLI_DOWNLOAD_URL || `${base}/${assetName}`
}

async function download(url, destination) {
  if (typeof fetchFn !== 'function') {
    throw new Error('global fetch is unavailable. Node.js 18+ is required to install sombra-cli.')
  }

  const response = await fetchFn(url)
  if (!response.ok || !response.body) {
    throw new Error(`download failed (${response.status} ${response.statusText})`)
  }

  const nodeStream = Readable.fromWeb(response.body)
  await pipeline(nodeStream, fs.createWriteStream(destination))
}

async function findBinary(root, expectedName) {
  const queue = [root]
  while (queue.length > 0) {
    const dir = queue.pop()
    const entries = await fsp.readdir(dir, { withFileTypes: true })
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name)
      if (entry.isFile() && entry.name === expectedName) {
        return fullPath
      }
      if (entry.isDirectory()) {
        queue.push(fullPath)
      }
    }
  }
  return null
}

async function cleanup(dir) {
  try {
    await fsp.rm(dir, { recursive: true, force: true })
  } catch {
    // best effort (directory lives in tmp)
  }
}

main().catch((error) => {
  console.error('[sombra-cli] unexpected failure')
  console.error(error instanceof Error ? error.stack : error)
  process.exitCode = 1
})
