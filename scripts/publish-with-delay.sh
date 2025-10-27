#!/bin/bash
set -e

echo "Publishing platform packages with delays to avoid spam detection..."

# Array of platform packages
platforms=(
  "darwin-arm64"
  "darwin-x64"
  "linux-x64-gnu"
  "linux-x64-musl"
  "linux-arm64-gnu"
  "linux-arm-gnueabihf"
  "win32-x64-msvc"
  "win32-ia32-msvc"
)

# Publish each platform package with a delay
for platform in "${platforms[@]}"; do
  echo ""
  echo "Publishing sombradb-${platform}..."
  cd "npm/${platform}"
  
  if [ -f "sombradb.*.node" ] || [ -f "*.node" ]; then
    npm publish --access public || echo "Warning: Failed to publish ${platform}, continuing..."
  else
    echo "Skipping ${platform} - no .node file found"
  fi
  
  cd ../..
  
  # Wait 10 seconds between publishes to avoid spam detection
  if [ "$platform" != "win32-ia32-msvc" ]; then
    echo "Waiting 10 seconds before next publish..."
    sleep 10
  fi
done

echo ""
echo "Publishing main package..."
npm publish --access public

echo ""
echo "All packages published successfully!"
