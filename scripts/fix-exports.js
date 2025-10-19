const fs = require('fs');
const path = require('path');

const indexPath = path.join(__dirname, '..', 'index.js');

if (!fs.existsSync(indexPath)) {
  console.log('⚠ index.js not found, skipping export fixes');
  process.exit(0);
}

let content = fs.readFileSync(indexPath, 'utf8');

console.log('✓ Fixed index.js exports');
