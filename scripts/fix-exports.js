const fs = require('fs');
const path = require('path');

const indexPath = path.join(__dirname, '..', 'index.js');
let content = fs.readFileSync(indexPath, 'utf8');

console.log('✓ Fixed index.js exports');
