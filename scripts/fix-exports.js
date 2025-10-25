const fs = require('fs');
const path = require('path');

const indexJsPath = path.join(__dirname, '..', 'packages', 'nodejs', 'index.js');
const indexDtsPath = path.join(__dirname, '..', 'packages', 'nodejs', 'index.d.ts');

// Fix JavaScript exports to provide clean aliases
if (fs.existsSync(indexJsPath)) {
  let content = fs.readFileSync(indexJsPath, 'utf8');
  
  // Check if we need to add clean exports
  if (!content.includes('module.exports.DegreeDistribution =')) {
    // Add clean exports after the existing module.exports lines
    const jsExports = [
      'module.exports.DegreeDistribution = nativeBinding.JsDegreeDistribution',
      'module.exports.Subgraph = nativeBinding.JsSubgraph',
      'module.exports.Pattern = nativeBinding.JsPattern',
      'module.exports.Match = nativeBinding.JsMatch'
    ];
    
    content = content + '\n' + jsExports.join('\n') + '\n';
    fs.writeFileSync(indexJsPath, content, 'utf8');
    console.log('✓ Added clean exports to index.js');
  }
}

// Fix TypeScript definitions to provide clean type aliases
if (fs.existsSync(indexDtsPath)) {
  let content = fs.readFileSync(indexDtsPath, 'utf8');
  
  // Add clean type aliases after the Js-prefixed interfaces
  const aliases = [
    'export type DegreeDistribution = JsDegreeDistribution',
    'export type Subgraph = JsSubgraph', 
    'export type Pattern = JsPattern',
    'export type Match = JsMatch'
  ];
  
  // Add after the last interface definition, before end of file
  const lastInterfaceMatch = content.lastIndexOf('\n}');
  if (lastInterfaceMatch !== -1) {
    const insertPoint = content.indexOf('\n', lastInterfaceMatch + 1);
    const before = content.substring(0, insertPoint);
    const after = content.substring(insertPoint);
    
    // Check if aliases already exist
    if (!content.includes('export type DegreeDistribution =')) {
      content = before + '\n\n' + aliases.join('\n') + after;
      fs.writeFileSync(indexDtsPath, content, 'utf8');
      console.log('✓ Added clean type aliases to index.d.ts');
    }
  }
}
