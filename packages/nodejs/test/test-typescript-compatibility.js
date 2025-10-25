"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
var index_1 = require("../index");
var path = __importStar(require("path"));
var fs = __importStar(require("fs"));
function testTypeScriptCompatibility() {
    var dbPath = path.join(__dirname, 'test-ts-compatibility.db');
    if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
    }
    try {
        console.log('Testing TypeScript type compatibility...');
        var db = new index_1.SombraDB(dbPath);
        var nodeId = db.addNode(['Test'], {
            name: { type: 'string', value: 'test' }
        });
        console.log("\u2713 Can call addNode() without 'as any' cast");
        console.log("\u2713 Node ID type: ".concat(typeof nodeId));
        var node = db.getNode(nodeId);
        console.log("\u2713 Can call getNode() without 'as any' cast");
        console.log("\u2713 Node: ".concat(JSON.stringify(node, null, 2)));
        var neighbors = db.getNeighbors(nodeId);
        console.log("\u2713 Can call getNeighbors() without 'as any' cast");
        var tx = db.beginTransaction();
        console.log("\u2713 Transaction type is correct");
        var txNode = tx.addNode(['TxTest'], {
            value: { type: 'int', value: 42 }
        });
        console.log("\u2713 Can call tx.addNode() without 'as any' cast");
        tx.commit();
        console.log("\u2713 Can call tx.commit() without 'as any' cast");
        var bfsResults = db.bfsTraversal(nodeId, 2);
        console.log("\u2713 Can call bfsTraversal() without 'as any' cast");
        console.log("\u2713 BFS results type is correct: Array length ".concat(bfsResults.length));
        console.log('\n✓ All TypeScript type checks passed - no casting needed!');
        if (fs.existsSync(dbPath)) {
            fs.unlinkSync(dbPath);
        }
    }
    catch (error) {
        console.error('✗ TypeScript compatibility test failed:', error);
        if (fs.existsSync(dbPath)) {
            fs.unlinkSync(dbPath);
        }
        process.exit(1);
    }
}
testTypeScriptCompatibility();
