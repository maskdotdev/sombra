const { SombraDB } = require('../index');
import type { SombraNode, SombraEdge, SombraPropertyValue } from '../sombra';

const db = new SombraDB('./example-ts.db');

const alice: number = db.addNode(['Person'], {
  name: { type: 'string', value: 'Alice' } as SombraPropertyValue,
  age: { type: 'int', value: 30 } as SombraPropertyValue
});

const bob: number = db.addNode(['Person'], {
  name: { type: 'string', value: 'Bob' } as SombraPropertyValue,
  age: { type: 'int', value: 25 } as SombraPropertyValue
});

const knows: number = db.addEdge(alice, bob, 'KNOWS', {
  since: { type: 'int', value: 2020 } as SombraPropertyValue
});

const aliceNode: SombraNode = db.getNode(alice);
console.log('Alice:', aliceNode);

const knowsEdge: SombraEdge = db.getEdge(knows);
console.log('Knows edge:', knowsEdge);

const outgoing: number[] = db.getOutgoingEdges(alice);
console.log('Outgoing edges:', outgoing);

const tx = db.beginTransaction();
console.log('Transaction ID:', tx.id());

const charlie: number = tx.addNode(['Person'], {
  name: { type: 'string', value: 'Charlie' } as SombraPropertyValue
});

tx.addEdge(alice, charlie, 'KNOWS');
tx.commit();

console.log('Charlie:', db.getNode(charlie));

db.flush();
db.checkpoint();
