const { SombraDB } = require("../index");

const db = new SombraDB("./example.db");

const alice = db.addNode(["Person"], {
  name: { type: "string", value: "Alice" },
  age: { type: "int", value: 30 },
});

const bob = db.addNode(["Person"], {
  name: { type: "string", value: "Bob" },
  age: { type: "int", value: 25 },
});

const knows = db.addEdge(alice, bob, "KNOWS", {
  since: { type: "int", value: 2020 },
});

console.log("Alice:", db.getNode(alice));
console.log("Bob:", db.getNode(bob));
console.log("Knows edge:", db.getEdge(knows));
console.log("Alice neighbors:", db.getNeighbors(alice));

const tx = db.beginTransaction();

const charlie = tx.addNode(["Person"], {
  name: { type: "string", value: "Charlie" },
  age: { type: "int", value: 28 },
});

tx.addEdge(alice, charlie, "KNOWS");
tx.commit();

console.log("Charlie (after commit):", db.getNode(charlie));

db.flush();
db.checkpoint();
