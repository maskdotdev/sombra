import {
	SombraDB,
	type SombraEdge,
	type SombraNode,
	type SombraPropertyValue,
} from "../index";

const db = new SombraDB("./example-ts.db");

const createProp = (
	type: "string" | "int" | "float" | "bool",
	value: string | number | boolean,
): SombraPropertyValue => ({
	type,
	value,
});

const alice: number = db.addNode(["Person"], {
	name: createProp("string", "Alice"),
	age: createProp("int", 30),
});

const bob: number = db.addNode(["Person"], {
	name: createProp("string", "Bob"),
	age: createProp("int", 25),
});

const knows: number = db.addEdge(alice, bob, "KNOWS", {
	since: createProp("int", 2020),
});

const aliceNode: SombraNode = db.getNode(alice);
console.log("Alice:", aliceNode);

const knowsEdge: SombraEdge = db.getEdge(knows);
console.log("Knows edge:", knowsEdge);

const outgoing: number[] = db.getOutgoingEdges(alice);
console.log("Outgoing edges:", outgoing);

const neighbors: number[] = db.getNeighbors(alice);
console.log("Neighbors:", neighbors);

const bfsResults = db.bfsTraversal(alice, 2);
console.log("BFS traversal:", bfsResults);

const tx = db.beginTransaction();
console.log("Transaction ID:", tx.id());

try {
	const charlie: number = tx.addNode(["Person"], {
		name: createProp("string", "Charlie"),
	});

	tx.addEdge(alice, charlie, "KNOWS");
	tx.commit();

	console.log("Charlie:", db.getNode(charlie));
} catch (error) {
	console.error("Transaction failed:", error);
	tx.rollback();
}

db.flush();
db.checkpoint();
