const { SombraDB: NativeSombraDB } = require("./index.js");

/**
 * Converts a JavaScript primitive value to SombraDB property format.
 * @param {string|number|boolean} value - The value to convert
 * @returns {{type: 'string'|'int'|'float'|'bool', value: any}} SombraDB property object
 */
function convertToSombraProperty(value) {
	if (typeof value === "string") {
		return { type: "string", value };
	} else if (typeof value === "number") {
		if (Number.isInteger(value)) {
			return { type: "int", value };
		} else {
			return { type: "float", value };
		}
	} else if (typeof value === "boolean") {
		return { type: "bool", value };
	}
	throw new Error(`Unsupported property type: ${typeof value}`);
}

/**
 * Converts a SombraDB property object to a JavaScript primitive value.
 * @param {{type: string, value: any}} sombraValue - The SombraDB property to convert
 * @returns {string|number|boolean} The primitive value
 */
function convertFromSombraProperty(sombraValue) {
	if (!sombraValue || typeof sombraValue !== "object") {
		return sombraValue;
	}
	return sombraValue.value;
}

/**
 * Converts an object of JavaScript primitive values to SombraDB property format.
 * @param {Object.<string, string|number|boolean>} properties - Properties to convert
 * @returns {Object.<string, {type: string, value: any}>} SombraDB properties
 */
function convertPropertiesToSombra(properties) {
	const result = {};
	for (const [key, value] of Object.entries(properties)) {
		result[key] = convertToSombraProperty(value);
	}
	return result;
}

/**
 * Converts SombraDB properties to JavaScript primitive values.
 * @param {Object.<string, {type: string, value: any}>} sombraProperties - SombraDB properties
 * @returns {Object.<string, string|number|boolean>} JavaScript properties
 */
function convertPropertiesFromSombra(sombraProperties) {
	const result = {};
	for (const [key, value] of Object.entries(sombraProperties)) {
		result[key] = convertFromSombraProperty(value);
	}
	return result;
}

function isTypedCall(labelOrLabels, properties) {
	// Check if it's a typed call (not raw API with {type, value} properties)
	if (!properties || typeof properties !== "object") {
		return false;
	}

	// Check if properties are in raw format
	const firstValue = Object.values(properties)[0];
	if (isTypedPropertyValue(firstValue)) {
		return false;
	}

	// Accept string or array of strings for labels
	if (typeof labelOrLabels === "string") {
		return true;
	}

	if (Array.isArray(labelOrLabels) && labelOrLabels.length > 0) {
		return labelOrLabels.every((label) => typeof label === "string");
	}

	return false;
}

function isTypedPropertyValue(value) {
	return (
		value && typeof value === "object" && "type" in value && "value" in value
	);
}

class TypedQueryBuilderImpl {
	constructor(db, builder) {
		this._db = db;
		this._builder = builder;
	}

	startFrom(nodeIds) {
		this._builder.startFrom(nodeIds);
		return this;
	}

	startFromLabel(label) {
		this._builder.startFromLabel(label);
		return this;
	}

	startFromProperty(label, key, value) {
		this._builder.startFromProperty(label, key, String(value));
		return this;
	}

	traverse(edgeTypes, direction, depth) {
		this._builder.traverse(edgeTypes, direction, depth);
		return this;
	}

	limit(n) {
		this._builder.limit(n);
		return this;
	}

	getIds() {
		return this._builder.getIds();
	}

	getNodes() {
		const nodes = this._builder.getNodes();
		return nodes.map((node) => ({
			id: node.id,
			labels: node.labels,
			properties: convertPropertiesFromSombra(node.properties),
		}));
	}

	execute() {
		return this._builder.getIds();
	}
}

/**
 * Unified SombraDB API class that works with or without TypeScript generics.
 *
 * @class
 * @example
 * // Type-safe usage with TypeScript
 * const db = new SombraDB<MySchema>('./db');
 * db.addNode('Person', { name: 'Alice', age: 30 });
 *
 * @example
 * // Raw usage (backwards compatible)
 * const db = new SombraDB('./db');
 * db.addNode(['Person'], { name: { type: 'string', value: 'Alice' } });
 */
class SombraDB {
	/**
	 * Creates a new SombraDB instance.
	 * @param {string} path - Path to the database file
	 */
	constructor(path) {
		this._db = new NativeSombraDB(path);
	}

	/**
	 * Get access to the underlying native SombraDB instance.
	 * @returns {NativeSombraDB} The native database instance
	 */
	get db() {
		return this._db;
	}

	/**
	 * Begin a new transaction.
	 * @returns {SombraTransaction} The transaction object
	 */
	beginTransaction() {
		return this._db.beginTransaction();
	}

	/**
	 * Add a node to the graph.
	 * @param {string|string[]} labelOrLabels - Node label (typed) or array of labels (typed/raw)
	 * @param {Object} properties - Node properties (plain values for typed, {type, value} for raw)
	 * @returns {number} The node ID
	 */
	addNode(labelOrLabels, properties = {}) {
		if (isTypedCall(labelOrLabels, properties)) {
			const sombraProps = convertPropertiesToSombra(properties);
			const labels = Array.isArray(labelOrLabels)
				? labelOrLabels
				: [labelOrLabels];
			return this._db.addNode(labels, sombraProps);
		}
		return this._db.addNode(labelOrLabels, properties);
	}

	/**
	 * Add an edge between two nodes.
	 * @param {number} sourceNodeId - Source node ID
	 * @param {number} targetNodeId - Target node ID
	 * @param {string} edgeTypeOrLabel - Edge type name
	 * @param {Object} properties - Edge properties (plain values for typed, {type, value} for raw)
	 * @returns {number} The edge ID
	 */
	addEdge(sourceNodeId, targetNodeId, edgeTypeOrLabel, properties = {}) {
		if (properties && Object.keys(properties).length > 0) {
			const firstValue = Object.values(properties)[0];
			if (isTypedPropertyValue(firstValue)) {
				return this._db.addEdge(
					sourceNodeId,
					targetNodeId,
					edgeTypeOrLabel,
					properties,
				);
			}
			const sombraProps = convertPropertiesToSombra(properties);
			return this._db.addEdge(
				sourceNodeId,
				targetNodeId,
				edgeTypeOrLabel,
				sombraProps,
			);
		}
		return this._db.addEdge(
			sourceNodeId,
			targetNodeId,
			edgeTypeOrLabel,
			properties,
		);
	}

	/**
	 * Get a node by ID. Properties are always returned as plain JavaScript values.
	 * @param {number} nodeId - The node ID
	 * @returns {Object|null} The node object with plain property values, or null if not found
	 */
	getNode(nodeId) {
		try {
			const node = this._db.getNode(nodeId);
			if (!node) return null;

			return {
				id: node.id,
				labels: node.labels,
				properties: convertPropertiesFromSombra(node.properties),
			};
		} catch (e) {
			return null;
		}
	}

	/**
	 * Get all node IDs with a specific label.
	 * @param {string} label - The node label
	 * @returns {number[]} Array of node IDs
	 */
	getNodesByLabel(label) {
		return this._db.getNodesByLabel(label);
	}

	/**
	 * Find the first node matching a label and property value.
	 * @param {string} label - The node label
	 * @param {string} key - Property key
	 * @param {string|number|boolean} value - Property value to match
	 * @returns {number|undefined} The node ID, or undefined if not found
	 */
	findNodeByProperty(label, key, value) {
		const result = this._db
			.query()
			.startFromProperty(label, key, String(value))
			.execute();
		return result.nodeIds[0];
	}

	/**
	 * Find all nodes matching a label and property value.
	 * @param {string} label - The node label
	 * @param {string} key - Property key
	 * @param {string|number|boolean} value - Property value to match
	 * @returns {number[]} Array of node IDs
	 */
	findNodesByProperty(label, key, value) {
		const result = this._db
			.query()
			.startFromProperty(label, key, String(value))
			.execute();
		return result.nodeIds;
	}

	/**
	 * Get an edge by ID. Properties are always returned as plain JavaScript values.
	 * @param {number} edgeId - The edge ID
	 * @returns {Object|null} The edge object with plain property values, or null if not found
	 */
	getEdge(edgeId) {
		try {
			const edge = this._db.getEdge(edgeId);
			if (!edge) return null;

			return {
				id: edge.id,
				sourceNodeId: edge.sourceNodeId,
				targetNodeId: edge.targetNodeId,
				typeName: edge.typeName,
				properties: convertPropertiesFromSombra(edge.properties),
			};
		} catch (e) {
			return null;
		}
	}

	/**
	 * Get all outgoing edge IDs from a node.
	 * @param {number} nodeId - The node ID
	 * @returns {number[]} Array of edge IDs
	 */
	getOutgoingEdges(nodeId) {
		return this._db.getOutgoingEdges(nodeId);
	}

	/**
	 * Get all incoming edge IDs to a node.
	 * @param {number} nodeId - The node ID
	 * @returns {number[]} Array of edge IDs
	 */
	getIncomingEdges(nodeId) {
		return this._db.getIncomingEdges(nodeId);
	}

	/**
	 * Get all neighboring node IDs (outgoing connections).
	 * @param {number} nodeId - The node ID
	 * @returns {number[]} Array of node IDs
	 */
	getNeighbors(nodeId) {
		return this._db.getNeighbors(nodeId);
	}

	/**
	 * Get all incoming neighboring node IDs.
	 * @param {number} nodeId - The node ID
	 * @returns {number[]} Array of node IDs
	 */
	getIncomingNeighbors(nodeId) {
		return this._db.getIncomingNeighbors(nodeId);
	}

	/**
	 * Delete a node and all its connected edges.
	 * @param {number} nodeId - The node ID to delete
	 */
	deleteNode(nodeId) {
		return this._db.deleteNode(nodeId);
	}

	/**
	 * Delete an edge.
	 * @param {number} edgeId - The edge ID to delete
	 */
	deleteEdge(edgeId) {
		return this._db.deleteEdge(edgeId);
	}

	/**
	 * Set a property on a node.
	 * @param {number} nodeId - The node ID
	 * @param {string} key - Property key
	 * @param {string|number|boolean|Object} value - Property value (plain value for typed, {type, value} for raw)
	 */
	setNodeProperty(nodeId, key, value) {
		if (isTypedPropertyValue(value)) {
			return this._db.setNodeProperty(nodeId, key, value);
		}
		const sombraValue = convertToSombraProperty(value);
		return this._db.setNodeProperty(nodeId, key, sombraValue);
	}

	/**
	 * Remove a property from a node.
	 * @param {number} nodeId - The node ID
	 * @param {string} key - Property key to remove
	 */
	removeNodeProperty(nodeId, key) {
		return this._db.removeNodeProperty(nodeId, key);
	}

	/**
	 * Flush pending writes to disk.
	 */
	flush() {
		return this._db.flush();
	}

	/**
	 * Create a checkpoint of the current database state.
	 */
	checkpoint() {
		return this._db.checkpoint();
	}

	/**
	 * Perform a breadth-first search traversal.
	 * @param {number} startNodeId - Starting node ID
	 * @param {number} maxDepth - Maximum depth to traverse
	 * @returns {Array<{nodeId: number, depth: number}>} Array of nodes with their depths
	 */
	bfsTraversal(startNodeId, maxDepth) {
		return this._db.bfsTraversal(startNodeId, maxDepth);
	}

	/**
	 * Create a new query builder for complex graph queries.
	 * @returns {TypedQueryBuilderImpl} Query builder instance
	 */
	query() {
		const builder = this._db.query();
		return new TypedQueryBuilderImpl(this._db, builder);
	}

	/**
	 * Count nodes grouped by label.
	 * @returns {Object.<string, number>} Object mapping labels to counts
	 */
	countNodesByLabel() {
		return this._db.countNodesByLabel();
	}

	/**
	 * Count edges grouped by type.
	 * @returns {Object.<string, number>} Object mapping edge types to counts
	 */
	countEdgesByType() {
		return this._db.countEdgesByType();
	}

	/**
	 * Count nodes with a specific label.
	 * @param {string} label - The node label
	 * @returns {number} Count of nodes
	 */
	countNodesWithLabel(label) {
		return this._db.countNodesWithLabel(label);
	}

	/**
	 * Count edges with a specific type.
	 * @param {string} edgeType - The edge type
	 * @returns {number} Count of edges
	 */
	countEdgesWithType(edgeType) {
		return this._db.countEdgesWithType(edgeType);
	}

	/**
	 * Get all node IDs in order.
	 * @returns {number[]} Array of all node IDs
	 */
	getAllNodeIdsOrdered() {
		return this._db.getAllNodeIdsOrdered();
	}

	/**
	 * Get the first node ID in the database.
	 * @returns {number|null} First node ID or null if database is empty
	 */
	getFirstNode() {
		return this._db.getFirstNode();
	}

	/**
	 * Get the last node ID in the database.
	 * @returns {number|null} Last node ID or null if database is empty
	 */
	getLastNode() {
		return this._db.getLastNode();
	}

	/**
	 * Get all ancestor nodes by traversing edges backwards.
	 * @param {number} startNodeId - Starting node ID
	 * @param {string} edgeType - Edge type to traverse
	 * @param {number} [maxDepth] - Maximum depth (optional)
	 * @returns {number[]} Array of ancestor node IDs
	 */
	getAncestors(startNodeId, edgeType, maxDepth) {
		return this._db.getAncestors(startNodeId, edgeType, maxDepth);
	}

	/**
	 * Get all descendant nodes by traversing edges forwards.
	 * @param {number} startNodeId - Starting node ID
	 * @param {string} edgeType - Edge type to traverse
	 * @param {number} [maxDepth] - Maximum depth (optional)
	 * @returns {number[]} Array of descendant node IDs
	 */
	getDescendants(startNodeId, edgeType, maxDepth) {
		return this._db.getDescendants(startNodeId, edgeType, maxDepth);
	}

	/**
	 * Find the shortest path between two nodes.
	 * @param {number} start - Start node ID
	 * @param {number} end - End node ID
	 * @param {string[]} [edgeTypes] - Optional edge types to traverse
	 * @returns {number[]|null} Array of node IDs in path, or null if no path exists
	 */
	shortestPath(start, end, edgeTypes) {
		return this._db.shortestPath(start, end, edgeTypes);
	}

	/**
	 * Find all paths between two nodes within depth constraints.
	 * @param {number} start - Start node ID
	 * @param {number} end - End node ID
	 * @param {number} minDepth - Minimum path depth
	 * @param {number} maxDepth - Maximum path depth
	 * @param {string[]} [edgeTypes] - Optional edge types to traverse
	 * @returns {number[][]} Array of paths, where each path is an array of node IDs
	 */
	findPaths(start, end, minDepth, maxDepth, edgeTypes) {
		return this._db.findPaths(start, end, minDepth, maxDepth, edgeTypes);
	}

	/**
	 * Verify database integrity.
	 * @param {Object} options - Verification options
	 * @param {boolean} [options.checksumOnly] - Only verify checksums
	 * @param {boolean} [options.verifyIndexes] - Verify indexes (default: true)
	 * @param {boolean} [options.verifyAdjacency] - Verify adjacency (default: true)
	 * @param {number} [options.maxErrors] - Maximum errors to report (default: 16)
	 * @returns {Object} Integrity report
	 */
	verifyIntegrity(options) {
		return this._db.verifyIntegrity(options);
	}

	/**
	 * Get database header state.
	 * @returns {Object} Header state
	 */
	getHeader() {
		return this._db.getHeader();
	}

	/**
	 * Get database metrics.
	 * @returns {Object} Metrics
	 */
	getMetrics() {
		return this._db.getMetrics();
	}
}

/**
 * Get the default page size used by the database.
 * @returns {number} Page size in bytes
 */
function getDefaultPageSize() {
	const native = require("./index.js");
	return native.getDefaultPageSize();
}

module.exports = {
	SombraDB,
	getDefaultPageSize,
};
