import {
	type QueryBuilder as NativeQueryBuilder,
	SombraDB as NativeSombraDB,
	SombraEdge,
	SombraNode,
	SombraPropertyValue,
	SombraTransaction,
} from "./index";

export type PropertyType = string | number | boolean;

export type InferPropertyValue<T> = T extends string
	? "string"
	: T extends number
		? "int" | "float"
		: T extends boolean
			? "bool"
			: never;

export type NodeSchema = Record<string, Record<string, PropertyType>>;
export type EdgeSchema = Record<
	string,
	{
		from: string;
		to: string;
		properties?: Record<string, PropertyType>;
	}
>;

export interface GraphSchema {
	nodes: NodeSchema;
	edges: EdgeSchema;
}

export type NodeLabel<Schema extends GraphSchema> = keyof Schema["nodes"] &
	string;
export type EdgeType<Schema extends GraphSchema> = keyof Schema["edges"] &
	string;

export type NodeProperties<
	Schema extends GraphSchema,
	Label extends NodeLabel<Schema>,
> = Schema["nodes"][Label];

type OneOrMore<T> = readonly [T, ...T[]];

type UnionToIntersection<U> = (
	U extends unknown
		? (k: U) => void
		: never
) extends (k: infer I) => void
	? I
	: never;

export type UnionNodeProperties<
	Schema extends GraphSchema,
	Labels extends OneOrMore<NodeLabel<Schema>>,
> = UnionToIntersection<
	Labels[number] extends infer L extends NodeLabel<Schema>
		? NodeProperties<Schema, L>
		: never
>;

type LabelTuple<
	Schema extends GraphSchema,
	T extends readonly NodeLabel<Schema>[] = readonly NodeLabel<Schema>[],
> = T;

// Helper type for all node properties across all labels (for autocomplete)
export type AllNodeProperties<Schema extends GraphSchema> = Partial<
	UnionToIntersection<NodeProperties<Schema, NodeLabel<Schema>>>
>;

export type EdgeProperties<
	Schema extends GraphSchema,
	Edge extends EdgeType<Schema>,
> = Schema["edges"][Edge]["properties"] extends Record<string, PropertyType>
	? Schema["edges"][Edge]["properties"]
	: Record<string, never>;

export type EdgeFrom<
	Schema extends GraphSchema,
	Edge extends EdgeType<Schema>,
> = Schema["edges"][Edge]["from"];

export type EdgeTo<
	Schema extends GraphSchema,
	Edge extends EdgeType<Schema>,
> = Schema["edges"][Edge]["to"];

export type TypedNode<
	Schema extends GraphSchema,
	Label extends NodeLabel<Schema>,
> = {
	id: number;
	labels: Label[];
	properties: NodeProperties<Schema, Label>;
};

export type TypedEdge<
	Schema extends GraphSchema,
	Edge extends EdgeType<Schema>,
> = {
	id: number;
	sourceNodeId: number;
	targetNodeId: number;
	typeName: Edge;
	properties: EdgeProperties<Schema, Edge>;
};

export interface TypedQueryBuilder<Schema extends GraphSchema> {
	startFrom(nodeIds: number[]): this;
	startFromLabel<L extends NodeLabel<Schema>>(label: L): this;
	startFromProperty<
		L extends NodeLabel<Schema>,
		K extends keyof NodeProperties<Schema, L>,
	>(label: L, key: K, value: NodeProperties<Schema, L>[K]): this;
	traverse<E extends EdgeType<Schema>>(
		edgeTypes: E[],
		direction: "incoming" | "outgoing" | "both",
		depth: number,
	): this;
	limit(n: number): this;
	getIds(): {
		startNodes: number[];
		nodeIds: number[];
		limited: boolean;
	};
	getNodes<L extends NodeLabel<Schema> = NodeLabel<Schema>>(): TypedNode<
		Schema,
		L
	>[];
	execute(): {
		startNodes: number[];
		nodeIds: number[];
		limited: boolean;
	};
}

export class SombraDB<Schema extends GraphSchema = GraphSchema> {
	constructor(path: string);

	beginTransaction(): SombraTransaction;

	addNode<L extends keyof Schema["nodes"]>(
		label: L,
		props: Schema["nodes"][L],
	): number;
	addNode<L1 extends keyof Schema["nodes"], L2 extends keyof Schema["nodes"]>(
		labels: [L1, L2],
		props: Schema["nodes"][L1] & Schema["nodes"][L2],
	): void;

	addNode<
		L1 extends keyof Schema["nodes"],
		L2 extends keyof Schema["nodes"],
		L3 extends keyof Schema["nodes"],
	>(
		labels: [L1, L2, L3],
		props: Schema["nodes"][L1] & Schema["nodes"][L2] & Schema["nodes"][L3],
	): void;
	addEdge<E extends EdgeType<Schema>>(
		sourceNodeId: number,
		targetNodeId: number,
		edgeType: E,
		properties: EdgeProperties<Schema, E>,
	): number;
	addEdge(
		sourceNodeId: number,
		targetNodeId: number,
		label: string,
		properties?: Record<string, SombraPropertyValue> | null,
	): number;

	getNode<L extends NodeLabel<Schema> = NodeLabel<Schema>>(
		nodeId: number,
	): TypedNode<Schema, L> | null;
	getNode(nodeId: number): SombraNode | null;

	getNodesByLabel<L extends NodeLabel<Schema>>(label: L): number[];
	getNodesByLabel(label: string): number[];

	findNodeByProperty<
		L extends NodeLabel<Schema>,
		K extends keyof NodeProperties<Schema, L>,
	>(label: L, key: K, value: NodeProperties<Schema, L>[K]): number | undefined;

	findNodesByProperty<
		L extends NodeLabel<Schema>,
		K extends keyof NodeProperties<Schema, L>,
	>(label: L, key: K, value: NodeProperties<Schema, L>[K]): number[];

	getEdge<E extends EdgeType<Schema> = EdgeType<Schema>>(
		edgeId: number,
	): TypedEdge<Schema, E> | null;
	getEdge(edgeId: number): SombraEdge;

	getOutgoingEdges(nodeId: number): number[];
	getIncomingEdges(nodeId: number): number[];
	getNeighbors(nodeId: number): number[];
	getIncomingNeighbors(nodeId: number): number[];

	deleteNode(nodeId: number): void;
	deleteEdge(edgeId: number): void;

	setNodeProperty<
		L extends NodeLabel<Schema>,
		K extends keyof NodeProperties<Schema, L>,
	>(nodeId: number, key: K, value: NodeProperties<Schema, L>[K]): void;
	setNodeProperty(
		nodeId: number,
		key: string,
		value: SombraPropertyValue,
	): void;

	removeNodeProperty(nodeId: number, key: string): void;

	flush(): void;
	checkpoint(): void;

	bfsTraversal(
		startNodeId: number,
		maxDepth: number,
	): Array<{ nodeId: number; depth: number }>;

	query(): TypedQueryBuilder<Schema>;
	query(): NativeQueryBuilder;

	countNodesByLabel(): Record<string, number>;
	countEdgesByType(): Record<string, number>;
	countNodesWithLabel<L extends NodeLabel<Schema>>(label: L): number;
	countNodesWithLabel(label: string): number;
	countEdgesWithType<E extends EdgeType<Schema>>(edgeType: E): number;
	countEdgesWithType(edgeType: string): number;

	getAllNodeIdsOrdered(): number[];
	getFirstNode(): number | null;
	getLastNode(): number | null;

	getAncestors(
		startNodeId: number,
		edgeType: string,
		maxDepth?: number,
	): number[];
	getDescendants(
		startNodeId: number,
		edgeType: string,
		maxDepth?: number,
	): number[];

	shortestPath(
		start: number,
		end: number,
		edgeTypes?: string[],
	): number[] | null;
	findPaths(
		start: number,
		end: number,
		minDepth: number,
		maxDepth: number,
		edgeTypes?: string[],
	): number[][];

	verifyIntegrity(options: IntegrityOptions): IntegrityReport;
	getHeader(): HeaderState;
	getMetrics(): Metrics;

	readonly db: NativeSombraDB;
}

export interface IntegrityOptions {
	checksumOnly?: boolean;
	verifyIndexes?: boolean;
	verifyAdjacency?: boolean;
	maxErrors?: number;
}

export interface IntegrityReport {
	checkedPages: number;
	checksumFailures: number;
	recordErrors: number;
	indexErrors: number;
	adjacencyErrors: number;
	errors: string[];
}

export interface HeaderState {
	nextNodeId: number;
	nextEdgeId: number;
	freePageHead?: number;
	lastRecordPage?: number;
	lastCommittedTxId: number;
	btreeIndexPage?: number;
	btreeIndexSize: number;
}

export interface Metrics {
	cacheHits: number;
	cacheMisses: number;
	nodeLookups: number;
	edgeTraversals: number;
	walBytesWritten: number;
	walSyncs: number;
	checkpointsPerformed: number;
	pageEvictions: number;
	transactionsCommitted: number;
	transactionsRolledBack: number;
}

export function getDefaultPageSize(): number;

export {
	NativeSombraDB,
	SombraPropertyValue,
	SombraNode,
	SombraEdge,
	SombraTransaction,
};
