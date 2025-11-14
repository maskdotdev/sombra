const { Database } = require('./main.js')

class SombraDB {
  constructor(pathOrDb, options = {}) {
    const connectOptions = options.connect ?? undefined
    if (pathOrDb instanceof Database) {
      this._db = pathOrDb
    } else if (typeof pathOrDb === 'string') {
      this._db = Database.open(pathOrDb, connectOptions ?? undefined)
    } else {
      throw new TypeError('SombraDB requires a file path or Database instance')
    }
    this._schema = options.schema ? normalizeGraphSchema(options.schema) : null
    if (this._schema) {
      this._db.withSchema(extractNodeSchema(this._schema))
    }
  }

  raw() {
    return this._db
  }

  addNode(label, props = {}) {
    this._assertNodeLabel(label, 'addNode')
    this._validateNodeProps(label, props)
    const id = this._db.createNode(label, props)
    if (id == null) {
      throw new Error('unable to create node')
    }
    return id
  }

  addEdge(src, dst, edgeType, props = {}) {
    const normalizedEdge = this._assertEdgeType(edgeType, 'addEdge')
    this._validateEdgeProps(edgeType, props)
    if (normalizedEdge && this._schema) {
      const expectedSrc = this._schema.edges[edgeType].from
      const expectedDst = this._schema.edges[edgeType].to
      assertIdMatchesLabel(src, expectedSrc, 'addEdge source')
      assertIdMatchesLabel(dst, expectedDst, 'addEdge target')
    }
    const edgeId = this._db.createEdge(src, dst, edgeType, props)
    if (edgeId == null) {
      throw new Error('unable to create edge')
    }
    return edgeId
  }

  getNode(nodeId, expectedLabel) {
    const record = this._db.getNodeRecord(nodeId)
    if (!record) {
      return null
    }
    const label = expectedLabel ?? (record.labels && record.labels[0]) ?? null
    if (label == null) {
      return null
    }
    return {
      id: nodeId,
      label,
      properties: record.properties ?? {},
    }
  }

  findNodeByProperty(label, prop, value) {
    const normalizedLabel = this._assertNodeLabel(label, 'findNodeByProperty')
    const nodes = this.listNodesWithLabel(normalizedLabel)
    for (const nodeId of nodes) {
      const node = this.getNode(nodeId, normalizedLabel)
      if (node && Object.prototype.hasOwnProperty.call(node.properties, prop)) {
        if (node.properties[prop] === value) {
          return nodeId
        }
      }
    }
    return null
  }

  listNodesWithLabel(label) {
    const normalized = this._assertNodeLabel(label, 'listNodesWithLabel')
    return this._db.listNodesWithLabel(normalized)
  }

  getIncomingNeighbors(nodeId, edgeType, distinct = true) {
    if (edgeType !== undefined && edgeType !== null) {
      this._assertEdgeType(edgeType, 'getIncomingNeighbors')
    }
    return this._db.getIncomingNeighbors(nodeId, edgeType, Boolean(distinct))
  }

  getOutgoingNeighbors(nodeId, edgeType, distinct = true) {
    if (edgeType !== undefined && edgeType !== null) {
      this._assertEdgeType(edgeType, 'getOutgoingNeighbors')
    }
    return this._db.getOutgoingNeighbors(nodeId, edgeType, Boolean(distinct))
  }

  countNodesWithLabel(label) {
    const normalized = this._assertNodeLabel(label, 'countNodesWithLabel')
    return this._db.countNodesWithLabel(normalized)
  }

  countEdgesWithType(edgeType) {
    const normalized = this._assertEdgeType(edgeType, 'countEdgesWithType')
    return this._db.countEdgesWithType(normalized)
  }

  bfsTraversal(nodeId, maxDepth, options = {}) {
    if (!Number.isInteger(maxDepth) || maxDepth < 0) {
      throw new TypeError('bfsTraversal requires a non-negative integer maxDepth')
    }
    return this._db.bfsTraversal(nodeId, maxDepth, options)
  }

  query() {
    return new TypedQueryBuilder(this)
  }

  flush() {
    return this
  }

  _assertNodeLabel(label, ctx) {
    if (typeof label !== 'string' || label.trim() === '') {
      throw new TypeError(`${ctx} requires a non-empty label string`)
    }
    if (this._schema && !Object.prototype.hasOwnProperty.call(this._schema.nodes, label)) {
      throw new TypeError(`${ctx} refers to unknown label '${label}'`)
    }
    return label
  }

  _assertEdgeType(edgeType, ctx) {
    if (typeof edgeType !== 'string' || edgeType.trim() === '') {
      throw new TypeError(`${ctx} requires a non-empty edge type string`)
    }
    if (this._schema && !Object.prototype.hasOwnProperty.call(this._schema.edges, edgeType)) {
      throw new TypeError(`${ctx} refers to unknown edge type '${edgeType}'`)
    }
    return edgeType
  }

  _validateNodeProps(label, props) {
    if (!this._schema || !props) {
      return
    }
    const definition = this._schema.nodes[label]
    if (!definition) {
      return
    }
    const allowed = definition.properties ? Object.keys(definition.properties) : []
    for (const key of Object.keys(props)) {
      if (!allowed.includes(key)) {
        throw new TypeError(`unknown property '${key}' for node '${label}'`)
      }
    }
  }

  _validateEdgeProps(edgeType, props) {
    if (!this._schema || !props) {
      return
    }
    const definition = this._schema.edges[edgeType]
    if (!definition) {
      return
    }
    const allowed = definition.properties ? Object.keys(definition.properties) : []
    for (const key of Object.keys(props)) {
      if (!allowed.includes(key)) {
        throw new TypeError(`unknown property '${key}' for edge '${edgeType}'`)
      }
    }
  }
}

class TypedQueryBuilder {
  constructor(db) {
    this._db = db
    this._startLabel = null
    this._edgeTypes = []
    this._direction = 'out'
    this._depth = 1
  }

  startFromLabel(label) {
    this._db._assertNodeLabel(label, 'query.startFromLabel')
    this._startLabel = label
    return this
  }

  traverse(edgeTypes, direction = 'out', depth = 1) {
    if (!Array.isArray(edgeTypes) || edgeTypes.length === 0) {
      throw new TypeError('traverse(edgeTypes) requires a non-empty array of edge types')
    }
    edgeTypes.forEach((ty) => this._db._assertEdgeType(ty, 'query.traverse'))
    if (!['out', 'in', 'both'].includes(direction)) {
      throw new TypeError('direction must be one of out, in, both')
    }
    if (!Number.isInteger(depth) || depth <= 0) {
      throw new TypeError('depth must be a positive integer')
    }
    this._edgeTypes = edgeTypes
    this._direction = direction
    this._depth = depth
    return this
  }

  getIds() {
    if (!this._startLabel) {
      throw new Error('startFromLabel() must be called before getIds()')
    }
    const startNodes = this._db.listNodesWithLabel(this._startLabel)
    const set = new Set()
    for (const nodeId of startNodes) {
      const visits = this._db.bfsTraversal(nodeId, this._depth, {
        direction: this._direction,
        edgeTypes: this._edgeTypes,
      })
      for (const visit of visits) {
        if (!visit || typeof visit.nodeId !== 'number') {
          continue
        }
        if (visit.depth > 0) {
          set.add(visit.nodeId)
        }
      }
    }
    return { nodeIds: Array.from(set) }
  }
}

function normalizeGraphSchema(schema) {
  if (!schema || typeof schema !== 'object') {
    throw new TypeError('graph schema must be an object')
  }
  const nodes = {}
  const nodeDefs = schema.nodes ?? {}
  Object.entries(nodeDefs).forEach(([label, def]) => {
    if (typeof label !== 'string' || label.trim() === '') {
      throw new TypeError('node labels must be non-empty strings')
    }
    if (!def || typeof def !== 'object') {
      throw new TypeError(`definition for node '${label}' must be an object`)
    }
    const props = def.properties ?? {}
    if (!isPlainObject(props)) {
      throw new TypeError(`properties for node '${label}' must be an object`)
    }
    nodes[label] = { properties: { ...props } }
  })
  const edges = {}
  const edgeDefs = schema.edges ?? {}
  Object.entries(edgeDefs).forEach(([edgeType, def]) => {
    if (typeof edgeType !== 'string' || edgeType.trim() === '') {
      throw new TypeError('edge type names must be non-empty strings')
    }
    if (!def || typeof def !== 'object') {
      throw new TypeError(`definition for edge '${edgeType}' must be an object`)
    }
    const from = def.from
    const to = def.to
    if (typeof from !== 'string' || typeof to !== 'string') {
      throw new TypeError(`edge '${edgeType}' must specify string from/to labels`)
    }
    if (!nodes[from] || !nodes[to]) {
      throw new TypeError(`edge '${edgeType}' references unknown nodes '${from}' -> '${to}'`)
    }
    const props = def.properties ?? {}
    if (!isPlainObject(props)) {
      throw new TypeError(`properties for edge '${edgeType}' must be an object`)
    }
    edges[edgeType] = { from, to, properties: { ...props } }
  })
  return { nodes, edges }
}

function extractNodeSchema(schema) {
  const nodeSchema = {}
  Object.entries(schema.nodes).forEach(([label, def]) => {
    nodeSchema[label] = { ...def.properties }
  })
  return nodeSchema
}

function isPlainObject(value) {
  if (!value || typeof value !== 'object') {
    return false
  }
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null
}

function assertIdMatchesLabel(nodeId, label, ctx) {
  if (nodeId == null || typeof nodeId !== 'number') {
    throw new TypeError(`${ctx} requires a numeric node id`)
  }
  if (label == null || typeof label !== 'string') {
    return
  }
}

module.exports = {
  SombraDB,
}
