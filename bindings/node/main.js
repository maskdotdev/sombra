// Fluent query builder facade for the Sombra Node bindings.
const native = require('./index.js')

const ALPHABET = 'abcdefghijklmnopqrstuvwxyz'
const CREATE_HANDLE_SYMBOL = Symbol('sombra.createHandle')

function autoVarName(idx) {
  const letter = ALPHABET[idx % ALPHABET.length]
  if (idx < ALPHABET.length) {
    return letter
  }
  return `${letter}${Math.floor(idx / ALPHABET.length)}`
}

function normalizeTarget(target, fallback) {
  if (typeof target === 'string') {
    return { var: fallback, label: target }
  }
  if (target && typeof target === 'object') {
    const varName = target.var ?? fallback
    const label = target.label ?? null
    return { var: varName, label }
  }
  throw new TypeError("target must be a string or an object with optional 'var'/'label'")
}

function normalizeLabels(input) {
  if (typeof input === 'string') {
    return [input]
  }
  if (Array.isArray(input)) {
    return input.map((label) => {
      if (typeof label !== 'string') {
        throw new TypeError('node labels must be strings')
      }
      return label
    })
  }
  throw new TypeError('node labels must be a string or an array of strings')
}

function literalSpec(value) {
  if (value === null || value === undefined) {
    return { type: 'null' }
  }
  if (typeof value === 'boolean') {
    return { type: 'bool', value }
  }
  if (typeof value === 'string') {
    return { type: 'string', value }
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError('numeric literals must be finite')
    }
    if (Number.isInteger(value)) {
      return { type: 'int', value }
    }
    return { type: 'float', value }
  }
  throw new TypeError(`unsupported literal type: ${typeof value}`)
}

function includedBound(value) {
  return { kind: 'included', value: literalSpec(value) }
}

function excludedBound(value) {
  return { kind: 'excluded', value: literalSpec(value) }
}

function cloneSpec(spec) {
  return JSON.parse(JSON.stringify(spec))
}

class MutationBatch {
  constructor() {
    this._ops = []
    this._sealed = false
  }

  _ensureMutable() {
    if (this._sealed) {
      throw new Error('transaction already committed')
    }
  }

  _queue(op) {
    this._ensureMutable()
    this._ops.push(op)
    return this
  }

  queue(op) {
    if (!op || typeof op !== 'object') {
      throw new TypeError('queued mutation must be an object')
    }
    return this._queue(cloneSpec(op))
  }

  createNode(labels, props = {}) {
    const labelList = Array.isArray(labels) ? labels : [labels]
    return this._queue({ op: 'createNode', labels: labelList, props })
  }

  updateNode(id, options = {}) {
    const { set = {}, unset = [] } = options
    return this._queue({ op: 'updateNode', id, set, unset })
  }

  deleteNode(id, cascade = false) {
    return this._queue({ op: 'deleteNode', id, cascade })
  }

  createEdge(src, dst, ty, props = {}) {
    return this._queue({ op: 'createEdge', src, dst, ty, props })
  }

  updateEdge(id, options = {}) {
    const { set = {}, unset = [] } = options
    return this._queue({ op: 'updateEdge', id, set, unset })
  }

  deleteEdge(id) {
    return this._queue({ op: 'deleteEdge', id })
  }

  drain() {
    this._sealed = true
    const ops = this._ops
    this._ops = []
    return ops
  }
}

class QueryStream {
  constructor(handle) {
    this._handle = handle
  }

  [Symbol.asyncIterator]() {
    return this
  }

  async next() {
    const value = this._handle.next()
    if (value === undefined || value === null) {
      return { done: true, value: undefined }
    }
    return { done: false, value }
  }
}

class QueryBuilder {
  constructor(db) {
    this._db = db
    this._matches = []
    this._edges = []
    this._predicates = []
    this._projections = []
    this._distinct = false
    this._lastVar = null
    this._nextVarIdx = 0
    this._pendingDirection = 'out'
  }

  match(target) {
    const fallback = this._nextAutoVar()
    const normalized = normalizeTarget(target, fallback)
    this._ensureMatch(normalized.var, normalized.label)
    this._lastVar = normalized.var
    return this
  }

  where(edgeType, target) {
    if (!this._lastVar) {
      throw new Error('where requires a preceding match clause')
    }
    const fallback = this._nextAutoVar()
    const normalized = normalizeTarget(target, fallback)
    this._ensureMatch(normalized.var, normalized.label)
    this._edges.push({
      from: this._lastVar,
      to: normalized.var,
      edge_type: edgeType ?? null,
      direction: this._pendingDirection,
    })
    this._lastVar = normalized.var
    this._pendingDirection = 'out'
    return this
  }

  direction(dir) {
    if (dir !== 'out' && dir !== 'in' && dir !== 'both') {
      throw new Error(`invalid direction: ${dir}`)
    }
    this._pendingDirection = dir
    return this
  }

  bidirectional() {
    return this.direction('both')
  }

  whereProp(varName, prop, op, value, value2 = undefined) {
    const opLower = op.toLowerCase()
    if (opLower === '=' || opLower === 'eq') {
      this._predicates.push({
        kind: 'eq',
        var: varName,
        prop,
        value: literalSpec(value),
      })
    } else if (opLower === 'between') {
      if (value2 === undefined) {
        throw new Error('between operator requires two values')
      }
      this._predicates.push({
        kind: 'range',
        var: varName,
        prop,
        lower: includedBound(value),
        upper: includedBound(value2),
      })
    } else if (opLower === '>' || opLower === 'gt') {
      this._predicates.push({
        kind: 'range',
        var: varName,
        prop,
        lower: excludedBound(value),
        upper: { kind: 'unbounded' },
      })
    } else if (opLower === '>=' || opLower === 'ge') {
      this._predicates.push({
        kind: 'range',
        var: varName,
        prop,
        lower: includedBound(value),
        upper: { kind: 'unbounded' },
      })
    } else if (opLower === '<' || opLower === 'lt') {
      this._predicates.push({
        kind: 'range',
        var: varName,
        prop,
        lower: { kind: 'unbounded' },
        upper: excludedBound(value),
      })
    } else if (opLower === '<=' || opLower === 'le') {
      this._predicates.push({
        kind: 'range',
        var: varName,
        prop,
        lower: { kind: 'unbounded' },
        upper: includedBound(value),
      })
    } else {
      throw new Error(`unsupported property operator: ${op}`)
    }
    return this
  }

  distinct(_on) {
    this._distinct = true
    return this
  }

  select(fields) {
    const projections = []
    for (const field of fields) {
      if (typeof field === 'string') {
        projections.push({ kind: 'var', var: field, alias: null })
        continue
      }
      if (field && typeof field === 'object') {
        if ('expr' in field) {
          const expr = field.expr
          const alias = field.as
          if (typeof expr !== 'string' || typeof alias !== 'string') {
            throw new TypeError('projection expression requires string expr and alias')
          }
          projections.push({ kind: 'expr', expr, alias })
          continue
        }
        if ('var' in field) {
          const varName = field.var
          const alias = field.as ?? null
          projections.push({ kind: 'var', var: varName, alias })
          continue
        }
      }
      throw new TypeError('unsupported projection field')
    }
    this._projections = projections
    return this
  }

  async explain() {
    return this._db._explain(this._build())
  }

  async execute() {
    return this._db._execute(this._build())
  }

  stream() {
    const handle = this._db._stream(this._build())
    return new QueryStream(handle)
  }

  _ensureMatch(varName, label) {
    const existing = this._matches.find((entry) => entry.var === varName)
    if (existing) {
      if (label != null && existing.label == null) {
        existing.label = label
      }
      return
    }
    this._matches.push({ var: varName, label: label ?? null })
  }

  _nextAutoVar() {
    const name = autoVarName(this._nextVarIdx)
    this._nextVarIdx += 1
    return name
  }

  _build() {
    return {
      matches: this._matches.map((clause) => ({
        var: clause.var,
        label: clause.label ?? null,
      })),
      edges: this._edges.map((edge) => ({
        from: edge.from,
        to: edge.to,
        edge_type: edge.edge_type ?? null,
        direction: edge.direction,
      })),
      predicates: this._predicates.map((pred) => cloneSpec(pred)),
      distinct: this._distinct,
      projections: this._projections.map((proj) => cloneSpec(proj)),
    }
  }
}

class CreateNodeHandle {
  constructor(builder, index) {
    this._builder = builder
    this._index = index
    this[CREATE_HANDLE_SYMBOL] = true
  }

  get index() {
    return this._index
  }

  node(labels, props = {}, alias) {
    return this._builder.node(labels, props, alias)
  }

  nodeWithAlias(labels, props = {}, alias) {
    return this._builder.nodeWithAlias(labels, props, alias)
  }

  edge(src, ty, dst, props = {}) {
    return this._builder.edge(src, ty, dst, props)
  }

  execute() {
    return this._builder.execute()
  }
}

class CreateBuilder {
  constructor(db) {
    this._db = db
    this._nodes = []
    this._edges = []
    this._sealed = false
  }

  node(labels, props = {}, alias) {
    this._ensureMutable()
    const labelList = normalizeLabels(labels)
    const aliasValue = alias ?? undefined
    if (aliasValue !== undefined && typeof aliasValue !== 'string') {
      throw new TypeError('node alias must be a string when provided')
    }
    const entry = {
      labels: labelList,
      props: cloneSpec(props),
    }
    if (aliasValue !== undefined) {
      if (!aliasValue.length) {
        throw new TypeError('node alias must be non-empty')
      }
      entry.alias = aliasValue
    }
    this._nodes.push(entry)
    return new CreateNodeHandle(this, this._nodes.length - 1)
  }

  nodeWithAlias(labels, propsOrAlias, maybeAlias) {
    if (typeof propsOrAlias === 'string' && maybeAlias === undefined) {
      return this.node(labels, {}, propsOrAlias)
    }
    if (typeof maybeAlias !== 'string' || maybeAlias.length === 0) {
      throw new TypeError('nodeWithAlias requires an alias string')
    }
    return this.node(labels, propsOrAlias ?? {}, maybeAlias)
  }

  edge(src, ty, dst, props = {}) {
    this._ensureMutable()
    if (typeof ty !== 'string' || ty.length === 0) {
      throw new TypeError('edge type must be a non-empty string')
    }
    this._edges.push({
      src: this._encodeRef(src),
      ty,
      dst: this._encodeRef(dst),
      props: cloneSpec(props),
    })
    return this
  }

  execute() {
    this._ensureMutable()
    this._sealed = true
    const script = {
      nodes: this._nodes.map((node) => {
        const spec = {
          labels: node.labels.slice(),
          props: cloneSpec(node.props),
        }
        if (node.alias !== undefined) {
          spec.alias = node.alias
        }
        return spec
      }),
      edges: this._edges.map((edge) => ({
        src: { ...edge.src },
        ty: edge.ty,
        dst: { ...edge.dst },
        props: cloneSpec(edge.props),
      })),
    }
    const summary = native.databaseCreate(this._db._handle, script)
    return createSummaryWithAliasHelper(summary)
  }

  _encodeRef(value) {
    if (value instanceof CreateNodeHandle) {
      return { kind: 'handle', index: value.index }
    }
    if (typeof value === 'string') {
      if (!value.length) {
        throw new TypeError('edge alias reference must be non-empty')
      }
      return { kind: 'alias', alias: value }
    }
    if (typeof value === 'number') {
      if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
        throw new TypeError('edge node id must be a non-negative integer')
      }
      return { kind: 'id', id: value }
    }
    throw new TypeError('edge endpoints must be node handles, alias strings, or numeric ids')
  }

  _ensureMutable() {
    if (this._sealed) {
      throw new Error('builder already executed')
    }
  }
}

function createSummaryWithAliasHelper(summary) {
  const result = {
    nodes: Array.isArray(summary?.nodes) ? summary.nodes : [],
    edges: Array.isArray(summary?.edges) ? summary.edges : [],
    aliases:
      summary && summary.aliases && typeof summary.aliases === 'object'
        ? { ...summary.aliases }
        : {},
  }
  Object.defineProperty(result, 'alias', {
    enumerable: false,
    value(name) {
      if (typeof name !== 'string' || name.length === 0) {
        throw new TypeError('alias lookup requires a non-empty string name')
      }
      return Object.prototype.hasOwnProperty.call(result.aliases, name)
        ? result.aliases[name]
        : undefined
    },
  })
  return result
}

class Database {
  constructor(handle) {
    this._handle = handle
  }

  static open(path, options) {
    const handle = native.openDatabase(path, options ?? undefined)
    return new Database(handle)
  }

  query() {
    return new QueryBuilder(this)
  }

  create() {
    return new CreateBuilder(this)
  }

  intern(name) {
    return native.databaseIntern(this._handle, name)
  }

  seedDemo() {
    native.databaseSeedDemo(this._handle)
    return this
  }

  mutate(script) {
    if (!script || typeof script !== 'object') {
      throw new TypeError('mutation script must be an object')
    }
    if (!Array.isArray(script.ops)) {
      throw new TypeError('mutation script requires an ops array')
    }
    return native.databaseMutate(this._handle, script)
  }

  createNode(labels, props = {}) {
    const labelList = Array.isArray(labels) ? labels : [labels]
    const summary = this.mutate({ ops: [{ op: 'createNode', labels: labelList, props }] })
    const ids = summary.createdNodes ?? []
    return ids.length > 0 ? ids[ids.length - 1] : null
  }

  updateNode(id, options = {}) {
    const { set = {}, unset = [] } = options
    this.mutate({ ops: [{ op: 'updateNode', id, set, unset }] })
    return this
  }

  deleteNode(id, cascade = false) {
    this.mutate({ ops: [{ op: 'deleteNode', id, cascade }] })
    return this
  }

  createEdge(src, dst, ty, props = {}) {
    const summary = this.mutate({
      ops: [{ op: 'createEdge', src, dst, ty, props }],
    })
    const ids = summary.createdEdges ?? []
    return ids.length > 0 ? ids[ids.length - 1] : null
  }

  deleteEdge(id) {
    this.mutate({ ops: [{ op: 'deleteEdge', id }] })
    return this
  }

  mutateMany(ops) {
    if (!Array.isArray(ops)) {
      throw new TypeError('mutateMany requires an array of operations')
    }
    return this.mutate({ ops: ops.map((op) => cloneSpec(op)) })
  }

  async transaction(fn) {
    if (typeof fn !== 'function') {
      throw new TypeError('transaction requires a callback')
    }
    const batch = new MutationBatch()
    const result = await fn(batch)
    const ops = batch.drain()
    const summary = this.mutate({ ops })
    return { summary, result }
  }

  pragma(name, value) {
    if (typeof name !== 'string') {
      throw new TypeError('pragma name must be a string')
    }
    if (arguments.length < 2) {
      return native.databasePragmaGet(this._handle, name)
    }
    return native.databasePragmaSet(this._handle, name, value)
  }

  _execute(spec) {
    return native.databaseExecute(this._handle, spec)
  }

  _explain(spec) {
    return native.databaseExplain(this._handle, spec)
  }

  _stream(spec) {
    return native.databaseStream(this._handle, spec)
  }
}

function openDatabase(path, options) {
  return Database.open(path, options)
}

module.exports = {
  Database,
  QueryBuilder,
  openDatabase,
  native,
}
