// Fluent query builder facade for the Sombra Node bindings.
const native = require('./index.js')

const ALPHABET = 'abcdefghijklmnopqrstuvwxyz'
const CREATE_HANDLE_SYMBOL = Symbol('sombra.createHandle')
const NS_PER_MILLISECOND = 1_000_000n
const I64_MIN = -(1n << 63n)
const I64_MAX = (1n << 63n) - 1n
const ISO_DATETIME_REGEX =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?(Z|([+-])(\d{2}):(\d{2}))$/
const ISO_DATETIME_PREFIX = /^\d{4}-\d{2}-\d{2}T/
const MIN_DATETIME_NS = BigInt(Date.UTC(1900, 0, 1, 0, 0, 0)) * NS_PER_MILLISECOND
const MAX_DATETIME_NS = BigInt(Date.UTC(2100, 0, 1, 0, 0, 0)) * NS_PER_MILLISECOND

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

function literalValue(value) {
  if (value === null || value === undefined) {
    return { t: 'Null' }
  }
  if (typeof value === 'boolean') {
    return { t: 'Bool', v: value }
  }
  const dateTimeLiteral = coerceDateTime(value)
  if (dateTimeLiteral !== null) {
    return { t: 'DateTime', v: dateTimeLiteral }
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError('numeric literals must be finite')
    }
    if (Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        throw new TypeError('integer literals must be safe 53-bit integers')
      }
      return { t: 'Int', v: value }
    }
    return { t: 'Float', v: value }
  }
  if (typeof value === 'string') {
    return { t: 'String', v: value }
  }
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
    return { t: 'Bytes', v: Array.from(value.values()) }
  }
  if (value instanceof Uint8Array) {
    return { t: 'Bytes', v: Array.from(value.values()) }
  }
  throw new TypeError(`unsupported literal type: ${typeof value}`)
}

function normalizePropName(prop) {
  if (typeof prop !== 'string' || prop.trim() === '') {
    throw new TypeError('property name must be a non-empty string')
  }
  return prop
}

function ensureScalarLiteral(value, ctx) {
  if (Array.isArray(value)) {
    throw new TypeError(`${ctx} does not accept nested arrays`)
  }
  if (value && typeof value === 'object') {
    if (value instanceof Date) {
      return
    }
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
      return
    }
    if (value instanceof Uint8Array) {
      return
    }
    throw new TypeError(`${ctx} requires scalar literal values`)
  }
}

function ensureDateTimeRange(nanos) {
  if (nanos < MIN_DATETIME_NS || nanos > MAX_DATETIME_NS) {
    throw new RangeError('datetime literal must be between 1900-01-01 and 2100-01-01 UTC')
  }
  return nanos
}

function parseIsoDateTimeLiteral(value) {
  if (!ISO_DATETIME_PREFIX.test(value)) {
    return null
  }
  const match = ISO_DATETIME_REGEX.exec(value)
  if (!match) {
    throw new TypeError('ISO 8601 datetime strings must include a timezone offset (Z or ±HH:MM)')
  }
  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  const hour = Number(match[4])
  const minute = Number(match[5])
  const second = Number(match[6])
  const fraction = match[7] ? match[7].slice(1) : ''
  const tz = match[8]
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    hour > 23 ||
    minute > 59 ||
    second > 60 // allow leap seconds
  ) {
    throw new RangeError(`invalid datetime components in '${value}'`)
  }
  const baseMs = Date.UTC(year, month - 1, day, hour, minute, second)
  const validation = new Date(baseMs)
  if (
    validation.getUTCFullYear() !== year ||
    validation.getUTCMonth() + 1 !== month ||
    validation.getUTCDate() !== day ||
    validation.getUTCHours() !== hour ||
    validation.getUTCMinutes() !== minute ||
    validation.getUTCSeconds() !== Math.min(second, 59)
  ) {
    throw new RangeError(`invalid calendar date '${value}'`)
  }
  let nanos = BigInt(baseMs) * NS_PER_MILLISECOND
  if (fraction) {
    const padded = (fraction + '000000000').slice(0, 9)
    nanos += BigInt(padded)
  }
  if (tz !== 'Z') {
    const sign = match[9]
    const offsetHours = Number(match[10])
    const offsetMinutes = Number(match[11])
    if (offsetHours > 23 || offsetMinutes > 59) {
      throw new RangeError(`invalid timezone offset in '${value}'`)
    }
    const totalMinutes = offsetHours * 60 + offsetMinutes
    const offsetNanos = BigInt(totalMinutes) * 60n * 1_000_000_000n
    nanos += sign === '+' ? -offsetNanos : offsetNanos
  }
  return ensureDateTimeRange(nanos).toString()
}

function coerceDateTime(value) {
  if (value instanceof Date) {
    const millis = value.getTime()
    if (!Number.isFinite(millis)) {
      throw new TypeError('invalid Date literal')
    }
    const nanos = BigInt(Math.trunc(millis)) * NS_PER_MILLISECOND
    return ensureDateTimeRange(nanos).toString()
  }
  if (typeof value === 'string') {
    return parseIsoDateTimeLiteral(value)
  }
  return null
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

class PredicateBuilder {
  constructor(parent, varName, mode = 'and') {
    if (typeof varName !== 'string' || varName.trim() === '') {
      throw new TypeError('where(var) requires a non-empty variable name')
    }
    this._parent = parent ?? null
    this._var = varName
    this._mode = mode
    this._exprs = []
    this._sealed = false
  }

  _ensureActive() {
    if (this._sealed) {
      throw new Error('predicate builder already finalized')
    }
  }

  _push(expr) {
    this._ensureActive()
    this._exprs.push(expr)
    return this
  }

  _finalizeExpr() {
    this._ensureActive()
    if (this._exprs.length === 0) {
      throw new Error('predicate builder must emit at least one predicate')
    }
    this._sealed = true
    if (this._exprs.length === 1) {
      return this._exprs[0]
    }
    return { op: this._mode, args: this._exprs }
  }

  done() {
    if (!this._parent) {
      throw new Error('cannot finalize nested predicate group directly')
    }
    const expr = this._finalizeExpr()
    this._parent._appendPredicate(expr)
    return this._parent
  }

  eq(prop, value) {
    return this._comparison('eq', prop, { value: literalValue(value) })
  }

  ne(prop, value) {
    return this._comparison('ne', prop, { value: literalValue(value) })
  }

  lt(prop, value) {
    return this._comparison('lt', prop, { value: literalValue(value) })
  }

  lte(prop, value) {
    return this.le(prop, value)
  }

  le(prop, value) {
    return this._comparison('le', prop, { value: literalValue(value) })
  }

  gt(prop, value) {
    return this._comparison('gt', prop, { value: literalValue(value) })
  }

  gte(prop, value) {
    return this.ge(prop, value)
  }

  ge(prop, value) {
    return this._comparison('ge', prop, { value: literalValue(value) })
  }

  between(prop, low, high, opts = undefined) {
    if (low === undefined || high === undefined) {
      throw new TypeError('between() requires both low and high values')
    }
    if (opts !== undefined && (opts === null || typeof opts !== 'object')) {
      throw new TypeError('between() options must be an object')
    }
    let inclusive = [true, true]
    if (opts && opts.inclusive !== undefined) {
      const tuple = opts.inclusive
      if (
        !Array.isArray(tuple) ||
        tuple.length !== 2 ||
        typeof tuple[0] !== 'boolean' ||
        typeof tuple[1] !== 'boolean'
      ) {
        throw new TypeError('between().inclusive must be a [boolean, boolean] tuple')
      }
      inclusive = tuple
    }
    return this._push({
      op: 'between',
      var: this._var,
      prop: normalizePropName(prop),
      low: literalValue(low),
      high: literalValue(high),
      inclusive,
    })
  }

  in(prop, values) {
    if (!Array.isArray(values)) {
      throw new TypeError('in() requires an array of literal values')
    }
    if (values.length === 0) {
      throw new TypeError('in() requires at least one literal value')
    }
    const tagged = values.map((value) => {
      ensureScalarLiteral(value, 'in()')
      return literalValue(value)
    })
    const exemplar = tagged.find((entry) => entry.t !== 'Null')
    if (exemplar) {
      for (const entry of tagged) {
        if (entry.t !== 'Null' && entry.t !== exemplar.t) {
          throw new TypeError('in() requires all literals to share the same type')
        }
      }
    }
    return this._push({
      op: 'in',
      var: this._var,
      prop: normalizePropName(prop),
      values: tagged,
    })
  }

  exists(prop) {
    return this._push({
      op: 'exists',
      var: this._var,
      prop: normalizePropName(prop),
    })
  }

  isNull(prop) {
    return this._push({
      op: 'isNull',
      var: this._var,
      prop: normalizePropName(prop),
    })
  }

  isNotNull(prop) {
    return this._push({
      op: 'isNotNull',
      var: this._var,
      prop: normalizePropName(prop),
    })
  }

  and(callback) {
    return this._group('and', callback)
  }

  or(callback) {
    return this._group('or', callback)
  }

  not(callback) {
    if (typeof callback !== 'function') {
      throw new TypeError('not() requires a callback')
    }
    const nested = new PredicateBuilder(null, this._var, 'and')
    callback(nested)
    const expr = nested._finalizeExpr()
    return this._push({ op: 'not', args: [expr] })
  }

  _comparison(op, prop, extras) {
    return this._push({
      op,
      var: this._var,
      prop: normalizePropName(prop),
      ...extras,
    })
  }

  _group(mode, callback) {
    if (typeof callback !== 'function') {
      throw new TypeError(`${mode}() requires a callback`)
    }
    const nested = new PredicateBuilder(null, this._var, mode)
    callback(nested)
    const expr = nested._finalizeExpr()
    return this._push(expr)
  }
}

class QueryBuilder {
  constructor(db) {
    this._db = db
    this._matches = []
    this._edges = []
    this._predicate = null
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

  where(arg1, arg2) {
    if (arguments.length === 0) {
      throw new Error('where requires at least one argument')
    }
    if (arguments.length === 1 || typeof arg2 === 'function') {
      return this._wherePredicate(arg1, arg2)
    }
    return this._whereEdge(arg1, arg2)
  }

  _whereEdge(edgeType, target) {
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

  _wherePredicate(varName, builderFn) {
    if (typeof varName !== 'string' || varName.trim() === '') {
      throw new TypeError('where(var) requires a non-empty variable name')
    }
    this._assertMatch(varName)
    const builder = new PredicateBuilder(this, varName)
    if (typeof builderFn === 'function') {
      builderFn(builder)
      return builder.done()
    }
    return builder
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
        if ('prop' in field) {
          const varName = field.var
          const prop = field.prop
          if (typeof varName !== 'string' || !varName) {
            throw new TypeError('property projection requires a variable name')
          }
          if (typeof prop !== 'string' || !prop) {
            throw new TypeError('property projection requires a property name')
          }
          const alias = field.as ?? null
          if (alias !== null && alias !== undefined && typeof alias !== 'string') {
            throw new TypeError('property projection alias must be a string when provided')
          }
          projections.push({ kind: 'prop', var: varName, prop, alias })
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

  /** @internal */
  _build() {
    return this._buildSpec()
  }

  _appendPredicate(expr) {
    if (!this._predicate) {
      this._predicate = expr
      return
    }
    if (this._predicate.op === 'and') {
      this._predicate.args.push(expr)
    } else {
      this._predicate = { op: 'and', args: [this._predicate, expr] }
    }
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

  _assertMatch(varName) {
    const exists = this._matches.some((entry) => entry.var === varName)
    if (!exists) {
      throw new Error(`unknown variable '${varName}' - call match() first`)
    }
  }

  _nextAutoVar() {
    const name = autoVarName(this._nextVarIdx)
    this._nextVarIdx += 1
    return name
  }

  _buildSpec() {
    const spec = {
      $schemaVersion: 1,
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
      distinct: this._distinct,
      projections: this._projections.map((proj) => cloneSpec(proj)),
    }
    if (this._predicate) {
      spec.predicate = cloneSpec(this._predicate)
    }
    return spec
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

function normalizeIdList(value) {
  if (Array.isArray(value)) {
    return value
  }
  if (value && typeof value === 'object' && typeof value.length === 'number') {
    if (ArrayBuffer.isView(value)) {
      return Array.prototype.slice.call(value)
    }
  }
  return []
}

function createSummaryWithAliasHelper(summary) {
  const result = {
    nodes: normalizeIdList(summary?.nodes),
    edges: normalizeIdList(summary?.edges),
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
  PredicateBuilder,
  QueryBuilder,
  openDatabase,
  native,
}
