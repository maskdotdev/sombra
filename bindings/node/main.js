// Fluent query builder facade for the Sombra Node bindings.
const native = require('./index.js')

const CREATE_HANDLE_SYMBOL = Symbol('sombra.createHandle')
const EXPR_BRAND = Symbol('sombra.expr')
const NS_PER_MILLISECOND = 1_000_000n
const I64_MIN = -(1n << 63n)
const I64_MAX = (1n << 63n) - 1n
const ISO_DATETIME_REGEX =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?(Z|([+-])(\d{2}):(\d{2}))$/
const ISO_DATETIME_PREFIX = /^\d{4}-\d{2}-\d{2}T/
const MIN_DATETIME_NS = BigInt(Date.UTC(1900, 0, 1, 0, 0, 0)) * NS_PER_MILLISECOND
const MAX_DATETIME_NS = BigInt(Date.UTC(2100, 0, 1, 0, 0, 0)) * NS_PER_MILLISECOND

function autoVarName(idx) {
  return `n${idx}`
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

function isPlainObject(value) {
  if (!value || typeof value !== 'object') {
    return false
  }
  if (Array.isArray(value)) {
    return false
  }
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null
}

function normalizeRuntimeSchema(schema) {
  if (schema === null || schema === undefined) {
    return null
  }
  if (typeof schema !== 'object') {
    throw new TypeError('schema must be an object mapping labels to property maps')
  }
  const normalized = {}
  for (const [label, props] of Object.entries(schema)) {
    if (typeof label !== 'string' || label.trim() === '') {
      throw new TypeError('schema labels must be non-empty strings')
    }
    if (!props || typeof props !== 'object') {
      throw new TypeError(`schema entry for label '${label}' must be an object`)
    }
    normalized[label] = {}
    for (const propName of Object.keys(props)) {
      if (typeof propName !== 'string' || propName.trim() === '') {
        throw new TypeError(`schema for label '${label}' contains an invalid property name`)
      }
      normalized[label][propName] = props[propName]
    }
  }
  return normalized
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

function encodeBytesLiteral(bytes) {
  if (typeof Buffer !== 'undefined' && Buffer.from) {
    return Buffer.from(bytes).toString('base64')
  }
  if (typeof Uint8Array !== 'undefined') {
    const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)
    if (typeof btoa === 'function') {
      let binary = ''
      for (const byte of view) {
        binary += String.fromCharCode(byte)
      }
      return btoa(binary)
    }
  }
  throw new TypeError('bytes literals require Buffer or btoa support in this environment')
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
    return { t: 'Bytes', v: encodeBytesLiteral(value) }
  }
  if (value instanceof Uint8Array) {
    return { t: 'Bytes', v: encodeBytesLiteral(value) }
  }
  throw new TypeError(`unsupported literal type: ${typeof value}`)
}

function normalizePropName(prop) {
  if (typeof prop !== 'string' || prop.trim() === '') {
    throw new TypeError('property name must be a non-empty string')
  }
  return prop
}

function createExpr(node) {
  if (!node || typeof node !== 'object') {
    throw new TypeError('expression node must be an object')
  }
  return Object.freeze({ __expr: EXPR_BRAND, _node: node })
}

function isExpr(value) {
  return Boolean(value && value.__expr === EXPR_BRAND && value._node)
}

function unwrapExpr(value, ctx = 'expression') {
  if (!isExpr(value)) {
    throw new TypeError(`${ctx} must be created via the sombra expression helpers`)
  }
  return value._node
}

function andExpr(...exprs) {
  if (exprs.length === 0) {
    throw new TypeError('and() requires at least one expression')
  }
  const nodes = exprs.map((expr, idx) => unwrapExpr(expr, `and()[${idx}]`))
  return createExpr({ op: 'and', args: nodes })
}

function orExpr(...exprs) {
  if (exprs.length === 0) {
    throw new TypeError('or() requires at least one expression')
  }
  const nodes = exprs.map((expr, idx) => unwrapExpr(expr, `or()[${idx}]`))
  return createExpr({ op: 'or', args: nodes })
}

function notExpr(expr) {
  const node = unwrapExpr(expr, 'not() argument')
  return createExpr({ op: 'not', args: [node] })
}

function assertPropName(prop, ctx) {
  if (typeof prop !== 'string' || prop.trim() === '') {
    throw new TypeError(`${ctx} requires a non-empty property name`)
  }
  return prop
}

function comparisonExpr(op, prop, payload) {
  return createExpr({ op, prop: assertPropName(prop, op), ...payload })
}

function eqExpr(prop, value) {
  return comparisonExpr('eq', prop, { value })
}

function neExpr(prop, value) {
  return comparisonExpr('ne', prop, { value })
}

function ltExpr(prop, value) {
  return comparisonExpr('lt', prop, { value })
}

function leExpr(prop, value) {
  return comparisonExpr('le', prop, { value })
}

function gtExpr(prop, value) {
  return comparisonExpr('gt', prop, { value })
}

function geExpr(prop, value) {
  return comparisonExpr('ge', prop, { value })
}

function betweenExpr(prop, low, high, opts = undefined) {
  if (low === undefined || high === undefined) {
    throw new TypeError('between() requires both low and high values')
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
  return comparisonExpr('between', prop, { low, high, inclusive })
}

function inListExpr(prop, values) {
  if (!Array.isArray(values)) {
    throw new TypeError('inList() requires an array of literal values')
  }
  if (values.length === 0) {
    throw new TypeError('inList() requires at least one literal value')
  }
  values.forEach((value) => ensureScalarLiteral(value, 'inList()'))
  return comparisonExpr('in', prop, { values: [...values] })
}

function existsExpr(prop) {
  return comparisonExpr('exists', prop, {})
}

function isNullExpr(prop) {
  return comparisonExpr('isNull', prop, {})
}

function isNotNullExpr(prop) {
  return comparisonExpr('isNotNull', prop, {})
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

function normalizeExplainPayload(payload) {
  if (payload && typeof payload === 'object') {
    if (!('request_id' in payload)) {
      payload.request_id = null
    }
    if (payload.plan === undefined || payload.plan === null) {
      payload.plan = []
    } else if (Array.isArray(payload.plan)) {
      // ok
    } else if (payload.plan && typeof payload.plan === 'object') {
      payload.plan = [payload.plan]
    } else {
      throw new TypeError('plan must be an object or array when present')
    }
  }
  return payload
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
  constructor(parent, varName, mode = 'and', combinator = 'and', validator = null) {
    if (typeof varName !== 'string' || varName.trim() === '') {
      throw new TypeError('where(var) requires a non-empty variable name')
    }
    this._parent = parent ?? null
    this._var = varName
    this._mode = mode
    this._combinator = combinator
    this._validator = typeof validator === 'function' ? validator : null
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
    this._parent._appendPredicate(expr, this._combinator)
    return this._parent
  }

  _normalizeProp(prop) {
    if (this._validator) {
      return this._validator(prop)
    }
    return normalizePropName(prop)
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
      prop: this._normalizeProp(prop),
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
      prop: this._normalizeProp(prop),
      values: tagged,
    })
  }

  exists(prop) {
    return this._push({
      op: 'exists',
      var: this._var,
      prop: this._normalizeProp(prop),
    })
  }

  isNull(prop) {
    return this._push({
      op: 'isNull',
      var: this._var,
      prop: this._normalizeProp(prop),
    })
  }

  isNotNull(prop) {
    return this._push({
      op: 'isNotNull',
      var: this._var,
      prop: this._normalizeProp(prop),
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
    const nested = new PredicateBuilder(null, this._var, 'and', 'and', this._validator)
    callback(nested)
    const expr = nested._finalizeExpr()
    return this._push({ op: 'not', args: [expr] })
  }

  _comparison(op, prop, extras) {
    return this._push({
      op,
      var: this._var,
      prop: this._normalizeProp(prop),
      ...extras,
    })
  }

  _group(mode, callback) {
    if (typeof callback !== 'function') {
      throw new TypeError(`${mode}() requires a callback`)
    }
    const nested = new PredicateBuilder(null, this._var, mode, 'and', this._validator)
    callback(nested)
    const expr = nested._finalizeExpr()
    return this._push(expr)
  }
}

class NodeScope {
  constructor(builder, varName) {
    this._builder = builder
    this._var = varName
  }

  where(exprOrFn) {
    const stamped = this._stampExpr(exprOrFn, 'where()')
    this._builder._appendPredicate(stamped, 'and')
    return this
  }

  andWhere(exprOrFn) {
    const stamped = this._stampExpr(exprOrFn, 'andWhere()')
    this._builder._appendPredicate(stamped, 'and')
    return this
  }

  orWhere(exprOrFn) {
    const stamped = this._stampExpr(exprOrFn, 'orWhere()')
    this._builder._appendPredicate(stamped, 'or')
    return this
  }

  select(...keys) {
    if (keys.length === 0) {
      throw new TypeError('select() requires at least one property name')
    }
    ensureScopeLabel(this._builder, this._var)
    const validator = this._builder._makePropValidator(this._var)
    for (const key of keys) {
      const normalized = validator(key)
      this._builder._projections.push({ kind: 'prop', var: this._var, prop: normalized, alias: null })
    }
    return this
  }

  distinct() {
    this._builder.distinct(true)
    return this
  }

  direction(dir) {
    this._builder.direction(dir)
    return this
  }

  bidirectional(flag = true) {
    return this.direction(flag === false ? 'out' : 'both')
  }

  requestId(value) {
    this._builder.requestId(value)
    return this
  }

  explain(options) {
    return this._builder.explain(options)
  }

  execute(withMeta = false) {
    return this._builder.execute(withMeta)
  }

  stream() {
    return this._builder.stream()
  }

  _stampExpr(exprOrFn, ctx) {
    let exprValue = exprOrFn
    if (typeof exprValue === 'function') {
      exprValue = exprValue(this)
    }
    return stampExprForVar(this._builder, this._var, exprValue, ctx)
  }
}

function ensureScopeLabel(builder, varName) {
  const label = builder._labelForVar(varName)
  if (!label) {
    throw new Error(`variable '${varName}' requires a label before applying predicates`)
  }
  return label
}

function stampExprForVar(builder, varName, exprValue, ctx) {
  if (!exprValue) {
    throw new TypeError(`${ctx} requires an expression built via the sombra helpers`)
  }
  ensureScopeLabel(builder, varName)
  const node = unwrapExpr(exprValue, ctx)
  return translateExprNode(builder, varName, node, ctx)
}

function translateExprNode(builder, varName, node, ctx, validator = null) {
  if (!node || typeof node !== 'object') {
    throw new TypeError(`${ctx} must be built via the sombra expression helpers`)
  }
  const op = node.op
  if (op === 'and' || op === 'or') {
    if (!Array.isArray(node.args) || node.args.length === 0) {
      throw new TypeError(`${ctx} ${op}() requires at least one child expression`)
    }
    return {
      op,
      args: node.args.map((child, idx) => translateExprNode(builder, varName, child, `${ctx}.${op}[${idx}]`, validator)),
    }
  }
  if (op === 'not') {
    if (!Array.isArray(node.args) || node.args.length !== 1) {
      throw new TypeError(`${ctx} not() requires exactly one child expression`)
    }
    return { op: 'not', args: [translateExprNode(builder, varName, node.args[0], `${ctx}.not`, validator)] }
  }
  const propValidator = validator ?? builder._makePropValidator(varName)
  return translateComparisonNode(builder, varName, node, ctx, propValidator)
}

function translateComparisonNode(builder, varName, node, ctx, validator) {
  const prop = validator(node.prop)
  switch (node.op) {
    case 'eq':
    case 'ne':
    case 'lt':
    case 'le':
    case 'gt':
    case 'ge':
      if (node.value === undefined) {
        throw new TypeError(`${ctx} ${node.op}() requires a value`)
      }
      return {
        op: node.op,
        var: varName,
        prop,
        value: literalValue(node.value),
      }
    case 'between': {
      if (node.low === undefined || node.high === undefined) {
        throw new TypeError(`${ctx} between() requires both low and high values`)
      }
      const inclusive = normalizeInclusiveTuple(node.inclusive)
      return {
        op: 'between',
        var: varName,
        prop,
        low: literalValue(node.low),
        high: literalValue(node.high),
        inclusive,
      }
    }
    case 'in': {
      if (!Array.isArray(node.values) || node.values.length === 0) {
        throw new TypeError(`${ctx} inList() requires at least one literal value`)
      }
      const tagged = convertInListValues(node.values)
      return {
        op: 'in',
        var: varName,
        prop,
        values: tagged,
      }
    }
    case 'exists':
    case 'isNull':
    case 'isNotNull':
      return { op: node.op, var: varName, prop }
    default:
      throw new Error(`unsupported expression operator '${node.op}'`)
  }
}

function normalizeInclusiveTuple(tuple) {
  if (tuple === undefined) {
    return [true, true]
  }
  if (
    !Array.isArray(tuple) ||
    tuple.length !== 2 ||
    typeof tuple[0] !== 'boolean' ||
    typeof tuple[1] !== 'boolean'
  ) {
    throw new TypeError('between().inclusive must be a [boolean, boolean] tuple')
  }
  return tuple
}

function convertInListValues(values) {
  const tagged = values.map((value) => {
    ensureScalarLiteral(value, 'inList()')
    return literalValue(value)
  })
  const exemplar = tagged.find((entry) => entry.t !== 'Null')
  if (exemplar) {
    for (const entry of tagged) {
      if (entry.t !== 'Null' && entry.t !== exemplar.t) {
        throw new TypeError('inList() requires all literals to share the same type')
      }
    }
  }
  return tagged
}

class QueryBuilder {
  constructor(db, schema = null) {
    this._db = db
    this._schema = schema ?? null
    this._matches = []
    this._edges = []
    this._predicate = null
    this._projections = []
    this._distinct = false
    this._lastVar = null
    this._nextVarIdx = 0
    this._pendingDirection = 'out'
    this._requestId = null
  }

  nodes(label) {
    if (typeof label !== 'string' || label.trim() === '') {
      throw new TypeError('nodes(label) requires a non-empty label string')
    }
    const varName = this._nextAutoVar()
    this._ensureMatch(varName, label)
    this._lastVar = varName
    return new NodeScope(this, varName)
  }

  match(target) {
    if (isPlainObject(target)) {
      const hasVarKey = Object.prototype.hasOwnProperty.call(target, 'var')
      const hasLabelKey = Object.prototype.hasOwnProperty.call(target, 'label')
      if (!hasVarKey && !hasLabelKey) {
        return this._matchMap(target)
      }
    }
    const fallback = this._nextAutoVar()
    const normalized = normalizeTarget(target, fallback)
    this._ensureMatch(normalized.var, normalized.label)
    this._lastVar = normalized.var
    return this
  }

  on(varName, callback) {
    if (typeof varName !== 'string' || varName.trim() === '') {
      throw new TypeError('on(var, fn) requires a non-empty variable name')
    }
    if (typeof callback !== 'function') {
      throw new TypeError('on(var, fn) requires a callback function')
    }
    this._assertMatch(varName)
    ensureScopeLabel(this, varName)
    const scope = new NodeScope(this, varName)
    callback(scope)
    return this
  }

  _matchMap(targetMap) {
    const entries = Object.entries(targetMap)
    if (entries.length === 0) {
      throw new TypeError('match({...}) requires at least one entry')
    }
    for (const [varNameRaw, value] of entries) {
      if (typeof varNameRaw !== 'string' || varNameRaw.trim() === '') {
        throw new TypeError('match({...}) keys must be non-empty strings')
      }
      const varName = varNameRaw
      let normalized
      if (typeof value === 'string') {
        normalized = { var: varName, label: value }
      } else if (value === null || value === undefined) {
        normalized = { var: varName, label: null }
      } else if (isPlainObject(value)) {
        normalized = normalizeTarget({ ...value, var: varName }, varName)
      } else {
        throw new TypeError("match({...}) values must be label strings or objects with optional 'label'")
      }
      this._ensureMatch(normalized.var, normalized.label)
    }
    const lastEntry = entries[entries.length - 1]
    if (lastEntry) {
      this._lastVar = lastEntry[0]
    }
    return this
  }

  where(arg1, arg2) {
    if (arguments.length === 0) {
      throw new Error('where requires at least one argument')
    }
    if (arguments.length === 1 || typeof arg2 === 'function') {
      return this._wherePredicate(arg1, arg2, 'and')
    }
    return this._whereEdge(arg1, arg2)
  }

  andWhere(varName, builderFn) {
    return this._wherePredicate(varName, builderFn, 'and')
  }

  orWhere(varName, builderFn) {
    return this._wherePredicate(varName, builderFn, 'or')
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

  _wherePredicate(varName, builderFn, combinator = 'and') {
    if (typeof varName !== 'string' || varName.trim() === '') {
      throw new TypeError('where(var) requires a non-empty variable name')
    }
    this._assertMatch(varName)
    const validator = this._makePropValidator(varName)
    const builder = new PredicateBuilder(this, varName, 'and', combinator, validator)
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

  requestId(value) {
    if (value === null || value === undefined) {
      this._requestId = null
      return this
    }
    if (typeof value !== 'string') {
      throw new TypeError('requestId requires a string value')
    }
    const trimmed = value.trim()
    if (trimmed === '') {
      throw new TypeError('requestId requires a non-empty string')
    }
    this._requestId = trimmed
    return this
  }

  select(fields) {
    const projections = []
    for (const field of fields) {
      if (typeof field === 'string') {
        this._assertMatch(field)
        projections.push({ kind: 'var', var: field, alias: null })
        continue
      }
      if (field && typeof field === 'object') {
        if ('prop' in field) {
          const varName = field.var
          const prop = field.prop
          if (typeof varName !== 'string' || !varName) {
            throw new TypeError('property projection requires a variable name')
          }
          if (typeof prop !== 'string' || !prop) {
            throw new TypeError('property projection requires a property name')
          }
          this._assertMatch(varName)
          const validator = this._makePropValidator(varName)
          const normalizedProp = validator(prop)
          const alias = field.as ?? null
          if (alias !== null && alias !== undefined && typeof alias !== 'string') {
            throw new TypeError('property projection alias must be a string when provided')
          }
          projections.push({ kind: 'prop', var: varName, prop: normalizedProp, alias })
          continue
        }
        if ('var' in field) {
          const varName = field.var
          this._assertMatch(varName)
          const alias = field.as ?? null
          projections.push({ kind: 'var', var: varName, alias })
          continue
        }
        if ('expr' in field) {
          throw new TypeError('expression projections are not supported; use property projections instead')
        }
      }
      throw new TypeError('unsupported projection field')
    }
    this._projections = projections
    return this
  }

  async explain(options) {
    const spec = this._build()
    if (options && options.redactLiterals) {
      spec.redact_literals = true
    }
    return this._db._explain(spec)
  }

  async execute(withMeta = false) {
    const payload = await this._db._execute(this._build())
    if (withMeta) {
      return payload
    }
    const rows = payload && Array.isArray(payload.rows) ? payload.rows : null
    if (!rows) {
      throw new Error('query execution payload missing rows array')
    }
    return rows
  }

  stream() {
    const handle = this._db._stream(this._build())
    return new QueryStream(handle)
  }

  /** @internal */
  _build() {
    return this._buildSpec()
  }

  _appendPredicate(expr, combinator = 'and') {
    if (!this._predicate) {
      this._predicate = expr
      return
    }
    if (combinator === 'and') {
      if (this._predicate.op === 'and') {
        this._predicate.args.push(expr)
      } else {
        this._predicate = { op: 'and', args: [this._predicate, expr] }
      }
      return
    }
    if (combinator === 'or') {
      if (this._predicate.op === 'or') {
        this._predicate.args.push(expr)
      } else {
        this._predicate = { op: 'or', args: [this._predicate, expr] }
      }
      return
    }
    throw new Error(`unsupported predicate combinator '${combinator}'`)
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
    const projections =
      this._projections.length > 0
        ? this._projections
        : this._matches.map((clause) => ({ kind: 'var', var: clause.var, alias: null }))
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
      projections: projections.map((proj) => cloneSpec(proj)),
    }
    if (this._requestId) {
      spec.request_id = this._requestId
    }
    if (this._predicate) {
      spec.predicate = cloneSpec(this._predicate)
    }
    return spec
  }

  _labelForVar(varName) {
    const entry = this._matches.find((clause) => clause.var === varName)
    return entry && entry.label ? entry.label : null
  }

  _makePropValidator(varName) {
    const hasSchema = this._schema && typeof this._schema === 'object'
    if (!hasSchema) {
      return (prop) => normalizePropName(prop)
    }
    const label = this._labelForVar(varName)
    if (!label) {
      return (prop) => normalizePropName(prop)
    }
    const labelSchema = this._schema[label]
    if (!labelSchema || typeof labelSchema !== 'object') {
      return (prop) => normalizePropName(prop)
    }
    return (prop) => {
      const normalized = normalizePropName(prop)
      if (!Object.prototype.hasOwnProperty.call(labelSchema, normalized)) {
        throw new Error(`Unknown property '${normalized}' on label '${label}'`)
      }
      return normalized
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
  constructor(handle, schema = null) {
    this._handle = handle
    this._schema = null
    if (schema !== null && schema !== undefined) {
      this.withSchema(schema)
    }
  }

  static open(path, options) {
    let schema = null
    let connectOptions = options ?? undefined
    if (options && typeof options === 'object' && Object.prototype.hasOwnProperty.call(options, 'schema')) {
      const { schema: schemaValue, ...rest } = options
      schema = schemaValue ?? null
      connectOptions = Object.keys(rest).length > 0 ? rest : undefined
    }
    const handle = native.openDatabase(path, connectOptions ?? undefined)
    return new Database(handle, schema)
  }

  query() {
    return new QueryBuilder(this, this._schema)
  }

  withSchema(schema) {
    this._schema = normalizeRuntimeSchema(schema)
    return this
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

  cancelRequest(requestId) {
    if (typeof requestId !== 'string' || requestId.trim() === '') {
      throw new TypeError('cancelRequest requires a non-empty request id string')
    }
    return native.databaseCancelRequest(this._handle, requestId)
  }

  _execute(spec) {
    return native.databaseExecute(this._handle, spec)
  }

  _explain(spec) {
    const payload = native.databaseExplain(this._handle, spec)
    return normalizeExplainPayload(payload)
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
  NodeScope,
  openDatabase,
  native,
  and: andExpr,
  or: orExpr,
  not: notExpr,
  eq: eqExpr,
  ne: neExpr,
  lt: ltExpr,
  le: leExpr,
  gt: gtExpr,
  ge: geExpr,
  between: betweenExpr,
  inList: inListExpr,
  exists: existsExpr,
  isNull: isNullExpr,
  isNotNull: isNotNullExpr,
}
