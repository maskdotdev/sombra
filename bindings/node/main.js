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

// Error code regex: [CODE_NAME] message
const ERROR_CODE_REGEX = /^\[([A-Z_]+)\]\s*/

/**
 * Error codes returned by the Sombra database engine.
 * These match the ErrorCode enum in the FFI layer.
 */
const ErrorCode = Object.freeze({
  UNKNOWN: 'UNKNOWN',
  MESSAGE: 'MESSAGE',
  ANALYZER: 'ANALYZER',
  JSON: 'JSON',
  IO: 'IO',
  CORRUPTION: 'CORRUPTION',
  CONFLICT: 'CONFLICT',
  SNAPSHOT_TOO_OLD: 'SNAPSHOT_TOO_OLD',
  CANCELLED: 'CANCELLED',
  INVALID_ARG: 'INVALID_ARG',
  NOT_FOUND: 'NOT_FOUND',
  CLOSED: 'CLOSED',
})

/**
 * Base error class for all Sombra database errors.
 * 
 * @example
 * try {
 *   db.query().nodes('User').execute();
 * } catch (err) {
 *   if (err instanceof SombraError) {
 *     console.log(`Error code: ${err.code}`);
 *   }
 * }
 */
class SombraError extends Error {
  /**
   * Creates a new SombraError.
   * @param {string} message - The error message
   * @param {string} [code=ErrorCode.UNKNOWN] - The error code from ErrorCode constants
   */
  constructor(message, code = ErrorCode.UNKNOWN) {
    super(message)
    /** @type {string} */
    this.name = 'SombraError'
    /** @type {string} */
    this.code = code
    Error.captureStackTrace?.(this, this.constructor)
  }
}

/**
 * Error thrown when a query analysis fails.
 */
class AnalyzerError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.ANALYZER)
    this.name = 'AnalyzerError'
  }
}

/**
 * Error thrown when JSON serialization/deserialization fails.
 */
class JsonError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.JSON)
    this.name = 'JsonError'
  }
}

/**
 * Error thrown when an I/O operation fails.
 */
class IoError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.IO)
    this.name = 'IoError'
  }
}

/**
 * Error thrown when data corruption is detected.
 */
class CorruptionError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.CORRUPTION)
    this.name = 'CorruptionError'
  }
}

/**
 * Error thrown when a transaction conflict occurs (write-write conflict).
 */
class ConflictError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.CONFLICT)
    this.name = 'ConflictError'
  }
}

/**
 * Error thrown when a snapshot is too old for an MVCC read.
 */
class SnapshotTooOldError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.SNAPSHOT_TOO_OLD)
    this.name = 'SnapshotTooOldError'
  }
}

/**
 * Error thrown when an operation is cancelled.
 */
class CancelledError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.CANCELLED)
    this.name = 'CancelledError'
  }
}

/**
 * Error thrown when an invalid argument is provided.
 */
class InvalidArgError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.INVALID_ARG)
    this.name = 'InvalidArgError'
  }
}

/**
 * Error thrown when a requested resource is not found.
 */
class NotFoundError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.NOT_FOUND)
    this.name = 'NotFoundError'
  }
}

/**
 * Error thrown when operations are attempted on a closed database.
 */
class ClosedError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.CLOSED)
    this.name = 'ClosedError'
  }
}

/**
 * Map of error code strings to their corresponding error classes.
 */
const ERROR_CLASS_MAP = {
  [ErrorCode.UNKNOWN]: SombraError,
  [ErrorCode.MESSAGE]: SombraError,
  [ErrorCode.ANALYZER]: AnalyzerError,
  [ErrorCode.JSON]: JsonError,
  [ErrorCode.IO]: IoError,
  [ErrorCode.CORRUPTION]: CorruptionError,
  [ErrorCode.CONFLICT]: ConflictError,
  [ErrorCode.SNAPSHOT_TOO_OLD]: SnapshotTooOldError,
  [ErrorCode.CANCELLED]: CancelledError,
  [ErrorCode.INVALID_ARG]: InvalidArgError,
  [ErrorCode.NOT_FOUND]: NotFoundError,
  [ErrorCode.CLOSED]: ClosedError,
}

/**
 * Parses an error message from the native layer and returns a typed error.
 * Native errors have format: "[CODE_NAME] actual message"
 *
 * @param {Error|string} err - The error from the native layer
 * @returns {SombraError} A typed error instance (e.g., AnalyzerError, IoError, etc.)
 * @example
 * try {
 *   nativeOperation();
 * } catch (err) {
 *   throw wrapNativeError(err);
 * }
 */
function wrapNativeError(err) {
  const message = err instanceof Error ? err.message : String(err)
  const match = ERROR_CODE_REGEX.exec(message)

  if (match) {
    const code = match[1]
    const cleanMessage = message.slice(match[0].length)
    const ErrorClass = ERROR_CLASS_MAP[code] ?? SombraError
    return new ErrorClass(cleanMessage, code)
  }

  // No code prefix, return as generic SombraError
  if (err instanceof SombraError) {
    return err
  }
  return new SombraError(message, ErrorCode.UNKNOWN)
}

function callNative(fn, ...args) {
  try {
    return fn(...args)
  } catch (err) {
    throw wrapNativeError(err)
  }
}

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

function emptyMutationSummary() {
  return {
    createdNodes: [],
    createdEdges: [],
    updatedNodes: 0,
    updatedEdges: 0,
    deletedNodes: 0,
    deletedEdges: 0,
  }
}

function mergeMutationSummaries(a, b) {
  const lhs = a ?? emptyMutationSummary()
  const rhs = b ?? emptyMutationSummary()
  return {
    createdNodes: [...(lhs.createdNodes ?? []), ...(rhs.createdNodes ?? [])],
    createdEdges: [...(lhs.createdEdges ?? []), ...(rhs.createdEdges ?? [])],
    updatedNodes: (lhs.updatedNodes ?? 0) + (rhs.updatedNodes ?? 0),
    updatedEdges: (lhs.updatedEdges ?? 0) + (rhs.updatedEdges ?? 0),
    deletedNodes: (lhs.deletedNodes ?? 0) + (rhs.deletedNodes ?? 0),
    deletedEdges: (lhs.deletedEdges ?? 0) + (rhs.deletedEdges ?? 0),
  }
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
    this._closed = false
  }

  [Symbol.asyncIterator]() {
    return this
  }

  async next() {
    if (this._closed) {
      return { done: true, value: undefined }
    }
    const value = callNative(this._handle.next.bind(this._handle))
    if (value === undefined || value === null) {
      this.close()
      return { done: true, value: undefined }
    }
    return { done: false, value }
  }

  async return() {
    this.close()
    return { done: true, value: undefined }
  }

  close() {
    if (this._closed) {
      return
    }
    this._closed = true
    if (typeof this._handle.close === 'function') {
      callNative(this._handle.close.bind(this._handle))
    }
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
    this._db._assertOpen()
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
    const summary = callNative(native.databaseCreate, this._db._handle, script)
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

// ============================================================================
// Typed Batch API - High-performance bulk creation bypassing JSON
// ============================================================================

const TYPED_BATCH_HANDLE_SYMBOL = Symbol('sombra.typedBatchHandle')

/**
 * Error thrown when typed batch operations fail.
 */
class BatchError extends SombraError {
  constructor(message) {
    super(message, ErrorCode.INVALID_ARG)
    this.name = 'BatchError'
  }
}

/**
 * Converts a JavaScript value to a typed property entry for the FFI layer.
 * @param {string} key - Property key name
 * @param {*} value - Property value
 * @returns {object} TypedPropEntry for FFI
 */
function toTypedPropEntry(key, value) {
  if (typeof key !== 'string' || key.trim() === '') {
    throw new BatchError('property key must be a non-empty string')
  }

  if (value === null || value === undefined) {
    return { key, kind: 'null' }
  }
  if (typeof value === 'boolean') {
    return { key, kind: 'bool', boolValue: value }
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new BatchError(`property '${key}' must be a finite number`)
    }
    if (Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        throw new BatchError(`property '${key}' integer exceeds safe integer range`)
      }
      return { key, kind: 'int', intValue: value }
    }
    return { key, kind: 'float', floatValue: value }
  }
  if (typeof value === 'string') {
    return { key, kind: 'string', stringValue: value }
  }
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
    return { key, kind: 'bytes', bytesValue: value.toString('base64') }
  }
  if (value instanceof Uint8Array) {
    return { key, kind: 'bytes', bytesValue: encodeBytesLiteral(value) }
  }
  throw new BatchError(`unsupported property type for '${key}': ${typeof value}`)
}

/**
 * Converts a props object to an array of typed property entries.
 * @param {object} props - Property object
 * @returns {Array} Array of TypedPropEntry
 */
function toTypedProps(props) {
  if (!props || typeof props !== 'object') {
    return []
  }
  const entries = []
  for (const [key, value] of Object.entries(props)) {
    entries.push(toTypedPropEntry(key, value))
  }
  return entries
}

/**
 * Handle returned when adding nodes to a BatchCreateBuilder.
 * Can be used as edge source/destination reference and supports chaining.
 */
class BatchNodeHandle {
  constructor(builder, index) {
    this._builder = builder
    this._index = index
    this[TYPED_BATCH_HANDLE_SYMBOL] = true
  }

  get index() {
    return this._index
  }

  /**
   * Adds another node to the batch.
   * @param {string} label - Node label
   * @param {object} [props={}] - Node properties
   * @param {string} [alias] - Optional alias starting with '$'
   * @returns {BatchNodeHandle} Handle for edge references
   */
  node(label, props = {}, alias = undefined) {
    return this._builder.node(label, props, alias)
  }

  /**
   * Adds a node with an alias (convenience method).
   * @param {string} label - Node label
   * @param {string} alias - Alias starting with '$'
   * @param {object} [props={}] - Node properties
   * @returns {BatchNodeHandle} Handle for edge references
   */
  nodeWithAlias(label, alias, props = {}) {
    return this._builder.nodeWithAlias(label, alias, props)
  }

  /**
   * Adds an edge to the batch.
   * @param {string|BatchNodeHandle|number|bigint} src - Source node
   * @param {string} ty - Edge type
   * @param {string|BatchNodeHandle|number|bigint} dst - Destination node
   * @param {object} [props={}] - Edge properties
   * @returns {BatchCreateBuilder} Builder for chaining
   */
  edge(src, ty, dst, props = {}) {
    return this._builder.edge(src, ty, dst, props)
  }

  /**
   * Executes the batch, creating all nodes and edges atomically.
   * @returns {object} Result with nodes[], edges[], aliases{}, and alias() helper
   */
  execute() {
    return this._builder.execute()
  }
}

/**
 * High-performance batch builder for creating nodes and edges.
 * 
 * Unlike the standard CreateBuilder, this bypasses JSON serialization
 * by passing typed values directly to the Rust FFI layer, providing
 * 3-5x better performance for bulk operations (10K+ records).
 * 
 * @example
 * const result = db.batchCreate()
 *   .node('User', { name: 'Alice', age: 30 }, '$alice')
 *   .node('User', { name: 'Bob', age: 25 }, '$bob')
 *   .edge('$alice', 'KNOWS', '$bob', { since: 2020 })
 *   .execute();
 * 
 * console.log(result.nodes);     // [1, 2]
 * console.log(result.alias('$alice')); // 1
 */
class BatchCreateBuilder {
  constructor(db) {
    this._db = db
    this._nodes = []
    this._edges = []
    this._aliasSet = new Set()
    this._sealed = false
  }

  /**
   * Adds a node to the batch.
   * @param {string} label - Node label (single label for performance)
   * @param {object} [props={}] - Node properties
   * @param {string} [alias] - Optional alias starting with '$' for edge references
   * @returns {BatchNodeHandle} Handle that can be used for edge references
   */
  node(label, props = {}, alias = undefined) {
    this._ensureMutable()

    if (typeof label !== 'string' || label.trim() === '') {
      throw new BatchError('node label must be a non-empty string')
    }

    if (alias !== undefined) {
      if (typeof alias !== 'string') {
        throw new BatchError('node alias must be a string')
      }
      if (alias.length === 0) {
        throw new BatchError('node alias must be non-empty')
      }
      if (!alias.startsWith('$')) {
        throw new BatchError("node alias must start with '$' prefix")
      }
      if (this._aliasSet.has(alias)) {
        throw new BatchError(`duplicate alias '${alias}'`)
      }
      this._aliasSet.add(alias)
    }

    const nodeSpec = {
      label,
      props: toTypedProps(props),
    }
    if (alias !== undefined) {
      nodeSpec.alias = alias
    }

    this._nodes.push(nodeSpec)

    return new BatchNodeHandle(this, this._nodes.length - 1)
  }

  /**
   * Adds a node with an alias (convenience method).
   * @param {string} label - Node label
   * @param {string} alias - Alias starting with '$'
   * @param {object} [props={}] - Node properties
   * @returns {BatchNodeHandle} Handle for edge references
   */
  nodeWithAlias(label, alias, props = {}) {
    return this.node(label, props, alias)
  }

  /**
   * Adds an edge to the batch.
   * @param {string|BatchNodeHandle|number|bigint} src - Source node (alias, handle, or ID)
   * @param {string} ty - Edge type
   * @param {string|BatchNodeHandle|number|bigint} dst - Destination node (alias, handle, or ID)
   * @param {object} [props={}] - Edge properties
   * @returns {BatchCreateBuilder} this for chaining
   */
  edge(src, ty, dst, props = {}) {
    this._ensureMutable()

    if (typeof ty !== 'string' || ty.trim() === '') {
      throw new BatchError('edge type must be a non-empty string')
    }

    this._edges.push({
      ty,
      src: this._encodeNodeRef(src, 'edge src'),
      dst: this._encodeNodeRef(dst, 'edge dst'),
      props: toTypedProps(props),
    })

    return this
  }

  /**
   * Executes the batch, creating all nodes and edges atomically.
   * @returns {object} Result with nodes[], edges[], aliases{}, and alias() helper
   */
  execute() {
    this._ensureMutable()
    this._db._assertOpen()
    this._sealed = true

    const spec = {
      nodes: this._nodes,
      edges: this._edges,
    }

    const result = callNative(native.databaseCreateTypedBatch, this._db._handle, spec)
    return createSummaryWithAliasHelper(result)
  }

  /**
   * Encodes a node reference for the FFI layer.
   * @private
   */
  _encodeNodeRef(value, ctx) {
    // Handle (index reference to a node in this batch)
    if (value && value[TYPED_BATCH_HANDLE_SYMBOL]) {
      return { kind: 'handle', handle: value.index }
    }

    // Alias (string starting with '$')
    if (typeof value === 'string') {
      if (value.length === 0) {
        throw new BatchError(`${ctx} alias must be non-empty`)
      }
      if (!value.startsWith('$')) {
        throw new BatchError(`${ctx} alias must start with '$' prefix`)
      }
      return { kind: 'alias', alias: value }
    }

    // Numeric ID
    if (typeof value === 'number') {
      if (!Number.isInteger(value) || value < 0) {
        throw new BatchError(`${ctx} id must be a non-negative integer`)
      }
      return { kind: 'id', id: BigInt(value) }
    }

    // BigInt ID
    if (typeof value === 'bigint') {
      if (value < 0n) {
        throw new BatchError(`${ctx} id must be a non-negative integer`)
      }
      return { kind: 'id', id: value }
    }

    throw new BatchError(
      `${ctx} must be a BatchNodeHandle, alias string (starting with '$'), or numeric id`,
    )
  }

  _ensureMutable() {
    if (this._sealed) {
      throw new BatchError('batch already executed')
    }
  }
}

function normalizeIdList(value, ctx = 'id list') {
  const target = []
  if (Array.isArray(value)) {
    for (let idx = 0; idx < value.length; idx += 1) {
      target.push(normalizeSingleId(value[idx], `${ctx}[${idx}]`))
    }
    return target
  }
  if (value && typeof value === 'object' && typeof value.length === 'number' && ArrayBuffer.isView(value)) {
    const view = Array.prototype.slice.call(value)
    for (let idx = 0; idx < view.length; idx += 1) {
      target.push(normalizeSingleId(view[idx], `${ctx}[${idx}]`))
    }
    return target
  }
  return target
}

function normalizeSingleId(value, ctx) {
  if (typeof value === 'number') {
    if (!Number.isInteger(value) || value < 0) {
      throw new TypeError(`${ctx} must be a non-negative integer`)
    }
    if (!Number.isSafeInteger(value)) {
      throw new RangeError(`${ctx} exceeds the safe integer range`)
    }
    return value
  }
  if (typeof value === 'bigint') {
    if (value < 0n) {
      throw new TypeError(`${ctx} must be a non-negative integer`)
    }
    const max = BigInt(Number.MAX_SAFE_INTEGER)
    if (value > max) {
      throw new RangeError(`${ctx} exceeds the safe integer range`)
    }
    return Number(value)
  }
  throw new TypeError(`${ctx} must contain numeric ids`)
}

function createSummaryWithAliasHelper(summary) {
  const result = {
    nodes: normalizeIdList(summary?.nodes, 'mutation summary nodes'),
    edges: normalizeIdList(summary?.edges, 'mutation summary edges'),
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

// ============================================================================
// Auto-tune batch size estimation
// ============================================================================

/**
 * Default options for batch size estimation.
 */
const DEFAULT_ESTIMATE_OPTIONS = {
  sampleSize: 100,
  targetBatchBytes: 64 * 1024, // 64KB target batch size
  minBatchSize: 100,
  maxBatchSize: 10000,
}

/**
 * Estimates the byte size of a single record (node or edge spec).
 * This is a rough heuristic based on property serialization overhead.
 * @param {object} record - A sample node/edge record
 * @returns {number} Estimated bytes
 */
function estimateRecordBytes(record) {
  if (!record || typeof record !== 'object') {
    return 64 // minimum overhead for empty record
  }

  let bytes = 32 // base overhead for struct/pointers

  // Label overhead
  if (record.label) {
    bytes += 8 + record.label.length * 2 // string overhead + chars
  }
  if (record.labels && Array.isArray(record.labels)) {
    for (const label of record.labels) {
      bytes += 8 + (typeof label === 'string' ? label.length * 2 : 0)
    }
  }

  // Edge type overhead
  if (record.ty) {
    bytes += 8 + record.ty.length * 2
  }

  // Property overhead
  const props = record.props
  if (props && typeof props === 'object') {
    if (Array.isArray(props)) {
      // TypedPropEntry array format
      for (const entry of props) {
        bytes += 16 // key pointer + kind discriminant
        if (entry.key) bytes += entry.key.length * 2
        if (entry.kind === 'string' && entry.stringValue) {
          bytes += 8 + entry.stringValue.length * 2
        } else if (entry.kind === 'bytes' && entry.bytesValue) {
          bytes += 8 + entry.bytesValue.length
        } else if (entry.kind === 'int' || entry.kind === 'float') {
          bytes += 8
        } else {
          bytes += 1 // bool/null
        }
      }
    } else {
      // Plain object format
      for (const [key, value] of Object.entries(props)) {
        bytes += 16 + key.length * 2 // key overhead
        if (value === null || value === undefined) {
          bytes += 1
        } else if (typeof value === 'boolean') {
          bytes += 1
        } else if (typeof value === 'number') {
          bytes += 8
        } else if (typeof value === 'string') {
          bytes += 8 + value.length * 2
        } else if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
          bytes += 8 + value.length
        }
      }
    }
  }

  return bytes
}

/**
 * Estimates the optimal batch size based on sample records.
 * 
 * This function samples records to estimate their average byte size,
 * then calculates how many records should fit in the target batch size
 * to balance throughput and memory usage.
 * 
 * @param {Array<object>} sampleRecords - Sample node/edge specs to measure
 * @param {object} [options] - Estimation options
 * @param {number} [options.sampleSize=100] - Max records to sample (uses all if fewer)
 * @param {number} [options.targetBatchBytes=65536] - Target batch size in bytes (64KB default)
 * @param {number} [options.minBatchSize=100] - Minimum batch size to return
 * @param {number} [options.maxBatchSize=10000] - Maximum batch size to return
 * @returns {number} Estimated optimal batch size
 * 
 * @example
 * // Estimate batch size for user nodes
 * const sampleUsers = [
 *   { label: 'User', props: { name: 'Alice', age: 30 } },
 *   { label: 'User', props: { name: 'Bob', email: 'bob@example.com' } },
 * ];
 * const batchSize = estimateBatchSize(sampleUsers);
 * // Returns ~1000-2000 depending on average property size
 * 
 * @example
 * // Estimate with custom options for larger batches
 * const batchSize = estimateBatchSize(samples, {
 *   targetBatchBytes: 256 * 1024, // 256KB batches
 *   maxBatchSize: 50000,
 * });
 */
function estimateBatchSize(sampleRecords, options = {}) {
  const opts = { ...DEFAULT_ESTIMATE_OPTIONS, ...options }

  if (!Array.isArray(sampleRecords) || sampleRecords.length === 0) {
    return opts.minBatchSize
  }

  // Sample records for estimation
  const samplesToUse = Math.min(sampleRecords.length, opts.sampleSize)
  let totalBytes = 0

  // If we have more records than sample size, pick evenly distributed samples
  const step = sampleRecords.length <= samplesToUse ? 1 : Math.floor(sampleRecords.length / samplesToUse)

  let sampledCount = 0
  for (let i = 0; i < sampleRecords.length && sampledCount < samplesToUse; i += step) {
    totalBytes += estimateRecordBytes(sampleRecords[i])
    sampledCount++
  }

  if (sampledCount === 0) {
    return opts.minBatchSize
  }

  const avgBytesPerRecord = totalBytes / sampledCount
  
  // Avoid division by zero
  if (avgBytesPerRecord <= 0) {
    return opts.minBatchSize
  }

  const estimated = Math.floor(opts.targetBatchBytes / avgBytesPerRecord)

  // Clamp to min/max bounds
  return Math.max(opts.minBatchSize, Math.min(opts.maxBatchSize, estimated))
}

/**
 * Creates a generator function that yields batches of optimal size.
 * Useful for processing large arrays in memory-efficient chunks.
 * 
 * @param {Array<object>} records - All records to batch
 * @param {object} [options] - Estimation options (same as estimateBatchSize)
 * @returns {Generator<Array<object>>} Generator yielding batches
 * 
 * @example
 * const users = generateLargeUserArray(100000);
 * for (const batch of batchRecords(users)) {
 *   const result = db.batchCreate();
 *   for (const user of batch) {
 *     result.node('User', user.props);
 *   }
 *   result.execute();
 * }
 */
function* batchRecords(records, options = {}) {
  if (!Array.isArray(records) || records.length === 0) {
    return
  }

  const batchSize = estimateBatchSize(records, options)

  for (let i = 0; i < records.length; i += batchSize) {
    yield records.slice(i, i + batchSize)
  }
}

/**
 * Main database class for interacting with a Sombra graph database.
 * 
 * @example
 * // Opening and using a database
 * const db = Database.open('/path/to/db');
 * db.seedDemo();
 * const users = await db.query().nodes('User').execute();
 * db.close();
 * 
 * @example
 * // With schema validation
 * const db = Database.open('/path/to/db', {
 *   schema: { User: { name: { type: 'string' } } }
 * });
 */
class Database {
  constructor(handle, schema = null) {
    this._handle = handle
    this._schema = null
    /** @type {boolean} */
    this._closed = false
    if (schema !== null && schema !== undefined) {
      this.withSchema(schema)
    }
  }

  /**
   * Opens a database at the specified path.
   * @param {string} path - The path to the database file
   * @param {object} [options] - Optional configuration
   * @param {object} [options.schema] - Runtime schema for validation
   * @returns {Database} A new Database instance
   */
  static open(path, options) {
    let schema = null
    let connectOptions = options ?? undefined
    if (options && typeof options === 'object' && Object.prototype.hasOwnProperty.call(options, 'schema')) {
      const { schema: schemaValue, ...rest } = options
      schema = schemaValue ?? null
      connectOptions = Object.keys(rest).length > 0 ? rest : undefined
    }
    const handle = callNative(native.openDatabase, path, connectOptions ?? undefined)
    return new Database(handle, schema)
  }

  /**
   * Closes the database, releasing all resources.
   * After calling close(), all subsequent operations on this instance will fail.
   * Calling close() multiple times is safe (subsequent calls are no-ops).
   * @returns {void}
   */
  close() {
    if (this._closed) {
      return
    }
    callNative(native.databaseClose, this._handle)
    this._closed = true
  }

  /**
   * Returns true if the database has been closed.
   * @type {boolean}
   */
  get isClosed() {
    return this._closed
  }

  _assertOpen() {
    if (this._closed) {
      throw new ClosedError('database is closed')
    }
  }

  query() {
    this._assertOpen()
    return new QueryBuilder(this, this._schema)
  }

  withSchema(schema) {
    this._assertOpen()
    this._schema = normalizeRuntimeSchema(schema)
    return this
  }

  create() {
    this._assertOpen()
    return new CreateBuilder(this)
  }

  /**
   * Creates a high-performance batch builder for bulk node/edge creation.
   * 
   * This method bypasses JSON serialization, providing 3-5x better performance
   * for bulk operations with 10K+ records.
   * 
   * @returns {BatchCreateBuilder} A builder for staging nodes and edges
   * @example
   * const result = db.batchCreate()
   *   .node('User', { name: 'Alice' }, '$alice')
   *   .node('User', { name: 'Bob' }, '$bob')
   *   .edge('$alice', 'KNOWS', '$bob')
   *   .execute();
   */
  batchCreate() {
    this._assertOpen()
    return new BatchCreateBuilder(this)
  }

  intern(name) {
    this._assertOpen()
    return callNative(native.databaseIntern, this._handle, name)
  }

  seedDemo() {
    this._assertOpen()
    callNative(native.databaseSeedDemo, this._handle)
    return this
  }

  mutate(script) {
    this._assertOpen()
    if (!script || typeof script !== 'object') {
      throw new TypeError('mutation script must be an object')
    }
    if (!Array.isArray(script.ops)) {
      throw new TypeError('mutation script requires an ops array')
    }
    return callNative(native.databaseMutate, this._handle, script)
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

  mutateBatched(ops, options = {}) {
    if (!Array.isArray(ops)) {
      throw new TypeError('mutateBatched requires an array of operations')
    }
    const { batchSize = 1024 } = options ?? {}
    if (!Number.isInteger(batchSize) || batchSize <= 0) {
      throw new RangeError('batchSize must be a positive integer')
    }
    if (ops.length === 0) {
      return emptyMutationSummary()
    }
    let summary = emptyMutationSummary()
    for (let i = 0; i < ops.length; i += batchSize) {
      const chunk = ops.slice(i, i + batchSize)
      const part = this.mutateMany(chunk)
      summary = mergeMutationSummaries(summary, part)
    }
    return summary
  }

  async transaction(fn) {
    this._assertOpen()
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
    this._assertOpen()
    if (typeof name !== 'string') {
      throw new TypeError('pragma name must be a string')
    }
    if (arguments.length < 2) {
      return callNative(native.databasePragmaGet, this._handle, name)
    }
    return callNative(native.databasePragmaSet, this._handle, name, value)
  }

  cancelRequest(requestId) {
    this._assertOpen()
    if (typeof requestId !== 'string' || requestId.trim() === '') {
      throw new TypeError('cancelRequest requires a non-empty request id string')
    }
    return callNative(native.databaseCancelRequest, this._handle, requestId)
  }

  getNodeRecord(nodeId) {
    this._assertOpen()
    const id = assertNodeId(nodeId, 'getNodeRecord')
    const record = callNative(native.databaseGetNode, this._handle, id)
    return record ?? null
  }

  getEdgeRecord(edgeId) {
    this._assertOpen()
    const id = assertEdgeId(edgeId, 'getEdgeRecord')
    const record = callNative(native.databaseGetEdge, this._handle, id)
    return record ?? null
  }

  countNodesWithLabel(label) {
    this._assertOpen()
    const normalized = assertLabel(label, 'countNodesWithLabel')
    if (typeof native.databaseCountNodesWithLabel === 'function') {
      return callNative(native.databaseCountNodesWithLabel, this._handle, normalized)
    }
    return this.listNodesWithLabel(normalized).length
  }

  countEdgesWithType(edgeType) {
    this._assertOpen()
    const normalized = assertEdgeType(edgeType, 'countEdgesWithType')
    if (typeof native.databaseCountEdgesWithType === 'function') {
      return callNative(native.databaseCountEdgesWithType, this._handle, normalized)
    }
    return this._countEdgesWithTypeFallback(normalized)
  }

  listNodesWithLabel(label) {
    this._assertOpen()
    const normalized = assertLabel(label, 'listNodesWithLabel')
    if (typeof native.databaseListNodesWithLabel === 'function') {
      return normalizeIdList(
        callNative(native.databaseListNodesWithLabel, this._handle, normalized),
        'listNodesWithLabel result',
      )
    }
    return this._listNodesWithLabelFallback(normalized)
  }

  neighbors(nodeId, options) {
    this._assertOpen()
    const id = assertNodeId(nodeId, 'neighbors')
    if (options !== undefined && (options === null || typeof options !== 'object')) {
      throw new TypeError('neighbors() options must be an object when provided')
    }
    return callNative(native.databaseNeighbors, this._handle, id, options ?? undefined)
  }

  getOutgoingNeighbors(nodeId, edgeType, distinct = true) {
    this._assertOpen()
    if (typeof distinct !== 'boolean') {
      throw new TypeError('distinct must be a boolean')
    }
    const opts = { direction: 'out', distinct }
    if (edgeType !== undefined && edgeType !== null) {
      if (typeof edgeType !== 'string' || edgeType.trim() === '') {
        throw new TypeError('edgeType must be a non-empty string when provided')
      }
      opts.edgeType = edgeType
    }
    return this.neighbors(nodeId, opts).map((neighbor) => neighbor.nodeId)
  }

  getIncomingNeighbors(nodeId, edgeType, distinct = true) {
    this._assertOpen()
    if (typeof distinct !== 'boolean') {
      throw new TypeError('distinct must be a boolean')
    }
    const opts = { direction: 'in', distinct }
    if (edgeType !== undefined && edgeType !== null) {
      if (typeof edgeType !== 'string' || edgeType.trim() === '') {
        throw new TypeError('edgeType must be a non-empty string when provided')
      }
      opts.edgeType = edgeType
    }
    return this.neighbors(nodeId, opts).map((neighbor) => neighbor.nodeId)
  }

  bfsTraversal(nodeId, maxDepth, options) {
    this._assertOpen()
    const id = assertNodeId(nodeId, 'bfsTraversal')
    if (!Number.isInteger(maxDepth) || maxDepth < 0) {
      throw new TypeError('bfsTraversal requires a non-negative integer maxDepth')
    }
    if (options !== undefined && (options === null || typeof options !== 'object')) {
      throw new TypeError('bfsTraversal options must be an object when provided')
    }
    return callNative(native.databaseBfsTraversal, this._handle, id, maxDepth, options ?? undefined)
  }

  _execute(spec) {
    this._assertOpen()
    return callNative(native.databaseExecute, this._handle, spec)
  }

  _explain(spec) {
    this._assertOpen()
    const payload = callNative(native.databaseExplain, this._handle, spec)
    return normalizeExplainPayload(payload)
  }

  _stream(spec) {
    this._assertOpen()
    return callNative(native.databaseStream, this._handle, spec)
  }

  _listNodesWithLabelFallback(label) {
    const matchVar = '__sombra_list_nodes'
    const builder = new QueryBuilder(this, this._schema)
    builder.match({ var: matchVar, label })
    const payload = this._execute(builder._build())
    const rows = Array.isArray(payload?.rows) ? payload.rows : []
    const ids = []
    for (const row of rows) {
      const entry = row && row[matchVar]
      if (entry && typeof entry._id === 'number') {
        ids.push(entry._id)
      }
    }
    return ids
  }

  _countEdgesWithTypeFallback(edgeType) {
    const srcVar = '__sombra_edge_src'
    const dstVar = '__sombra_edge_dst'
    const builder = new QueryBuilder(this, this._schema)
    builder.match({ var: srcVar, label: null })
    builder.where(edgeType, { var: dstVar, label: null })
    const payload = this._execute(builder._build())
    const rows = Array.isArray(payload?.rows) ? payload.rows : []
    return rows.length
  }
}

if (typeof Symbol.dispose === 'symbol') {
  Database.prototype[Symbol.dispose] = function disposeDatabase() {
    this.close()
  }
  QueryStream.prototype[Symbol.dispose] = function disposeStream() {
    this.close()
  }
}

if (typeof Symbol.asyncDispose === 'symbol') {
  Database.prototype[Symbol.asyncDispose] = async function asyncDisposeDatabase() {
    this.close()
  }
  QueryStream.prototype[Symbol.asyncDispose] = async function asyncDisposeStream() {
    this.close()
  }
}

function openDatabase(path, options) {
  return Database.open(path, options)
}

module.exports = {
  // Classes
  Database,
  PredicateBuilder,
  QueryBuilder,
  NodeScope,
  BatchCreateBuilder,
  BatchNodeHandle,
  // Database factory
  openDatabase,
  // Native module access
  native,
  // Expression helpers
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
  // Batch utilities
  estimateBatchSize,
  batchRecords,
  // Error types
  ErrorCode,
  SombraError,
  AnalyzerError,
  JsonError,
  IoError,
  CorruptionError,
  ConflictError,
  SnapshotTooOldError,
  CancelledError,
  InvalidArgError,
  NotFoundError,
  ClosedError,
  BatchError,
  // Error utilities
  wrapNativeError,
}

// Lazy getter to expose the typed facade without creating a hard circular dependency.
Object.defineProperty(module.exports, 'SombraDB', {
  enumerable: true,
  get() {
    return require('./typed.js').SombraDB
  },
})

function assertNodeId(nodeId, ctx) {
  if (typeof nodeId !== 'number' || !Number.isInteger(nodeId) || nodeId < 0) {
    throw new TypeError(`${ctx} requires a non-negative integer node id`)
  }
  return nodeId
}

function assertEdgeId(edgeId, ctx) {
  if (typeof edgeId !== 'number' || !Number.isInteger(edgeId) || edgeId < 0) {
    throw new TypeError(`${ctx} requires a non-negative integer edge id`)
  }
  return edgeId
}

function assertLabel(label, ctx) {
  if (typeof label !== 'string' || label.trim() === '') {
    throw new TypeError(`${ctx} requires a non-empty label string`)
  }
  return label
}

function assertEdgeType(edgeType, ctx) {
  if (typeof edgeType !== 'string' || edgeType.trim() === '') {
    throw new TypeError(`${ctx} requires a non-empty edge type string`)
  }
  return edgeType
}
