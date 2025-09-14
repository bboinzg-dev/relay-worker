function toVectorLiteral(vec) {
  if (!Array.isArray(vec)) return '[]';
  // keep reasonable precision, avoid NaN/Inf
  const safe = vec.map(v => Number.isFinite(v) ? Number(v) : 0);
  return '[' + safe.map(v => v.toFixed(6)).join(',') + ']';
}
module.exports = { toVectorLiteral };
