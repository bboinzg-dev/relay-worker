const { safeJsonParse } = require('./safe-json');

const UNITS = {
  '': 1,
  'k': 1e3, 'K': 1e3,
  'M': 1e6,
  'G': 1e9,
  'm': 1e-3,
  'u': 1e-6, 'µ': 1e-6,
  'n': 1e-9,
  'p': 1e-12,
};
function parseNumberWithUnit(s) {
  if (s == null) return null;
  if (typeof s === 'number') return s;
  const str = String(s).trim();
  if (!str) return null;
  // Extract number and multiplier like "12.3k", "4.7 uF", "1M", "10kΩ"
  const m = str.replace(',', '.').match(/([-+]?\d*\.?\d+)(?:\s*([kKMmµunp]?))?/);
  if (!m) return Number(str) || null;
  const v = parseFloat(m[1]);
  const mul = UNITS[m[2] || ''] || 1;
  if (isNaN(v)) return null;
  return v * mul;
}
function normalizeBrand(v){ return (v||'').trim(); }
function normalizeCode(v){ return (v||'').trim(); }
function normalizeText(v){ return (v==null? null : String(v).trim()); }
function normalizeBool(v){
  if (v===true||v===false) return v;
  const s = String(v||'').toLowerCase().trim();
  if (['y','yes','true','1','t'].includes(s)) return true;
  if (['n','no','false','0','f'].includes(s)) return false;
  return null;
}
function normalizeByType(type, v){
  switch(String(type||'text').toLowerCase()){
    case 'number':
    case 'numeric':
    case 'float': return parseNumberWithUnit(v);
    case 'integer':
    case 'int': return v==null? null : parseInt(String(v).replace(/[^-\d]/g,'')) || null;
    case 'boolean':
    case 'bool': return normalizeBool(v);
    case 'json':
    case 'jsonb':
      if (v==null) return null;
      if (typeof v === 'object') return v;
      try { return safeJsonParse(String(v)) ?? { value: v }; } catch { return { value: v }; }
    default:
      return normalizeText(v);
  }
}
module.exports = { parseNumberWithUnit, normalizeBrand, normalizeCode, normalizeText, normalizeBool, normalizeByType };
