// relay-worker/src/utils/regex.js
'use strict';

const ORDERING_KEYS = [
  'ordering information','ordering','how to order','part numbering','type number',
  'model key','selection guide','type','part number','part no','product number'
];

function tokenize(text) {
  return String(text||'')
    .split(/[^A-Za-z0-9\-\._\/]+/g)
    .map(s => s.trim())
    .filter(s => s.length >= 2 && !/^[\.\-]$/.test(s))
    .slice(0, 5000);
}

function keywordWindows(text, window = 900) {
  const t = String(text||'').toLowerCase();
  const out = [];
  for (const k of ORDERING_KEYS) {
    let idx = 0;
    while (idx >= 0) {
      idx = t.indexOf(k, idx);
      if (idx < 0) break;
      const s = Math.max(0, idx - window);
      const e = Math.min(t.length, idx + k.length + window);
      out.push(text.slice(s, e));
      idx = idx + k.length;
    }
  }
  return out;
}

module.exports = { tokenize, keywordWindows, ORDERING_KEYS };
