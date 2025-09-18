// 공용 정규식 & 토큰 유틸
'use strict';

const PART_TOKEN = /[A-Z0-9](?:[A-Z0-9][A-Z0-9._/-]?){3,23}/g; // 4~24자, 대문자/숫자/._/-
const BAD_TOKEN = /^(?:PDF|PAGE|TABLE|INDEX|FIG|SEE|HTTP|WWW|VOLT|AMP|SEC|Hz|mm|inch|DATE)$/i;
const NEAR_KEYS = [
  'part no', 'model', 'type', 'ordering', 'ordering information', 'types',
  'catalog no', 'product code', 'sku'
];

function tokenize(text) {
  if (!text) return [];
  const uniq = new Set();
  for (const m of text.toUpperCase().matchAll(PART_TOKEN)) {
    const t = m[0].replace(/^[\W_]+|[\W_]+$/g, '');
    if (t.length >= 4 && t.length <= 24 && !BAD_TOKEN.test(t)) uniq.add(t);
  }
  return [...uniq];
}

function keywordWindows(text, window = 250) {
  const out = [];
  const low = (text || '').toLowerCase();
  for (const k of NEAR_KEYS) {
    let idx = 0;
    const needle = k.toLowerCase();
    while ((idx = low.indexOf(needle, idx)) !== -1) {
      const s = Math.max(0, idx - window);
      const e = Math.min(text.length, idx + needle.length + window);
      out.push(text.slice(s, e));
      idx += needle.length;
    }
  }
  return out;
}

module.exports = { tokenize, keywordWindows };
