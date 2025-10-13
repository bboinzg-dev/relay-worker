'use strict';

const { PN_CANDIDATE_SOURCE } = require('./patterns');

const ORDERING_ANCHOR_RE =
  /(ORDER(?:ING)?\s+(INFO|INFORMATION|GUIDE|CODE|NUMBER)|HOW\s+TO\s+ORDER|주문\s*정보|주문\s*코드|형명\s*구분|형명구분|형명\s*구성|형식\s*구분|Part\s*selection|PART\s*(?:NO\.?|NUMBER)|品番|型番|품번|订购信息|订货信息)/i;
const ORDERING_CANDIDATE_RE = new RegExp(PN_CANDIDATE_SOURCE, 'g');
const ORDERING_BLACKLIST_RE = /^(ISO|ROHS|UL|VDC|VAC|A|MA|MM|Ω|OHM|PDF|PAGE|NOTE|DATE|LOT|WWW|HTTP|HTTPS)$/i;

function normalizeCode(str) {
  return String(str || '')
    .replace(/[–—]/g, '-')
    .replace(/\s+/g, '')
    .replace(/-+/g, '-')
    .toUpperCase();
}

function clampWindow(text, index, before = 8000, after = 12000) {
  const len = text.length;
  if (!Number.isFinite(index) || index < 0 || index > len) {
    return { text, start: 0, end: len, anchorIndex: -1 };
  }
  const start = Math.max(0, index - before);
  const end = Math.min(len, index + after);
  return { text: text.slice(start, end), start, end, anchorIndex: index };
}

function extractOrderingWindow(full, { before = 8000, after = 12000 } = {}) {
  const raw = String(full || '');
  if (!raw) {
    return { text: '', start: 0, end: 0, anchorIndex: -1 };
  }
  const anchorMatch = raw.match(ORDERING_ANCHOR_RE);
  if (anchorMatch && typeof anchorMatch.index === 'number') {
    return clampWindow(raw, anchorMatch.index, before, after);
  }
  return { text: raw.slice(0, Math.min(raw.length, before + after)), start: 0, end: Math.min(raw.length, before + after), anchorIndex: -1 };
}

function rankCodesInWindow(windowText, limit = 50) {
  const text = String(windowText || '');
  if (!text) return [];
  const raw = text.match(ORDERING_CANDIDATE_RE) || [];
  const stats = new Map();

  for (const candidate of raw) {
    const code = normalizeCode(candidate);
    if (!/[0-9]/.test(code)) continue;
    if (code.length < 4 || code.length > 20) continue;
    if (ORDERING_BLACKLIST_RE.test(code)) continue;

    const pos = text.indexOf(candidate);
    const ctx = text.slice(Math.max(0, pos - 80), Math.min(text.length, pos + 80));
    let score = 1;
    if (/(coil|voltage|vdc|form|contact|series|type|형식|전압|코일)/i.test(ctx)) score += 2;
    if (/(model|part\s*no\.?|ordering|주문|订购)/i.test(ctx)) score += 2;
    stats.set(code, (stats.get(code) || 0) + score);
    if (stats.size >= limit * 3) break;
  }

  return [...stats.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([code, score]) => ({ code, score }));
}

function rankPartNumbersFromOrderingSections(full, limit = 50, opts = {}) {
  const window = extractOrderingWindow(full, opts);
  return rankCodesInWindow(window.text, limit);
}

function extractOrderingInfo(full, limit = 50, opts = {}) {
  const window = extractOrderingWindow(full, opts);
  if (!window.text) return null;
  const ranked = rankCodesInWindow(window.text, limit);
  if (!ranked.length) return null;
  const codes = [];
  const seen = new Set();
  for (const { code } of ranked) {
    if (!code || seen.has(code)) continue;
    seen.add(code);
    codes.push(code);
  }
  if (!codes.length) return null;
  const info = {
    codes,
    scored: ranked,
    text: window.text,
    start: window.start,
    end: window.end,
    anchor_index: window.anchorIndex,
  };
  return info;
}

module.exports = {
  rankPartNumbersFromOrderingSections,
  extractOrderingInfo,
  extractOrderingWindow,
};