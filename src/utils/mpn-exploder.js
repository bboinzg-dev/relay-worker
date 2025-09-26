'use strict';

const LIST_SEP = /[\s,;/|·•]+/;
const NON_MPN_WORDS = new Set([
  'relay', 'relays', 'coil', 'vdc', 'vac', 'form', 'series', 'typ', 'max', 'min'
]);

function splitAndCarryPrefix(raw) {
  if (!raw) return [];
  const tokens = String(raw)
    .split(LIST_SEP)
    .map((t) => t.trim())
    .filter(Boolean);

  const out = [];
  let lastPrefix = '';
  const prefixRegex = /^([A-Za-z][A-Za-z0-9\-_/]*?)(?=\d|$)/;

  for (const token of tokens) {
    const lower = token.toLowerCase();
    if (NON_MPN_WORDS.has(lower)) continue;
    if (/^[0-9]/.test(token)) {
      if (lastPrefix) {
        out.push(lastPrefix + token);
      }
      continue;
    }

    out.push(token);
    const m = token.match(prefixRegex);
    if (m) lastPrefix = m[1];
  }

  const seen = new Set();
  const deduped = [];
  for (const token of out) {
    const norm = token.toLowerCase();
    if (seen.has(norm)) continue;
    seen.add(norm);
    deduped.push(token);
  }
  return deduped;
}

function normalizeList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.flatMap(normalizeList);
  if (typeof value === 'string') {
    return value
      .split(LIST_SEP)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [value];
}

function cartesian(lists) {
  return lists.reduce(
    (acc, list) => acc.flatMap((a) => list.map((b) => a.concat([b]))),
    [[]]
  );
}

function renderTemplate(tpl, ctx) {
  if (!tpl) return '';
  return tpl.replace(/\{\{([^}]+)\}\}/g, (_, expr) => {
    const [head, ...pipes] = expr.split('|').map((s) => s.trim());
    let val = ctx[head];
    for (const pipe of pipes) {
      if (pipe === 'upper') val = String(val ?? '').toUpperCase();
      else if (pipe === 'lower') val = String(val ?? '').toLowerCase();
      else if (pipe.startsWith('pad=')) {
        const n = parseInt(pipe.slice(4), 10);
        const str = String(val ?? '');
        val = Number.isFinite(n) ? str.padStart(n, '0') : str;
      }
    }
    return val == null ? '' : String(val);
  });
}

function assignValue(target, key, value) {
  if (!key) return;
  target[key] = value;
  const lower = String(key).toLowerCase();
  if (lower && lower !== key) target[lower] = value;
}

function collectMpnSeeds(base) {
  const seeds = [];
  const seen = new Set();

  const push = (val) => {
    if (val == null) return;
    if (Array.isArray(val)) {
      for (const item of val) push(item);
      return;
    }
    if (typeof val === 'string') {
      const trimmed = val.trim();
      if (!trimmed) return;
      const parts = splitAndCarryPrefix(trimmed);
      if (parts.length > 1) {
        for (const part of parts) push(part);
        return;
      }
      const norm = trimmed.toLowerCase();
      if (seen.has(norm)) return;
      seen.add(norm);
      seeds.push(trimmed);
      return;
    }
    const str = String(val);
    const norm = str.toLowerCase();
    if (seen.has(norm)) return;
    seen.add(norm);
    seeds.push(str);
  };

  push(base?.mpn);
  if (base?.code) push(base.code);

  const extraKeys = ['mpn', 'part_number', 'part_no', 'code'];
  const values = base && typeof base.values === 'object' ? base.values : {};
  for (const key of extraKeys) push(values[key]);
  return seeds;
}

function explodeToRows(base, options = {}) {
  const variantKeys = Array.isArray(options.variantKeys)
    ? options.variantKeys.map((k) => String(k || '').trim()).filter(Boolean)
    : [];
  const pnTemplate = options.pnTemplate || null;

  const values = {};
  if (base && typeof base.values === 'object') {
    for (const [rawKey, rawValue] of Object.entries(base.values)) {
      const key = String(rawKey || '').trim();
      if (!key) continue;
      assignValue(values, key, rawValue);
    }
  }
  if (base?.series && values.series == null) assignValue(values, 'series', base.series);
  if (base?.series_code && values.series_code == null) assignValue(values, 'series_code', base.series_code);

  const lists = variantKeys.map((key) => {
    const fromExact = values[key];
    if (fromExact != null) {
      const normed = normalizeList(fromExact);
      return normed.length ? normed : [fromExact];
    }
    const lower = key.toLowerCase();
    if (values[lower] != null) {
      const normed = normalizeList(values[lower]);
      return normed.length ? normed : [values[lower]];
    }
    return [null];
  });

  const combos = lists.length ? cartesian(lists) : [[]];
  const mpnCandidates = collectMpnSeeds(base);

  const rows = [];
  combos.forEach((combo, idx) => {
    const rowValues = { ...values };
    variantKeys.forEach((key, keyIdx) => {
      const val = combo[keyIdx];
      if (val == null || val === '') return;
      assignValue(rowValues, key, val);
    });

    let code = null;
    if (pnTemplate) {
      code = renderTemplate(pnTemplate, {
        ...rowValues,
        series: base?.series ?? base?.series_code ?? rowValues.series ?? rowValues.series_code ?? '',
        series_code: base?.series_code ?? rowValues.series_code ?? '',
      });
    } else if (mpnCandidates[idx]) {
      code = mpnCandidates[idx];
    } else if (mpnCandidates.length) {
      code = mpnCandidates[0];
    } else {
      const parts = [];
      if (base?.series) parts.push(base.series);
      else if (base?.series_code) parts.push(base.series_code);
      const suffix = variantKeys
        .map((key) => rowValues[key] ?? rowValues[key?.toLowerCase()])
        .filter((v) => v != null && v !== '')
        .map((v) => String(v))
        .join('');
      if (suffix) parts.push(suffix);
      code = parts.join('');
    }

    code = String(code || '').trim();
    if (!code) return;

    const codeNorm = code.toLowerCase();
    if (rows.some((r) => r.code_norm === codeNorm)) return;

    rows.push({
      code,
      code_norm: codeNorm,
      values: rowValues,
    });
  });

  return rows;
}

module.exports = {
  LIST_SEP,
  NON_MPN_WORDS,
  splitAndCarryPrefix,
  normalizeList,
  cartesian,
  renderTemplate,
  explodeToRows,
};