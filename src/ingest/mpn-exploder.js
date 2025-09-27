'use strict';

const LIST_SEP = /[\,\s/;|·•]+/;
const CANDIDATE_KEYS = [
  'candidates',
  'codes',
  'mpn_candidates',
  'mpnCandidates',
  'mpn_list',
  'mpnList',
  'mpns',
];

function normalizeList(raw) {
  if (raw == null) return [];
  if (Array.isArray(raw)) return raw.filter(v => v != null && String(v).trim() !== '');
  const s = String(raw).trim();
  if (!s) return [];
  // "DC5V", "5 V", "05" 등에서 숫자만 뽑아 pad에 쓰기 좋게
  return s.split(LIST_SEP).map(tok => tok.trim()).filter(Boolean);
}

function pad(val, n = 2) {
  const s = String(val).replace(/\D+/g, '') || String(val);
  return s.padStart(n, '0');
}

function renderTemplate(tpl, obj) {
  return String(tpl || '').replace(/\{\{([^}]+)\}\}/g, (_, expr) => {
    const [rawKey, ...mods] = expr.split('|').map(s => s.trim());
    let v = obj[rawKey];
    for (const mod of mods) {
      const m = /^pad=(\d+)$/.exec(mod);
      if (m) v = pad(v, Number(m[1]));
    }
    return v == null ? '' : String(v);
  });
}

// 곱집합
function cartesian(arrays) {
  return arrays.reduce((acc, arr) => {
    const out = [];
    for (const a of acc) for (const b of arr) out.push(a.concat([b]));
    return out;
  }, [[]]);
}

function collectCandidates(row = {}) {
  const out = [];
  const seen = new Set();

  const push = (val) => {
    if (val == null) return;
    if (Array.isArray(val)) {
      for (const v of val) push(v);
      return;
    }
    if (typeof val === 'object') {
      if (val.raw != null) push(val.raw);
      if (val.code != null) push(val.code);
      return;
    }
    const code = String(val || '').trim();
    if (!code) return;
    const norm = code.toLowerCase();
    if (seen.has(norm)) return;
    seen.add(norm);
    out.push(code);
  };

  for (const key of CANDIDATE_KEYS) {
    if (key in row) push(row[key]);
  }

  return out;
}

function stripCandidateFields(target) {
  for (const key of CANDIDATE_KEYS) {
    if (key in target) delete target[key];
  }
}

function dedupeByBrandCode(rows = []) {
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue;
    let code = row.code;
    if (code == null || code === '') continue;
    code = String(code).trim();
    if (!code) continue;
    const codeNorm = (row.code_norm || code.toLowerCase());
    const brandNorm = String(row.brand_norm || row.brand || '').trim().toLowerCase();
    const key = `${brandNorm}::${codeNorm}`;
    if (seen.has(key)) continue;
    if (!row.code_norm) row.code_norm = codeNorm;
    seen.add(key);
    out.push(row);
  }
  return out;
}

/**
 * @param {object} blueprint - { ingest_options: { variant_keys, pn_template }, ... }
 * @param {Array<object>} rows - 추출된 행(스펙행). 각 행은 { series, series_code, ..., field:value }
 * @returns {Array<object>} - 변형 축을 모두 풀어낸 행들. code/code_norm 포함
 */
function explodeToRows(blueprint, rows = [], options = {}) {
  const ingest = blueprint?.ingest_options || blueprint?.ingestOptions || {};
  const variantKeys = Array.isArray(ingest.variant_keys) ? ingest.variant_keys : [];
  const tpl = ingest.pn_template || ingest.pnTemplate || null;

  const parseVariantsFromCode = typeof options.parseVariantsFromCode === 'function'
    ? options.parseVariantsFromCode
    : null;

  const baseRows = rows.length ? rows : [{}];
  const candidateRows = [];
  const expandedRows = [];

  for (const row0 of baseRows) {
    const row = { ...(row0 || {}) };
    const candidates = collectCandidates(row);
    stripCandidateFields(row);

    if (Array.isArray(candidates) && candidates.length) {
      const seen = new Set();
      for (const raw of candidates) {
        const code = String(raw || '').trim();
        if (!code) continue;
        const norm = code.toLowerCase();
        if (seen.has(norm)) continue;
        seen.add(norm);

        const v = parseVariantsFromCode
          ? parseVariantsFromCode(code, blueprint?.code_rules || {})
          : {};

        candidateRows.push({
          ...row,
          ...v,
          code,
          code_norm: norm,
        });
      }
     continue;
    }

    const series = row.series_code || row.series || '';

    const lists = variantKeys.map((k) => {
      const v = row[k];
      const list = normalizeList(v);
      return list.length ? list : [null];
    });

    const combos = lists.length ? cartesian(lists) : [[]];
    for (const combo of combos) {
      const r = { ...row };
      variantKeys.forEach((k, i) => { r[k] = combo[i]; });

      let code = tpl
        ? renderTemplate(tpl, { ...r, series })
        : [series, ...variantKeys.map((k) => r[k]).filter(Boolean)].join('');

      code = String(code).replace(/\s+/g, '').trim();
      if (!code) continue;

      r.code = code;
      r.code_norm = code.toLowerCase();
      expandedRows.push(r);
    }
  }

  const rowsOut = [...expandedRows, ...candidateRows];
  return dedupeByBrandCode(rowsOut);
}

module.exports = { explodeToRows, renderTemplate, normalizeList, cartesian };