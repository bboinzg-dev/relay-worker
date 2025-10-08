'use strict';

const LIST_SEP = /[\,\s/;|·•]+/;
// 🔹 키 별칭(범용): 템플릿 키 → 실제 필드명 후보
const KEY_ALIASES = {
  contact_form: [
    'contact_form',
    'contact_arrangement',
    'configuration',
    'arrangement',
    'poles_form',
    'form',
  ],
  coil_voltage_vdc: [
    'coil_voltage_vdc',
    'voltage_vdc',
    'rated_voltage_vdc',
    'vdc',
    'coil_voltage',
    'voltage',
  ],
  series: ['series', 'series_code'],
  suffix: ['suffix', 'packing', 'package', 'packaging', 'mount_type', 'mounting', 'terminal'],
};
const CANDIDATE_KEYS = [
  'candidates',
  'codes',
  'mpn_candidates',
  'mpnCandidates',
  'mpn_list',
  'mpnList',
  'mpns',
];

const CONTACT_FORM_ENUM = {
  SPST: '1A',
  SPDT: '1C',
  DPDT: '2C',
  DPST: '2A',
};

function pick(obj, key) {
  const cand = new Set([key, String(key || '').toLowerCase()]);
  for (const a of KEY_ALIASES[String(key || '').toLowerCase()] || []) {
    cand.add(a);
    cand.add(String(a).toLowerCase());
  }
  for (const k of cand) {
    const v = obj?.[k];
    if (v == null) continue;
    if (Array.isArray(v)) {
      const first = v.find((x) => x != null && String(x).trim() !== '');
      if (first != null) return first;
    }
    const s = String(v).trim();
    if (s !== '') return v;
  }
  return null;
}

function normalizeList(raw) {
  if (raw == null) return [];
  if (Array.isArray(raw)) return raw.filter(v => v != null && String(v).trim() !== '');
  const s = String(raw).trim();
  if (!s) return [];
  // "DC5V", "5 V", "05" 등에서 숫자만 뽑아 pad에 쓰기 좋게
  return s.split(LIST_SEP).map(tok => tok.trim()).filter(Boolean);
}

function escapeRegExp(str) {
  return String(str || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function applyTemplateMods(value, mods = []) {
  if (value == null) return '';

  let out = Array.isArray(value) ? value[0] : value;
  if (out == null) return '';
  if (typeof out !== 'string') out = String(out);

  for (const rawMod of mods) {
    const token = String(rawMod || '').trim();
    if (!token) continue;
    const [opRaw, argRaw = ''] = token.split('=').map((t) => t.trim());
    const op = opRaw.toLowerCase();
    if (!op) continue;

    if (op === 'pad') {
      const width = Number(argRaw);
      if (Number.isFinite(width) && width > 0) out = out.padStart(width, '0');
      continue;
    }

    if (op === 'map') {
      try {
        const lut = JSON.parse(argRaw);
        const key = String(out);
        if (lut[key] != null) out = lut[key];
      } catch {}
      continue;
    }

    if (op === 'first') {
      const parts = out.split(',');
      out = parts.length ? parts[0].trim() : out;
      continue;
    }

    if (op === 'alnum') {
      out = out.replace(/[^0-9A-Z]/gi, '');
      continue;
    }

    if (op === 'digits') {
      const match = out.match(/\d+/g) || [''];
      out = match.join('');
      continue;
    }

    if (op === 'upper' || op === 'upcase' || op === 'uppercase') {
      out = out.toUpperCase();
      continue;
    }

    if (op === 'lower' || op === 'downcase' || op === 'lowercase') {
      out = out.toLowerCase();
      continue;
    }

    if (op === 'trim') {
      out = out.trim();
      continue;
    }

    if (op === 'prefix') {
      out = `${argRaw}${out}`;
      continue;
    }

    if (op === 'suffix') {
      out = `${out}${argRaw}`;
      continue;
    }

    if (op === 'replace' && argRaw) {
      const [search, replacement = ''] = argRaw.split(':');
      if (search != null) {
        const matcher = new RegExp(escapeRegExp(search), 'g');
        out = out.replace(matcher, replacement);
      }
      continue;
    }
  }

  return out;
}

function normalizeTemplateValue(key, value) {
  if (value == null) return value;
  let out = Array.isArray(value) ? value[0] : value;
  if (out == null) return out;
  let str = typeof out === 'string' ? out : String(out);

  const keyNorm = String(key || '').toLowerCase();
  if (keyNorm.includes('contact')) {
    const formMatch = str.match(/(\d)\s*form\s*([ABC])/i);
    if (formMatch) {
      str = `${formMatch[1]}${formMatch[2].toUpperCase()}`;
    } else {
      const compact = str.replace(/\s+/g, '').toUpperCase();
      if (CONTACT_FORM_ENUM[compact]) {
        str = CONTACT_FORM_ENUM[compact];
      }
    }
  }

  return str;
}

function renderTemplate(tpl, obj) {
  if (!tpl) return '';

  const render = (template, pattern) => template.replace(pattern, (_, expr) => {
    const parts = String(expr || '')
      .split('|')
      .map((s) => s.trim())
      .filter(Boolean);
    if (!parts.length) return '';
    const rawKey = parts.shift();
    if (!rawKey) return '';
    // 🔹 별칭까지 고려해 값 선택
    const value = normalizeTemplateValue(rawKey, pick(obj, rawKey));
    if (value == null || value === '') return '';
    const applied = applyTemplateMods(value, parts);
    return applied == null ? '' : String(applied);
  });

  let out = String(tpl);
  out = render(out, /\{\{\s*([^{}]+?)\s*\}\}/g);
  out = render(out, /\{\s*([^{}]+?)\s*\}/g);
  // 🔹 미치환 토큰 방지: 남은 중괄호 제거(=치환 실패 → 공백)
  out = out.replace(/\{[^{}]+\}/g, '');
  return out.replace(/\s+/g, '').trim();
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