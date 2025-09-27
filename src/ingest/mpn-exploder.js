'use strict';

const LIST_SEP = /[\,\s/;|·•]+/;

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

/**
 * @param {object} blueprint - { ingest_options: { variant_keys, pn_template }, ... }
 * @param {Array<object>} rows - 추출된 행(스펙행). 각 행은 { series, series_code, ..., field:value }
 * @returns {Array<object>} - 변형 축을 모두 풀어낸 행들. code/code_norm 포함
 */
function explodeToRows(blueprint, rows = []) {
  const ingest = blueprint?.ingest_options || blueprint?.ingestOptions || {};
  const variantKeys = Array.isArray(ingest.variant_keys) ? ingest.variant_keys : [];
  const tpl = ingest.pn_template || ingest.pnTemplate || null;

  const out = [];

  for (const row0 of (rows.length ? rows : [{}])) {
    const row = { ...row0 };
    const series = row.series_code || row.series || '';

    // 각 variant key를 리스트화
    const lists = variantKeys.map(k => {
      const v = row[k];
      const list = normalizeList(v);
      return list.length ? list : [null]; // 값이 없으면 단일 null 유지
    });

    const combos = lists.length ? cartesian(lists) : [[]];
    for (const combo of combos) {
      const r = { ...row };
      variantKeys.forEach((k, i) => { r[k] = combo[i]; });

      // code 생성: pn_template 있으면 그걸로, 없으면 series + 주요키
      let code = tpl
        ? renderTemplate(tpl, { ...r, series })
        : [series, ...variantKeys.map(k => r[k]).filter(Boolean)].join('');

      // 후처리: 공백/슬래시 제거 등 최소 정규화
      code = String(code).replace(/\s+/g, '').trim();

      r.code = code;
      r.code_norm = code.toLowerCase();
      out.push(r);
    }
  }
  return out;
}

module.exports = { explodeToRows, renderTemplate, normalizeList, cartesian };