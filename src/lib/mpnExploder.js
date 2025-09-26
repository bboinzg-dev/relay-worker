'use strict';

/** 공통 토큰/리스트 유틸 */
const LIST_SEP = /[\s,;/|·•、，；／]+/;
const NON_MPN_WORDS = new Set([
  'relay','relays','series','datasheet','catalog','typ','max','min',
  'vdc','vac','a','ma','mm','pdf','page','note','date','lot','www','http','https'
]);

/** 후보 MPN 토큰 뽑기 */
function extractMpnCandidates(text) {
  if (!text) return [];
  // 대문자 시작 + 숫자 포함 + 4~24자
  const candRe = /[A-Z][A-Z0-9][A-Z0-9\-\.]{1,22}/g;
  const raw = text.match(candRe) || [];
  const out = [];
  for (const r of raw) {
    const t = r.replace(/[\.\u200b]+/g, '').trim();
    if (t.length < 4 || t.length > 24) continue;
    if (!/\d/.test(t)) continue;
    if (NON_MPN_WORDS.has(t.toLowerCase())) continue;
    out.push(t);
  }
  return Array.from(new Set(out));
}

/** 블루프린트 code_rules 기반으로 MPN → 변형축 키 추출 */
function parseVariantsFromCode(code, rules = {}) {
  const vars = {};
  const { patterns = [] } = rules;
  for (const p of patterns) {
    const re = new RegExp(p.regex, p.flags || 'i');
    const m = code.match(re);
    if (!m) continue;
    if (p.assign) {
      for (const [k, v] of Object.entries(p.assign)) vars[k] = v;
    }
    if (p.groups) {
      for (const [k, src] of Object.entries(p.groups)) {
        const val = m.groups?.[src] ?? m[Number(src)];
        if (val != null) vars[k] = val;
      }
    }
  }
  return vars;
}

/** 템플릿 렌더링 (예: {{series}}{{contact_form}}{{coil_voltage_vdc|pad=2}}{{suffix}}) */
function renderTemplate(tpl, ctx) {
  return tpl.replace(/\{\{([^}|]+)(\|pad=(\d+))?\}\}/g, (_, key, _pad, n) => {
    let v = (ctx[key] ?? '').toString();
    if (_pad) v = v.padStart(Number(n), '0');
    return v;
  });
}

/** variant 교차곱 */
function cartesian(obj) {
  const keys = Object.keys(obj);
  if (!keys.length) return [{}];
  return keys.reduce((acc, k) => {
    const vals = Array.isArray(obj[k]) ? obj[k] : [obj[k]];
    const next = [];
    for (const a of acc) for (const v of vals) next.push({ ...a, [k]: v });
    return next;
  }, [{}]);
}

/** 전개 로직 – 후보 MPN과 블루프린트를 함께 사용 */
function explodeToRows({ brand, series, baseFields = {}, blueprint, textCandidates = [] }) {
  const rows = [];

  // 1) 텍스트에서 후보 MPN 가져오기
  const candidates = Array.from(new Set([
    ...(textCandidates || []),
    ...extractMpnCandidates(baseFields.__raw_text || '')
  ]));

  // 2) 후보 MPN 하나씩 → variant 역추정 → 템플릿 렌더링
  for (const cand of candidates) {
    const v = parseVariantsFromCode(cand, blueprint?.code_rules || {});
    const series_code = series || (cand.match(/^[A-Z]+/)?.[0] ?? '');

    let code = cand;
    if (blueprint?.ingest_options?.pn_template) {
      // 템플릿으로 계산 가능하면 cand 무시하고 템플릿 우선
      const ctx = { brand, series: series_code, ...baseFields, ...v };
      const rendered = renderTemplate(blueprint.ingest_options.pn_template, ctx);
      if (rendered && /[A-Z0-9]/i.test(rendered)) code = rendered;
    }

    rows.push({
      brand,
      series: series_code,
      code,
      ...baseFields,
      ...v,
    });
  }

  // 3) variant_keys가 주어졌다면 추가 전개(교차곱) 수행
  const vk = blueprint?.ingest_options?.variant_keys || [];
  if (vk.length) {
    const expanded = [];
    for (const r of rows) {
      const axes = {};
      for (const k of vk) if (r[k] != null) axes[k] = r[k];
      const combos = Object.keys(axes).length ? cartesian(axes) : [{}];
      for (const c of combos) {
        const ctx = { ...r, ...c };
        let code = r.code;
        if (blueprint?.ingest_options?.pn_template) {
          code = renderTemplate(blueprint.ingest_options.pn_template, ctx);
        }
        expanded.push({ ...r, ...c, code });
      }
    }
    return dedupeByBrandCode(expanded);
  }

  return dedupeByBrandCode(rows);
}

function dedupeByBrandCode(rows) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const key = `${(r.brand||'').toLowerCase()}::${(r.code||'').toUpperCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  return out;
}

module.exports = {
  extractMpnCandidates,
  parseVariantsFromCode,
  cartesian,
  renderTemplate,
  explodeToRows,
};
