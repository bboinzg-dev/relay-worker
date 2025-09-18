'use strict';

const db = require('../utils/db');
const { extractText } = require('../utils/extract');
const { detectBrandAndCode } = require('../utils/brandcode');
const { classifyFamily, extractByBlueprint } = require('../utils/vertex');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');

function normSlug(s) { return String(s || '').trim().toLowerCase().replace(/[^a-z0-9_]+/g, '_'); }
function safeTempCode(uri) {
  const h = require('crypto').createHash('sha1').update(String(uri || Date.now())).digest('hex').slice(0, 6);
  return `TMP_${h}`.toUpperCase();
}

async function loadFamilies() {
  try {
    const r = await db.query('SELECT DISTINCT family_slug FROM public.component_registry ORDER BY 1');
    return r.rows.map(x => x.family_slug).filter(Boolean);
  } catch (_) {
    return ['relay_power', 'relay_signal', 'capacitor', 'resistor', 'ic', 'connector'];
  }
}

async function loadBlueprint(family_slug) {
  try {
    const r = await db.query(`
      SELECT b.fields_json, b.prompt_template, r.specs_table
      FROM public.component_spec_blueprint b
      JOIN public.component_registry r USING (family_slug)
      WHERE b.family_slug = $1 LIMIT 1
    `, [family_slug]);
    return r.rows[0] || null;
  } catch (_) {
    return null;
  }
}

async function runAutoIngest({ gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null }) {
  if (!gcsUri) throw new Error('gcsUri required');

  // 1) 텍스트/코퍼스 생성(범용)
  const { text, pages } = await extractText(gcsUri);
  const corpus = (text || pages?.map(p => p.text).join('\n\n') || '').toString();

  // 2) 브랜드/코드 감지(범용)
  let picked = await detectBrandAndCode(corpus);
  if (!brand) brand = picked.brand || '';
  if (!code)  code  = picked.code  || '';
  if (!series) series = picked.series || '';
  if (!display_name) display_name = series ? `${brand} ${series}`.trim() : brand;

  // 3) 패밀리 분류(주어지지 않았다면)
  const families = await loadFamilies();
  if (!family_slug) {
    family_slug = await classifyFamily(corpus, families);
  }
  family_slug = normSlug(family_slug || ''); // 비어있으면 아래에서 폴백

  // 4) 블루프린트 기반 스펙 추출(있으면)
  let values = {};
  let fields = {};
  let specs_table = '';
  const bp = family_slug ? await loadBlueprint(family_slug) : null;
  if (bp && bp.fields_json) {
    fields = bp.fields_json || {};
    values = await extractByBlueprint(corpus, fields, bp.prompt_template || '');
    specs_table = bp.specs_table || `${family_slug}_specs`;
  } else {
    // 블루프린트 없으면 family 미확정으로 취급
    specs_table = family_slug ? `${family_slug}_specs` : 'common_specs';
  }

  // 5) 최종 폴백 (범용)
  if (!brand) brand = 'unknown';
  if (!code)  code  = safeTempCode(gcsUri);
  if (!family_slug) family_slug = 'common';

  // 6) 테이블 보장 & 업서트
  const table = specs_table.replace(/[^a-z0-9_]/g, '');
  await ensureSpecsTable(table, fields || {});
  const row = await upsertByBrandCode(table, {
    brand, code, series, display_name, family_slug,
    source_gcs_uri: gcsUri,
    raw_json: { picked, fields, values, pages_preview: (pages||[]).slice(0,2).map(p=>p.text?.slice(0,2000)||'') },
    ...values,
  });

  return { ok: true, table, row };
}

module.exports = { runAutoIngest };
