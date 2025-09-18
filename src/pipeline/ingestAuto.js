// relay-worker/src/pipeline/ingestAuto.js
// CommonJS ONLY. 함수 내부에 require 넣지 말 것.

const db = require('../utils/db');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject } = require('../utils/gcs');
const { identifyFamilyBrandCode, extractByBlueprintGemini } = require('../utils/vertex');

// family 유틸 (안전 가드)
const famUtil = require('../utils/family');
const normalizeFamilySlug = (typeof famUtil.normalizeFamilySlug === 'function')
  ? famUtil.normalizeFamilySlug : (s) => (s || '').toString().trim().toLowerCase();
const chooseCanonicalFamilySlug = (typeof famUtil.chooseCanonicalFamilySlug === 'function')
  ? famUtil.chooseCanonicalFamilySlug : () => null;

/** registry+blueprint 로드 */
async function fetchBlueprint(family_slug) {
  const r = await db.query(`
    SELECT r.specs_table, b.fields_json, b.prompt_template
    FROM public.component_registry r
    JOIN public.component_spec_blueprint b ON b.family_slug = r.family_slug
    WHERE r.family_slug = $1 LIMIT 1`, [family_slug]);
  if (!r.rows.length) throw new Error(`Blueprint not found for family=${family_slug}`);
  return r.rows[0];
}
async function getFamilies() {
  const r = await db.query(`SELECT family_slug FROM public.component_registry ORDER BY family_slug`);
  return r.rows.map(x => x.family_slug);
}
async function getTableColumnsQualified(targetTable) {
  const m = /^(.+)\.(.+)$/.exec(targetTable);
  const schema = m ? m[1] : 'public';
  const table  = m ? m[2] : targetTable;
  const colsRes = await db.query(
    `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,
    [schema, table]
  );
  return new Set(colsRes.rows.map(r => r.column_name));
}

/* ---- 폴백 유틸 ---- */
function guessBrandCodeFromPath(gcsUri) {
  try {
    const name = String(gcsUri || '').split('/').pop() || '';
    const base = name.replace(/\.(pdf|zip|png|jpg|jpeg)$/i, '');
    const m1 = /^([A-Za-z0-9]+)[_\-\s]+([A-Za-z0-9\.\-]+)$/.exec(base);
    if (m1) return { brand: m1[1], code: m1[2] };
    if (/^[A-Za-z0-9\.\-]+$/.test(base)) return { brand: null, code: base };
  } catch {}
  return { brand: null, code: null };
}
function safeTempCodeFromUri(gcsUri) {
  const crypto = require('crypto');
  return `tmp_${crypto.createHash('sha256').update(String(gcsUri || '')).digest('hex').slice(0,6)}`;
}

/** 메인 */
async function runAutoIngest({ gcsUri, family_slug, brand, code, series=null, display_name=null }) {
  if (!gcsUri) throw new Error('gcsUri required');

  // 1) 감지
  if (!family_slug || !brand || !code) {
    const families = await getFamilies();
    const det = await identifyFamilyBrandCode(gcsUri, families).catch(() => ({}));

    const rawFam = family_slug || det.family_slug || null;
    family_slug  = rawFam ? normalizeFamilySlug(rawFam) : null;

    try {
      const picked = chooseCanonicalFamilySlug(family_slug, families);
      if (picked) family_slug = picked;
    } catch {}

    brand        = brand || det.brand || null;
    code         = code  || det.code  || null;
    series       = series || det.series || null;
    display_name = display_name || det.display_name || null;

    // 여전히 family가 없으면 휴리스틱/폴백
    if (!family_slug) {
      const fname = String(gcsUri || '').split('/').pop() || '';
      const guess = chooseCanonicalFamilySlug(fname, families);
      if (guess) family_slug = guess;
      if (!family_slug) family_slug = families.includes('relay_power') ? 'relay_power' : (families[0] || 'relay_power');
    }

    // 브랜드/코드도 폴백
    if (!brand || !code) {
      const gc = guessBrandCodeFromPath(gcsUri);
      brand = brand || gc.brand || 'unknown';
      code  = code  || gc.code  || safeTempCodeFromUri(gcsUri);
    }
  }
  if (!family_slug) throw new Error('Unable to determine family');

  // 2) blueprint
  let bp = await fetchBlueprint(family_slug);
  let specs_table     = bp.specs_table;
  const fields_json   = bp.fields_json || {};
  const prompt_template = bp.prompt_template || null;

  // 3) 추출
  const ext = await extractByBlueprintGemini(gcsUri, fields_json, prompt_template);
  const v   = ext?.values || {};
  const raw_json = ext?.raw_json || null;

  // ⚖️ family 보정: 접점 DC 정격이 2A 이하이면 시그널 릴레이로 전환
  const a_dc = Number(v.contact_rating_dc_a ?? v.contact_rating_a ?? 0);
  if (a_dc && a_dc <= 2 && family_slug !== 'relay_signal') {
    family_slug = 'relay_signal';
    bp = await fetchBlueprint(family_slug);
    await ensureSpecsTable(bp.specs_table, bp.fields_json);
    specs_table = bp.specs_table;
  }

  // 4) 테이블 보장
  await ensureSpecsTable(specs_table, fields_json);

  // 5) 경로
  const bucketEnv = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//, '');
  const bucket    = bucketEnv.split('/')[0] || '';
  const datasheet_url = canonicalDatasheetPath(bucket, family_slug, brand, code);
  const cover         = canonicalCoverPath(bucket, family_slug, brand, code);

  // 6) 업서트 — brand_norm/code_norm 반드시 포함(제약 충족)
  const allowed  = await getTableColumnsQualified(specs_table);
  const normBrand = String(brand || 'unknown').trim();
  const normCode  = String(code  || safeTempCodeFromUri(gcsUri)).trim();
  const base = {
    brand: normBrand,
    code:  normCode,
    brand_norm: normBrand.toLowerCase(),
    code_norm:  normCode.toLowerCase(),
    series, display_name, family_slug,
    datasheet_url, cover,
    source_gcs_uri: gcsUri,
    raw_json,
  };
  const filtered = {};
  for (const [k, val] of Object.entries({ ...base, ...v })) {
    if (allowed.has(k)) filtered[k] = val;
  }
  const row = await upsertByBrandCode(specs_table, filtered);

  // 7) 파일 이동
  try {
    const finalGsUri = datasheet_url;
    if (typeof finalGsUri === 'string' && finalGsUri.startsWith('gs://') && finalGsUri !== gcsUri) {
      await moveObject(gcsUri, finalGsUri);
    }
  } catch (e) {
    console.warn('[ingest] moveObject failed:', e?.message || e);
  }

  // (선택) 서명 URL
  let signed_pdf = null;
  try { signed_pdf = await getSignedUrl(datasheet_url, { minutes: 30 }); } catch {}

  return {
    ok: true,
    family_slug, specs_table,
    brand: normBrand, code: normCode,
    series, display_name, datasheet_url, cover, signed_pdf, row,
  };
}

module.exports = { runAutoIngest };
