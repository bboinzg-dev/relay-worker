// relay-worker/src/pipeline/ingestAuto.js
// CommonJS ONLY. 함수 내부에 require 넣지 말 것.

const db = require('../utils/db');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');
const {
  getSignedUrl,
  canonicalDatasheetPath,
  canonicalCoverPath,
  moveObject,
} = require('../utils/gcs');
const { identifyFamilyBrandCode, extractByBlueprintGemini } = require('../utils/vertex');

// ✅ family 유틸(안전 가드 포함)
const famUtil = require('../utils/family');
const normalizeFamilySlug =
  (typeof famUtil.normalizeFamilySlug === 'function')
    ? famUtil.normalizeFamilySlug
    : (s) => (s || '').toString().trim().toLowerCase();
const chooseCanonicalFamilySlug =
  (typeof famUtil.chooseCanonicalFamilySlug === 'function')
    ? famUtil.chooseCanonicalFamilySlug
    : () => null;

/** registry+blueprint 로드 */
async function fetchBlueprint(family_slug) {
  const r = await db.query(`
    SELECT r.specs_table, b.fields_json, b.prompt_template
      FROM public.component_registry r
      JOIN public.component_spec_blueprint b
        ON b.family_slug = r.family_slug
     WHERE r.family_slug = $1
     LIMIT 1`, [family_slug]);
  if (!r.rows.length) throw new Error(`Blueprint not found for family=${family_slug}`);
  return r.rows[0];
}

/** 등록된 family 목록 */
async function getFamilies() {
  const r = await db.query(`SELECT family_slug FROM public.component_registry ORDER BY family_slug`);
  return r.rows.map(x => x.family_slug);
}

/** 테이블 실제 컬럼 세트(교집합 업서트용) */
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

/* ---------------- Fallback helpers ---------------- */

/** 파일명/경로에서 브랜드·코드 힌트 추출 (아주 보수적으로) */
function guessBrandCodeFromPath(gcsUri) {
  try {
    const name = String(gcsUri || '').split('/').pop() || '';
    const base = name.replace(/\.(pdf|zip|png|jpg|jpeg)$/i, '');
    // 흔한 패턴: BRAND_CODE, BRAND-CODE, BRAND CODE
    const m1 = /^([A-Za-z0-9]+)[_\-\s]+([A-Za-z0-9\.\-]+)$/.exec(base);
    if (m1) return { brand: m1[1], code: m1[2] };
    // 코드만 분리 가능한 경우
    if (/^[A-Za-z0-9\.\-]+$/.test(base)) return { brand: null, code: base };
  } catch {}
  return { brand: null, code: null };
}

/** 안전 임시 코드(유니크 보장) */
function safeTempCodeFromUri(gcsUri) {
  const crypto = require('crypto');
  const sha6 = crypto.createHash('sha256').update(String(gcsUri || '')).digest('hex').slice(0, 6);
  return `tmp_${sha6}`;
}

/**
 * Auto ingest pipeline:
 * - Detect {family,brand,code,series,display_name} if missing (Gemini)
 * - Fetch blueprint (fields/prompt)
 * - LLM extract
 * - ensureSpecsTable + safe upsert(컬럼 교집합만)
 * - Move PDF to canonical path; set datasheet_url / cover
 */
async function runAutoIngest({
  gcsUri,
  family_slug,
  brand,
  code,
  series = null,
  display_name = null,
}) {
  if (!gcsUri) throw new Error('gcsUri required');

  // 1) detection (필요 시)
  if (!family_slug || !brand || !code) {
    const families = await getFamilies();
    const det = await identifyFamilyBrandCode(gcsUri, families).catch(() => ({}));

    // 1차: 별칭 정규화
    const rawFam = family_slug || det.family_slug || null;
    family_slug  = rawFam ? normalizeFamilySlug(rawFam) : null;

    // 2차: 레지스트리 중 최적 선택(모호하면 보정)
    try {
      const picked = chooseCanonicalFamilySlug(family_slug, families);
      if (picked) family_slug = picked;
    } catch {}

    brand        = brand || det.brand || null;
    code         = code  || det.code  || null;
    series       = series || det.series || null;
    display_name = display_name || det.display_name || null;

    // 3차: 여전히 family가 없으면 휴리스틱/폴백
    if (!family_slug) {
      const fname = String(gcsUri || '').split('/').pop() || '';
      const guess = chooseCanonicalFamilySlug(fname, families);
      if (guess) family_slug = guess;
    }
    if (!family_slug) {
      // 레지스트리에 있는 기본값 우선: relay_power → 없으면 첫 가족
      if (families.includes('relay_power')) family_slug = 'relay_power';
      else if (families.length) family_slug = families[0];
    }

    // 4차: brand/code 폴백 — 파일명 힌트 → 임시코드
    if (!brand || !code) {
      const gc = guessBrandCodeFromPath(gcsUri);
      brand = brand || gc.brand || 'unknown';
      code  = code  || gc.code  || safeTempCodeFromUri(gcsUri);
    }
  }

  // 👉 더 이상 brand/code 때문에 실패하지 않도록, 최소 family만 확인
  if (!family_slug) {
    throw new Error('Unable to determine family');
  }

  // 2) blueprint
  const bp = await fetchBlueprint(family_slug);
  const specs_table     = bp.specs_table;
  const fields_json     = bp.fields_json || {};
  const prompt_template = bp.prompt_template || null;

  // 3) extraction (Gemini)
  const ext = await extractByBlueprintGemini(gcsUri, fields_json, prompt_template);
  const extractedValues = (ext && ext.values) ? ext.values : {};
  const raw_json        = ext && ext.raw_json ? ext.raw_json : null;

  // 4) ensure table
  await ensureSpecsTable(specs_table, fields_json);

  // 5) canonical paths
  const bucketEnv = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//, '');
  const bucket    = bucketEnv.split('/')[0] || '';
  const datasheet_url = canonicalDatasheetPath(bucket, family_slug, brand, code);
  const cover         = canonicalCoverPath(bucket, family_slug, brand, code); // TODO: 썸네일 생성

  // 6) 안전 업서트(실제 존재 컬럼에 한해)
  const allowed = await getTableColumnsQualified(specs_table);
  const base = {
    brand, code, series, display_name,
    family_slug, datasheet_url, cover,
    source_gcs_uri: gcsUri, raw_json,
  };
  const filtered = {};
  for (const [k, v] of Object.entries({ ...base, ...extractedValues })) {
    if (allowed.has(k)) filtered[k] = v;
  }
  const row = await upsertByBrandCode(specs_table, filtered);

  // 7) 파일 이동 (원본 → canonical)
  try {
    const finalGsUri = datasheet_url;
    if (typeof finalGsUri === 'string' && finalGsUri.startsWith('gs://') && finalGsUri !== gcsUri) {
      await moveObject(gcsUri, finalGsUri);
    }
  } catch (e) {
    console.warn('[ingest] moveObject failed:', e?.message || e);
  }

  // (선택) 서명 URL (읽기 편의)
  let signed_pdf = null;
  try { signed_pdf = await getSignedUrl(datasheet_url, { minutes: 30 }); } catch {}

  return {
    ok: true,
    family_slug, specs_table, brand, code, series, display_name,
    datasheet_url, cover, signed_pdf, row,
  };
}

module.exports = { runAutoIngest };
