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
const { normalizeFamilySlug } = require('../utils/family');

/** registry+blueprint 로드 */
async function fetchBlueprint(family_slug) {
  const r = await db.query(
    `
    SELECT r.specs_table, b.fields_json, b.prompt_template
      FROM public.component_registry r
      JOIN public.component_spec_blueprint b
        ON b.family_slug = r.family_slug
     WHERE r.family_slug = $1
     LIMIT 1
    `,
    [family_slug]
  );
  if (!r.rows.length) throw new Error(`Blueprint not found for family=${family_slug}`);
  return r.rows[0];
}

/** 등록된 family 목록 */
async function getFamilies() {
  const r = await db.query(
    `SELECT family_slug FROM public.component_registry ORDER BY family_slug`
  );
  return r.rows.map((x) => x.family_slug);
}

/** 테이블 실제 컬럼 세트(교집합 업서트용) */
async function getTableColumnsQualified(targetTable) {
  const m = /^(.+)\.(.+)$/.exec(targetTable);
  const schema = m ? m[1] : 'public';
  const table = m ? m[2] : targetTable;
  const colsRes = await db.query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema=$1 AND table_name=$2`,
    [schema, table]
  );
  return new Set(colsRes.rows.map((r) => r.column_name));
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
    const det = await identifyFamilyBrandCode(gcsUri, families);
    family_slug = normalizeFamilySlug(family_slug || det.family_slug);
    brand = brand || det.brand;
    code = code || det.code;
    series = series || det.series || null;
    display_name = display_name || det.display_name || null;
  }
  if (!family_slug || !brand || !code) {
    throw new Error('Unable to determine family/brand/code');
  }

  // 2) blueprint
  const bp = await fetchBlueprint(family_slug);
  const specs_table = bp.specs_table;
  const fields_json = bp.fields_json || {};
  const prompt_template = bp.prompt_template || null;

  // 3) extraction (Gemini)
  const ext = await extractByBlueprintGemini(gcsUri, fields_json, prompt_template);
  const extractedValues = (ext && ext.values) ? ext.values : {};
  const raw_json = ext && ext.raw_json ? ext.raw_json : null;

  // 4) ensure table
  await ensureSpecsTable(specs_table, fields_json);

  // 5) canonical paths
  const bucketEnv = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//, '');
  const bucket = bucketEnv.split('/')[0] || '';
  const datasheet_url = canonicalDatasheetPath(bucket, family_slug, brand, code);
  const cover = canonicalCoverPath(bucket, family_slug, brand, code); // 썸네일 생성은 TODO

  // 6) 안전 업서트(실제 존재 컬럼에 한해)
  const allowed = await getTableColumnsQualified(specs_table);
  const base = {
    brand,
    code,
    series,
    display_name,
    family_slug,
    datasheet_url,
    cover,
    source_gcs_uri: gcsUri,
    raw_json,
  };
  const filtered = {};
  for (const [k, v] of Object.entries({ ...base, ...extractedValues })) {
    if (allowed.has(k)) filtered[k] = v;
  }

  const row = await upsertByBrandCode(specs_table, filtered);

  // 7) 파일 이동 (원본 → canonical)
  try {
    // canonicalDatasheetPath 가 "gs://<bucket>/..." 형태를 돌려준다고 가정
    const finalGsUri = datasheet_url;
    if (typeof finalGsUri === 'string' && finalGsUri.startsWith('gs://')) {
      if (finalGsUri !== gcsUri) {
        await moveObject(gcsUri, finalGsUri);
      }
    }
  } catch (e) {
    // 이동 실패해도 업서트는 이미 끝났으므로 경고만
    console.warn('[ingest] moveObject failed:', e?.message || e);
  }

  // (선택) 서명 URL (읽기 편의)
  let signed_pdf = null;
  try {
    signed_pdf = await getSignedUrl(datasheet_url, { minutes: 30 });
  } catch {
    // ignore
  }

  return {
    ok: true,
    family_slug,
    specs_table,
    brand,
    code,
    series,
    display_name,
    datasheet_url,
    cover,
    signed_pdf,
    row,
  };
}

module.exports = { runAutoIngest };
