// relay-worker/src/pipeline/ingestAuto.js
'use strict';

const db = require('../utils/db');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');
const {
  getSignedUrl,
  canonicalDatasheetPath,
  canonicalCoverPath,
  moveObject,
} = require('../utils/gcs');

// ▼ 아래 3개 유틸은 레포에 있는 구현을 그대로 재사용하세요.
//    (없으면 try/catch에서 안전 폴백으로 넘어갑니다)
let extractText, detectBrandAndCode, classifyFamily, extractByBlueprint;
try { ({ extractText } = require('../utils/extract')); } catch { extractText = null; }
try { ({ detectBrandAndCode } = require('../utils/brandcode')); } catch { detectBrandAndCode = null; }
try {
  const vx = require('../utils/vertex');
  classifyFamily = vx.classifyFamily || null;
  extractByBlueprint = vx.extractByBlueprint || null;
} catch { classifyFamily = null; extractByBlueprint = null; }

// ────────────── 공용 도우미 ──────────────
function normSlug(s) { return String(s || '').trim().toLowerCase().replace(/[^a-z0-9_]+/g, '_'); }
function safeTempCode(uri) {
  const h = require('crypto').createHash('sha1').update(String(uri || Date.now())).digest('hex').slice(0, 6);
  return `TMP_${h}`.toUpperCase();
}
async function loadFamilies() {
  try {
    const r = await db.query('SELECT DISTINCT family_slug FROM public.component_registry ORDER BY 1');
    return r.rows.map(x => x.family_slug).filter(Boolean);
  } catch (_) { return ['relay_power']; }
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
  } catch (_) { return null; }
}

// ────────────── 메인 파이프라인 ──────────────
async function runAutoIngest({ gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null }) {
  if (!gcsUri) throw new Error('gcsUri required');

  // 디버그 태그
  const t0 = Date.now();
  const tag = `[ingest:${t0}]`;

  // 0) 텍스트 확보 (DocAI/LLM 유틸 없을 때도 안전)
  let corpus = '';
  try {
    if (extractText) {
      const { text, pages } = await extractText(gcsUri);
      corpus = (text || (pages || []).map(p => p.text).join('\n\n') || '').toString();
    }
  } catch (e) {
    console.warn(tag, 'WARN extractText:', e?.message || e);
  }

  // 1) 브랜드/코드 (유틸 없거나 실패해도 폴백)
  try {
    if ((!brand || !code) && detectBrandAndCode && corpus) {
      const picked = await detectBrandAndCode(corpus);
      if (!brand)  brand  = picked?.brand  || '';
      if (!code)   code   = picked?.code   || '';
      if (!series) series = picked?.series || '';
    }
  } catch (e) {
    console.warn(tag, 'WARN detectBrandAndCode:', e?.message || e);
  }

  // 2) 패밀리 분류 (없으면 폴백: relay_power)
  try {
    if (!family_slug && classifyFamily && corpus) {
      const families = await loadFamilies();
      family_slug = await classifyFamily(corpus, families);
    }
  } catch (e) {
    console.warn(tag, 'WARN classifyFamily:', e?.message || e);
  }
  family_slug = normSlug(family_slug || '') || 'relay_power';

  // 3) 블루프린트 로드 (없어도 계속 진행)
  let fields = {};
  let specs_table = `${family_slug}_specs`;
  let prompt_template = '';
  try {
    const bp = await loadBlueprint(family_slug);
    if (bp) {
      fields = bp.fields_json || {};
      prompt_template = bp.prompt_template || '';
      specs_table = bp.specs_table || specs_table;
    } else {
      console.warn(tag, `WARN blueprint not found for family=${family_slug}, proceed with base columns only`);
    }
  } catch (e) {
    console.warn(tag, 'WARN loadBlueprint:', e?.message || e);
  }

  // 4) 스펙 추출 (실패해도 values는 비운 채 진행)
  let values = {};
  try {
    if (extractByBlueprint && Object.keys(fields || {}).length && corpus) {
      const out = await extractByBlueprint(corpus, fields, prompt_template);
      values = out && typeof out === 'object' ? out : {};
    }
  } catch (e) {
    console.warn(tag, 'WARN extractByBlueprint:', e?.message || e);
  }

  // 5) 경로 계산
  const bucketEnv = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//, '');
  const bucket    = bucketEnv.split('/')[0] || '';

  // 6) 정상화 + 최소 제약 충족 (항상 채움)
  const normBrand = String(brand || 'unknown').trim();
  const normCode  = String(code  || safeTempCode(gcsUri)).trim();
   // ✅ 경로는 정규화된 brand/code로 계산해야 // 빈 경로 방지
 const datasheet_uri = bucket
   ? canonicalDatasheetPath(bucket, family_slug, normBrand, normCode)
   : null;
 const cover = bucket
   ? canonicalCoverPath(bucket, family_slug, normBrand, normCode)
   : null;
  const base = {
    brand      : normBrand,
    code       : normCode,
    brand_norm : normBrand.toLowerCase(),
    code_norm  : normCode.toLowerCase(),
    series, display_name, family_slug,
    datasheet_uri, cover, source_gcs_uri: gcsUri,
    raw_json: { meta: { t0, corpus_len: corpus?.length || 0 }, values },
  };

  // 7) 테이블 보장
  try {
    await ensureSpecsTable(specs_table, fields || {});
  } catch (e) {
    console.warn(tag, 'WARN ensureSpecsTable:', e?.message || e, '→ fallback to relay_power_specs');
    // 마지막 폴백: 존재 보장된 테이블로
    specs_table = 'relay_power_specs';
    await ensureSpecsTable(specs_table, {});
  }

  // 8) 허용 컬럼만 필터링 후 업서트 (실패해도 한 번 더 폴백)
  const filtered = {};
  try {
    const allowed = await (async () => {
      const m = /^(.+)\.(.+)$/.exec(specs_table);
      const schema = m ? m[1] : 'public';
      const table  = m ? m[2] : specs_table;
      const rows = await db.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,
        [schema, table]
      );
      return new Set(rows.rows.map(r => r.column_name));
    })();
    for (const [k, v] of Object.entries({ ...base, ...(values || {}) })) {
      if (allowed.has(k)) filtered[k] = v;
    }
  } catch (e) {
    console.warn(tag, 'WARN load allowed columns:', e?.message || e);
    Object.assign(filtered, base); // 최악의 경우 베이스만 업서트
  }

  // 9) 업서트 & 파일 이동(실패해도 계속)
  let row = null;
  try {
    row = await upsertByBrandCode(specs_table, filtered);
  } catch (e) {
    console.error(tag, 'ERROR upsert first try:', e?.message || e, '→ fallback table relay_power_specs');
    try {
      specs_table = 'relay_power_specs';
      await ensureSpecsTable(specs_table, {});
      row = await upsertByBrandCode(specs_table, {
        brand: base.brand, code: base.code,
        brand_norm: base.brand_norm, code_norm: base.code_norm,
        family_slug: 'relay_power', source_gcs_uri: gcsUri,
        datasheet_uri: base.datasheet_uri, cover: base.cover,
        series: base.series || null, display_name: base.display_name || null
      });
    } catch (e2) {
      console.error(tag, 'ERROR upsert fallback failed:', e2?.message || e2);
      // 마지막: 아무 것도 못 넣었지만, 200으로 리턴(태스크 재시도 방지) + 상세 메시지
      return {
        ok: false,
        error: 'UPSERT_FAILED',
        detail: String(e2?.message || e2),
        family_slug, specs_table, brand: base.brand, code: base.code
      };
    }
  }

  // 파일 이동(원본→표준 경로)
  try {
    if (datasheet_uri && /^gs:\/\//i.test(datasheet_uri) && datasheet_uri !== gcsUri) {
      await moveObject(gcsUri, datasheet_uri);
    }
  } catch (e) {
    console.warn(tag, 'WARN moveObject:', e?.message || e);
  }

  // (선택) 서명 URL
  let signed_pdf = null;
  try { if (datasheet_uri) signed_pdf = await getSignedUrl(datasheet_uri, { minutes: 30 }); } catch {}

  // 10) 최종 결과
  const ms = Date.now() - t0;
  console.log(tag, 'OK', { table: specs_table, brand: base.brand, code: base.code, ms });
  return {
    ok: true,
    family_slug, specs_table,
    brand: base.brand, code: base.code, series, display_name,
    datasheet_uri, cover, signed_pdf, row,
    ms
  };
}

module.exports = { runAutoIngest };
