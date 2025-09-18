// relay-worker/src/pipeline/ingestAuto.js
'use strict';

const db = require('../utils/db');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject } = require('../utils/gcs');

let extractText, detectBrandAndCode;
try { ({ extractText } = require('../utils/extract')); } catch { extractText = null; }
try { ({ detectBrandAndCode } = require('../utils/brandcode')); } catch { detectBrandAndCode = null; }

const vx = (() => {
  try { return require('../utils/vertex'); }
  catch { return {}; }
})();
const classifyFamily = vx.classifyFamily || null;
const extractByBlueprint = vx.extractByBlueprint || null;
const extractPartNumbersFromTypes = vx.extractPartNumbersFromTypes || (async () => ({ parts: [], table_hint: '' }));

// utils
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

async function runAutoIngest({ gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null }) {
  if (!gcsUri) throw new Error('gcsUri required');

  const tag = `[ingest:${Date.now()}]`;
  let corpus = '';
  try {
    if (extractText) {
      const { text, pages } = await extractText(gcsUri);
      corpus = (text || (pages || []).map(p => p.text).join('\n\n') || '').toString();
    }
  } catch (e) {
    console.warn(tag, 'WARN extractText:', e?.message || e);
  }

  // 1) 감지(브랜드/코드)
  try {
    if ((!brand || !code) && detectBrandAndCode && corpus) {
      const picked = await detectBrandAndCode(corpus);
      if (!brand)  brand  = picked?.brand  || '';
      if (!code)   code   = picked?.code   || '';
      if (!series) series = picked?.series || '';
      if (!display_name) display_name = series ? `${brand} ${series}`.trim() : brand;
    }
  } catch (e) {
    console.warn(tag, 'WARN detectBrandAndCode:', e?.message || e);
  }

  // 2) 패밀리 분류
  try {
    if (!family_slug) {
      const families = await loadFamilies();
      if (classifyFamily && corpus) {
        family_slug = await classifyFamily(corpus, families);
      }
    }
  } catch (e) {
    console.warn(tag, 'WARN classifyFamily:', e?.message || e);
  }
  family_slug = normSlug(family_slug || '') || 'relay_power';

  // 3) 블루프린트
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

  // 4) 스펙 추출
  let values = {};
  try {
    if (extractByBlueprint && Object.keys(fields || {}).length && corpus) {
      const out = await extractByBlueprint(corpus, fields, prompt_template);
      values = out?.values || {};
    }
  } catch (e) {
    console.warn(tag, 'WARN extractByBlueprint:', e?.message || e);
  }

  // 5) 타입 표 기반 품번 리스트 (있으면 팬아웃, 없으면 단건)
  let partList = [];
  try {
    const out = await extractPartNumbersFromTypes(gcsUri);
    partList = Array.isArray(out?.parts) ? out.parts : [];
    if (out?.table_hint && out.table_hint.toUpperCase().includes('ORDERING')) {
      // 주문정보만 있고 조합식이면 리스트를 비움 (요청사항: 조합식은 생성하지 않음)
      partList = [];
    }
  } catch (e) {
    console.warn(tag, 'WARN extractPartNumbersFromTypes:', e?.message || e);
  }

  // 6) 공통 경로 계산을 위한 버킷
  const bucketEnv = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//, '');
  const bucket    = bucketEnv.split('/')[0] || '';

  // 7) 실제 업서트 수행 함수(단건)
  async function upsertOne(oneCode) {
    const normBrand = String(brand || 'unknown').trim();
    const normCode  = String(oneCode || code || safeTempCode(gcsUri)).trim();

    // 경로는 정규화된 brand/code로 계산
    const datasheet_uri = bucket ? canonicalDatasheetPath(bucket, family_slug, normBrand, normCode) : null;
    const cover         = bucket ? canonicalCoverPath(bucket, family_slug, normBrand, normCode) : null;

    const base = {
      brand      : normBrand,
      code       : normCode,
      brand_norm : normBrand.toLowerCase(),
      code_norm  : normCode.toLowerCase(),
      series, display_name, family_slug,
      datasheet_uri, cover, source_gcs_uri: gcsUri,
      raw_json: { values }
    };

    // 테이블 보장
    try { await ensureSpecsTable(specs_table, fields || {}); }
    catch (e) {
      console.warn(tag, 'WARN ensureSpecsTable:', e?.message || e, '→ fallback to relay_power_specs');
      specs_table = 'relay_power_specs';
      await ensureSpecsTable(specs_table, {});
    }

    // 허용 컬럼 필터
    const filtered = {};
    try {
      const m = /^(.+)\.(.+)$/.exec(specs_table);
      const schema = m ? m[1] : 'public';
      const table  = m ? m[2] : specs_table;
      const rows = await db.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,
        [schema, table]
      );
      const allowed = new Set(rows.rows.map(r => r.column_name));
      for (const [k, v] of Object.entries({ ...base, ...(values || {}) })) {
        if (allowed.has(k)) filtered[k] = v;
      }
    } catch (e) {
      console.warn(tag, 'WARN load allowed columns:', e?.message || e);
      Object.assign(filtered, base);
    }

    // 업서트
    let row;
    try { row = await upsertByBrandCode(specs_table, filtered); }
    catch (e) {
      console.error(tag, 'ERROR upsert:', e?.message || e, '→ fallback relay_power_specs');
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
        return { ok:false, error:String(e2?.message || e2) };
      }
    }

    // 파일 이동
    try {
      if (datasheet_uri && /^gs:\/\//i.test(datasheet_uri) && datasheet_uri !== gcsUri) {
        await moveObject(gcsUri, datasheet_uri);
      }
    } catch (e) {
      console.warn(tag, 'WARN moveObject:', e?.message || e);
    }

    return { ok:true, table: specs_table, brand: base.brand, code: base.code, datasheet_uri, row };
  }

  // 8) 팬아웃 or 단건
  if (Array.isArray(partList) && partList.length > 0) {
    // TYPES 표에 적힌 품번만 삽입
    const seen = new Set();
    const results = [];
    for (const pn of partList) {
      if (!pn) continue;
      const k = `${(brand || 'unknown').toLowerCase()}::${pn.toUpperCase()}`;
      if (seen.has(k)) continue;
      seen.add(k);

      const r = await upsertOne(pn);
      results.push(r);
    }
    // 요약 반환
    const okCount = results.filter(r => r?.ok).length;
    console.log(tag, 'OK FANOUT', { count: okCount });
    return { ok:true, fanout:true, count: okCount, results };
  }

  // 단건 처리
  const single = await upsertOne(code);
  console.log(tag, 'OK SINGLE', { brand: single.brand, code: single.code });
  return { ok:true, fanout:false, ...single };
}

module.exports = { runAutoIngest };
