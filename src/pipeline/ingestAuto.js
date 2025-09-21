// src/pipeline/ingestAuto.js
// 모든 부품군 공용 ingestion 파이프라인 (기존 기능 보존, 릴레이 하드코딩 제거)
// - family_slug 지정이 오면 우선 적용
// - 없으면 텍스트 휴리스틱 + (가능할 때 Vertex)로 판별
// - component_registry -> specs_table 조회/보장(ensure_specs_table)
// - component_spec_blueprint.fields_json 을 스키마로 LLM 추출(없으면 통과)
// - 공통 필드(width_mm/height_mm/length_mm/image_uri/datasheet_uri) 포함
// - brand_norm/code_norm 로 UPSERT (모든 *_specs 테이블의 유니크 키와 일치)

'use strict';

const db = require('../utils/db');              // 기존 연결 모듈 사용
const { extractDataset } = require('../utils/extract'); // 기존 텍스트 추출 재사용
let VertexAI;
try { VertexAI = require('@google-cloud/vertexai').VertexAI; } catch (_) { /* optional */ }

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_CLS  = process.env.GEMINI_MODEL_CLASSIFY || 'gemini-2.5-flash';
const MODEL_EXT  = process.env.GEMINI_MODEL_EXTRACT  || 'gemini-2.5-flash';
const USE_VERTEX = !!(VertexAI && PROJECT_ID);

// ---------- 유틸 ----------
const lc = (s) => (s || '').toString().trim().toLowerCase();
const now = () => new Date();

// 휴리스틱(가벼운 정규식) + Vertex(가능 시) + 명시값 우선
async function decideFamily({ explicit, text }) {
  if (explicit) return lc(explicit);

  const t = ' ' + lc(text) + ' ';
  const has = (rx) => new RegExp(rx, 'i').test(t);

  // 넓은 카테고리 우선 매칭 (필요시 추가 확장)
  if (has('\\bmlcc\\b|ceramic capacitor|c\\d{2}\\d{2}')) return 'capacitor_mlcc';
  if (has('electrolytic|aluminium capacitor'))          return 'capacitor_elec';
  if (has('\\bfilm capacitor\\b'))                       return 'capacitor_film';
  if (has('\\bchip resistor\\b|\\bresistor\\b'))        return 'resistor_chip';
  if (has('\\bmosfet\\b|vds|rds_on'))                    return 'mosfet';
  if (has('\\brectifier\\b|schottky|trr'))               return 'diode_rectifier';
  if (has('\\bigbt\\b|igbt module'))                     return 'igbt_module';
  if (has('\\brelay\\b')) {
    if (has('reed'))         return 'relay_reed';
    if (has('automotive|vehicle|car')) return 'relay_automotive';
    if (has('solid state|ssr'))        return 'relay_ssr';
    return 'relay_power';
  }

  if (!USE_VERTEX) return 'unknown';

  // Vertex 분류 (선택 사항; 환경/권한 준비돼 있으면 자동)
  try {
    const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });
    const model  = vertex.getGenerativeModel({ model: MODEL_CLS });
    // 레지스트리에서 선택지 가져오기
    const reg = await db.query(`SELECT family_slug FROM public.component_registry ORDER BY family_slug`);
    const choices = reg.rows.map(r => r.family_slug).join(', ');
    const prompt = [
      'Classify the electronic component family from this datasheet text.',
      `Return ONLY one slug from this list (else return "unknown"): ${choices}`
    ].join('\n');

    const resp = await model.generateContent({ contents: [{ role: 'user', parts: [{ text: prompt }, { text }]}] });
    const out  = (resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '').trim();
    const slug = out.split(/\s+/)[0].toLowerCase().replace(/[^a-z0-9_]/g,'');
    return slug || 'unknown';
  } catch {
    return 'unknown';
  }
}

// 블루프린트 로드
async function loadBlueprint(family) {
  const r = await db.query(
    `SELECT fields_json FROM public.component_spec_blueprint
      WHERE family_slug=$1 ORDER BY version DESC LIMIT 1`, [family]
  );
  return r.rows[0]?.fields_json || {};
}

// LLM 추출 (블루프린트 스키마 + 치수 3종 강제 포함). 실패시 {}.
async function extractByBlueprint({ rawText, code, blueprint }) {
  try {
    const schema = Object.entries(blueprint || {}).map(([k, t]) => ({ name: k, type: String(t || 'text') }));
    const have = new Set(schema.map(s => s.name));
    for (const k of ['width_mm','height_mm','length_mm']) if (!have.has(k)) schema.push({ name: k, type: 'numeric' });

    if (!USE_VERTEX) return {};

    const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });
    const model  = vertex.getGenerativeModel({ model: MODEL_EXT });
    const prompt = [
      'You extract exact fields from an electronic component datasheet.',
      `Target model code: ${code}. Return a strict JSON with ONLY the keys below.`,
      'If unknown, return null. Numeric fields are plain numbers (no units).',
      'Dimensions in millimeters; voltage in volts; current in amperes.',
      `Fields: ${JSON.stringify(schema)}`
    ].join('\n');

    const resp = await model.generateContent({
      contents: [{ role: 'user', parts: [{ text: prompt }, { text: rawText.slice(0, 32000) }]}]
    });
    const out = resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
    const obj = JSON.parse(out);

    // 숫자 캐스팅
    for (const { name, type } of schema) {
      if (String(type).toLowerCase().startsWith('num') && obj[name] != null) {
        const v = Number(String(obj[name]).replace(/[^0-9.+-eE]/g,''));
        obj[name] = Number.isFinite(v) ? v : null;
      }
    }
    return obj;
  } catch {
    return {};
  }
}

// allowed 화이트리스트 기반 UPSERT 쿼리 빌더
function buildUpsertSQL(table, payload, conflictCols = ['brand_norm','code_norm']) {
  const cols = Object.keys(payload);
  const colNames = cols.map(c => `"${c}"`).join(', ');
  const params = cols.map((_, i) => `$${i+1}`).join(', ');
  const updates = cols
    .filter(c => !conflictCols.includes(c))
    .map(c => `"${c}" = EXCLUDED."${c}"`).join(', ');
  const text = `
    INSERT INTO ${table} (${colNames})
    VALUES (${params})
    ON CONFLICT (${conflictCols.join(',')})
    DO UPDATE SET ${updates}`;
  return { text, values: cols.map(c => payload[c]) };
}

// ---------- 메인 엔트리 ----------
/**
 * @param {Object} job - 요청 페이로드
 * @param {string} job.gcsUri - gs://...pdf
 * @param {string=} job.family_slug - 명시 가족 (있으면 우선)
 * @param {string=} job.brand
 * @param {string=} job.code
 * @param {string=} job.filename
 */
async function ingestAuto(job) {
  const startedAt = now();
  const gcsUri = job.gcsUri;
  const filename = job.filename || gcsUri?.split('/').pop() || 'doc.pdf';

  // 1) 텍스트 추출(기존 함수 재사용)
  const ds = await extractDataset({
    gcsUri,
    filename,
    brandHint: job.brand,
    codeHint: job.code,
    seriesHint: job.series
  });

  const brand = ds.brand || job.brand || 'unknown';
  const code  = (Array.isArray(ds.rows) && ds.rows[0]?.code) || job.code || 'TMP_' + Math.random().toString(16).slice(2).toUpperCase();
  const text  = ds.text || '';

  // 2) family 결정
  const family = await decideFamily({ explicit: job.family_slug, text });
  // 2-1) specs 테이블 확인/보장
  await db.query('SELECT public.ensure_specs_table($1)', [family]); // DB 함수 사용. :contentReference[oaicite:3]{index=3}
  const reg = await db.query('SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1', [family]);
  const tableName = reg.rows[0]?.specs_table;
  if (!tableName) throw new Error(`Unknown family_slug: ${family} (component_registry)`); // :contentReference[oaicite:4]{index=4}
  const table = `public.${tableName}`;

  // 3) 블루프린트 기반 스펙 추출(선택)
  const blueprint = await loadBlueprint(family); // 없으면 {}
  const extracted = await extractByBlueprint({ rawText: text, code, blueprint });

  // 4) 공통 + 블루프린트 값 구성
  const brand_norm = lc(brand);
  const code_norm  = lc(code);

  const base = {
    family_slug: family,
    brand, code, brand_norm, code_norm,
    datasheet_uri: gcsUri,        // DB 스키마 표준 컬럼명(…_url 아님) :contentReference[oaicite:5]{index=5}
    image_uri: extracted.image_uri ?? null,
    updated_at: now()
  };

  // 블루프린트에 정의된 키 + 공통치수만 허용
  const allowed = new Set([
    ...Object.keys(blueprint || {}),
    'family_slug','brand','code','brand_norm','code_norm',
    'datasheet_uri','image_uri','width_mm','height_mm','length_mm','updated_at',
    'series','display_name','product_name' // 일부 가족군에서 사용
  ]);

  const merged = Object.fromEntries(
    Object.entries({ ...base, ...extracted, series: ds.series || null }).filter(([k]) => allowed.has(k))
  );

  // 5) UPSERT
  const { text: sql, values } = buildUpsertSQL(table, merged);
  await db.query(sql, values); // Postgres ON CONFLICT … DO UPDATE (원자적 upsert) :contentReference[oaicite:6]{index=6}

  return {
    ok: true,
    family_slug: family,
    specs_table: table,
    brand, code,
    rows: 1,
    ms: Date.now() - startedAt.getTime()
  };
}

module.exports = { ingestAuto };
