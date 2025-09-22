'use strict';
/**
 * 개선된 ingestion 파이프라인(멀티 품번/표 우선) - partsplan 배포용.
 *
 * - family_slug 미지정 시 파일명/미리보기로 휴리스틱 추정(기본 relay_power).
 * - component_registry → blueprint를 읽어 추출 허용 컬럼 확정.
 * - Document AI 활성 환경이면 표 기반 추출, 아니면 pdf-parse 폴백.
 * - 표의 "Part Number/Ordering Code/Type/Model" 우선, 조합형은 폭 과다 시 스킵(추출기 내부).
 * - (brand_norm, code_norm) 유니크 키 기준 멱등 업서트.
 * - DB의 public.ensure_specs_table(family) 호출로 스키마 보장(서버 권위).
 * - 리레이 계열 등 특수 컬럼(cover, display_name, datasheet_url 등) 존재 시 안전 반영.
 * - 1~2페이지 최대 이미지 추출(pdfimages 있으면) → 커버 저장.
 */

const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const db = require('../utils/db');
const { storage, parseGcsUri, readText, canonicalCoverPath } = require('../utils/gcs');
const { upsertByBrandCode } = require('../utils/schema'); // DB upsert 유틸은 재사용
const { getBlueprint } = require('../utils/blueprint');
const { extractPartsAndSpecsFromPdf } = require('../ai/datasheetExtract');

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

function normLower(s) {
  return String(s || '').trim().toLowerCase();
}

async function getTableColumns(qualified /* e.g. public.relay_power_specs or relay_power_specs */) {
  const [schema, table] = qualified.includes('.') ? qualified.split('.') : ['public', qualified];
  const q = `
    SELECT a.attname AS col
      FROM pg_attribute a
      JOIN pg_class c ON a.attrelid = c.oid
      JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1
       AND c.relname = $2
       AND a.attnum > 0
       AND NOT a.attisdropped
  `;
  const r = await db.query(q, [schema, table]);
  return new Set(r.rows.map(x => x.col));
}

/** 서버측 스키마 보장 함수 호출(권위: DB plpgsql). */
async function ensureSpecsTableForFamily(family) {
  await db.query(`SELECT public.ensure_specs_table($1)`, [family]);
}

/** 1~2페이지에서 최대 PNG 추출(pdfimages 필요). 추출 실패는 무시. */
async function extractCoverToGcs(gcsPdfUri, { family, brand, code }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmpDir  = path.join(os.tmpdir(), 'pdf-' + Date.now());
    const pdfPath = path.join(tmpDir, 'doc.pdf');
    await fs.mkdir(tmpDir, { recursive: true });

    // download
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(pdfPath, buf);

    // poppler-utils(pdfimages) 기반 추출
    await execFileP('pdfimages', ['-f','1','-l','2','-png', pdfPath, path.join(tmpDir, 'img')]);

    const files = (await fs.readdir(tmpDir)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!files.length) return null;

    let pick = null, size = -1;
    for (const f of files) {
      const st = await fs.stat(path.join(tmpDir, f));
      if (st.size > size) { pick = f; size = st.size; }
    }
    if (!pick) return null;

    const dst = canonicalCoverPath(
      (process.env.ASSET_BUCKET || process.env.GCS_BUCKET || '').replace(/^gs:\/\//,''),
      family, brand, code
    );
    const { bucket: outBkt, name: outName } = parseGcsUri(dst);
    await storage.bucket(outBkt).upload(path.join(tmpDir, pick), {
      destination: outName, resumable: false,
    });
    return dst;
  } catch {
    return null; // best-effort
  }
}

function guessFamilySlug({ fileName = '', previewText = '' }) {
  const s = (fileName + ' ' + previewText).toLowerCase();
  if (/\b(resistor|r-clamp|ohm)\b/.test(s)) return 'resistor_chip';
  if (/\b(capacitor|mlcc|electrolytic|tantalum)\b/.test(s)) return 'capacitor_mlcc';
  if (/\b(inductor|choke)\b/.test(s)) return 'inductor_power';
  if (/\b(bridge|rectifier|diode)\b/.test(s)) return 'bridge_rectifier';
  if (/\b(relay|coil|omron|finder)\b/.test(s)) return 'relay_power';
  return null;
}

// ----------------------------------------------------------------------------
// main
// ----------------------------------------------------------------------------

/**
 * @param {Object} params
 * @param {string} params.gcsUri - gs://... PDF 경로(필수)
 * @param {string|null} params.family_slug - 강제 패밀리 지정(옵션)
 * @param {string|null} params.brand - 단일 업서트 시 브랜드 강제(옵션)
 * @param {string|null} params.code - 단일 업서트 시 코드 강제(옵션)
 * @param {string|null} params.series - 단일 업서트 시 시리즈 명시(옵션)
 * @param {string|null} params.display_name - 단일 업서트 시 표시명(옵션)
 */
async function runAutoIngest({
  gcsUri,
  family_slug = null,
  brand = null,
  code = null,
  series = null,
  display_name = null,
}) {
  const started = Date.now();
  if (!gcsUri) throw new Error('gcsUri required');

  // 0) Quick family guess
  let fileName = '';
  try {
    const { name } = parseGcsUri(gcsUri);
    fileName = path.basename(name);
  } catch {}
  let family = normLower(family_slug) || guessFamilySlug({ fileName }) || 'relay_power';

  // 미리보기 텍스트로 보정(최대 256KB)
  if (!family) {
    try {
      const text = await readText(gcsUri, 256 * 1024);
      family = guessFamilySlug({ fileName, previewText: text }) || 'relay_power';
    } catch {
      family = 'relay_power';
    }
  }

  // 1) registry에서 목적 테이블 조회
  const reg = await db.query(
    `SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`,
    [family]
  );
  const table = reg.rows[0]?.specs_table || 'relay_power_specs';

  // 2) 서버측 스키마 보장(블루프린트 기반) - DB plpgsql 호출
  await ensureSpecsTableForFamily(family);

  // 2.5) Blueprint 로딩(추출 허용 키)
  const { allowedKeys } = await getBlueprint(family);

  // 3) 멀티 품번 추출 (DocAI → 표 우선)
  let extracted = { brand: brand || 'unknown', rows: [] };
  if (!brand || !code) {
    extracted = await extractPartsAndSpecsFromPdf({
      gcsUri,
      allowedKeys,
      brandHint: brand || null,
      // 환경에서 DocAI가 활성화되어 있으면 내부에서 DocAI 우선 사용
      useDocAi: !!(process.env.DOCAI_PROCESSOR_ID && process.env.DOCAI_LOCATION && process.env.DOCAI_PROJECT_ID),
    });
  }

  // 4) 커버 이미지(첫 번째 품번 기준, best-effort)
  let coverUri = null;
  try {
    const brandForCover = brand || extracted.brand || 'unknown';
    const codeForCover  = code  || extracted.rows?.[0]?.code || path.parse(fileName).name;
    coverUri = await extractCoverToGcs(gcsUri, {
      family, brand: brandForCover, code: codeForCover,
    });
  } catch {}

  // 5) 업서트 대상 레코드 구성
  const qualified = table.startsWith('public.') ? table : `public.${table}`;
  const colsSet = await getTableColumns(qualified);

  const records = [];
  if (code) {
    // API에서 단일 업서트 지정
    const rec = {
      family_slug: family,
      brand: brand || extracted.brand || 'unknown',
      code,
      series: series || null,
      display_name: display_name || null,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      verified_in_doc: false,
      updated_at: new Date(),
    };
    records.push(rec);
  } else {
    // PDF에서 추출한 다수 품번
    for (const r of (extracted.rows || [])) {
      const base = {
        family_slug: family,
        brand: extracted.brand || 'unknown',
        code: r.code,
        datasheet_uri: gcsUri,
        image_uri: coverUri || null,
        display_name: `${extracted.brand || 'unknown'} ${r.code}`,
        verified_in_doc: !!r.verified_in_doc,
        updated_at: new Date(),
      };
      // 블루프린트 허용 키만 주입
      for (const k of allowedKeys) {
        if (r[k] != null) base[k] = r[k];
      }
      records.push(base);
    }
  }

  // 폴백: 아무 것도 못 찾았으면 TMP_* 한 건 남김
  if (!records.length) {
    const tmp = 'TMP_' + (Math.random().toString(16).slice(2, 8)).toUpperCase();
    records.push({
      family_slug: family,
      brand: brand || extracted.brand || 'unknown',
      code: tmp,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      display_name: `${brand || extracted.brand || 'unknown'} ${tmp}`,
      verified_in_doc: false,
      updated_at: new Date(),
    });
  }

  // 6) 업서트 (컬럼 존재 여부를 보고 안전 주입)
  let upserted = 0;
  for (const rec of records) {
    const safe = {};

    // 공통 메타
    if (colsSet.has('family_slug')) safe.family_slug = rec.family_slug;
    if (colsSet.has('brand'))       safe.brand       = rec.brand;
    if (colsSet.has('code'))        safe.code        = rec.code;

    // brand_norm / code_norm 있으면 채워줌(일부 테이블은 생성형 컬럼이 아니라 수동 입력)
    if (colsSet.has('brand_norm'))  safe.brand_norm  = normLower(rec.brand);
    if (colsSet.has('code_norm'))   safe.code_norm   = normLower(rec.code);

    // 공통 링크/시각화
    if (colsSet.has('datasheet_uri')) safe.datasheet_uri = rec.datasheet_uri;
    if (colsSet.has('image_uri'))     safe.image_uri     = rec.image_uri;

    // 일부 스키마에는 datasheet_url(별칭)도 있음 → 동일 값 반영
    if (colsSet.has('datasheet_url')) safe.datasheet_url = rec.datasheet_uri;

    // 릴레이 구형/신형 컬럼 호환
    if (colsSet.has('display_name'))  safe.display_name  = rec.display_name;
    if (colsSet.has('displayname'))   safe.displayname   = rec.display_name; // 구 필드

    // 릴레이 전용 cover 컬럼 있으면 커버 경로 동시 반영
    if (colsSet.has('cover') && rec.image_uri) safe.cover = rec.image_uri;

    // 문서 검증 플래그
    if (colsSet.has('verified_in_doc')) safe.verified_in_doc = !!rec.verified_in_doc;

    // 블루프린트 허용 키들 중 실제 컬럼 존재하는 것만 반영
    for (const k of Object.keys(rec)) {
      if (['family_slug','brand','code','brand_norm','code_norm','datasheet_uri','image_uri','datasheet_url','display_name','displayname','verified_in_doc','updated_at'].includes(k)) continue;
      if (colsSet.has(k)) safe[k] = rec[k];
    }

    // 타임스탬프
    if (colsSet.has('updated_at')) safe.updated_at = new Date();

    await upsertByBrandCode(table, safe);
    upserted++;
  }

  return {
    ok: true,
    ms: Date.now() - started,
    family,
    specs_table: table,
    brand: records[0]?.brand,
    code: records[0]?.code,
    datasheet_uri: gcsUri,
    cover: records[0]?.image_uri || null,
    rows: upserted,
  };
}

module.exports = { runAutoIngest };
