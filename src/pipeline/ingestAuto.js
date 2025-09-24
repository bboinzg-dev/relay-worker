'use strict';

const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const db = require('../utils/db');
const { storage, parseGcsUri, readText, canonicalCoverPath } = require('../utils/gcs');
const { upsertByBrandCode } = require('../utils/schema');
const { getBlueprint } = require('../utils/blueprint');
const { extractPartsAndSpecsFromPdf } = require('../ai/datasheetExtract');
const { extractFields } = require('./extractByBlueprint');
const { saveExtractedSpecs } = require('./persist');

function normLower(s){ return String(s||'').trim().toLowerCase(); }

async function getTableColumns(qualified) {
  const [schema, table] = qualified.includes('.') ? qualified.split('.') : ['public', qualified];
  const q = `
    SELECT a.attname AS col
      FROM pg_attribute a
      JOIN pg_class c ON a.attrelid = c.oid
      JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2
       AND a.attnum > 0 AND NOT a.attisdropped`;
  const r = await db.query(q, [schema, table]);
  return new Set(r.rows.map(x=>x.col));
}

// DB 함수로 스키마 보장 (ensure_specs_table)
async function ensureSpecsTableByFamily(family){
  await db.query(`SELECT public.ensure_specs_table($1)`, [family]);
}

async function extractCoverToGcs(gcsPdfUri, { family, brand, code }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmp = path.join(os.tmpdir(), 'pdf-'+Date.now());
    const pdf = path.join(tmp, 'doc.pdf');
    await fs.mkdir(tmp, { recursive: true });
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(pdf, buf);

    // 일부 PDF에서 pdfimages가 매우 오래 걸리거나 멈추는 사례 방지
    await execFileP('pdfimages', ['-f','1','-l','2','-png', pdf, path.join(tmp,'img')], {
      timeout: Number(process.env.COVER_EXTRACT_TIMEOUT_MS || 45000), // 45s
      maxBuffer: 16 * 1024 * 1024,
    });
    const list = (await fs.readdir(tmp)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!list.length) return null;
    let pick=null, size=-1;
    for (const f of list) {
      const st = await fs.stat(path.join(tmp, f));
      if (st.size > size) { pick=f; size=st.size; }
    }
    if (!pick) return null;

    const dst = canonicalCoverPath((process.env.ASSET_BUCKET || process.env.GCS_BUCKET || '').replace(/^gs:\/\//,''), family, brand, code);
    const { bucket: outBkt, name: outName } = parseGcsUri(dst);
    await storage.bucket(outBkt).upload(path.join(tmp, pick), { destination: outName, resumable:false });
    return dst;
  } catch { return null; }
}

function guessFamilySlug({ fileName = '', previewText = '' }) {
  const s = (fileName + ' ' + previewText).toLowerCase();

  // 1) 우선 매칭: 시그널 릴레이의 대표 키워드
  if (/\bsignal\s+relay\b/.test(s) ||
      /\bsubminiature\b.*\brelay\b/.test(s) ||
      /\btelecom\b.*\brelay\b/.test(s) ||
      /\bty\b(?![a-z0-9])/i.test(s)) {
    return 'relay_signal';
  }

  // 2) 그 외 일반 릴레이는 power로 폴백
  if (/\b(relay|coil|omron|finder)\b/.test(s)) return 'relay_power';

  // 3) 기존 다른 부품군 규칙 유지
  if (/\b(resistor|r-clamp|ohm)\b/.test(s)) return 'resistor_chip';
  if (/\b(capacitor|mlcc|electrolytic|tantalum)\b/.test(s)) return 'capacitor_mlcc';
  if (/\b(inductor|choke)\b/.test(s)) return 'inductor_power';
  if (/\b(bridge|rectifier|diode)\b/.test(s)) return 'bridge_rectifier';
  return null;
}

function normalizeCode(str) {
  return String(str || '')
    .replace(/[–—]/g, '-')      // 유니코드 대시 정규화
    .replace(/\s+/g, '')        // 내부 공백 제거
    .replace(/-+/g, '-')        // 대시 연속 정리
    .toUpperCase();
}

// “Ordering Information / How to Order / 주문 정보” 인접영역에서 품번 후보를 뽑아 점수화
function extractPartNumbersFromText(full, limit = 50) {
  const text = String(full || '');
  if (!text) return [];

  // 앵커: 다국어 포함 (영/한/중 기본)
  const anchorRe = /(ORDER(?:ING)?\s+(INFO|INFORMATION|GUIDE|CODE|NUMBER)|HOW\s+TO\s+ORDER|주문\s*정보|주문\s*코드|订购信息|订货信息)/i;
  let windowStart = 0, windowEnd = text.length;
  const m = text.match(anchorRe);
  if (m) {
    // 앵커 앞뒤 약 8~12KB 윈도 선택(표와 주석이 보통 이 범위에 몰린다)
    const idx = m.index || 0;
    windowStart = Math.max(0, idx - 8000);
    windowEnd   = Math.min(text.length, idx + 12000);
  }
  const windowTxt = text.slice(windowStart, windowEnd);

  // 일반적 품번 패턴: 영문+숫자 조합, 하이픈 허용. (너무 짧음/너무 김/순수숫자/순수영문은 제외)
  const candRe = /[A-Z][A-Z0-9](?:[A-Z0-9\-\.]{1,18})/g;
  const raw = windowTxt.match(candRe) || [];

  // 노이즈 필터 (규격·단위·규정 키워드 제거)
  const blacklist = /^(ISO|RoHS|UL|VDC|VAC|A|mA|mm|Ω|OHM|PDF|PAGE|NOTE|DATE|LOT|WWW|HTTP|HTTPS)$/i;
  const stats = new Map();

  for (const r of raw) {
    const code = normalizeCode(r);
    if (!/[0-9]/.test(code)) continue;          // 숫자 포함 필수
    if (code.length < 4 || code.length > 20) continue;
    if (blacklist.test(code)) continue;

    // 근접 컨텍스트로 가중치 (코일 전압/폼/패키지 등 키워드 주변의 후보를 우대)
    const pos = windowTxt.indexOf(r);
    const ctx = windowTxt.slice(Math.max(0, pos - 80), Math.min(windowTxt.length, pos + 80));
    let score = 1;
    if (/(coil|voltage|vdc|form|contact|series|type|형식|전압|코일)/i.test(ctx)) score += 2;
    if (/(model|part\s*no\.?|ordering|주문|订购)/i.test(ctx)) score += 2;

    stats.set(code, (stats.get(code) || 0) + score);
  }

  return [...stats.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([code, score]) => ({ code, score }));
}


async function runAutoIngest({
  gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null,
}) {
  const started = Date.now();
  if (!gcsUri) throw new Error('gcsUri required');
   // 기본 2분로 단축 (원하면 ENV로 재조정)
  const BUDGET = Number(process.env.INGEST_BUDGET_MS || 120000);
  const FAST = /^(1|true|on)$/i.test(process.env.FAST_INGEST || '1');
  const PREVIEW_BYTES = Number(process.env.PREVIEW_BYTES || (FAST ? 32768 : 65536));
  const EXTRACT_HARD_CAP_MS = Number(process.env.EXTRACT_HARD_CAP_MS || (FAST ? 30000 : Math.round(BUDGET * 0.6)));

  const withTimeout = (p, ms, label) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`TIMEOUT:${label}`)), ms);
    Promise.resolve(p)
      .then((val) => { clearTimeout(timer); resolve(val); })
      .catch((err) => { clearTimeout(timer); reject(err); });
  });


    // family 추정 (미지정 시 일부 텍스트만 읽어 빠르게 추정)
  let fileName = '';
  try { const { name } = parseGcsUri(gcsUri); fileName = path.basename(name); } catch {}
  let family = (family_slug||'').toLowerCase() || guessFamilySlug({ fileName }) || 'relay_power';
  if (!family && !FAST) {
    try {
     const text = await readText(gcsUri, 256*1024);
     family = guessFamilySlug({ fileName, previewText: text }) || 'relay_power';
     // ★ 강제 보정: 제목/본문에 Signal Relay가 있으면 무조건 signal로
     if (/subminiature\s+signal\s+relay|signal\s+relay/i.test(text)) family = 'relay_signal';
   } catch { family = 'relay_power'; }
  }

  // 목적 테이블
  const reg = await db.query(
    `SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`,
    [family]
  );
  const table = reg.rows[0]?.specs_table || 'relay_power_specs';
  const qualified = table.startsWith('public.')? table : `public.${table}`;

  // 스키마 보장 (DB 함수) + 컬럼셋 확보
    if (!/^(1|true|on)$/i.test(process.env.NO_SCHEMA_ENSURE || '0')) {
    await ensureSpecsTableByFamily(family);
  }
  const colsSet = await getTableColumns(qualified);

  // 블루프린트 허용 키
  const { allowedKeys } = await getBlueprint(family);

  // PDF → 품번/스펙 추출
  let extracted = { brand: brand || 'unknown', rows: [] };
  if (!brand || !code) {
    try {
      if (FAST) {
        // 텍스트만 빠르게 읽어 블루프린트 기반 추출
        const raw = await readText(gcsUri, PREVIEW_BYTES);
        if (raw && raw.length > 1000) {
          const fieldsJson = Object.fromEntries((allowedKeys||[]).map(k => [k, 'text']));
          const vals = await extractFields(raw, code || '', fieldsJson);
          extracted = {
            brand: brand || 'unknown',
            rows: [{ brand: brand || 'unknown', code: code || (path.parse(fileName).name), ...(vals||{}) }],
          };
        } else {
          // 스캔/이미지형 PDF 등 텍스트가 없으면 정밀 추출을 1회만 하드캡으로 시도
          extracted = await withTimeout(
            extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, brandHint: brand || null }),
            EXTRACT_HARD_CAP_MS,
            'extract',
          );
        }
      } else {
        extracted = await withTimeout(
          extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, brandHint: brand || null }),
          EXTRACT_HARD_CAP_MS,
          'extract',
        );
      }
    } catch (e) { console.warn('[extract timeout/fail]', e?.message || e); }
  }

  if (!code) {
  try {
    const fullText = await readText(gcsUri, 300 * 1024); // 300KB 정도면 대부분의 'How to order' 커버
    const picks = extractPartNumbersFromText(fullText);
    if (picks.length && (!extracted.rows || !extracted.rows.length)) {
      // 추출 결과가 비었을 때는 우선순위 상위 1~N개로 rows를 구성해준다.
      extracted.rows = picks.slice(0, 10).map(p => ({ code: p.code }));
    } else if (picks.length && extracted.rows?.length) {
      // 이미 rows가 있다면, 상위 후보를 첫 레코드 코드로 보정
      extracted.rows[0].code = extracted.rows[0].code || picks[0].code;
    }
  } catch (e) {
    // 텍스트 읽기 실패는 무시 (DocAI/직접 추출 둘 다 실패할 수 있으니)
  }
}

  // 커버 추출 비활성(요청에 따라 완전 OFF)
  let coverUri = null;
  if (/^(1|true|on)$/i.test(process.env.COVER_CAPTURE || '0')) {
    try {
      const bForCover = brand || extracted.brand || 'unknown';
      const cForCover = code || extracted.rows?.[0]?.code || path.parse(fileName).name;
      coverUri = await withTimeout(
        extractCoverToGcs(gcsUri, { family, brand: bForCover, code: cForCover }),
        Math.min(30000, Math.round(BUDGET * 0.15)),
        'cover',
      );
    } catch (e) { console.warn('[cover fail]', e?.message || e); }
  }

  // 레코드 구성
  const records = [];
  const now = new Date();

  if (code) {
    // 단일 강제 인입
    records.push({
      family_slug: family,
      brand: brand || extracted.brand || 'unknown',
      code,
      series: series || null,
      display_name: display_name || null,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      verified_in_doc: false,
      updated_at: now,
    });
  } else {
    for (const r of (extracted.rows || [])) {
      const base = {
        family_slug: family,
        brand: extracted.brand || 'unknown',
        code: r.code,
        datasheet_uri: gcsUri,
        image_uri: coverUri || null,
        display_name: `${extracted.brand || 'unknown'} ${r.code}`,
        verified_in_doc: true,
        updated_at: now,
      };
      // 블루프린트 허용 값만 추가
      for (const k of allowedKeys) { if (r[k] != null) base[k] = r[k]; }
      records.push(base);
    }
  }

  // 최후 폴백 줄이기
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
      updated_at: now,
    });
  }
await saveExtractedSpecs(predFamily, {
  brand: finalBrand,
  code: finalCode,
  mfr_full: maybeMfr,
  datasheet_uri: finalDatasheetUrl
}, extractedSpecsObject);

// (예시) familySlug, pool, values 등 준비된 상황에서:
if (!process.env.NO_SCHEMA_ENSURE || process.env.NO_SCHEMA_ENSURE === "0") {
  await pool.query('SELECT public.ensure_specs_table($1)', [familySlug]);
}
// 이어서 UPSERT 실행...


  // 업서트
  let upserted = 0;
  for (const rec of records) {
    const safe = {};
    // 공통 키
    if (colsSet.has('family_slug')) safe.family_slug = rec.family_slug;
    if (colsSet.has('brand'))       safe.brand = rec.brand;
    if (colsSet.has('code'))        safe.code  = rec.code;
    if (colsSet.has('brand_norm'))  safe.brand_norm = normLower(rec.brand);
    if (colsSet.has('code_norm'))   safe.code_norm  = normLower(rec.code);
    if (colsSet.has('datasheet_uri')) safe.datasheet_uri = rec.datasheet_uri;
    if (colsSet.has('image_uri'))     safe.image_uri     = rec.image_uri;
    if (colsSet.has('datasheet_url')) safe.datasheet_url = rec.datasheet_uri; // 별칭 호환
    if (colsSet.has('display_name'))  safe.display_name  = rec.display_name;
    if (colsSet.has('displayname'))   safe.displayname   = rec.display_name;
    if (colsSet.has('cover') && rec.image_uri) safe.cover = rec.image_uri;
    if (colsSet.has('verified_in_doc')) safe.verified_in_doc = !!rec.verified_in_doc;

    // 블루프린트 값
    for (const [k,v] of Object.entries(rec)) {
      if (['family_slug','brand','code','brand_norm','code_norm','datasheet_uri','image_uri','datasheet_url','display_name','displayname','cover','verified_in_doc','updated_at'].includes(k)) continue;
      if (colsSet.has(k)) safe[k] = v;
    }
    if (colsSet.has('updated_at')) safe.updated_at = now;

    await upsertByBrandCode(table, safe);
    upserted++;
  }

  return {
    ok: true,
    ms: Date.now() - started,
    family,
    specs_table: table,
    brand: records[0]?.brand,
    code:  records[0]?.code,
    datasheet_uri: gcsUri,
    cover: records[0]?.image_uri || null,
    rows: upserted,
  };
}

module.exports = { runAutoIngest };
