'use strict';

const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const db = require('../utils/db');
const { storage, parseGcsUri, readText, canonicalCoverPath } = require('../utils/gcs');
const { extractText } = require('../utils/extract');
const { getBlueprint } = require('../utils/blueprint');
const { extractPartsAndSpecsFromPdf } = require('../ai/datasheetExtract');
const { extractFields } = require('./extractByBlueprint');
const { saveExtractedSpecs } = require('./persist');
const { explodeToRows } = require('../ingest/mpn-exploder');
const { splitAndCarryPrefix } = require('../utils/mpn-exploder');
const { ensureSpecColumnsForBlueprint } = require('./ensure-spec-columns');
const { inferVariantKeys, normalizeSlug } = require('./variant-keys');

const FAST = String(process.env.INGEST_MODE || '').toUpperCase() === 'FAST' || process.env.FAST_INGEST === '1';
const FAST_PAGES = [0, 1, -1]; // 첫 페이지, 2페이지, 마지막 페이지만

const META_KEYS = new Set(['variant_keys','pn_template','ingest_options']);
const BASE_KEYS = new Set([
  'family_slug','brand','code','pn','brand_norm','code_norm','pn_norm','series_code',
  'datasheet_uri','image_uri','datasheet_url','display_name','displayname',
  'cover','verified_in_doc','updated_at'
]);

const PN_CANDIDATE_RE = /\b[A-Z0-9][A-Z0-9\-_/\.]{3,29}[A-Z0-9]\b/gi;
const PN_BLACKLIST_RE = /(pdf|font|xref|object|type0|ffff)/i;

function escapeRegex(str) {
  return String(str || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function textContainsExact(text, pn) {
  if (!text || !pn) return false;
  const pattern = escapeRegex(String(pn).trim());
  if (!pattern) return false;
  const re = new RegExp(`(^|[^A-Za-z0-9])${pattern}(?=$|[^A-Za-z0-9])`, 'i');
  return re.test(String(text));
}

function normLower(s){ return String(s||'').trim().toLowerCase(); }

function harvestMpnCandidates(text, series){
  const hay = String(text || '');
  if (!hay) return [];
  const ser = String(series || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const seen = new Set();
  const out = [];
  PN_CANDIDATE_RE.lastIndex = 0;
  let match;
  while ((match = PN_CANDIDATE_RE.exec(hay)) != null) {
    const raw = match[0];
    if (!raw) continue;
    if (PN_BLACKLIST_RE.test(raw)) continue;
    const norm = raw.toUpperCase();
    if (ser && norm && !norm.startsWith(ser)) continue;
    if (seen.has(norm)) continue;
    seen.add(norm);
    out.push(raw.trim());
  }
  return out;
}

// DB 컬럼 타입 조회 (fallback용)
async function getColumnTypes(qualified) {
  const [schema, table] = qualified.includes('.') ? qualified.split('.') : ['public', qualified];
  const q = `
    SELECT lower(column_name) AS col, lower(data_type) AS dt
    FROM information_schema.columns
    WHERE table_schema=$1 AND table_name=$2`;
  const { rows } = await db.query(q, [schema, table]);
  const out = new Map();
  for (const { col, dt } of rows) {
    if (/(integer|bigint|smallint)/.test(dt)) out.set(col, 'int');
    else if (/(numeric|decimal|double precision|real)/.test(dt)) out.set(col, 'numeric');
    else if (/boolean/.test(dt)) out.set(col, 'bool');
    else out.set(col, 'text');
  }
  return out;
}

// 숫자 강제정규화(콤마/단위/리스트/범위 허용 → 첫 숫자만)
function coerceNumeric(x) {
  if (x == null || x === '') return null;
  if (typeof x === 'number') return x;
  let s = String(x).toLowerCase().replace(/(?<=\d),(?=\d{3}\b)/g, '').replace(/\s+/g, ' ').trim();
  if (/-?\d+(?:\.\d+)?\s*(?:to|~|–|—|-)\s*-?\d+(?:\.\d+)?/.test(s)) return null;
  const m = s.match(/(-?\d+(?:\.\d+)?)(?:\s*([kmgmunpµ]))?/i);
  if (!m) return null;
  let n = parseFloat(m[1]);
  const mul = (m[2] || '').toLowerCase();
  const scale = { k:1e3, m:1e-3, 'µ':1e-6, u:1e-6, n:1e-9, p:1e-12, g:1e9 };
  if (mul && scale[mul] != null) n *= scale[mul];
  return Number.isFinite(n) ? n : null;
}

function normTableText(value) {
  return String(value || '')
    .normalize('NFKC')
    .replace(/[–—−]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
}

function pickSkuListFromTables(extracted = {}) {
  const tables = Array.isArray(extracted.tables) ? extracted.tables : [];
  if (!tables.length) return [];

  const PN_HEADER = /(part\s*(?:no\.?|number)|type\s*(?:no\.?|number)?|catalog\s*(?:no\.?|number)|model|品番|型式|형명|주문\s*번호|order(?:ing)?\s*code)/i;
  const TABLE_HINT = /(ordering|part\s*number|type\s*number|catalog|selection|list\s*of\s*types|品番|型式|형명)/i;

  const set = new Set();
  for (const table of tables) {
    if (!table || typeof table !== 'object') continue;
    const headers = Array.isArray(table.headers) ? table.headers : [];
    if (!headers.length) continue;
    const headerNorms = headers.map((h) => normTableText(h));
    if (!headerNorms.length) continue;
    const headerText = headerNorms.join(' ');
    if (!TABLE_HINT.test(headerText) && !headerNorms.some((h) => PN_HEADER.test(h))) continue;

    const pnIndexes = headerNorms
      .map((h, idx) => (PN_HEADER.test(h) ? idx : -1))
      .filter((idx) => idx >= 0);
    if (!pnIndexes.length) continue;

    for (const row of Array.isArray(table.rows) ? table.rows : []) {
      if (!Array.isArray(row)) continue;
      for (const idx of pnIndexes) {
        const cell = normTableText(row[idx]);
        if (!cell) continue;
        PN_CANDIDATE_RE.lastIndex = 0;
        let m;
        while ((m = PN_CANDIDATE_RE.exec(cell)) != null) {
          const raw = m[0];
          if (!raw) continue;
          if (PN_BLACKLIST_RE.test(raw)) continue;
          set.add(raw.trim());
        }
      }
    }
  }

  return Array.from(set);
}

function expandFromCodeSystem(extracted, bp, docText = '') {
  const tpl = bp?.fields?.code_template;
  const vars = bp?.fields?.code_vars;
  if (!tpl || !vars) return [];
  const haystack = String(docText || '');
  if (!haystack.trim()) return [];

  const keys = Object.keys(vars);
  const out = new Set();
  const MAX_EXPANSION = 400;
  function dfs(i, ctx) {
    if (i >= keys.length) {
      let code = tpl;
      for (const k of Object.keys(ctx)) {
        const v = ctx[k];
        code = code.replace(new RegExp(`\\{${k}(:[^}]*)?\\}`, 'g'), (_, fmt) => {
          if (!fmt) return String(v);
          const m = fmt.match(/^:0(\d+)d$/);
          if (m) return String(v).padStart(Number(m[1]), '0');
          return String(v);
        });
      }
      const cleaned = String(code || '').trim();
      if (!cleaned) return;
      if (!textContainsExact(haystack, cleaned)) return;
      if (PN_BLACKLIST_RE.test(cleaned)) return;
      out.add(cleaned);
      return;
    }
    const k = keys[i];
    const list = Array.isArray(vars[k]) ? vars[k] : [];
    for (const v of list) {
      if (out.size >= MAX_EXPANSION) break;
      dfs(i + 1, { ...ctx, [k]: v });
    }
  }
  dfs(0, {});
  return Array.from(out).slice(0, MAX_EXPANSION);
}

function applyCodeRules(code, out, rules, colTypes) {
  if (!Array.isArray(rules)) return;
  const src = String(code || '');
  for (const r of rules) {
    const re = new RegExp(r.pattern, r.flags || 'i');
    const m = src.match(re);
    if (!m) continue;
    for (const [col, spec] of Object.entries(r.set || {})) {
      if (!colTypes.has(col)) continue;
      let v;
      const gname = spec.from || '1';
      v = (m.groups && m.groups[gname]) || m[gname] || m[1] || null;
      if (v == null) continue;
      if (spec.map) v = spec.map[v] ?? v;
      if (spec.numeric) v = coerceNumeric(v);
      if (v == null || v === '') continue;
      out[col] = v;
    }
  }
}


// DB 함수로 스키마 보장 (ensure_specs_table)
async function ensureSpecsTableByFamily(family){
  await db.query(`SELECT public.ensure_specs_table($1)`, [family]);
}

async function ensureBlueprintVariantColumns(family) {
  await db.query(`SELECT public.ensure_blueprint_variant_columns($1)`, [family]);
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
function rankPartNumbersFromOrderingSections(full, limit = 50) {
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



// --- NEW: doc type detector (catalog vs. single datasheet) ---
function detectDocType(full) {
  const t = String(full || '').toLowerCase();
  // HOW TO ORDER / ORDERING INFORMATION / 주문 정보 / 订购信息 / 订货信息 / TYPES / Part No.
  if (/(how to order|ordering information|주문\s*정보|订购信息|订货信息|\btypes\b|\bpart\s*no\.?\b)/i.test(t)) {
    return 'catalog';
  }
  return 'single';
}

// ---- NEW: "TYPES / Part No." 표에서 품번 열거 추출 ----
function _expandAorS(code) {
  return code.includes('*') ? [code.replace('*','A'), code.replace('*','S')] : [code];
}
function _looksLikePn(s) {
  const c = s.toUpperCase();
  if (!/[0-9]/.test(c)) return false;
  if (c.length < 4 || c.length > 24) return false;
  // 명백한 단위/잡토큰 제거
  if (/^(ISO|ROHS|VDC|VAC|V|A|MA|MM|Ω|OHM|PDF|PAGE|NOTE|DATE|LOT|WWW|HTTP|HTTPS)$/i.test(c)) return false;
  return true;
}
function extractPartNumbersFromTypesTables(full, limit = 200) {
  const text = String(full || '');
  if (!text) return [];
  // TYPES, Part No. 주변 10~16KB 윈도우로 좁힌다
  const idxTypes = text.search(/\bTYPES\b/i);
  const idxPart  = text.search(/\bPart\s*No\.?\b/i);
  const anchor   = (idxTypes >= 0 ? idxTypes : 0);
  const start    = Math.max(0, Math.min(anchor, idxPart >= 0 ? idxPart : anchor) - 4000);
  const end      = Math.min(text.length, (anchor || 0) + 16000);
  const win      = text.slice(start, end);

  // Part No. 패턴: 대문자+숫자 혼합(하이픈/별표 허용)
  const raw = win.match(/[A-Z][A-Z0-9][A-Z0-9\-\*]{2,}/g) || [];

  // “* = A/S” 치환 규칙 감지(있으면 확장)
  const hasStarRule = /"\s*\*\s*"\s*:.*A\s*type\s*:\s*A.*S\s*type\s*:\s*S/i.test(win);

  const set = new Set();
  for (const r of raw) {
    const code = normalizeCode(r);
    if (!_looksLikePn(code)) continue;
    const list = hasStarRule ? _expandAorS(code) : [code];
    for (const c of list) set.add(c);
  }
  return Array.from(set).slice(0, limit).map(c => ({ code: c }));
}

// --- NEW: 표를 못 찾을 때를 위한 "시리즈 접두 기반" 보조 휴리스틱 ---
function extractPartNumbersBySeriesHeuristic(full, limit = 200) {
  const text = String(full || '');
  if (!text) return [];
  // 1) 문서 전체에서 "문자 2~5개 + 숫자 시작" 패턴으로 접두 후보 수집
  const seed = text.match(/[A-Z]{2,5}(?=\d)/g) || [];
  const freq = new Map();
  for (const p of seed) freq.set(p, (freq.get(p) || 0) + 1);
  const tops = [...freq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3).map(([p]) => p);
  if (!tops.length) return [];
  // 2) 각 접두에 대해 PN 후보 수집
  const set = new Set();
  for (const pref of tops) {
    const re = new RegExp(`${pref}[A-Z0-9*\\-]{3,}`, 'g');
    const raw = text.toUpperCase().match(re) || [];
    for (const candidate of raw) {
      // 숫자/길이/노이즈 필터
      if (!/[0-9]/.test(candidate)) continue;
      if (candidate.length < 4 || candidate.length > 24) continue;
      if (/^(ISO|ROHS|VDC|VAC|V|A|MA|MM|Ω|OHM|PDF|PAGE|NOTE|DATE|LOT|WWW|HTTP|HTTPS)$/.test(candidate)) continue;
      // A/S 확장
      if (candidate.includes('*')) {
        set.add(candidate.replace('*', 'A'));
        set.add(candidate.replace('*', 'S'));
      } else {
        set.add(candidate);
      }
      if (set.size >= limit) break;
    }
    if (set.size >= limit) break;
  }
  return [...set].slice(0, limit).map(code => ({ code }));
}



// 품번 후보 추출 (ordering/types/series 휴리스틱 재사용)
async function extractPartNumbersFromText(text, { series } = {}) {
  const src = String(text || '');
  if (!src) return [];

  const prefix = series ? normalizeCode(series) : null;
  const seen = new Set();
  const out = [];

  const push = (raw) => {
    if (!raw) return;
    const norm = normalizeCode(raw);
    if (!norm) return;
    if (prefix && !norm.startsWith(prefix)) return;
    if (seen.has(norm)) return;
    seen.add(norm);
    const cleaned = typeof raw === 'string' ? raw.trim() : String(raw || '');
    out.push(cleaned || norm);
  };

  for (const { code } of extractPartNumbersFromTypesTables(src, 200)) push(code);
  for (const { code } of rankPartNumbersFromOrderingSections(src, 200)) push(code);
  for (const { code } of extractPartNumbersBySeriesHeuristic(src, 200)) push(code);

  return out;
}



async function runAutoIngest(input = {}) {
  let {
    gcsUri: rawGcsUri = null,
    gsUri: rawGsUri = null,
    family_slug = null,
    brand = null,
    code = null,
    series = null,
    display_name = null,
  } = input;

  const gcsUri = (rawGcsUri || rawGsUri || '').trim();

  const started = Date.now();
  if (!gcsUri) throw new Error('gcsUri/gsUri required');
   // 기본 2분로 단축 (원하면 ENV로 재조정)
  const BUDGET = Number(process.env.INGEST_BUDGET_MS || 120000);
  const FAST = /^(1|true|on)$/i.test(process.env.FAST_INGEST || '1');
  const PREVIEW_BYTES = Number(process.env.PREVIEW_BYTES || (FAST ? 32768 : 65536));
  const EXTRACT_HARD_CAP_MS = Number(process.env.EXTRACT_HARD_CAP_MS || (FAST ? 30000 : Math.round(BUDGET * 0.6)));
  const FIRST_PASS_CODES = parseInt(process.env.FIRST_PASS_CODES || '20', 10);

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

  let blueprint = await getBlueprint(family);

  // 블루프린트 허용 키
  let allowedKeys = Array.isArray(blueprint?.allowedKeys)
    ? [...blueprint.allowedKeys]
    : [];
  let variantKeys = Array.isArray(blueprint?.ingestOptions?.variant_keys)
    ? blueprint.ingestOptions.variant_keys.map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
    : (Array.isArray(blueprint?.variant_keys)
      ? blueprint.variant_keys.map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
      : []);
  const pnTemplate = blueprint?.ingestOptions?.pn_template || blueprint?.ingestOptions?.pnTemplate || null;
  const requiredFields = [];
  if (blueprint?.fields && typeof blueprint.fields === 'object') {
    for (const [fieldKey, meta] of Object.entries(blueprint.fields)) {
      const isRequired = meta && typeof meta === 'object' && Boolean(meta.required);
      if (!isRequired) continue;
      const normalized = String(fieldKey || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, '');
      if (normalized && !requiredFields.includes(normalized)) {
        requiredFields.push(normalized);
      }
    }
  }

  let colTypes;
  const disableEnsure = /^(1|true|on)$/i.test(process.env.NO_SCHEMA_ENSURE || '0');

  // -------- 공용 강제정규화 유틸 --------

  if (code && !/\d/.test(String(code))) {
    // "AGN","TQ" 처럼 숫자 없는 시리즈는 series로 넘기고 code는 비움
    series = code; code = null;
  }


  // ❶ PDF 텍스트 일부에서 품번 후보 우선 확보
  let previewText = '';
  try { previewText = await readText(gcsUri, PREVIEW_BYTES) || ''; } catch {}
  if (!previewText || previewText.length < 1000) {
    try {
      const r = await extractText(gcsUri);
      previewText = r?.text || previewText;
    } catch {}
  }
  let candidates = [];
  try {
    candidates = await extractPartNumbersFromText(previewText, { series: series || code });
  } catch { candidates = []; }

  // PDF → 품번/스펙 추출
  let extracted = { brand: brand || 'unknown', rows: [] };
  if (!brand || !code) {
    try {
      if (FAST) {
        // 텍스트만 빠르게 읽어 블루프린트 기반 추출
        let raw = previewText;
        if (!raw) {
          try { raw = await readText(gcsUri, PREVIEW_BYTES); } catch { raw = ''; }
        }
        if (raw && raw.length > 1000) {
          const fieldsJson = blueprint?.fields || {};
          const vals = await extractFields(raw, code || '', fieldsJson);
          extracted = {
            brand: brand || 'unknown',
            rows: [{ brand: brand || 'unknown', code: code || (path.parse(fileName).name), ...(vals||{}) }],
          };
        } else {
          // 스캔/이미지형 PDF 등 텍스트가 없으면 정밀 추출을 1회만 하드캡으로 시도
          extracted = await withTimeout(
            extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, family, brandHint: brand || null }),
            EXTRACT_HARD_CAP_MS,
            'extract',
          );
        }
      } else {
        extracted = await withTimeout(
          extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, family, brandHint: brand || null }),
          EXTRACT_HARD_CAP_MS,
          'extract',
        );
      }
    } catch (e) { console.warn('[extract timeout/fail]', e?.message || e); }
  }

  // 🔹 이 변수가 "데이터시트 분석에서 바로 뽑은 MPN 리스트"가 됨
  let codes = [];
  if (!code) {
    const skuFromTable = pickSkuListFromTables(extracted);
    const docText = extracted?.text || previewText || '';
    codes = skuFromTable.length ? skuFromTable : expandFromCodeSystem(extracted, blueprint, docText);
    const maxEnv = Number(process.env.FIRST_PASS_CODES || FIRST_PASS_CODES || 20);
    const maxCodes = Number.isFinite(maxEnv) && maxEnv > 0 ? maxEnv : 20;
    if (codes.length > maxCodes) codes = codes.slice(0, maxCodes);
  }

  // 🔹 후보(candidates)가 아직 비었고, 방금 수집한 codes가 있으면 candidates로 승격
  if (!candidates.length && codes.length) {
    const merged = [];
    const seen = new Set();
    for (const raw of codes) {
      const trimmed = typeof raw === 'string' ? raw.trim() : String(raw || '');
      if (!trimmed) continue;
      const norm = normalizeCode(trimmed);
      if (seen.has(norm)) continue;
      seen.add(norm);
      merged.push(trimmed);
    }
    if (merged.length) candidates = merged;
  }

  // 🔹 “애초에 분석단계에서 여러 MPN을 리스트업” — 추출 결과에 명시적으로 부착
  if (extracted && typeof extracted === 'object') {
    const list = (Array.isArray(codes) ? codes : []).filter(Boolean);
    extracted.codes = list;        // <- 최종 MPN 배열
    extracted.mpn_list = list;     // <- 동의어(외부에서 쓰기 쉽도록)
  }

  if (!code && !codes.length) {
    let fullText = '';
    try { fullText = await readText(gcsUri, 300 * 1024) || ''; } catch {}

    const fromTypes  = extractPartNumbersFromTypesTables(fullText, FIRST_PASS_CODES * 4); // TYPES 표 우선
    const fromOrder  = rankPartNumbersFromOrderingSections(fullText, FIRST_PASS_CODES);
    const fromSeries = extractPartNumbersBySeriesHeuristic(fullText, FIRST_PASS_CODES * 4);
    // 가장 신뢰 높은 순서로 병합
    const picks = fromTypes.length ? fromTypes : (fromOrder.length ? fromOrder : fromSeries);

    if (!candidates.length && picks.length) {
      const merged = [];
      const seen = new Set();
      for (const p of picks) {
        const raw = typeof p === 'string' ? p : p?.code;
        const trimmed = typeof raw === 'string' ? raw.trim() : '';
        if (!trimmed) continue;
        const norm = normalizeCode(trimmed);
        if (seen.has(norm)) continue;
        seen.add(norm);
        merged.push(trimmed);
      }
      if (merged.length) {
        candidates = merged;
        // 🔹 types/order/series 휴리스틱으로도 찾은 경우, 이것도 추출 결과에 반영
        if (extracted && typeof extracted === 'object') {
          const uniq = Array.from(new Set([...(extracted.codes || []), ...merged]));
          extracted.codes = uniq;
          extracted.mpn_list = uniq;
        }
      }
    }

    // 분할 여부는 별도 판단. 여기서는 후보만 모아둠.
    // extracted.rows는 건드리지 않음.
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

  if (code) {
    const trimmedCode = String(code || '').trim();
    if (trimmedCode) {
      const norm = normalizeCode(trimmedCode);
      if (!candidates.some((c) => normalizeCode(c) === norm)) {
        candidates = [trimmedCode, ...candidates];
      }
    }
  }

  // 레코드 구성
  const records = [];
  const now = new Date();
  const brandName = brand || extracted.brand || 'unknown';
  const baseSeries = series || code || null;

  let variantColumnsEnsured = false;
  try {
    const { detected: inferredKeys = [], newKeys: freshKeys = [] } = await inferVariantKeys({
      family,
      brand: brandName,
      series: baseSeries,
      blueprint,
      extracted,
    });

    if (Array.isArray(inferredKeys) && inferredKeys.length) {
      const brandSlug = normalizeSlug(brandName);
      const seriesSlug = normalizeSlug(baseSeries);
      try {
        await db.query(
          `SELECT public.upsert_variant_keys($1,$2,$3,$4::jsonb)`,
          [family, brandSlug, seriesSlug, JSON.stringify(inferredKeys)],
        );
      } catch (err) {
        console.warn('[variant] upsert_variant_keys failed:', err?.message || err);
      }

      const mergedVariant = new Set(variantKeys);
      for (const key of inferredKeys) mergedVariant.add(key);
      variantKeys = Array.from(mergedVariant);

      const mergedAllowed = new Set(allowedKeys);
      for (const key of variantKeys) mergedAllowed.add(key);
      allowedKeys = Array.from(mergedAllowed);

      if (!blueprint.ingestOptions || typeof blueprint.ingestOptions !== 'object') {
        blueprint.ingestOptions = {};
      }
      blueprint.ingestOptions.variant_keys = variantKeys;
      blueprint.variant_keys = variantKeys;
      blueprint.allowedKeys = Array.isArray(blueprint.allowedKeys)
        ? Array.from(new Set([...blueprint.allowedKeys, ...variantKeys]))
        : [...allowedKeys];

      if (!disableEnsure) {
        try {
          await ensureBlueprintVariantColumns(family);
          variantColumnsEnsured = true;
        } catch (err) {
          console.warn('[variant] ensure_blueprint_variant_columns failed:', err?.message || err);
        }
      }
    }

    if (Array.isArray(freshKeys) && freshKeys.length) {
      console.log('[variant] detected new keys', { family, brand: brandName, series: baseSeries, keys: freshKeys });
    }
  } catch (err) {
    console.warn('[variant] inferVariantKeys failed:', err?.message || err);
  }

  if (!disableEnsure) {
    await ensureSpecsTableByFamily(family);
    if (!variantColumnsEnsured) {
      try {
        await ensureBlueprintVariantColumns(family);
        variantColumnsEnsured = true;
      } catch (err) {
        console.warn('[variant] ensure_blueprint_variant_columns fallback failed:', err?.message || err);
      }
    }
  }

  await ensureSpecColumnsForBlueprint(qualified, blueprint);
  colTypes = await getColumnTypes(qualified);

  const mpnsFromDoc = harvestMpnCandidates(
    extracted?.text ?? '',
    (baseSeries || series || code || '')
  );
  const mpnNormFromDoc = new Set(mpnsFromDoc.map((m) => normalizeCode(m)).filter(Boolean));

  const candidateMap = [];
  const candidateNormSet = new Set();
  for (const cand of candidates) {
    const trimmed = typeof cand === 'string' ? cand.trim() : String(cand || '');
    if (!trimmed) continue;
    const norm = normalizeCode(trimmed);
    if (!norm || candidateNormSet.has(norm)) continue;
    candidateNormSet.add(norm);
    candidateMap.push({ raw: trimmed, norm });
  }

  const rawRows = Array.isArray(extracted.rows) && extracted.rows.length ? extracted.rows : [];
  const baseRows = (rawRows.length ? rawRows : [{}]).map((row) => {
    const obj = row && typeof row === 'object' ? { ...row } : {};
    if (obj.brand == null) obj.brand = brandName;
    const fallbackSeries = obj.series_code || obj.series || baseSeries || null;
    if (fallbackSeries != null) {
      if (obj.series == null) obj.series = fallbackSeries;
      if (obj.series_code == null) obj.series_code = fallbackSeries;
    }
    if (obj.datasheet_uri == null) obj.datasheet_uri = gcsUri;
    if (coverUri && obj.cover == null) obj.cover = coverUri;
    return obj;
  });

  const explodedRows = explodeToRows(blueprint, baseRows);
  const physicalCols = new Set(colTypes ? [...colTypes.keys()] : []);
  const allowedSet = new Set((allowedKeys || []).map((k) => String(k || '').trim().toLowerCase()).filter(Boolean));
  const variantSet = new Set(variantKeys);

  const seenCodes = new Set();
  for (const row of explodedRows) {
    const seeds = [];
    const seenSeed = new Set();
    const pushSeed = (val) => {
      if (val == null) return;
      if (Array.isArray(val)) { val.forEach(pushSeed); return; }
      const str = String(val).trim();
      if (!str) return;
      const parts = splitAndCarryPrefix(str);
      if (parts.length > 1) { parts.forEach(pushSeed); return; }
      const normed = str.toLowerCase();
      if (seenSeed.has(normed)) return;
      seenSeed.add(normed);
      seeds.push(str);
    };
    pushSeed(row.code);
    pushSeed(row.mpn);
    pushSeed(row.part_number);
    pushSeed(row.part_no);

    let mpn = seeds.length ? seeds[0] : '';
    if (!mpn && candidateMap.length) mpn = candidateMap[0].raw;
    mpn = String(mpn || '').trim();
    if (!mpn) continue;
    const mpnNorm = normalizeCode(mpn);
    const brandKey = normLower(row.brand || brandName);
    const rec = {};
    const naturalKey = `${brandKey}::${mpnNorm}`;
    if (!mpnNorm || seenCodes.has(naturalKey)) continue;
    seenCodes.add(naturalKey);

    rec.family_slug = family;
    rec.brand = row.brand || brandName;
    rec.pn = mpn;
    if (row.code != null) rec.code = row.code;
    if (!rec.code) rec.code = mpn;
    rec.series_code = row.series_code ?? row.series ?? baseSeries ?? null;
    if (row.series != null && physicalCols.has('series')) rec.series = row.series;
    rec.datasheet_uri = row.datasheet_uri || gcsUri;
    if (row.datasheet_url) rec.datasheet_url = row.datasheet_url;
    else if (rec.datasheet_uri && rec.datasheet_url == null) rec.datasheet_url = rec.datasheet_uri;
    if (row.mfr_full != null) rec.mfr_full = row.mfr_full;
    let verified;
    if (row.verified_in_doc != null) {
      if (typeof row.verified_in_doc === 'string') {
        verified = row.verified_in_doc.trim().toLowerCase() === 'true';
      } else {
        verified = Boolean(row.verified_in_doc);
      }
    } else {
      verified = candidateNormSet.has(mpnNorm) || mpnNormFromDoc.has(mpnNorm);
    }
    rec.verified_in_doc = Boolean(verified);
    rec.image_uri = row.image_uri || coverUri || null;
    if (coverUri && rec.cover == null) rec.cover = coverUri;
    const displayName = row.display_name || row.displayname || `${rec.brand} ${mpn}`;
    rec.display_name = displayName;
    if (rec.displayname == null && displayName != null) rec.displayname = displayName;
    rec.updated_at = now;
    if (row.raw_json != null) rec.raw_json = row.raw_json;

    for (const [rawKey, rawValue] of Object.entries(row)) {
      const key = String(rawKey || '').trim();
      if (!key) continue;
      const lower = key.toLowerCase();
      if (META_KEYS.has(lower) || BASE_KEYS.has(lower)) continue;
      if (physicalCols.has(lower) || allowedSet.has(lower) || variantSet.has(lower)) {
        rec[lower] = rawValue;
      }
    }

    if (blueprint?.code_rules) applyCodeRules(rec.code, rec, blueprint.code_rules, colTypes);
    records.push(rec);
  }

  if (candidateMap.length) {
    const fallbackSeries = baseSeries || null;
    for (const cand of candidateMap) {
      const norm = cand.norm;
      const naturalKey = `${normLower(brandName)}::${norm}`;
      if (seenCodes.has(naturalKey)) continue;
      seenCodes.add(naturalKey);
      const verified = mpnNormFromDoc.has(norm);
      const rec = {
        family_slug: family,
        brand: brandName,
        pn: cand.raw,
        code: cand.raw,
        series_code: fallbackSeries,
        datasheet_uri: gcsUri,
        image_uri: coverUri || null,
        display_name: `${brandName} ${cand.raw}`,
        verified_in_doc: verified,
        updated_at: now,
      };
      if (coverUri) rec.cover = coverUri;
      if (physicalCols.has('series') && fallbackSeries != null) rec.series = fallbackSeries;
      if (rec.datasheet_url == null) rec.datasheet_url = rec.datasheet_uri;
      if (rec.display_name != null && rec.displayname == null) rec.displayname = rec.display_name;
      records.push(rec);
    }
  }

  console.log('[MPNDBG]', {
    picks: candidateMap.length,
    vkeys: Array.isArray(blueprint?.ingestOptions?.variant_keys) ? blueprint.ingestOptions.variant_keys : [],
    expanded: explodedRows.length,
    recs: records.length,
    colsSanitized: colTypes?.size || 0,
  });

  const processedPayload = {
    started,
    gcsUri,
    family,
    table,
    qualified,
    pnTemplate,
    requiredFields,
    coverUri,
    records,
    mpnList: Array.isArray(extracted?.mpn_list) ? extracted.mpn_list : [],
    extractedBrand: extracted?.brand || null,
    brandName,
    baseSeries,
  };

  if (Array.isArray(extracted?.codes)) processedPayload.candidateCodes = extracted.codes;
  if (display_name != null) processedPayload.display_name = display_name;
  if (code != null) processedPayload.code = code;
  if (series != null) processedPayload.series = series;

  if (input && typeof input === 'object' && input.skipPersist) {
    return { ok: true, phase: 'process', processed: processedPayload };
  }

  const persistOverrides = { brand, code, series, display_name };
  return persistProcessedData(processedPayload, persistOverrides);
}

async function persistProcessedData(processed = {}, overrides = {}) {
  const {
    started = Date.now(),
    gcsUri = null,
    family = null,
    table = null,
    qualified: qualifiedInput = null,
    pnTemplate = null,
    requiredFields = [],
    coverUri = null,
    records = [],
    mpnList = [],
    extractedBrand = null,
    brandName = null,
    baseSeries = null,
  } = processed || {};

  const qualified = qualifiedInput || (table ? (table.startsWith('public.') ? table : `public.${table}`) : null);

  let persistResult = { upserts: 0, written: [], skipped: [], warnings: [] };
  if (qualified && family && records.length) {
    persistResult = await saveExtractedSpecs(qualified, family, records, {
      brand: overrides?.brand ?? null, // 폼/페이로드 브랜드 우선 활용
      pnTemplate,
      requiredKeys: Array.isArray(requiredFields) ? requiredFields : [],
      coreSpecKeys: Array.isArray(requiredFields) ? requiredFields : [],
    }) || persistResult;
  } else if (!records.length) {
    persistResult.skipped = [{ reason: 'missing_pn' }];
  }

  const persistedCodes = new Set(
    (persistResult.written || [])
      .map((pn) => String(pn || '').trim())
      .filter(Boolean)
  );

  if (!persistedCodes.size && records.length) {
    for (const rec of records) {
      const pn = String(rec.pn || rec.code || '').trim();
      if (pn) persistedCodes.add(pn);
    }
  }

  const persistedList = Array.from(persistedCodes);
  const mpnListSafe = Array.isArray(mpnList) ? mpnList : [];
  const mergedMpns = Array.from(new Set([...persistedList, ...mpnListSafe]));

  const rejectReasons = new Set(
    (persistResult.skipped || [])
      .map((it) => (it && typeof it === 'object' ? it.reason : it))
      .filter(Boolean)
  );
  const warningReasons = new Set(
    (persistResult.warnings || []).filter(Boolean)
  );

  const ms = Number.isFinite(processed?.ms) ? processed.ms : (typeof started === 'number' ? Date.now() - started : null);
  const upsertsCount = typeof persistResult.upserts === 'number' ? persistResult.upserts : 0;
  const ok = upsertsCount > 0;

  const fallbackBrand = overrides.brand || brandName || extractedBrand || null;
  const primaryRecord = records[0] || null;
  const finalBrand = primaryRecord?.brand || fallbackBrand;
  const finalCode =
    persistedList[0] ||
    primaryRecord?.pn ||
    primaryRecord?.code ||
    overrides.code ||
    null;

  const response = {
    ok,
    ms,
    family,
    final_table: table,
    specs_table: table,
    brand: finalBrand,
    code: finalCode,
    datasheet_uri: gcsUri,
    cover: coverUri || primaryRecord?.image_uri || null,
    rows: upsertsCount,        // 실제 반영된 개수만 기록
    codes: Array.from(persistedCodes),  // 표시는 그대로
    mpn_list: mergedMpns,
    reject_reasons: Array.from(rejectReasons),
    warnings: Array.from(warningReasons),
  };

  if (response.code == null && Array.isArray(processed?.candidateCodes) && processed.candidateCodes.length) {
    response.code = processed.candidateCodes[0];
  }

  if (response.code == null && baseSeries != null) {
    response.code = baseSeries;
  }

  return response;
}

module.exports = { runAutoIngest, persistProcessedData };
