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

const FAST = String(process.env.INGEST_MODE || '').toUpperCase() === 'FAST' || process.env.FAST_INGEST === '1';
const FAST_PAGES = [0, 1, -1]; // 첫 페이지, 2페이지, 마지막 페이지만

// family별 "최소 키셋" (필요 최소치만 저장)
const MIN_KEYS = {
  relay_power: [
    'contact_form','contact_rating_ac','contact_rating_dc',
    'coil_voltage_vdc','size_l_mm','size_w_mm','size_h_mm'
  ],
  relay_signal: [
    'contact_form','contact_rating_ac','contact_rating_dc','coil_voltage_vdc'
  ]
};

const META_KEYS = new Set(['variant_keys','pn_template','ingest_options']);
const BASE_KEYS = new Set([
  'family_slug','brand','code','brand_norm','code_norm','series_code',
  'datasheet_uri','image_uri','datasheet_url','display_name','displayname',
  'cover','verified_in_doc','updated_at'
]);

function normLower(s){ return String(s||'').trim().toLowerCase(); }
function normIdent(s){ return String(s||'').trim().toLowerCase().replace(/[^a-z0-9_]/g, ''); }

const VOLTAGE_GRID = [1.5,3,5,6,9,12,18,24,48];
function parseVoltageList(v){
  if (Array.isArray(v)) return v.map(Number).filter((n)=>Number.isFinite(n));
  const s = String(v||'').toLowerCase();
  const multi = s.match(/(\d+(?:\.\d+)?)/g);
  if (multi && multi.length>1) return multi.map(Number).filter(Number.isFinite);
  const range = s.match(/(\d+(?:\.\d+)?)\s*(?:v|vdc)?\s*(?:to|~|-)\s*(\d+(?:\.\d+)?)/);
  if (range){
    const lo = parseFloat(range[1]);
    const hi = parseFloat(range[2]);
    return VOLTAGE_GRID.filter((x)=>x>=lo && x<=hi);
  }
  const single = s.match(/(\d+(?:\.\d+)?)/);
  if (single){ const n = Number(single[1]); return Number.isFinite(n)?[n]:[]; }
  return [];
}

function tokenOf(x){
  if (x==null) return '';
  const s = String(x).toUpperCase();
  const m = s.match(/[A-Z0-9\.]+/g);
  return m ? m.join('') : '';
}

function harvestMpnCandidates(text, series){
  const hay = String(text||'');
  if (!hay) return [];
  const ser = String(series||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
  const lines = hay.split(/\n+/);
  const near = [];
  for (const ln of lines){
    if (/ordering|part\s*number|order code|品番|型番/i.test(ln)) near.push(ln);
  }
  const src = (near.length? near.join(' ') : hay).toUpperCase();
  const rx = ser ? new RegExp(`\\b${ser}[A-Z0-9\\-]+\\b`,'g') : /\b[A-Z][A-Z0-9\-]{3,}\b/g;
  const set = new Set();
  let m; while((m = rx.exec(src))) set.add(m[0]);
  return [...set];
}

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
  const m = s.match(/(-?\d+(?:\.\d+)?)(?:\s*([kmgmunpµ]))?/i);
  if (!m) return null;
  let n = parseFloat(m[1]);
  const mul = (m[2] || '').toLowerCase();
  const scale = { k:1e3, m:1e-3, 'µ':1e-6, u:1e-6, n:1e-9, p:1e-12, g:1e9 };
  if (mul && scale[mul] != null) n *= scale[mul];
  return Number.isFinite(n) ? n : null;
}

function parseListOrRange(s) {
  if (Array.isArray(s)) return s;
  const raw = String(s ?? '').trim();
  if (!raw) return [];
  const volts = parseVoltageList(raw);
  if (volts.length) return volts;
  if (/[;,、\/\s]/.test(raw)) return raw.split(/[;,、\/\s]+/).map(x => x.trim()).filter(Boolean);
  return [raw];
}

// 분할 여부: PN 후보≥2 또는 variant 열거형 곱≥2 이면 분할
function decideSplit({ pnCandidates = [], seriesCandidates = [], variantKeys = [], specs = {} }) {
  if ((pnCandidates?.length || 0) >= 2) return true;
  if ((seriesCandidates?.length || 0) >= 2) return true;
  if (Array.isArray(variantKeys) && variantKeys.length) {
    let count = 1;
    for (const k of variantKeys) {
      const vals = parseListOrRange(specs[k]);
      const n = vals.length || (specs[k] != null ? 1 : 0);
      count *= Math.max(1, n);
    }
    if (count >= 2) return true;
  }
  return false;
}

// variant_keys 교차곱으로 base/specs 병합 배열 생성
function explodeVariants(base = {}, specs = {}, bp = {}) {
  const keys = Array.isArray(bp.variant_keys) ? bp.variant_keys : [];
  if (!keys.length) return [{ ...base, ...specs }];
  const lists = keys.map(k => {
    const arr = parseListOrRange(specs[k]);
    if (arr.length) return arr;
    return (specs[k] != null ? [specs[k]] : []);
  });
  if (!lists.length || lists.some(a => !a.length)) return [{ ...base, ...specs }];
  const out = [];
  const dfs = (i, cur) => {
    if (i === keys.length) { out.push({ ...base, ...cur }); return; }
    const key = keys[i];
    for (const v of lists[i]) dfs(i + 1, { ...cur, [key]: v });
  };
  dfs(0, { ...specs });
  return out;
}

// pn_template 로 개별 MPN 조립
function buildMpn(rec, bp) {
  const t = bp?.pn_template || bp?.ingestOptions?.pn_template;
  if (!t) return rec.code;
  return t.replace(/\$\{(\w+)\}/g, (_, k) => String(rec[k] ?? ''));
}

function pickSkuListFromTables(extracted = {}) {
  const out = new Set();
  const HEAD_RE = /(part\s*no\.?|ordering\s*information|part\s*number)/i;
  for (const t of (extracted.tables || [])) {
    if (!t) continue;
    const headers = Array.isArray(t.headers) ? t.headers : [];
    const head = headers.join(' ').trim();
    if (!HEAD_RE.test(head)) continue;
    let partIdx = 0;
    const idx = headers.findIndex((h) => /part\s*no\.?/i.test(String(h || '')));
    if (idx >= 0) partIdx = idx;
    for (const row of (t.rows || [])) {
      if (!Array.isArray(row)) continue;
      const cand = String(row[partIdx] || '').trim();
      if (/^[A-Z0-9][A-Z0-9\-_/\.]{3,}$/i.test(cand)) out.add(cand);
    }
  }
  return [...out];
}

function expandFromCodeSystem(extracted, bp) {
  const tpl = bp?.fields?.code_template;
  const vars = bp?.fields?.code_vars;
  if (!tpl || !vars) return [];

  const keys = Object.keys(vars);
  const out = [];
  function dfs(i, ctx) {
    if (i >= keys.length) {
      let code = tpl;
      for (const k of Object.keys(ctx)) {
        const v = ctx[k];
        code = code.replace(new RegExp(`\\{${k}(:[^}]*)?\\}`,'g'), (_, fmt) => {
          if (!fmt) return String(v);
          const m = fmt.match(/^:0(\d+)d$/);
          if (m) return String(v).padStart(Number(m[1]), '0');
          return String(v);
        });
      }
      out.push(code);
      return;
    }
    const k = keys[i];
    const list = Array.isArray(vars[k]) ? vars[k] : [];
    for (const v of list) dfs(i + 1, { ...ctx, [k]: v });
  }
  dfs(0, {});
  return out;
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


// 컬럼 타입에 맞춰 값 정리: 숫자/정수/불리언만 강제 변환, 실패하면 해당 키 제거
function sanitizeByColTypes(obj, colTypes) {
  for (const [k, v] of Object.entries({ ...obj })) {
    const t = colTypes.get(k);
    if (t === 'numeric') {
      const n = coerceNumeric(v);
      if (n == null) delete obj[k]; else obj[k] = n;
    } else if (t === 'int') {
      const n = coerceNumeric(v);
      if (n == null) delete obj[k]; else obj[k] = Math.round(n);
    } else if (t === 'bool') {
      if (typeof v === 'boolean') continue;
      const s = String(v ?? '').toLowerCase().trim();
      if (!s) delete obj[k];
      else obj[k] = /^(true|yes|y|1|on|enable|enabled|pass)$/i.test(s);
    }
  }
  return obj;
}

function normalizeKeysOnce(obj = {}) {
  const out = {};
  for (const [key, value] of Object.entries(obj || {})) {
    const normalized = String(key || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_]+/g, '_')
      .replace(/^_+|_+$/g, '');
    if (!normalized) continue;
    if (!(normalized in out)) out[normalized] = value;
  }
  return out;
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

  // 스키마 보장 (DB 함수) + 컬럼셋 확보
    if (!/^(1|true|on)$/i.test(process.env.NO_SCHEMA_ENSURE || '0')) {
    await ensureSpecsTableByFamily(family);
  }
  const colsSet = new Set([
    ...await getTableColumns(qualified)
  ].map((c) => String(c || '').toLowerCase()));
  const colTypes = await getColumnTypes(qualified);

  // 블루프린트 허용 키
  const blueprint = await getBlueprint(family);
  const allowedKeys = blueprint?.allowedKeys || [];
  const variantKeys = Array.isArray(blueprint?.variant_keys) ? blueprint.variant_keys : [];

  // -------- 공용 강제정규화 유틸 --------

  if (code && !/\d/.test(String(code))) {
    // "AGN","TQ" 처럼 숫자 없는 시리즈는 series로 넘기고 code는 비움
    series = code; code = null;
  }


  // ❶ PDF 텍스트 일부에서 품번 후보 우선 확보
  let previewText = '';
  try { previewText = await readText(gcsUri, PREVIEW_BYTES) || ''; } catch {}
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

  let codes = [];
  if (!code) {
    const skuFromTable = pickSkuListFromTables(extracted);
    codes = skuFromTable.length ? skuFromTable : expandFromCodeSystem(extracted, blueprint);
    const maxEnv = Number(process.env.FIRST_PASS_CODES || FIRST_PASS_CODES || 20);
    const maxCodes = Number.isFinite(maxEnv) && maxEnv > 0 ? maxEnv : 20;
    if (codes.length > maxCodes) codes = codes.slice(0, maxCodes);
  }

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
      if (merged.length) candidates = merged;
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

  const mpnsFromDoc = harvestMpnCandidates(extracted?.text || '', baseSeries || series || code || '');
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
  const specRows = rawRows.length ? rawRows.slice(0) : [{}];

  let explodedEntries = [];
  for (const row of specRows) {
    const specsObj = row && typeof row === 'object' ? { ...row } : {};
    const fallbackSeries = specsObj.series_code || specsObj.series || baseSeries || null;
    const baseSeed = {
      brand: brandName,
      series: fallbackSeries,
      series_code: fallbackSeries,
      code: specsObj.code ?? fallbackSeries ?? null,
    };
    const expanded = explodeVariants(baseSeed, specsObj, blueprint).map((entry) => ({
      ...entry,
      brand: entry.brand || brandName,
      series_code: entry.series_code || entry.series || fallbackSeries,
      code: entry.code ?? specsObj.code ?? fallbackSeries ?? null,
    }));
    explodedEntries.push(...expanded);
  }
  if (!explodedEntries.length) {
    explodedEntries = [{ brand: brandName, series_code: baseSeries || null, code: baseSeries || null }];
  }

  // ---- 분할 여부 결정 ----
  const pnCands = candidateMap.map((c) => c.raw);
  const seriesCands = (candidates.length && series) ? candidateMap.filter((c) => /\d/.test(c.norm)).map((c) => c.raw) : [];
  const mustSplit = decideSplit({
    pnCandidates: pnCands,
    seriesCandidates: seriesCands,
    variantKeys,
    specs: (rawRows[0] || {})
  });

  if (!mustSplit && explodedEntries.length > 1) explodedEntries.splice(1);
  if (mustSplit && candidateMap.length > 1 && explodedEntries.length <= 1) {
    const max = Math.min(candidateMap.length, FIRST_PASS_CODES || 20);
    const tmpl = explodedEntries[0] || { brand: brandName, series_code: baseSeries || null };
    explodedEntries = candidateMap.slice(0, max)
      .map((c) => ({ ...tmpl, code: c.raw, code_norm: c.norm }));
  }

  const seenCodes = new Set();
  for (const entry of explodedEntries) {
    const baseInfo = entry || {};
    const specs = {};
    for (const k of allowedKeys) if (entry[k] != null) specs[k] = entry[k];
    const pickNumeric = (vals) => {
      for (const v of (Array.isArray(vals) ? vals : [vals])) { const n = coerceNumeric(v); if (Number.isFinite(n)) return n; }
      return null;
    };
    const voltageNum   = pickNumeric(specs.coil_voltage_vdc || specs.coil_voltage || specs.voltage_vdc || specs.voltage_dc || specs.voltage);
    const voltageToken = voltageNum != null ? String(Math.round(voltageNum)).padStart(2,'0') : null;

    let mpn = baseInfo.code ? String(baseInfo.code).trim() : null;   // 후보 복제 시 base.code가 곧 MPN

    if (!mpn && candidateMap.length) {
      const match = voltageToken ? candidateMap.find(c => c.norm.includes(voltageToken)) : null;
      mpn = (match || candidateMap[0])?.raw || null;
    }
    const variantTokens = [];
    for (const key of variantKeys) {
      const rawKey = String(key || '');
      const normKey = normIdent(rawKey);
      let val = entry[rawKey];
      if (val == null && normKey) val = entry[normKey];
      if (val == null && specs[rawKey] != null) val = specs[rawKey];
      if (val == null && normKey && specs[normKey] != null) val = specs[normKey];
      if (val != null) variantTokens.push(tokenOf(val));
    }

    if (
      !mpn &&
      (blueprint?.pn_template || blueprint?.ingestOptions?.pn_template)
    ) {
      const templated = buildMpn({ ...specs, ...baseInfo }, blueprint);
      if (templated) mpn = templated;
    }

    if (!mpn && mpnsFromDoc.length) {
      const want = variantTokens.filter(Boolean);
      const cand = want.length
        ? mpnsFromDoc.find((code) => want.every((w) => code.includes(w)))
        : mpnsFromDoc[0];
      if (cand) mpn = cand;
    }
    if (!mpn && specs.code) mpn = String(specs.code).trim();

    if (!mpn) {
      const prefix = baseInfo.series_code || baseInfo.series || baseSeries || '';
      const tokens = variantTokens.filter(Boolean);
      if (!tokens.length) {
        for (const v of Object.values(specs)) {
          const token = tokenOf(v);
          if (token) tokens.push(token);
        }
      }
      const suffix = tokens.length ? tokens.join('') : (voltageToken || (voltageNum != null ? String(Math.round(voltageNum)) : ''));
      if (prefix || suffix) mpn = `${prefix}${suffix}`.trim();
    }
    if (!mpn) continue;

    const mpnNorm = normalizeCode(mpn);
    if (!mpnNorm || seenCodes.has(mpnNorm)) continue;
    seenCodes.add(mpnNorm);

    const rec = {
      family_slug: family,
      brand: baseInfo.brand || brandName,
      code: mpn,
      code_norm: mpnNorm,
      series_code: baseInfo.series_code || baseSeries || null,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      display_name: `${baseInfo.brand || brandName} ${mpn}`,
      verified_in_doc: candidateNormSet.has(mpnNorm) || mpnNormFromDoc.has(mpnNorm),
      updated_at: now,
    };
    for (const k of allowedKeys) {
      let v = specs[k];
      if (v == null) continue;
      if (Array.isArray(v)) v = v[0];
      rec[k] = v;
    }
    if (blueprint?.code_rules) applyCodeRules(rec.code, rec, blueprint.code_rules, colTypes);
    records.push(rec);
  }

  // 최후 폴백 줄이기
  if (!records.length) {
    const tmp = 'TMP_' + (Math.random().toString(16).slice(2, 8)).toUpperCase();
    records.push({
      family_slug: family,
      brand: brand || extracted.brand || 'unknown',
      code: tmp,
      series_code: series || code || null,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      display_name: `${brand || extracted.brand || 'unknown'} ${tmp}`,
      verified_in_doc: false,
      updated_at: now,
    });
  }

  console.log('[MPNDBG]', {
    picks: candidateMap.length,
    vkeys: Array.isArray(blueprint?.ingestOptions?.variant_keys) ? blueprint.ingestOptions.variant_keys : [],
    exploded: explodedEntries.length,
    mustSplit,
    recs: records.length,
    colsSanitized: Object.keys(colTypes || {}).length,
  });

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
    if (colsSet.has('series_code')) safe.series_code = rec.series_code;
    if (colsSet.has('datasheet_uri')) safe.datasheet_uri = rec.datasheet_uri;
    if (colsSet.has('image_uri'))     safe.image_uri     = rec.image_uri;
    if (colsSet.has('datasheet_url')) safe.datasheet_url = rec.datasheet_uri; // 별칭 호환
    if (colsSet.has('display_name'))  safe.display_name  = rec.display_name;
    if (colsSet.has('displayname'))   safe.displayname   = rec.display_name;
    if (colsSet.has('cover') && rec.image_uri) safe.cover = rec.image_uri;
    if (colsSet.has('verified_in_doc')) safe.verified_in_doc = !!rec.verified_in_doc;

    // 블루프린트 값
    for (const [k,v] of Object.entries(rec)) {
      const kk = String(k || '').toLowerCase();
      if (BASE_KEYS.has(kk)) continue;
      if (!colsSet.has(kk)) continue;
      if (META_KEYS.has(kk)) continue;
      safe[kk] = v;
    }
    if (colsSet.has('updated_at')) safe.updated_at = now;

    // ← 업서트 전에 숫자/정수/불리언 컬럼을 타입에 맞춰 정리(실패 키는 삭제)
    sanitizeByColTypes(safe, colTypes);
    await upsertByBrandCode(table, normalizeKeysOnce(safe));
    upserted++;
  }

  return {
    ok: true,
    ms: Date.now() - started,
    family,
    final_table: table,
    brand: records[0]?.brand,
    code:  records[0]?.code,
    datasheet_uri: gcsUri,
    cover: records[0]?.image_uri || null,
    rows: upserted,
  };
}

module.exports = { runAutoIngest };
