// src/ai/datasheetExtract.js
'use strict';
const path = require('node:path');
const os = require('node:os');
const fs = require('node:fs/promises');
const { storage, parseGcsUri } = require('../utils/gcs');
const db = require('../utils/db');

async function detectBrandFromText(fullText) {
  // manufacturer_alias를 전부 불러와 alias/brand가 본문에 등장하는지 검사
  const r = await db.query(`SELECT brand, alias FROM public.manufacturer_alias`);
  const hay = fullText.toLowerCase();
  let best = null;
  for (const row of r.rows) {
    const alias = String(row.alias || '').toLowerCase();
    const brand = String(row.brand || '').toLowerCase();
    if (!alias && !brand) continue;
    if ((alias && hay.includes(alias)) || (brand && hay.includes(brand))) {
      const cand = row.brand;
      if (!best || (alias && alias.length > best.aliasLen)) {
        best = { brand: cand, aliasLen: alias.length || 0 };
      }
    }
  }
  return best?.brand || null;
}

function normalizeHeader(h) {
  const s = h.toLowerCase().replace(/\s+/g,' ').trim();
  return s
    .replace(/no\./g, 'no')
    .replace(/catalogue/g, 'catalog')
    .replace(/ordering info(?:rmation)?/g,'ordering information');
}

function isCodeLike(token) {
  const t = (token || '').trim();
  if (!t) return false;
  // 대문자/숫자/하이픈/슬래시 혼합, 길이 4~24, 전형적인 단위/일반단어 제외
  if (!/^[A-Z0-9][A-Z0-9\-_/\.]{2,23}$/.test(t)) return false;
  if (/(VDC|VAC|V|A|W|Ω|OHM|RoHS|UL|REACH|ISO|RELAY|COIL|CONTACT|DIM|TABLE)/i.test(t)) return false;
  return true;
}

function mapFieldName(header) {
  const h = normalizeHeader(header);
  if (/(^| )part( |-)?(number|no|#)\b|^model\b|^type\b|catalog( |-)?(no|number)\b|ordering code\b|ordering no\b/.test(h))
    return 'code';
  if (/coil (voltage|vdc)\b|^voltage\b/.test(h))
    return 'coil_voltage_vdc';
  if (/contact (form|arrangement)\b/.test(h))
    return 'contact_form';
  if (/contact rating\b|rating\b/.test(h))
    return 'contact_rating_text';
  if (/series\b/.test(h))
    return 'series';
  // 치수는 데이터시트마다 표기가 다양해서 자유 텍스트로 두고 LLM으로 정규화하는 대신 여기선 보수적으로 스킵
  return null;
}

async function parseWithDocumentAI(gcsUri) {
  const projectId  = process.env.DOC_AI_PROJECT_ID;
  const location   = process.env.DOC_AI_LOCATION || 'us';
  const processor  = process.env.DOC_AI_PROCESSOR_ID; // e.g. Form Parser
  if (!projectId || !processor) return null;

  let client;
  try {
    ({ v1: { DocumentProcessorServiceClient: client } } = require('@google-cloud/documentai'));
  } catch {
    return null; // 라이브러리 미설치 시 폴백
  }

  const name = `projects/${projectId}/locations/${location}/processors/${processor}`;

  const { bucket, name: obj } = parseGcsUri(gcsUri);
  const [pdf] = await storage.bucket(bucket).file(obj).download();

  const [result] = await new client().processDocument({
    name,
    rawDocument: { content: pdf, mimeType: 'application/pdf' },
  });

  const doc = result?.document;
  if (!doc) return null;

  const getText = (anchor) => {
    if (!anchor?.textSegments?.length) return '';
    const { text } = doc;
    let out = '';
    for (const seg of anchor.textSegments) {
      const start = Number(seg.startIndex || 0);
      const end = Number(seg.endIndex);
      out += text.substring(start, end);
    }
    return out;
  };

  // 1) 모든 표를 행렬 문자열로 변환
  const tables = [];
  for (const p of (doc.pages || [])) {
    for (const t of (p.tables || [])) {
      const headers = t.headerRows?.[0]?.cells?.map(c => getText(c.layout.textAnchor).trim()) || [];
      const rows = [];
      for (const r of (t.bodyRows || [])) {
        rows.push(r.cells.map(c => getText(c.layout.textAnchor).trim()));
      }
      tables.push({ headers, rows });
    }
  }

  // 2) 자유 텍스트 (브랜드 탐지 등)
  const fullText = (doc.text || '').replace(/\u0000/g, '');

  return { tables, fullText };
}

async function parseWithPdfParse(gcsUri) {
  let pdfParse;
  try {
    pdfParse = require('pdf-parse');
  } catch {
    return { fullText: '' };
  }
  const { bucket, name } = parseGcsUri(gcsUri);
  const [buf] = await storage.bucket(bucket).file(name).download();
  const r = await pdfParse(buf);
  return { fullText: r.text || '' };
}

function extractFromTables(tables, allowedKeys) {
  const out = [];
  for (const t of (tables || [])) {
    const idxMap = {};
    t.headers.forEach((h, i) => {
      const k = mapFieldName(h);
      if (k && (k === 'code' || allowedKeys.includes(k))) {
        idxMap[k] = i;
      }
    });
    if (idxMap.code == null) continue; // 코드 열이 없으면 스킵

    for (const row of t.rows) {
      const code = String(row[idxMap.code] || '').trim().toUpperCase();
      if (!isCodeLike(code)) continue;

      const rec = { code, verified_in_doc: true };
      for (const k of Object.keys(idxMap)) {
        if (k === 'code') continue;
        const v = String(row[idxMap[k]] || '').trim();
        if (!v) continue;
        // 숫자 필드는 단위 제거 후 숫자만 보관(너무 공격적이지 않게)
        if (k === 'coil_voltage_vdc') {
          const m = v.match(/(\d+(?:\.\d+)?)/);
          rec[k] = m ? Number(m[1]) : null;
        } else {
          rec[k] = v;
        }
      }

      // series가 없고 코드 접두가 뚜렷하면 추정 (예: AGN21024 -> AGN 시리즈)
      if (!rec.series) {
        const m = code.match(/^([A-Z]{2,5})\d/);
        if (m) rec.series = m[1];
      }

      out.push(rec);
    }
  }
  return out;
}

function extractCodesFromFreeText(fullText) {
  const set = new Set();
  // 후보: 대문자 2~5 + 숫자, 뒤에 옵션 문자가 붙을 수 있음 (예: AGN200S12, G2R-2-SN, etc.)
  const re = /\b([A-Z]{2,6}[0-9]{2,6}[A-Z0-9\-_/]{0,8})\b/g;
  let m;
  while ((m = re.exec(fullText)) !== null) {
    const cand = m[1].toUpperCase();
    if (isCodeLike(cand)) set.add(cand);
  }
  return Array.from(set).slice(0, 200).map(code => ({ code, verified_in_doc: false }));
}

async function extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, brandHint = null }) {
  // 1) Document AI 시도
  const docai = await parseWithDocumentAI(gcsUri);

  // 2) 기본 텍스트 확보
  const fullText = docai?.fullText || (await parseWithPdfParse(gcsUri)).fullText || '';

  // 3) 브랜드 감지
  const brandDetected = brandHint || (await detectBrandFromText(fullText)) || 'unknown';

  // 4) 표 기반 추출
  let rows = [];
  if (docai?.tables?.length) {
    rows = extractFromTables(docai.tables, allowedKeys);
  }

  // 5) 표에서 못 뽑았으면 자유 텍스트 패턴 기반
  if (!rows.length && fullText) {
    rows = extractCodesFromFreeText(fullText);
  }

  // 6) 너무 많은 조합 방지: 상한 400개
  if (rows.length > 400) rows = rows.slice(0, 400);

  // 7) 중복 제거
  const uniq = new Map();
  for (const r of rows) {
    if (!r.code) continue;
    uniq.set(r.code, r);
  }
  rows = Array.from(uniq.values());

  return { brand: brandDetected, rows };
}

module.exports = { extractPartsAndSpecsFromPdf };
