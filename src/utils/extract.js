// relay-worker/src/utils/extract.js
'use strict';

const { pickPagesByVertex, getPdfText, callDocAI } = require('./vision');
const { parseDocAiTables } = require('./table_extractor');
const { chooseBrandCode } = require('./brandcode');

async function extractDataset({ gcsUri, filename = '', maxInlinePages = Number(process.env.MAX_DOC_PAGES_INLINE || 15) }) {
  // 1) 앞쪽 일부 페이지 텍스트 확보
  const meta = await getPdfText(gcsUri, { limit: maxInlinePages + 2 });
  const pageCountApprox = meta.pages?.length || 0;

  let rows = [];
  let verifiedPages = [];

  // 2) 키워드 기반 페이지 후보 → DocAI 표 파싱
  const pick = pickPagesByVertex(meta);
  for (const p of pick.pages) {
    const doc = await callDocAI(gcsUri, { pageNumbers: [p] });
    const r = parseDocAiTables(doc);
    if (r.length) {
      rows.push(...r);
      verifiedPages.push(p);
    }
  }

  // 3) 그래도 행이 없으면 키워드 윈도우에서 최소 1개 보조
  if (rows.length === 0) {
    const windows = (meta.pages||[]).map(x => x.text).join('\n').slice(0, 8000);
    const { brand, code, series } = await chooseBrandCode(windows, filenameHints(filename));
    if (code) rows.push({ code, series, desc:'', raw:[] });
  }

  // 4) 브랜드 확정(문서 전체 텍스트 + 파일명 힌트)
  const { brand } = await chooseBrandCode(meta.text, filenameHints(filename));

  // 5) normalize
  rows = rows.map(r => ({
    code: String(r.code||'').toUpperCase().replace(/\s+/g,''),
    series: r.series ? r.series.trim().toUpperCase() : filenameHints(filename).series || '',
    desc: r.desc || '',
    raw: r.raw || []
  })).filter(r => /^[A-Z0-9][A-Z0-9\-._/]*$/.test(r.code));

  return {
    brand: brand || filenameHints(filename).brandHints?.[0] || 'unknown',
    series: filenameHints(filename).series || '',
    rows,
    verifiedPages,
    pageCountApprox,
    note: rows.length ? '' : 'no_rows_extracted',
  };
}

function filenameHints(name='') {
  const low = name.toLowerCase();
  const brandHints = [];
  if (/panasonic|matsushita/i.test(name)) brandHints.push('Panasonic');
  if (/omron/i.test(name)) brandHints.push('OMRON');

  let series = '';
  const m = low.match(/mech[_-]eng[_-]([a-z0-9]+)/i);
  if (m) series = m[1].toUpperCase();

  const al = name.match(/\b(ALDP[0-9A-Z\-]+)/i);
  const codeHints = al ? [al[1].toUpperCase()] : [];

  return { brandHints, codeHints, series };
}

module.exports = { extractDataset };
