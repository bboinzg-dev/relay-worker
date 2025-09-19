// relay-worker/src/utils/extract.js
'use strict';

const { keywordWindows, tokenize } = require('./regex');
const { chooseBrandCode } = require('./brandcode');
const { callDocAI, getPdfText, pickPagesByVertex } = require('./vision'); // 아래 설명
const { parseDocAiTables } = require('./table_extractor');

/**
 * @param {Object} args
 * - gcsUri: 'gs://bucket/path.pdf'
 * - filename: 원본 파일명
 * - maxInlinePages: env MAX_DOC_PAGES_INLINE
 */
async function extractDataset(args) {
  const { gcsUri, filename = '', maxInlinePages = 15 } = args;

  // 1) 텍스트/페이지 후보
  const meta = await getPdfText(gcsUri, { limit: maxInlinePages + 2 });
  const pageCount = meta.pages?.length || 0;

  // 파일명/헬더 힌트
  const hints = filenameHints(filename);

  let rows = [];
  let verifiedPages = [];

  if (pageCount > maxInlinePages) {
    // 2-a) 페이지 선택 → DocAI 표 추출
    const pick = await pickPagesByVertex(meta, { target: ['ordering','type','selection'] });
    const candidates = [...new Set(pick.pages || [])].slice(0, 8);
    for (const p of candidates) {
      const doc = await callDocAI(gcsUri, { pageNumbers: [p] });
      const r = parseDocAiTables(doc);
      if (r.length) {
        rows.push(...r);
        verifiedPages.push(p);
      }
    }
  } else {
    // 2-b) 키워드 윈도우 기반 LLM 선택(표가 없을 때 보조)
    const windows = keywordWindows(meta.text).slice(0, 6).join('\n---\n');
    const { brand, code, series } = await chooseBrandCode(windows, hints);
    if (code) rows.push({ code, series, desc:'', raw:[] });
  }

  // 3) 브랜드 확정 (문서 전체 텍스트 + 힌트)
  const { brand } = await chooseBrandCode(meta.text, hints);

  // 4) 결과 정리
  rows = normalizeRows(rows, brand, hints.series);

  return {
    brand: brand || (hints.brandHints?.[0] || 'unknown'),
    series: hints.series || '',
    codes: rows.map(r => r.code),
    rows,
    verifiedPages,
    note: rows.length ? '' : 'no_rows_extracted',
  };
}

function filenameHints(name='') {
  const low = name.toLowerCase();
  const brandHints = [];
  if (/(panasonic|matsushita|matsushita electric)/i.test(name)) brandHints.push('Panasonic');
  if (/(omron)/i.test(name)) brandHints.push('OMRON');
  // 확장 가능

  let series = '';
  // mech_eng_gn.pdf → GN, mech_eng_tq.pdf → TQ
  const m = low.match(/mech[_-]eng[_-]([a-z0-9]+)/i);
  if (m) series = m[1].toUpperCase();

  // ALDP… 유형
  const al = name.match(/\b(ALDP[0-9A-Z\-]+)/i);
  const codeHints = al ? [al[1].toUpperCase()] : [];

  return { brandHints, codeHints, series };
}

function normalizeRows(rows, brand, seriesHint) {
  return rows.map(r => {
    const code = String(r.code||'').toUpperCase().replace(/\s+/g,'');
    const series = r.series ? r.series.trim().toUpperCase()
                 : (seriesHint ? seriesHint.toUpperCase() : '');
    return { code, series, desc:r.desc||'', raw:r.raw||[] };
  }).filter(r => /^[A-Z0-9][A-Z0-9\-._/]*$/.test(r.code));
}

module.exports = { extractDataset };
