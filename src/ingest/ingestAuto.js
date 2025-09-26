'use strict';

const { getBlueprint } = require('../utils/blueprint');
const { explodeToRows, extractMpnCandidates } = require('../lib/mpnExploder');
const { upsertSpecsRows } = require('../lib/persist');
const { extractDocTextAndTables } = require('../lib/docai'); // 기존 DocAI 래퍼 사용

/** 메인 */
async function runAutoIngest({ gcsUri, brand, family }) {
  const started = Date.now();

  // 1) 텍스트/표 추출
  const { fullText, tables, coverImage } = await extractDocTextAndTables(gcsUri);

  // 2) 시리즈/브랜드 추정(선행 단계에서 확정되어 들어온다고 가정. 필요시 alias 테이블 매칭)
  const seriesFromText =
    (fullText.match(/\b[A-Z]{2,6}\d?(?:-[A-Z])?\b(?=.*?(?:RELAY|RELAYS))/i)?.[0] || '').toUpperCase();

  // 3) 블루프린트
  const bp = await getBlueprint(family);

  // 4) 표에서 노출된 명시적 MPN 리스트(주문정보/라인업 표 우선)
  const tableMpns = [];
  for (const t of tables || []) {
    for (const row of t.rows || []) {
      for (const cell of row) {
        const cands = extractMpnCandidates(cell.text || '');
        for (const c of cands) tableMpns.push(c);
      }
    }
  }

  // 5) 변환 공용 필드
  const base = {
    brand,
    series: seriesFromText,
    datasheet_uri: gcsUri,
    __raw_text: fullText,
    verified_in_doc: true,
    image_uri: coverImage || null,
  };

  // 6) 전개
  const rows = explodeToRows({
    brand,
    series: seriesFromText,
    baseFields: base,
    blueprint: bp,
    textCandidates: tableMpns
  });

  // 7) 저장
  const { inserted, updated } = await upsertSpecsRows(family, rows);

  return {
    ok: true,
    ms: Date.now() - started,
    family,
    brand,
    series: seriesFromText,
    datasheet_uri: gcsUri,
    rows: rows.length, inserted, updated
  };
}

module.exports = { runAutoIngest };
