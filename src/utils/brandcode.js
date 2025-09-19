// relay-worker/src/utils/brandcode.js
'use strict';

/**
 * 브랜드/품번 감지 유틸(안전한 모듈)
 * - 모듈 로드 시점에 예외가 나지 않도록, 전역 실행 로직 없음
 * - 브랜드/별칭 사전 + 후보 토큰 스코어링 + LLM 최종 선택(후보 밖 선택 금지)
 */

const db = require('./db');
const { tokenize, keywordWindows } = require('./regex');
const { callModelJson } = require('./vertex');

/** 제조사 별칭 로딩 (있으면 테이블, 없으면 폴백 사전) */
async function loadBrandAliases() {
  // 1) 별칭 테이블 우선
  try {
    const r = await db.query('SELECT brand, alias FROM public.manufacturer_alias');
    const map = new Map();
    for (const { brand, alias } of r.rows || []) {
      const k = (brand || '').trim();
      const a = (alias || '').trim();
      if (!k || !a) continue;
      if (!map.has(k)) map.set(k, new Set());
      map.get(k).add(a);
    }
    if (map.size) return map;
  } catch (_) {}

  // 2) 폴백 사전
  const seed = [
    'Panasonic','OMRON','TE Connectivity','Molex','Phoenix Contact','Honeywell',
    'Texas Instruments','Analog Devices','STMicroelectronics','Murata','Microchip',
    'NXP','Infineon','onsemi','Vishay','ROHM','Toshiba','Renesas'
  ];
  const map = new Map(seed.map(b => [b, new Set()]));
  return map;
}

/** 텍스트에서 사전 기반으로 브랜드 후보 뽑기 */
function brandCandidatesFromCorpus(corpus, aliasMap) {
  const out = new Set();
  const low = String(corpus || '').toLowerCase();
  for (const [b, aliases] of aliasMap.entries()) {
    const names = [b, ...aliases];
    for (const n of names) {
      const key = String(n || '').trim();
      if (!key) continue;
      if (low.includes(key.toLowerCase())) out.add(b);
    }
  }
  return [...out];
}

/** 텍스트에서 품번 후보 토큰 뽑기(본문 + 키워드 주변) */
function codeCandidatesFromCorpus(corpus) {
  const set = new Set();
  const text = String(corpus || '');
  for (const t of tokenize(text)) set.add(t);
  for (const win of keywordWindows(text)) for (const t of tokenize(win)) set.add(t);
  return [...set].slice(0, 200);
}

/** LLM으로 최종 1건 선택(후보 밖 선택 금지) */
async function chooseBrandCode(corpus) {
  const aliases = await loadBrandAliases();
  const brandCands = brandCandidatesFromCorpus(corpus, aliases);
  const codeCands  = codeCandidatesFromCorpus(corpus);

  const sys = [
    'You are a product catalog analyzer.',
    'Return strict JSON: {"brand": "...", "code": "...", "series": ""}.',
    'Choose ONLY from the provided candidates. Empty string if none fits.',
    'Do not fabricate.'
  ].join('\n');

  const usr = JSON.stringify({
    text: String(corpus || '').slice(0, 9000),
    candidates: {
      brands: brandCands,
      codes:  codeCands,
    }
  });

  const out = await callModelJson(sys, usr);

  const pickedBrand  = String(out?.brand  || '').trim();
  const pickedCode   = String(out?.code   || '').replace(/^[\W_]+|[\W_]+$/g, '').toUpperCase();
  const pickedSeries = String(out?.series || '').trim();

  return { brand: pickedBrand, code: pickedCode, series: pickedSeries };
}

module.exports = { chooseBrandCode };
