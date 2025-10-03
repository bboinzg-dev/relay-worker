// relay-worker/src/utils/brandcode.js
'use strict';

const db = require('../../db');
const { tokenize, keywordWindows } = require('./regex');
const { callModelJson } = require('./vertex');

/** 제조사 별칭 로딩 (있으면 테이블, 없으면 폴백 사전) */
async function loadBrandAliases() {
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

  const seed = [
    'Panasonic','OMRON','TE Connectivity','Molex','Phoenix Contact','Honeywell',
    'Texas Instruments','Analog Devices','STMicroelectronics','Murata','Microchip',
    'NXP','Infineon','onsemi','Vishay','ROHM','Toshiba','Renesas'
  ];
  return new Map(seed.map(b => [b, new Set()]));
}

function brandCandidatesFromCorpus(corpus, aliasMap) {
  const out = new Set();
  const low = String(corpus || '').toLowerCase();
  for (const [b, aliases] of aliasMap.entries()) {
    const names = [b, ...aliases];
    for (const n of names) {
      const k = String(n || '').trim().toLowerCase();
      if (!k) continue;
      if (low.includes(k)) out.add(b);
    }
  }
  return [...out];
}

/** 간단 코드 후보: 토큰 + 키워드 윈도우 */
function codeCandidatesFromCorpus(corpus) {
  const set = new Set();
  const text = String(corpus || '');
  for (const t of tokenize(text)) set.add(t);
  for (const win of keywordWindows(text)) for (const t of tokenize(win)) set.add(t);
  return [...set].slice(0, 300);
}

/** LLM 최종 선택(후보 밖 금지) */
async function chooseBrandCode(corpus, extra = {}) {
  const aliases = await loadBrandAliases();
  const brandCands = [...new Set([
    ...(extra.brandHints || []),
    ...brandCandidatesFromCorpus(corpus, aliases),
  ])];
  const codeCands  = [...new Set([
    ...(extra.codeHints || []),
    ...codeCandidatesFromCorpus(corpus),
  ])];

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
      codes:  codeCands
    }
  });

  const out = await callModelJson(sys, usr);
  const brand  = String(out?.brand  || '').trim();
  const code   = String(out?.code   || '').replace(/^[\W_]+|[\W_]+$/g, '').toUpperCase();
  const series = String(out?.series || '').trim();
  return { brand, code, series, brandCands, codeCands };
}

module.exports = { chooseBrandCode, loadBrandAliases, brandCandidatesFromCorpus, codeCandidatesFromCorpus };
