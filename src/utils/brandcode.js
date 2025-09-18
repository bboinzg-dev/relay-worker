'use strict';

const db = require('./db');
const { tokenize, keywordWindows } = require('./regex');
const { chooseBrandCode } = require('./vertex');

async function loadBrandAliases() {
  // 1순위: 별칭 테이블
  try {
    const r = await db.query('SELECT brand, alias FROM public.manufacturer_alias');
    const map = new Map();
    for (const { brand, alias } of r.rows || []) {
      const k = brand?.trim();
      const a = alias?.trim();
      if (!k || !a) continue;
      if (!map.has(k)) map.set(k, new Set());
      map.get(k).add(a);
    }
    if (map.size) return map;
  } catch (_) {}

  // 2순위: 기존 스펙 테이블에서 브랜드 추출(폴백)
  try {
    const r2 = await db.query(`
      SELECT DISTINCT brand FROM (
        SELECT brand FROM public.relay_specs WHERE brand IS NOT NULL
        UNION ALL
        SELECT brand FROM public.common_specs WHERE brand IS NOT NULL
      ) t LIMIT 500
    `);
    const map = new Map();
    for (const { brand } of r2.rows || []) {
      const k = String(brand || '').trim();
      if (k) map.set(k, new Set());
    }
    if (map.size) return map;
  } catch (_) {}

  // 3순위: 미니 사전(폴백)
  const seed = [
    'Panasonic', 'OMRON', 'TE Connectivity', 'Molex', 'Phoenix Contact',
    'Honeywell', 'Texas Instruments', 'Analog Devices', 'STMicroelectronics',
    'Murata', 'Microchip', 'NXP', 'Infineon', 'ON Semiconductor', 'Vishay'
  ];
  const map = new Map(seed.map(b => [b, new Set()]));
  return map;
}

function brandCandidatesFromCorpus(corpus, aliasMap) {
  const text = corpus || '';
  const low = text.toLowerCase();
  const out = new Set();

  for (const [brand, aliases] of aliasMap.entries()) {
    const names = [brand, ...aliases];
    for (const n of names) {
      const needle = String(n || '').trim();
      if (!needle) continue;
      if (low.includes(needle.toLowerCase())) out.add(brand);
    }
  }
  return [...out];
}

function codeCandidatesFromCorpus(corpus) {
  const set = new Set();
  const toks = tokenize(corpus);
  toks.forEach(t => set.add(t));

  // 키워드 주변 가중치를 더 줌
  for (const win of keywordWindows(corpus || '')) {
    tokenize(win).forEach(t => set.add(t));
  }
  return [...set];
}

async function detectBrandAndCode(corpus) {
  const aliasMap = await loadBrandAliases();
  const brandCands = brandCandidatesFromCorpus(corpus, aliasMap);

  // 후보가 너무 적어도 상관없음(LLM이 빈 값을 허용)
  const codeCands = codeCandidatesFromCorpus(corpus);

  // LLM이 후보 중에서만 고르게 강제 → 범용/안전
  const picked = await chooseBrandCode(corpus, brandCands, codeCands);

  // 정규화
  const brand = String(picked.brand || '').trim();
  const code  = String(picked.code  || '').replace(/^[\W_]+|[\W_]+$/g, '').toUpperCase();

  return { brand, code, series: String(picked.series || '').trim() };
}

module.exports = { detectBrandAndCode };
