// relay-worker/src/utils/brandcode.js
'use strict';

const db = require('./db');
const { tokenize, keywordWindows } = require('./regex');
const { callModelJson } = require('./vertex');

async function loadBrandAliases() {
  // 1) 별칭 테이블
  try {
    const r = await db.query('SELECT brand, alias FROM public.manufacturer_alias');
    const map = new Map();
    for (const { brand, alias } of r.rows || []) {
      const k = brand?.trim(), a = alias?.trim();
      if (!k || !a) continue;
      if (!map.has(k)) map.set(k, new Set());
      map.get(k).add(a);
    }
    if (map.size) return map;
  } catch (_) {}
  // 2) 폴백 사전
  const seed = ['Panasonic','OMRON','TE Connectivity','Molex','Phoenix Contact','Honeywell',
                'Texas Instruments','Analog Devices','STMicroelectronics','Murata','Microchip',
                'NXP','Infineon','onsemi','Vishay','ROHM','Toshiba','Renesas'];
  return new Map(seed.map(b => [b, new Set()]));
}

function brandCandidates(corpus, aliasMap) {
  const out = new Set();
  const low = (corpus || '').toLowerCase();
  for (const [brand, aliases] of aliasMap.entries()) {
    const names = [brand, ...aliases];
    for (const n of names) if (n && low.includes(String(n).toLowerCase())) out.add(brand);
  }
  return [...out];
}

function codeCandidates(corpus) {
  // 본문/키워드 주변 토큰을 모아 후보 리스트 생성
  const set = new Set();
  for (const t of tokenize(corpus || '')) set.add(t);
  for (const win of keywordWindows(corpus || '')) for (const t of tokenize(win)) set.add(t);
  return [...set].slice(0, 200);
}

async function chooseBrandCode(corpus) {
  const aliases = await loadBrandAliases();
  const bcands  = brandCandidates(corpus, aliases);
  const ccands  = codeCandidates(corpus);

  const sys = [
    'You are a product catalog analyzer.',
    'Return strict JSON {"brand": "...", "code": "...", "series": ""}.',
    'Choose only from candidates; if none fits, return empty string.'
  ].join('\n');

  const usr = JSON.stringify({
    text: String(corpus || '').slice(0, 9000),
    candidates: { brands: bcands, codes: ccands }
  });

  const out = await callModelJson(sys, usr);
  return {
    brand:  String(out?.brand  || '').trim(),
    code:   String(out?.code   || '').replace(/^[\W_]+|[\W_]+$/g,'').toUpperCase(),
    series: String(out?.series || '').trim(),
  };
}

// 텍스트에서 브랜드를 직관적으로 보정(사전 미스시)
if (!brand && /panasonic/i.test(corpus)) brand = 'Panasonic';
if (!brand && /omron/i.test(corpus))     brand = 'OMRON';
// … 필요 시 manufacturer_alias 테이블을 채우면 여기 보정은 최소화 가능


module.exports = { chooseBrandCode };
