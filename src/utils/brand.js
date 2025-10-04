'use strict';

const { generateJSON } = require('./ai');

// 아주 일반적인 회사 접미어/기호 제거 (브랜드 고정 매핑 아님)
const COMPANY_SUFFIX = /\b(co|co\.|corp|corporation|inc|ltd|gmbh|s\.?a\.?|llc|주식회사|有限公司|株式会社)\b/ig;

function stripJunk(s) {
  if (!s) return s;
  let x = (s||'').replace(/[®™©]/g,' ').replace(COMPANY_SUFFIX,' ').replace(/\s{2,}/g,' ').trim();
  // 첫 글자 대문자/나머지 소문자 정도만
  if (/^[a-z]/i.test(x)) x = x.replace(/\w\S*/g, t => t[0].toUpperCase() + t.slice(1).toLowerCase());
  return x;
}

function firstBrandFromText(text) {
  if (!text) return null;
  // 가장 단순한 후보: 표지 상단/바닥 로고 캡션, "Panasonic", "OMRON Corporation" 등
  const m = text.match(/\b([A-Z][A-Za-z0-9\- ]{1,24})(?:\s+(?:Corporation|Co\.|Inc\.|Ltd\.))?\b/);
  return m ? m[1] : null;
}

async function resolveBrand({ rawText, hint }) {
  // 1) 우선순위: hint(override) → 텍스트에서 첫 후보
  const seed = hint || firstBrandFromText(rawText) || null;

  // 2) LLM 시도 (있으면)
  let aiBrand = null;
  try {
    aiBrand = await generateJSON({
      system: 'You normalize manufacturer brand names found in electronics datasheets. Output JSON only.',
      input: {
        hint: seed, textHead: rawText ? rawText.slice(0, 3000) : null
      },
      schema: {
        type: 'object',
        required: ['brand_effective'],
        properties: {
          brand_effective: { type: 'string', description: 'Canonical brand (human-facing). Avoid legal suffixes.' },
          evidence: { type: 'string', description: 'Short snippet or reason', nullable: true }
        }
      }
    });
  } catch {}

  const fromAI = aiBrand?.brand_effective?.trim();
  if (fromAI) return { brand_effective: stripJunk(fromAI), source: 'ai' };

  // 3) 휴리스틱
  const cleaned = stripJunk(seed);
  if (cleaned) return { brand_effective: cleaned, source: 'heuristic' };

  // 4) 마지막 폴백: 아무것도 못 찾음
  return { brand_effective: null, source: 'none' };
}

module.exports = { resolveBrand };