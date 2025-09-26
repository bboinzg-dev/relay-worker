'use strict';

// JSON 텍스트 복구(코드펜스/트레일링 콤마/최대 {} 블록 추출)
function repairJsonText(txt = '') {
  if (typeof txt !== 'string') txt = String(txt ?? '');
  txt = txt.trim();
  // ```json … ``` 코드펜스 제거
  txt = txt.replace(/^```(?:json)?/i, '').replace(/```$/, '').trim();
  // 가장 큰 {...} 블록만 남기기
  const a = txt.indexOf('{'), b = txt.lastIndexOf('}');
  if (a !== -1 && b !== -1 && b > a) txt = txt.slice(a, b + 1);
  // BOM 제거
  txt = txt.replace(/^\uFEFF/, '');
  // 트레일링 콤마 제거  ,}  ,]
  txt = txt.replace(/,\s*([}\]])/g, '$1');
  return txt;
}

// 실패 시 복구를 시도하는 안전 파서
function safeJsonParse(maybe) {
  if (maybe == null) return null;
  if (typeof maybe === 'object') return maybe;
  const raw = String(maybe);
  try {
    return JSON.parse(raw);
  } catch {
    const fixed = repairJsonText(raw);
    try {
      return JSON.parse(fixed);
    } catch (e2) {
      const err = new Error(`JSON_PARSE_FAILED: ${e2.message}`);
      err.raw = raw.slice(0, 2000);
      throw err;
    }
  }
}

module.exports = { safeJsonParse, repairJsonText };