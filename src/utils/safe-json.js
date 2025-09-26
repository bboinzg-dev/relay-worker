
module.exports.safeJsonParse = function safeJsonParse(input) {
  if (input == null) return null;
  if (typeof input === 'object') return input;
    const raw = String(input);
  try { return JSON.parse(raw); }
  catch (_) {
    let t = raw.replace(/^```(?:json)?/i, '').replace(/```$/, '')
               .replace(/[“”]/g, '"').replace(/[‘’]/g, "'")
               .replace(/'([^'\\]+)'\s*:/g, '"$1":')       // 키의 단따옴표 → 쌍따옴표
               .replace(/,\s*([}\]])/g, '$1')               // 트레일링 콤마 제거
               .replace(/[\u0000-\u001F]/g, ' ');           // 제어문자 정리
    try { return JSON.parse(t); }
    catch (e2) {
       const err = new Error(`JSON_PARSE_FAILED: ${e2.message}`);
        err.sample = raw.slice(0, 2000);
      throw err;
    }
  }
  };
