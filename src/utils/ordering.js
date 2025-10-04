'use strict';

const { generateJSON } = require('./ai');

// 블루프린트 vkeys는 후순위. 데이터시트의 실제 Ordering code 표가 있으면 그 키들을 채택.
async function detectVariantKeys({ rawText, family, blueprintVariantKeys }) {
  let ai = null;
  try {
    ai = await generateJSON({
      system: 'From electronics datasheet free text, extract the list of parameter keys used in the product ordering code table (e.g., coil voltage, contact form, package, tolerance). Output JSON only.',
      input: {
        family, textHead: rawText ? rawText.slice(0, 6000) : null,
        examples: ['coil_voltage_vdc','contact_form','suffix','package','tolerance_pct','temperature_grade']
      },
      schema: {
        type: 'object',
        required: ['variant_keys'],
        properties: {
          variant_keys: { type: 'array', items: { type: 'string' }, description: 'ordered keys by importance (max 6)' }
        }
      }
    });
  } catch {}

  // LLM 결과 정상 → 그거 사용, 아니면 블루프린트, 아니면 빈 배열
  const vkeys = Array.isArray(ai?.variant_keys) ? ai.variant_keys : (blueprintVariantKeys || []);
  // 키 이름 안전화(공백, 기호 제거)
  const cleaned = vkeys.map(k => String(k || '').toLowerCase().replace(/[^a-z0-9_]+/g,'_')).filter(Boolean);
  // 과한 길이 제한
  return cleaned.slice(0, 6);
}

module.exports = { detectVariantKeys };