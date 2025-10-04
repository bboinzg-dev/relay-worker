'use strict';

const { generateJSON } = require('./ai');

/**
 * Detect variant keys used in a datasheet's Ordering Code table.
 * - 우선 AI가 텍스트에서 실제 사용 키를 찾아 반환
 * - 실패/부족 시 블루프린트 키(후순위)로 폴백
 * - allowedKeys가 있으면 그 화이트리스트와 교집합만 반환
 *
 * @param {Object} opts
 * @param {string}   opts.rawText                  - datasheet 텍스트(상당 부분)
 * @param {string}   [opts.family]                 - family slug (힌트)
 * @param {string[]} [opts.blueprintVariantKeys]   - 블루프린트 기본 vkeys(후순위)
 * @param {string[]} [opts.allowedKeys]            - 허용 키 화이트리스트(옵션)
 * @returns {Promise<string[]>} normalized variant keys (max 6)
 */
async function detectVariantKeys({ rawText, family, blueprintVariantKeys, allowedKeys }) {
  // 0) 텍스트가 너무 짧으면 즉시 폴백
  if (!rawText || rawText.length < 200) {
    return Array.isArray(blueprintVariantKeys) ? blueprintVariantKeys : [];
  }

  // 1) LLM 시도
  let ai = null;
  try {
    ai = await generateJSON({
      system:
        'From electronics datasheet free text, extract the list of parameter keys used in the product ordering code table. ' +
        'Return concise machine keys (snake_case, ascii). Output JSON only.',
      input: {
        family,
        textHead: rawText.slice(0, 6000),
        examples: [
          'coil_voltage_vdc',
          'contact_form',
          'suffix',
          'package',
          'mounting_type',
          'tolerance_pct',
          'temperature_grade',
          'lead_spacing_mm',
          'current_rating_a',
          'voltage_rating_vdc'
        ]
      },
      schema: {
        type: 'object',
        required: ['variant_keys'],
        properties: {
          variant_keys: {
            type: 'array',
            items: { type: 'string' },
            description: 'ordered keys by importance (max 6)'
          }
        }
      }
    });
  } catch {
    // 무시하고 폴백으로 진행
  }

  // 2) 결과 선택: AI → 블루프린트 → 빈 배열
  let vkeys = Array.isArray(ai?.variant_keys)
    ? ai.variant_keys
    : (Array.isArray(ai) ? ai : (blueprintVariantKeys || []));

  // 3) 정규화(공백/기호 제거, 앞뒤 언더스코어 제거)
  let cleaned = vkeys
    .map(k => String(k || '')
      .toLowerCase()
      .replace(/[^a-z0-9_]+/g, '_')
      .replace(/^_+|_+$/g, '')
    )
    .filter(Boolean);

  // 4) 중복 제거(순서 보존)
  cleaned = cleaned.filter((k, i, a) => a.indexOf(k) === i);

  // 5) (옵션) 허용 키 화이트리스트와 교집합
  if (Array.isArray(allowedKeys) && allowedKeys.length) {
    const allow = new Set(allowedKeys.map(s => String(s).toLowerCase()));
    cleaned = cleaned.filter(k => allow.has(k));
  }

  // 6) 과도한 길이 제한
  return cleaned.slice(0, 6);
}

module.exports = { detectVariantKeys };
