'use strict';

const db = require('../../db');
const { generateJSON } = require('../utils/ai');

// ORDERING/TYPE NO. 영역 + 추출된 rows를 보고 PN 템플릿을 유도
async function learnPnTemplate({ family, brand, series, docText = '', rows = [] }) {
  const examples = rows
    .slice(0, 6)
    .map(r => ({
      code: r?.code || r?.pn || null,
      series: r?.series || r?.series_code || null,
      form: r?.contact_form || r?.contact_arrangement || r?.configuration || null,
      voltage: r?.coil_voltage_vdc || r?.voltage || null,
      suffix: r?.suffix || r?.terminal_form || null
    }))
    .filter(x => x.code);

  if (!examples.length || !docText) return null;

  // LLM에게 “코드 조합법 → 템플릿”을 산출하게 함 (규칙 문자열과 신뢰도만 받음)
  const out = await generateJSON({
    system:
      'From datasheet ordering section and examples, infer a concise part-number template DSL. Use tokens like {series},{form|map:1A>3,1C>1},{voltage|pad=2},{suffix}. Do NOT include packaging-only marks (e.g., S for tape). Output JSON only.',
    input: { textHead: docText.slice(0, 12000), examples },
    schema: {
      type: 'object',
      required: ['template', 'confidence'],
      properties: {
        template: { type: 'string' },
        confidence: { type: 'number' }
      }
    }
  });

  const tpl = out?.template && String(out.template).trim();
  const conf = Number(out?.confidence || 0);
  if (!tpl || conf < 0.7) return null;
  return tpl.replace(/\s+/g, '');
}

// brand/series 스코프로 extraction_recipe에 저장
async function upsertExtractionRecipe({ family, brand, series, pnTemplate }) {
  if (!pnTemplate) return;
  const brandSlug = (brand || '').toLowerCase().replace(/[^a-z0-9]+/g, '-') || null;
  const seriesSlug = (series || '').toLowerCase().replace(/[^a-z0-9]+/g, '-') || null;

  await db.query(
    `
    INSERT INTO public.extraction_recipe (family_slug, brand_slug, series_slug, recipe)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (family_slug, COALESCE(brand_slug,''), COALESCE(series_slug,''))
    DO UPDATE SET recipe = 
      CASE WHEN extraction_recipe.recipe IS NULL THEN EXCLUDED.recipe
           ELSE jsonb_strip_nulls(extraction_recipe.recipe) || jsonb_build_object('pn_template', EXCLUDED.recipe->>'pn_template')::jsonb
      END
  `,
    [family, brandSlug, seriesSlug, JSON.stringify({ pn_template: pnTemplate })]
  );
}

module.exports = { learnPnTemplate, upsertExtractionRecipe };