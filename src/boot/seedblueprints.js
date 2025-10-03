// src/boot/seedBlueprints.js
'use strict';
const db = require('../../db');
const seeds = require('./blueprint.seeds.json'); // 최소 기본 몇 종만

async function seedIfEmpty() {
  const r = await db.query(`SELECT COUNT(*)::int AS n FROM public.component_spec_blueprint`);
  if (r.rows[0].n > 0) return;

  for (const { family_slug, specs_table, fields_json } of seeds) {
    await db.query(
      `INSERT INTO public.component_registry (family_slug, specs_table)
       VALUES ($1,$2)
       ON CONFLICT (family_slug) DO UPDATE SET specs_table=EXCLUDED.specs_table`,
      [family_slug, specs_table]
    );
    await db.query(
      `INSERT INTO public.component_spec_blueprint (family_slug, fields_json)
       VALUES ($1, $2::jsonb)
       ON CONFLICT (family_slug) DO UPDATE SET fields_json=EXCLUDED.fields_json`,
      [family_slug, JSON.stringify(fields_json)]
    );
  }
  console.log(`[BOOT] seeded ${seeds.length} families`);
}
module.exports = { seedIfEmpty };
