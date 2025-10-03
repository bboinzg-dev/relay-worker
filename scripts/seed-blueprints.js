#!/usr/bin/env node
'use strict';

const { getPool } = require('../db');
// 기존에 쓰던 seed JSON이 있다면 이 경로로 옮겨두세요.
const seeds = require('../src/boot/blueprint.seeds.json');

const pool = getPool();

(async () => {
  for (const it of seeds) {
    const { family_slug, specs_table, fields_json } = it;
    await pool.query(
      `INSERT INTO public.component_registry (family_slug, specs_table)
       VALUES ($1, $2) ON CONFLICT (family_slug) DO NOTHING`,
       [family_slug, specs_table]
    );
    await pool.query(
      `INSERT INTO public.component_spec_blueprint (family_slug, fields_json)
       VALUES ($1, $2::jsonb) ON CONFLICT (family_slug) DO NOTHING`,
       [family_slug, JSON.stringify(fields_json || {})]
    );
    console.log('seeded (if missing):', family_slug);
  }
  await pool.end();
})();
