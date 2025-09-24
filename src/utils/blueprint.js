// src/utils/blueprint.js
'use strict';
const db = require('./db');

const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS || 60_000);
const cache = new Map(); // key -> { t, v }

async function fetchFromDB(family) {
  const r = await db.query(`
    SELECT b.family_slug, r.specs_table, b.fields_json
      FROM public.component_spec_blueprint b
      JOIN public.component_registry r USING (family_slug)
     WHERE b.family_slug = $1
     LIMIT 1`, [family]);
  if (!r.rows.length) throw new Error(`BLUEPRINT_NOT_FOUND:${family}`);
  const row = r.rows[0];
  return {
    family_slug : row.family_slug,
    specs_table : row.specs_table.startsWith('public.') ? row.specs_table : `public.${row.specs_table}`,
    fields_json : row.fields_json || {},
    allowedKeys : Object.keys(row.fields_json || {}),
  };
}

async function getBlueprint(family) {
  const key = String(family || '').toLowerCase();
  const hit = cache.get(key);
  if (hit && Date.now() - hit.t < TTL) return hit.v;

  const v = await fetchFromDB(key);
  cache.set(key, { t: Date.now(), v });
  return v;
}

module.exports = { getBlueprint };
