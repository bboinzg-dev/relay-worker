'use strict';

const { pool } = require('./db');

const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS || 60_000);
const cache = new Map();
const pending = new Map();

async function getBlueprint(familySlug) {
  const now = Date.now();
  const hit = cache.get(familySlug);
  if (hit && hit.exp > now) return hit.value;

  if (pending.has(familySlug)) return pending.get(familySlug);

  const p = (async () => {
    const { rows } = await pool.query(
      `SELECT r.family_slug, r.specs_table, b.fields_json, b.version
         FROM public.component_registry r
         JOIN public.component_spec_blueprint b ON b.family_slug = r.family_slug
        WHERE r.family_slug = $1`,
      [familySlug]
    );
    if (!rows[0]) throw new Error('unknown family: ' + familySlug);
    const v = rows[0];
    cache.set(familySlug, { value: v, exp: now + TTL });
    return v;
  })();

  pending.set(familySlug, p);
  try { return await p; } finally { pending.delete(familySlug); }
}

async function listFamilies() {
  const { rows } = await pool.query(
    'SELECT family_slug FROM public.component_registry ORDER BY family_slug'
  );
  return rows.map(r => r.family_slug);
}

module.exports = { getBlueprint, listFamilies };
