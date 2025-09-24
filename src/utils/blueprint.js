// src/utils/blueprint.js
'use strict';
const { pool } = require('./db');
const { BLUEPRINT_CACHE_TTL_MS } = require('../config');

const cache = new Map();
const pending = new Map();

function keyOf(family) { return String(family).toLowerCase(); }

async function getBlueprint(family) {
  const key = keyOf(family);
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && hit.exp > now) return hit.val;
  if (pending.has(key)) return pending.get(key);

  const p = (async () => {
    const { rows } = await pool.query(
      `SELECT r.family_slug, r.specs_table, b.fields_json, b.version
         FROM public.component_registry r
         JOIN public.component_spec_blueprint b USING (family_slug)
        WHERE r.family_slug = $1`, [key]
    );
    const row = rows[0];
    if (!row) throw new Error('unknown family: ' + key);
    cache.set(key, { val: row, exp: now + BLUEPRINT_CACHE_TTL_MS });
    return row;
  })();

  pending.set(key, p);
  try { return await p; } finally { pending.delete(key); }
}

async function listFamilies() {
  const { rows } = await pool.query(
    `SELECT family_slug FROM public.component_registry ORDER BY family_slug`
  );
  return rows.map(r => r.family_slug);
}

module.exports = { getBlueprint, listFamilies };
