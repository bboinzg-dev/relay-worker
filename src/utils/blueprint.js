'use strict';
const { pool } = require('./db');

const CACHE = new Map();
const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS || 60_000);

function now(){ return Date.now(); }

async function getBlueprint(familySlug) {
  const k = `bp:${familySlug}`;
  const hit = CACHE.get(k);
  if (hit && (now() - hit.t) < TTL) return hit.v;

  const { rows } = await pool.query(
    `SELECT family_slug, fields_json, ingest_options, code_rules
       FROM component_spec_blueprint
      WHERE family_slug = $1`,
    [familySlug]
  );
  const v = rows[0] || null;
  CACHE.set(k, { t: now(), v });
  return v;
}

module.exports = { getBlueprint };
