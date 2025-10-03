const { getPool } = require('../db');
const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS || 60000);
const cache = new Map();

async function getFamilies() {
  const key = 'families';
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && hit.exp > now) return hit.value;
  const { rows } = await getPool().query(
    `SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`
  );
  cache.set(key, { value: rows, exp: now + TTL });
  return rows;
}

async function getBlueprint(family) {
  const key = `bp:${family}`;
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && hit.exp > now) return hit.value;
  const { rows } = await getPool().query(
    `SELECT fields_json FROM public.component_spec_blueprint WHERE family_slug=$1`,
    [family]
  );
  if (!rows[0]) throw new Error(`No blueprint for ${family}`);
  const bp = rows[0].fields_json;
  cache.set(key, { value: bp, exp: now + TTL });
  return bp;
}

module.exports = { getFamilies, getBlueprint };