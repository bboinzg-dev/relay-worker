// relay-worker/src/utils/blueprint.js
const LRU = new Map();
const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS || 60_000);

function now() { return Date.now(); }

async function fetchBlueprint(pool, family) {
  const { rows } = await pool.query(
    `SELECT b.fields_json, r.specs_table
       FROM public.component_spec_blueprint b
       JOIN public.component_registry r USING (family_slug)
      WHERE b.family_slug=$1`, [family]);
  if (!rows[0]) throw new Error(`Blueprint not found for ${family}`);
  return { fields: rows[0].fields_json, specsTable: rows[0].specs_table };
}

exports.getBlueprint = async function getBlueprint(pool, family) {
  const k = `bp:${family}`;
  const hit = LRU.get(k);
  if (hit && hit.exp > now()) return hit.val;

  const val = await fetchBlueprint(pool, family);
  LRU.set(k, { val, exp: now() + TTL });
  return val;
};
