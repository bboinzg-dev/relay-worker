// relay-worker/src/utils/blueprint.js
const LRU = new Map();
const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS || 60_000);

function now() { return Date.now(); }

function normalizeFieldNames(fieldsJson) {
  if (!fieldsJson) return [];
  if (Array.isArray(fieldsJson)) {
    const names = [];
    for (const entry of fieldsJson) {
      if (!entry) continue;
      if (typeof entry === 'string') {
        const name = entry.trim();
        if (name) names.push(name);
        continue;
      }
      if (typeof entry === 'object' && entry.name) {
        const name = String(entry.name).trim();
        if (name) names.push(name);
      }
    }
    return names;
  }
  if (typeof fieldsJson === 'object') {
    return Object.keys(fieldsJson);
  }
  return [];
}

async function fetchBlueprint(pool, family) {
  const { rows } = await pool.query(
    `SELECT b.fields_json, r.specs_table
       FROM public.component_spec_blueprint b
       JOIN public.component_registry r USING (family_slug)
      WHERE b.family_slug=$1`, [family]);
  if (!rows[0]) throw new Error(`Blueprint not found for ${family}`);
  const fields = rows[0].fields_json || {};
  const specsTable = rows[0].specs_table;
  return {
    fields,
    specsTable,
    allowedKeys: normalizeFieldNames(fields),
  };
}

exports.getBlueprint = async function getBlueprint(pool, family) {
  const k = `bp:${family}`;
  const hit = LRU.get(k);
  if (hit && hit.exp > now()) return hit.val;

  const val = await fetchBlueprint(pool, family);
  LRU.set(k, { val, exp: now() + TTL });
  return val;
};
