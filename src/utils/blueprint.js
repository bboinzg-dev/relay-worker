// relay-worker/src/utils/blueprint.js
const { pool: defaultPool } = require('./db');

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
    `SELECT b.fields_json, b.required_fields, r.specs_table
       FROM public.component_spec_blueprint b
       JOIN public.component_registry r USING (family_slug)
      WHERE b.family_slug=$1`, [family]);
  if (!rows[0]) throw new Error(`Blueprint not found for ${family}`);
  const fields = rows[0].fields_json || {};
  const specsTable = rows[0].specs_table;
  const requiredFields = Array.isArray(rows[0].required_fields)
    ? rows[0].required_fields.map((k) => String(k || '').trim()).filter(Boolean)
    : [];
  return {
    fields,
    specsTable,
    allowedKeys: normalizeFieldNames(fields),
    requiredFields,
  };
}

function resolvePool(poolLike) {
  if (poolLike && typeof poolLike.query === 'function') return poolLike;
  return defaultPool;
}

exports.getBlueprint = async function getBlueprint(poolOrFamily, maybeFamily) {
  const pool = maybeFamily ? resolvePool(poolOrFamily) : resolvePool(null);
  const family = maybeFamily || poolOrFamily;
  if (!family) throw new Error('family required');

  const k = `bp:${family}`;
  const hit = LRU.get(k);
  if (hit && hit.exp > now()) return hit.val;

  const val = await fetchBlueprint(pool, family);
  LRU.set(k, { val, exp: now() + TTL });
  return val;
};

function pickFastKeys(blueprint, limitOverride) {
  if (!blueprint) return [];
  const allowed = Array.isArray(blueprint.allowedKeys)
    ? blueprint.allowedKeys.map((k) => String(k || '').trim()).filter(Boolean)
    : [];
  if (!allowed.length) return [];

  const envLimit = Number(process.env.FAST_FIELD_LIMIT || 12);
  const limit = Number.isFinite(limitOverride) && limitOverride > 0
    ? limitOverride
    : (envLimit > 0 ? envLimit : allowed.length);

  const required = Array.isArray(blueprint.requiredFields)
    ? blueprint.requiredFields.map((k) => String(k || '').trim()).filter(Boolean)
    : [];

  const out = [];
  for (const key of required) {
    if (!allowed.includes(key)) continue;
    if (out.includes(key)) continue;
    out.push(key);
    if (limit && out.length >= limit) return out;
  }

  if (!limit || limit >= allowed.length) {
    for (const key of allowed) {
      if (!out.includes(key)) out.push(key);
    }
    return out;
  }

  for (const key of allowed) {
    if (out.includes(key)) continue;
    out.push(key);
    if (out.length >= limit) break;
  }
  return out;
}

exports.computeFastKeys = pickFastKeys;
