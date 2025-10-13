'use strict';

const { pool } = require('../../db');

const CACHE = new Map();
const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS || 60_000);

function now() {
  return Date.now();
}

function normalizeKeyList(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  const out = [];
  for (const raw of list) {
    const key = String(raw || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_]/g, '');
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  return out;
}

function deriveAllowedKeys(fieldsJson = {}, ingestOptions = {}) {
  if (Array.isArray(ingestOptions.allowed_keys)) {
    const explicit = normalizeKeyList(ingestOptions.allowed_keys);
    if (explicit.length) return explicit;
  }
  return normalizeKeyList(Object.keys(fieldsJson || {}));
}

function deriveVariantKeys(blueprintRow = {}) {
  const ingestOptions = blueprintRow.ingest_options || blueprintRow.ingestOptions || {};
  if (Array.isArray(ingestOptions.variant_keys)) {
    return normalizeKeyList(ingestOptions.variant_keys);
  }
  if (Array.isArray(blueprintRow.variant_keys)) {
    return normalizeKeyList(blueprintRow.variant_keys);
  }
  return [];
}

function ensureFamilyOverrides(blueprint, registryRow) {
  if (!blueprint) return blueprint;

  const ingestOptions = blueprint.ingestOptions || {};
  const family = String(blueprint.family_slug || '').trim().toLowerCase();
  const forcedVariantKeys = new Set(blueprint.variant_keys || []);

  const addVariantKeys = (keys = []) => {
    for (const key of keys) {
      const norm = normalizeKeyList([key])[0];
      if (!norm) continue;
      forcedVariantKeys.add(norm);
    }
  };

  if (family === 'relay_power') {
    addVariantKeys(['coil_voltage_vdc', 'contact_form', 'suffix']);
  }

  if (family === 'relay_signal') {
    // 템플릿은 강제하지 않고 LLM/표에서 추론한 pn_template 사용
  }

  const variantKeys = Array.from(forcedVariantKeys);
  blueprint.variant_keys = variantKeys;
  ingestOptions.variant_keys = variantKeys;
  blueprint.ingestOptions = ingestOptions;

  const allowed = Array.isArray(blueprint.allowedKeys) ? blueprint.allowedKeys : [];
  const generic = [
    'pn_jp',
    'pn_aliases',
    'ordering_market',
    'coil_voltage_vdc',
    'coil_voltage_vac',
    'contact_form',
    'contact_arrangement',
    'terminal_form',
    'terminal_shape',
    'contact_rating_text',
    'dielectric_strength_v',
    'operate_time_ms',
    'release_time_ms',
    'coil_resistance_ohm',
    'operating_function',
    'packing_style',
    'insulation_resistance_mohm',
    'dim_l_mm',
    'dim_w_mm',
    'dim_h_mm',
  ];
  blueprint.allowedKeys = Array.from(new Set([...allowed, ...variantKeys, ...generic]));

  if (registryRow?.specs_table && !blueprint.specsTable) {
    blueprint.specsTable = registryRow.specs_table;
  }

  return blueprint;
}

function normalizeBlueprint(row, registryRow) {
  if (!row) return null;
  const fields = row.fields_json || row.fields || {};
  const ingestOptionsRaw = row.ingest_options || row.ingestOptions || {};
  const ingestOptions = ingestOptionsRaw && typeof ingestOptionsRaw === 'object'
    ? { ...ingestOptionsRaw }
    : {};
  const codeRules = row.code_rules || row.codeRules || null;
  const allowedKeys = deriveAllowedKeys(fields, ingestOptionsRaw);
  let variantKeys = deriveVariantKeys(row);
  const specsTable = registryRow?.specs_table || registryRow?.specsTable || `${row.family_slug}_specs`;

  if (Array.isArray(ingestOptions.variant_keys)) {
    ingestOptions.variant_keys = normalizeKeyList(ingestOptions.variant_keys);
  }

  variantKeys = Array.isArray(variantKeys) ? variantKeys : [];

  const allowedKeysFinal = Array.from(new Set([...(allowedKeys || []), ...variantKeys]));

  const blueprint = {
    family_slug: row.family_slug,
    fields,
    ingestOptions,
    code_rules: codeRules,
    allowedKeys: allowedKeysFinal,
    variant_keys: variantKeys,
    specsTable,
  };

  return ensureFamilyOverrides(blueprint, registryRow);
}

function computeFastKeys(blueprint) {
  const ingestOptions = blueprint?.ingestOptions || {};
  if (Array.isArray(ingestOptions.fast_keys)) {
    const list = normalizeKeyList(ingestOptions.fast_keys);
    if (list.length) return list;
  }

  const required = [];
  if (blueprint?.fields && typeof blueprint.fields === 'object') {
    for (const [key, meta] of Object.entries(blueprint.fields)) {
      const isRequired = meta && typeof meta === 'object' && Boolean(meta.required);
      if (isRequired) {
        const norm = String(key || '')
          .trim()
          .toLowerCase()
          .replace(/[^a-z0-9_]/g, '');
        if (norm && !required.includes(norm)) required.push(norm);
      }
    }
  }
  if (required.length) return required;
  return Array.isArray(blueprint?.allowedKeys) ? blueprint.allowedKeys : [];
}

async function getBlueprint(poolOrFamily, maybeFamily) {
  let familySlug = null;
  let client = pool;

  if (typeof poolOrFamily === 'string' && maybeFamily == null) {
    familySlug = poolOrFamily;
  } else {
    client = poolOrFamily || pool;
    familySlug = maybeFamily;
  }

  if (!familySlug) throw new Error('familySlug required');

  const cacheKey = `bp:${familySlug}`;
  const hit = CACHE.get(cacheKey);
  if (hit && now() - hit.t < TTL) return hit.v;

  const [bpRes, regRes] = await Promise.all([
    client.query(
      `SELECT family_slug, fields_json, ingest_options, code_rules
         FROM public.component_spec_blueprint
        WHERE family_slug = $1
        LIMIT 1`,
      [familySlug]
    ),
    client.query(
      `SELECT specs_table
         FROM public.component_registry
        WHERE family_slug = $1
        LIMIT 1`,
      [familySlug]
    ),
  ]);

  const blueprint = normalizeBlueprint(bpRes.rows[0], regRes.rows[0]);
  CACHE.set(cacheKey, { t: now(), v: blueprint });
  return blueprint;
}

module.exports = { getBlueprint, computeFastKeys };
