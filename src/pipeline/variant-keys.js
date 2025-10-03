'use strict';

const db = require('../../db');

const MIN_VARIATION_COUNT = 2;

function normalizeKeyName(value) {
  return String(value || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

function normalizeAlias(value) {
  return String(value || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeSlug(value) {
  const norm = String(value || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  return norm || null;
}

function addAlias(target, alias, key, weight = 0) {
  const normalized = normalizeAlias(alias);
  if (!normalized || normalized.length < 2) return;
  target.push({ alias: normalized, key, weight: weight || normalized.replace(/\s+/g, '').length });
}

function flattenValues(value) {
  if (value == null) return [];
  if (Array.isArray(value)) {
    const out = [];
    for (const item of value) out.push(...flattenValues(item));
    return out;
  }
  if (typeof value === 'object') {
    const candidates = [];
    for (const prop of ['raw', 'text', 'value', 'display', 'label']) {
      if (Object.prototype.hasOwnProperty.call(value, prop)) {
        candidates.push(...flattenValues(value[prop]));
      }
    }
    if (candidates.length) return candidates;
    const str = String(value).trim();
    return str ? [str] : [];
  }
  const str = String(value)
    .normalize('NFKC')
    .replace(/[\u2013\u2014\u2212]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
  return str ? [str] : [];
}

function normalizeCell(value) {
  const list = flattenValues(value);
  return list.length ? list[0] : '';
}

function findKeyForLabel(label, aliasEntries, keySet) {
  const normKey = normalizeKeyName(label);
  if (normKey && keySet.has(normKey)) return normKey;

  if (normKey) {
    for (const key of keySet) {
      if (!key) continue;
      if (normKey === key) return key;
      const idx = normKey.indexOf(key);
      if (idx >= 0) {
        const beforeOk = idx === 0 || normKey[idx - 1] === '_';
        const afterOk = idx + key.length === normKey.length || normKey[idx + key.length] === '_';
        if (beforeOk && afterOk) return key;
      }
    }
  }

  const normAlias = normalizeAlias(label);
  if (!normAlias) return null;

  let best = null;
  for (const entry of aliasEntries) {
    if (!entry || !entry.alias || !entry.key) continue;
    if (normAlias === entry.alias || entry.alias === normAlias) {
      const score = entry.weight || entry.alias.length;
      if (!best || score > best.score) best = { key: entry.key, score };
      continue;
    }
    if (normAlias.includes(entry.alias)) {
      const score = entry.weight || entry.alias.length;
      if (!best || score > best.score) best = { key: entry.key, score };
      continue;
    }
    if (entry.alias.includes(normAlias) && normAlias.length >= 3) {
      const score = entry.weight || entry.alias.length;
      if (!best || score > best.score) best = { key: entry.key, score };
    }
  }
  return best ? best.key : null;
}

function detectKeysFromTables(tables, aliasEntries, keySet) {
  const map = new Map();
  for (const table of tables || []) {
    if (!table || typeof table !== 'object') continue;
    const headers = Array.isArray(table.headers) ? table.headers : [];
    if (!headers.length) continue;
    const rows = Array.isArray(table.rows) ? table.rows : [];
    for (let idx = 0; idx < headers.length; idx += 1) {
      const header = headers[idx];
      if (header == null) continue;
      const key = findKeyForLabel(header, aliasEntries, keySet);
      if (!key) continue;
      let set = map.get(key);
      if (!set) {
        set = new Set();
        map.set(key, set);
      }
      for (const row of rows) {
        if (!Array.isArray(row)) continue;
        const cell = row[idx];
        const norm = normalizeCell(cell);
        if (norm) set.add(norm);
        if (set.size >= MIN_VARIATION_COUNT) break;
      }
    }
  }
  const result = new Set();
  for (const [key, set] of map.entries()) {
    if (set.size >= MIN_VARIATION_COUNT) result.add(key);
  }
  return result;
}

function detectKeysFromRows(rows, aliasEntries, keySet) {
  const map = new Map();
  for (const row of rows || []) {
    if (!row || typeof row !== 'object') continue;
    for (const [rawKey, rawValue] of Object.entries(row)) {
      if (rawKey == null) continue;
      const key = findKeyForLabel(rawKey, aliasEntries, keySet);
      if (!key) continue;
      let set = map.get(key);
      if (!set) {
        set = new Set();
        map.set(key, set);
      }
      for (const val of flattenValues(rawValue)) {
        if (val) set.add(val);
        if (set.size >= MIN_VARIATION_COUNT) break;
      }
    }
  }
  const result = new Set();
  for (const [key, set] of map.entries()) {
    if (set.size >= MIN_VARIATION_COUNT) result.add(key);
  }
  return result;
}

async function loadRecipeRows(family) {
  if (!family) return [];
  try {
    const { rows } = await db.query(
      `SELECT family_slug, brand_slug, series_slug, recipe
         FROM public.extraction_recipe
        WHERE family_slug = $1`,
      [family]
    );
    return rows || [];
  } catch (err) {
    console.warn('[variant] load recipe failed:', err?.message || err);
    return [];
  }
}

function buildAliasEntries({ blueprint, recipes, brand, series }) {
  const entries = [];
  const keySet = new Set();

  const pushKey = (key, weight = 0) => {
    const norm = normalizeKeyName(key);
    if (!norm) return;
    keySet.add(norm);
    addAlias(entries, key, norm, weight || norm.length + 4);
    addAlias(entries, key.replace(/_/g, ' '), norm, weight || norm.length + 2);
  };

  const blueprintVariants = Array.isArray(blueprint?.ingestOptions?.variant_keys)
    ? blueprint.ingestOptions.variant_keys
    : Array.isArray(blueprint?.variant_keys)
      ? blueprint.variant_keys
      : [];
  for (const key of blueprintVariants || []) pushKey(key, 20);

  for (const row of recipes || []) {
    const recipe = row?.recipe && typeof row.recipe === 'string'
      ? (() => { try { return JSON.parse(row.recipe); } catch { return null; } })()
      : row?.recipe;
    if (!recipe || typeof recipe !== 'object') continue;

    const aliases = recipe.key_alias || recipe.keyAliases || recipe.variant_aliases || null;
    if (aliases && typeof aliases === 'object') {
      for (const [key, list] of Object.entries(aliases)) {
        const normKey = normalizeKeyName(key);
        if (!normKey) continue;
        keySet.add(normKey);
        pushKey(normKey, 25);
        const values = Array.isArray(list) ? list : [list];
        for (const alias of values) addAlias(entries, alias, normKey, (alias || '').length + 4);
      }
    }

    const extraKeys = recipe.variant_keys || recipe.variantKeys || null;
    if (Array.isArray(extraKeys)) {
      for (const key of extraKeys) pushKey(key, 15);
    }
  }

  // Series/brand specific hints might be embedded in alias map already. Ensure dedupe by alias/key pair.
  const seen = new Set();
  const deduped = [];
  for (const entry of entries) {
    if (!entry || !entry.alias || !entry.key) continue;
    const token = `${entry.alias}::${entry.key}`;
    if (seen.has(token)) continue;
    seen.add(token);
    deduped.push(entry);
  }

  return { entries: deduped, keySet };
}

function filterRecipes(rows, brand, series) {
  if (!Array.isArray(rows) || !rows.length) return [];
  const brandNorm = normalizeSlug(brand);
  const seriesNorm = normalizeSlug(series);
  return rows.filter((row) => {
    const rowBrand = normalizeSlug(row?.brand_slug ?? row?.brand);
    const rowSeries = normalizeSlug(row?.series_slug ?? row?.series);
    const brandMatch = !rowBrand || !brandNorm ? !rowBrand : rowBrand === brandNorm;
    const seriesMatch = !rowSeries || !seriesNorm ? !rowSeries : rowSeries === seriesNorm;
    return brandMatch && seriesMatch;
  });
}

async function inferVariantKeys({ family, brand, series, blueprint, extracted }) {
  const recipes = filterRecipes(await loadRecipeRows(family), brand, series);
  const { entries, keySet } = buildAliasEntries({ blueprint, recipes, brand, series });

  if (!entries.length && !keySet.size) {
    return { detected: [], newKeys: [] };
  }

  const tables = Array.isArray(extracted?.tables) ? extracted.tables : [];
  const rows = Array.isArray(extracted?.rows) ? extracted.rows : [];

  const tableKeys = detectKeysFromTables(tables, entries, keySet);
  const rowKeys = detectKeysFromRows(rows, entries, keySet);

  const detectedSet = new Set();
  for (const key of tableKeys) detectedSet.add(key);
  for (const key of rowKeys) detectedSet.add(key);

  if (!detectedSet.size) return { detected: [], newKeys: [] };

  const existing = new Set();
  const blueprintKeys = Array.isArray(blueprint?.ingestOptions?.variant_keys)
    ? blueprint.ingestOptions.variant_keys
    : Array.isArray(blueprint?.variant_keys)
      ? blueprint.variant_keys
      : [];
  for (const key of blueprintKeys || []) {
    const norm = normalizeKeyName(key);
    if (norm) existing.add(norm);
  }

  const detected = Array.from(detectedSet).map(normalizeKeyName).filter(Boolean);
  const seen = new Set();
  const deduped = [];
  for (const key of detected) {
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(key);
  }

  const newKeys = deduped.filter((key) => !existing.has(key));
  return { detected: deduped, newKeys };
}

module.exports = { inferVariantKeys, normalizeSlug };