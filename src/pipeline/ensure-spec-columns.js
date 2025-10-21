'use strict';

const path = require('node:path');
const tryRequire = require('../utils/try-require');

let normalizeSpecKey = (value) => {
  if (value == null) return '';
  let str = String(value || '').trim();
  if (!str) return '';
  let prefix = '';
  const leading = str.match(/^_+/);
  if (leading) {
    prefix = leading[0];
    str = str.slice(prefix.length);
  }
  if (!str) return prefix.toLowerCase();
  const camelConverted = str.replace(/([a-z0-9])([A-Z])/g, '$1_$2');
  const sanitized = camelConverted.replace(/[^a-zA-Z0-9_]/g, '_');
  const collapsed = sanitized.replace(/__+/g, '_').replace(/^_+|_+$/g, '');
  const final = collapsed ? `${prefix}${collapsed}` : prefix;
  return final.toLowerCase();
};
try {
  ({ normalizeSpecKey } = require('../utils/key-normalize'));
} catch (e) {
  // optional
}

const db = tryRequire([
  path.join(__dirname, '../../db'),
  path.join(__dirname, '../db'),
  path.join(__dirname, './db'),
  path.join(process.cwd(), 'db'),
]);

const TYPE_MAP = {
  int: 'integer',
  integer: 'integer',
  numeric: 'numeric',
  number: 'numeric',
  float: 'double precision',
  double: 'double precision',
  'double precision': 'double precision',
  decimal: 'numeric',
  bool: 'boolean',
  boolean: 'boolean',
  text: 'text',
  string: 'text',
};

function normKey(k) {
  return String(k || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '');
}

async function getColumnsOf(qualified) {
  const [schema, table] = qualified.includes('.')
    ? qualified.split('.', 2)
    : ['public', qualified];
  const { rows } = await db.query(
    `SELECT lower(column_name) AS col
       FROM information_schema.columns
      WHERE table_schema = $1
        AND table_name   = $2`,
    [schema, table]
  );
  return new Set(rows.map((r) => r.col));
}

function toNormalizedKeyList(list) {
  if (!Array.isArray(list)) return [];
  return list
    .map((value) => normalizeSpecKey(value) || normKey(value))
    .filter(Boolean)
    .map((value) => String(value).toLowerCase());
}

async function ensureSpecColumnsForFamily(dbOrFamily, maybeFamilySlug, fieldsInput = {}, ingestInput = {}) {
  let client = db;
  let familySlug = null;
  let fieldSource = fieldsInput;
  let ingestSource = ingestInput;

  if (dbOrFamily && typeof dbOrFamily.query === 'function') {
    client = dbOrFamily;
    familySlug = maybeFamilySlug;
  } else {
    familySlug = dbOrFamily;
    fieldSource = maybeFamilySlug || {};
    ingestSource = fieldsInput || {};
  }

  if (!familySlug) throw new Error('familySlug required');

  const allowedKeys = Object.keys(fieldSource || {})
    .map((key) => String(key || '').trim())
    .filter(Boolean);

  const fieldKeys = Array.from(
    new Set(
      allowedKeys
        .map((key) => normalizeSpecKey(key) || normKey(key))
        .filter(Boolean)
        .map((key) => String(key).toLowerCase()),
    ),
  );

  const variantSource =
    (ingestSource && ingestSource.variant_keys) ||
    (ingestSource && ingestSource.variantKeys) ||
    [];
  const variantKeys = Array.from(
    new Set(
      toNormalizedKeyList(Array.isArray(variantSource) ? variantSource : [])
        .map((key) => String(key).toLowerCase()),
    ),
  );

  // 1) 블루프린트 정의(fields_json) 기반으로 기본 필드 컬럼 보장 (1-인자)
  await client.query(`SELECT public.ensure_blueprint_fields_columns($1)`, [familySlug]);

  // 2) 런타임 필드 키가 있으면 dynamic 보강 (2-인자: jsonb 배열)
  const fieldKeysJson = JSON.stringify(Array.isArray(fieldKeys) ? fieldKeys : []);
  await client.query(
    `SELECT public.ensure_dynamic_spec_columns($1, $2::jsonb)`,
    [familySlug, fieldKeysJson],
  );

  const variantKeysJson = JSON.stringify(Array.isArray(variantKeys) ? variantKeys : []);
  await client.query(
    `SELECT public.ensure_blueprint_variant_columns($1, $2::jsonb)`,
    [familySlug, variantKeysJson],
  );

  return { fieldKeys, variantKeys, allowedKeys };
}

async function ensureSpecColumnsForBlueprint(qualifiedTable, blueprint) {
  const familySlug = blueprint?.family_slug || blueprint?.familySlug || null;
  const fieldsJson =
    (blueprint?.fields && typeof blueprint.fields === 'object' && blueprint.fields) ||
    blueprint?.fields_json ||
    {};
  const ingestOptions =
    (blueprint?.ingestOptions && typeof blueprint.ingestOptions === 'object' && blueprint.ingestOptions) ||
    (blueprint?.ingest_options && typeof blueprint.ingest_options === 'object' && blueprint.ingest_options) ||
    {};

  if (familySlug) {
    await ensureSpecColumnsForFamily(familySlug, fieldsJson, ingestOptions);
  }

  // 호환성: 호출부가 기존 반환값을 기대할 수 있으므로 구조 유지
  return { added: 0, columns: [] };
}

async function ensureSpecColumnsForKeys(qualifiedTable, keys = [], sample = {}) {
  const have = await getColumnsOf(qualifiedTable);
  const haveNormalized = new Set(
    [...have]
      .map((col) => normalizeSpecKey(col) || normKey(col))
      .filter((col) => col)
      .map((col) => String(col).toLowerCase()),
  );
  const toAdd = [];

  const list = Array.isArray(keys) ? keys : [];
  for (const rawKey of list) {
    const normalized = normalizeSpecKey(rawKey) || normKey(rawKey);
    if (!normalized) continue;
    const lower = String(normalized).toLowerCase();
    if (haveNormalized.has(lower)) continue;

    let value = undefined;
    if (sample && typeof sample === 'object') {
      if (Object.prototype.hasOwnProperty.call(sample, normalized)) {
        value = sample[normalized];
      } else if (Object.prototype.hasOwnProperty.call(sample, rawKey)) {
        value = sample[rawKey];
      } else {
        const matchKey = Object.keys(sample).find(
          (candidate) => (normalizeSpecKey(candidate) || normKey(candidate)) === normalized,
        );
        if (matchKey) value = sample[matchKey];
      }
    }

    let pgType = 'text';
    if (typeof value === 'number' && Number.isFinite(value)) {
      pgType = 'double precision';
    } else if (typeof value === 'boolean') {
      pgType = 'boolean';
    } else if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed && /-?\d+(?:\.\d+)?$/.test(trimmed)) {
        pgType = 'double precision';
      }
    }

    haveNormalized.add(lower);
    toAdd.push({ key: normalized, pgType });
  }

  for (const { key, pgType } of toAdd) {
    await db.query(`ALTER TABLE ${qualifiedTable} ADD COLUMN IF NOT EXISTS "${key}" ${pgType}`);
  }

  return { added: toAdd.length, columns: toAdd };
}

module.exports = {
  ensureSpecColumnsForFamily,
  ensureSpecColumnsForBlueprint,
  ensureSpecColumnsForKeys,
  getColumnsOf,
};
