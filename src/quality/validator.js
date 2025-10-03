const db = require('../../db');
const { normalizeByType } = require('../utils/normalize');

function normalizeBlueprint(bp){
  // bp.fields_json: [{name, type, required?, enum?, min?, max?, pattern?}, ...] OR {name:{type,...}}
  const out = [];
  if (!bp) return out;
  if (Array.isArray(bp.fields_json)) return bp.fields_json;
  if (bp.fields_json && typeof bp.fields_json === 'object'){
    for (const [name, def] of Object.entries(bp.fields_json)) {
      out.push({ name, ...(def||{}) });
    }
  }
  return out;
}

function coerceRow(fields, row){
  const out = {};
  for (const f of fields) {
    const key = f.name;
    const type = f.type || 'text';
    out[key] = normalizeByType(type, row[key]);
  }
  return out;
}

function* validateRow(fields, row){
  const issues = [];
  for (const f of fields) {
    const key = f.name;
    const type = f.type || 'text';
    const v = row[key];
    if (f.required && (v==null || (typeof v==='string' && v.trim()===''))) {
      yield { code: 'required.missing', severity: 'error', field: key, message: `required field missing` };
    }
    if (v != null) {
      if (type === 'number' || type === 'numeric' || type === 'float' || type === 'integer' || type === 'int') {
        if (typeof v !== 'number' || Number.isNaN(v)) {
          yield { code: 'type.numeric', severity: 'error', field: key, message: `not a number`, observed: row[key] };
        }
        if (typeof f.min === 'number' && v < f.min) {
          yield { code: 'range.min', severity: 'warn', field: key, message: `below min ${f.min}`, observed: v };
        }
        if (typeof f.max === 'number' && v > f.max) {
          yield { code: 'range.max', severity: 'warn', field: key, message: `above max ${f.max}`, observed: v };
        }
      }
      if (type === 'text' && f.pattern) {
        try {
          const re = new RegExp(f.pattern);
          if (!re.test(String(v))) yield { code: 'pattern.mismatch', severity: 'warn', field: key, message: `pattern mismatch`, observed: v, expected: f.pattern };
        } catch {}
      }
      if (f.enum && Array.isArray(f.enum)) {
        if (!f.enum.map(x=>String(x).toLowerCase()).includes(String(v).toLowerCase())) {
          yield { code: 'enum.invalid', severity: 'warn', field: key, message: `not in enum`, observed: v, expected: f.enum };
        }
      }
    }
  }
}

async function runValidation(specsTable, blueprint, limit=500){
  const fields = normalizeBlueprint(blueprint);
  // Pull rows
  const r = await db.query(`SELECT * FROM public/${specsTable} ORDER BY updated_at DESC NULLS LAST LIMIT $1`, [limit]);
  const issues = [];
  for (const row of r.rows) {
    const brand = row.brand_norm || (row.brand||'').toLowerCase();
    const code = row.code_norm || (row.code||'').toLowerCase();
    const coerced = coerceRow(fields, row);
    for (const it of validateRow(fields, coerced)) {
      issues.push({
        family_slug: blueprint.family_slug || null,
        specs_table: specsTable,
        brand_norm: brand,
        code_norm: code,
        field_name: it.field,
        issue_code: it.code,
        severity: it.severity,
        message: it.message,
        observed_value: row[it.field],
        expected: it.expected || null,
        row_pk: row.id || null,
        meta: { raw: row[it.field] }
      });
    }
  }
  return issues;
}

module.exports = { runValidation, normalizeBlueprint, validateRow, coerceRow };
