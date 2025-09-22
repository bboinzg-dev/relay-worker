// src/utils/blueprint.js
'use strict';
const db = require('./../utils/db');

async function getBlueprint(family) {
  const q = `
    SELECT fields_json, prompt_template
      FROM public.component_spec_blueprint
     WHERE family_slug = $1
     LIMIT 1`;
  const r = await db.query(q, [family]);
  const fields = r.rows[0]?.fields_json || {};
  const allowedKeys = Object.keys(fields);
  return { fields, allowedKeys, prompt: r.rows[0]?.prompt_template || null };
}

module.exports = { getBlueprint };
