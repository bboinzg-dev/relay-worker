'use strict';

const express = require('express');
const { Storage } = require('@google-cloud/storage');
const db = require('./db');

const router = express.Router();
const storage = new Storage();

const PROJECT_ID = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || '';
const BUCKET = process.env.GCS_BUCKET;
const EXPORT_PATH = 'retail/catalog/products.ndjson';

function sanitizeKey(key) {
  return String(key).replace(/[^A-Za-z0-9_]/g, '_').slice(0, 128) || 'attr';
}

function toCustomAttributes(raw) {
  const attrs = {};
  if (!raw || typeof raw !== 'object') return attrs;

  for (const [k, v] of Object.entries(raw)) {
    const key = sanitizeKey(k);
    if (v === null || v === undefined || v === '') continue;

    if (Array.isArray(v)) {
      const nums = v.map(Number).filter((n) => Number.isFinite(n));
      if (nums.length === v.length && nums.length > 0) {
        attrs[key] = { numbers: nums.slice(0, 400) };
      } else {
        const texts = v.map((x) => String(x)).filter(Boolean);
        if (texts.length) attrs[key] = { text: texts.slice(0, 400) };
      }
      continue;
    }

    const num = Number(v);
    if (!Number.isNaN(num) && Number.isFinite(num)) {
      attrs[key] = { numbers: [num] };
      continue;
    }

    const str = String(v);
    if (str) attrs[key] = { text: [str.slice(0, 256)] };
  }

  return attrs;
}

router.post('/api/retail/export-catalog', async (req, res, next) => {
  try {
    if (!BUCKET) {
      res.status(500).json({ ok: false, error: 'GCS_BUCKET not configured' });
      return;
    }

    const baseRows = await db.query(
      `SELECT id, family_slug, brand, code, display_name,
              COALESCE(image_uri, cover) AS image_uri,
              COALESCE(datasheet_url, datasheet_uri) AS datasheet_uri,
              updated_at
         FROM public.component_specs`
    );

    const families = await db.query(
      'SELECT family_slug, specs_table FROM public.component_registry'
    );

    const rawMap = new Map();
    for (const f of families.rows) {
      if (!f?.specs_table) continue;
      const tableName = String(f.specs_table).replace(/[^A-Za-z0-9_]/g, '');
      if (!tableName) continue;

      const sql = `SELECT id, raw_json FROM public.${tableName}`;
      try {
        const r = await db.query(sql);
        for (const row of r.rows) {
          const key = `${f.family_slug}:${row.id}`;
          rawMap.set(key, row.raw_json || {});
        }
      } catch (err) {
        console.warn('[retail][export] skip specs table', tableName, err?.message || err);
      }
    }

    const lines = baseRows.rows.map((r) => {
      const brand = r.brand || '';
      const code = r.code || r.id;
      const pid = `${r.family_slug}:${brand.toLowerCase()}:${code}`.slice(0, 128);
      const raw = rawMap.get(`${r.family_slug}:${r.id}`) || {};
      const titleBase = (r.display_name || `${brand} ${code}` || '').trim();
      const title = titleBase || String(code || r.id || pid);

      const product = {
        id: pid,
        title,
        brands: brand ? [brand] : [],
        categories: r.family_slug ? [r.family_slug] : [],
        images: r.image_uri ? [{ uri: r.image_uri }] : [],
        uri: r.datasheet_uri || undefined,
        attributes: toCustomAttributes(raw),
        availability: 'IN_STOCK',
        fulfillmentTypes: ['pickup-in-store'],
        audience: { genders: ['male', 'female'], ageGroups: ['adult'] },
        primaryProductId: pid,
      };

      return JSON.stringify(product);
    });

    const file = storage.bucket(BUCKET).file(EXPORT_PATH);
    await file.save(lines.join('\n'), {
      contentType: 'application/x-ndjson',
      metadata: {
        'x-goog-meta-project-id': PROJECT_ID || 'unknown',
        'x-goog-meta-exported-at': new Date().toISOString(),
        'x-goog-meta-count': String(lines.length),
      },
    });

    res.json({ ok: true, gcs: `gs://${BUCKET}/${EXPORT_PATH}`, count: lines.length });
  } catch (err) {
    next(err);
  }
});

module.exports = router;