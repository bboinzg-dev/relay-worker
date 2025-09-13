const express = require('express');
const { Pool } = require('pg');

const pgPool = global.pgPool || (global.pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
}));

async function fetchRelayDetail(brand, code) {
  const client = await pgPool.connect();
  try {
    const sql = `
      SELECT brand, code, series, display_name, family_slug, contact_form,
             coil_voltage_vdc, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm,
             datasheet_url, cover
      FROM public.relay_specs
      WHERE lower(brand)=lower($1) AND lower(code)=lower($2)
      LIMIT 1
    `;
    const { rows } = await client.query(sql, [brand, code]);
    return rows[0] || null;
  } finally {
    client.release();
  }
}

function withFallbackUrls(row) {
  if (!row) return null;
  const bucket = process.env.GCS_BUCKET;
  const safeBrand = String(row.brand || '').toLowerCase();
  const safeCode  = String(row.code  || '').toLowerCase();
  if (!row.cover && bucket) {
    row.cover = `https://storage.googleapis.com/${bucket}/images/${safeBrand}/${safeCode}/cover.png`;
  }
  if (!row.datasheet_url && bucket) {
    row.datasheet_url = `https://storage.googleapis.com/${bucket}/datasheets/${safeCode}.pdf`;
  }
  return row;
}

module.exports = function registerPartsRoutes(app) {
  const router = express.Router();

  const detailHandler = async (req, res) => {
    const brand = (req.query.brand || '').toString().trim();
    const code  = (req.query.code  || '').toString().trim();
    if (!brand || !code) return res.status(400).json({ error: 'brand and code are required' });

    try {
      const row = await fetchRelayDetail(brand, code);
      if (!row) return res.status(404).json({ erro

cd ~/relay-worker
git fetch origin
git checkout -B main origin/main

# 1) 라우트 파일 생성/갱신
mkdir -p routes
cat > routes/parts.js <<'JS'
const express = require('express');
const { Pool } = require('pg');

const pgPool = global.pgPool || (global.pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
}));

async function fetchRelayDetail(brand, code) {
  const client = await pgPool.connect();
  try {
    const sql = `
      SELECT brand, code, series, display_name, family_slug, contact_form,
             coil_voltage_vdc, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm,
             datasheet_url, cover
      FROM public.relay_specs
      WHERE lower(brand)=lower($1) AND lower(code)=lower($2)
      LIMIT 1
    `;
    const { rows } = await client.query(sql, [brand, code]);
    return rows[0] || null;
  } finally {
    client.release();
  }
}

function withFallbackUrls(row) {
  if (!row) return null;
  const bucket = process.env.GCS_BUCKET;
  const safeBrand = String(row.brand || '').toLowerCase();
  const safeCode  = String(row.code  || '').toLowerCase();
  if (!row.cover && bucket) {
    row.cover = `https://storage.googleapis.com/${bucket}/images/${safeBrand}/${safeCode}/cover.png`;
  }
  if (!row.datasheet_url && bucket) {
    row.datasheet_url = `https://storage.googleapis.com/${bucket}/datasheets/${safeCode}.pdf`;
  }
  return row;
}

module.exports = function registerPartsRoutes(app) {
  const router = express.Router();

  const detailHandler = async (req, res) => {
    const brand = (req.query.brand || '').toString().trim();
    const code  = (req.query.code  || '').toString().trim();
    if (!brand || !code) return res.status(400).json({ error: 'brand and code are required' });

    try {
      const row = await fetchRelayDetail(brand, code);
      if (!row) return res.status(404).json({ error: 'not found' });
      res.set('Cache-Control', 'public, max-age=60');
      return res.json(withFallbackUrls(row));
    } catch (err) {
      console.error('detail error', err);
      return res.status(500).json({ error: 'internal' });
    }
  };

  router.get('/parts/detail', detailHandler);
  router.get('/api/parts/detail', detailHandler);

  app.use(router);
};
