const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { v4: uuidv4 } = require('uuid');
const db = require('./src/utils/db');
const { embedImage } = require('./src/utils/vision');
const { toVectorLiteral } = require('./src/utils/pgvector');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));

// Vision index ensure
async function ensureVisionTable() {
  await db.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";');
  try { await db.query('CREATE EXTENSION IF NOT EXISTS vector;'); } catch {}
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.vision_index (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      family_slug text,
      brand text,
      code text,
      brand_norm text GENERATED ALWAYS AS (lower(brand)) STORED,
      code_norm text GENERATED ALWAYS AS (lower(code)) STORED,
      gcs_uri text UNIQUE,
      ocr_text text,
      embedding vector(1408),
      created_at timestamptz NOT NULL DEFAULT now()
    );
  `);
  // ivfflat index (best-effort)
  try {
    await db.query(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='ix_vision_index_embedding_ivf'
        ) THEN
          CREATE INDEX ix_vision_index_embedding_ivf ON public.vision_index
          USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
        END IF;
      END $$;
    `);
  } catch {}
}

// POST /api/vision/index  { gcsUri, family_slug?, brand?, code?, ocr_text? }
app.post('/api/vision/index', async (req, res) => {
  try {
    const { gcsUri, family_slug=null, brand=null, code=null, ocr_text=null } = req.body || {};
    if (!gcsUri) return res.status(400).json({ error: 'gcsUri required' });
    await ensureVisionTable();
    const vec = await embedImage(gcsUri);
    const vecLit = toVectorLiteral(vec);
    const id = uuidv4();
    const row = await db.query(
      `INSERT INTO public.vision_index (id, family_slug, brand, code, gcs_uri, ocr_text, embedding)
       VALUES ($1,$2,$3,$4,$5,$6,$7::vector)
       ON CONFLICT (gcs_uri) DO UPDATE
         SET family_slug=EXCLUDED.family_slug,
             brand=EXCLUDED.brand,
             code=EXCLUDED.code,
             ocr_text=EXCLUDED.ocr_text,
             embedding=EXCLUDED.embedding
       RETURNING *;`,
      [id, family_slug, brand, code, gcsUri, ocr_text, vecLit]
    );
    res.json(row.rows[0]);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// POST /api/vision/identify { gcsUri, k? }
app.post('/api/vision/identify', async (req, res) => {
  try {
    const { gcsUri, k=5 } = req.body || {};
    if (!gcsUri) return res.status(400).json({ error: 'gcsUri required' });
    await ensureVisionTable();
    const vec = await embedImage(gcsUri);
    const vecLit = toVectorLiteral(vec);
    const r = await db.query(
      `SELECT id, family_slug, brand, code, gcs_uri, ocr_text,
              (embedding <=> $1::vector) AS dist
       FROM public.vision_index
       ORDER BY embedding <=> $1::vector
       LIMIT $2`,
      [vecLit, Math.min(Number(k)||5, 50)]
    );
    res.json({ items: r.rows });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app; // for require-merge
