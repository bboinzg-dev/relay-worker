const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const crypto = require('crypto');
const { Storage } = require('@google-cloud/storage');
const db = require('./src/utils/db');
const { embedImageBytes, embedImageGcs, DEFAULT_DIM } = require('./src/vision/vertexImage');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '20mb' }));
const upload = multer({ storage: multer.memoryStorage() });
const storage = new Storage();

function toVectorSQL(arr){
  const safe = Array.from(arr || []).map(v => Number.isFinite(v) ? Number(v) : 0);
  return `'[${safe.join(',')}]'`;
}

function gcsPath(bucket, parts=[]) {
  const p = Array.isArray(parts) ? parts.join('/') : String(parts||'').replace(/^\/+/, '');
  return `gs://${bucket}/${p}`;
}

async function ensureImageIndexTable(){
  await db.query(`CREATE EXTENSION IF NOT EXISTS vector`);
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.image_index (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      family_slug text,
      brand_norm text,
      code_norm text,
      gcs_uri text,
      image_sha256 text UNIQUE,
      embedding vector(${DEFAULT_DIM}),
      meta jsonb,
      created_at timestamptz DEFAULT now()
    );
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS ix_image_index_brand_code ON public.image_index(brand_norm, code_norm)`);
  await db.query(`CREATE INDEX IF NOT EXISTS ix_image_index_vec ON public.image_index USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);`).catch(()=>{});
}

// POST /api/vision/index — index an uploaded image or a gcsUri, optionally link to brand/code
app.post('/api/vision/index', upload.single('file'), async (req, res) => {
  try {
    await ensureImageIndexTable();

    const bucket = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//,'').split('/')[0];
    if (!bucket) return res.status(400).json({ error: 'GCS_BUCKET env not set' });

    const { family_slug=null, brand=null, code=null, gcsUri=null } = req.body || {};
    let bytes = null, gcsOut = null;

    if (gcsUri && gcsUri.startsWith('gs://')) {
      // read bytes from gcs
      const [bkt, ...rest] = gcsUri.replace('gs://','').split('/');
      const file = new (require('@google-cloud/storage').Storage)().bucket(bkt).file(rest.join('/'));
      bytes = (await file.download())[0];
      gcsOut = gcsUri;
    } else if (req.file && req.file.buffer) {
      bytes = req.file.buffer;
      // store to canonical path
      const sha = crypto.createHash('sha256').update(bytes).digest('hex');
      const name = brand && code ? `${family_slug||'unknown'}/${brand.toLowerCase()}/${code.toLowerCase()}/${sha}.bin`
                                 : `unknown/${sha}.bin`;
      await storage.bucket(bucket).file(`images/${name}`).save(bytes, { resumable: false, contentType: req.file.mimetype || 'application/octet-stream' });
      gcsOut = gcsPath(bucket, ['images', name]);
    } else {
      return res.status(400).json({ error: 'file or gcsUri required' });
    }

    const emb = await embedImageBytes(bytes);
    const sha = crypto.createHash('sha256').update(bytes).digest('hex');
    const brand_norm = brand ? String(brand).toLowerCase() : null;
    const code_norm = code ? String(code).toLowerCase() : null;

    const sql = `
      INSERT INTO public.image_index (family_slug, brand_norm, code_norm, gcs_uri, image_sha256, embedding, meta)
      VALUES ($1,$2,$3,$4,$5, ${toVectorSQL(emb)}::vector, $6)
      ON CONFLICT (image_sha256) DO UPDATE SET family_slug=EXCLUDED.family_slug, brand_norm=EXCLUDED.brand_norm, code_norm=EXCLUDED.code_norm, gcs_uri=EXCLUDED.gcs_uri, meta=EXCLUDED.meta
      RETURNING *;
    `;
    const r = await db.query(sql, [family_slug, brand_norm, code_norm, gcsOut, sha, { mime: req.file?.mimetype || null }]);
    res.json({ ok: true, item: r.rows[0], dim: emb.length });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// POST /api/vision/identify — find nearest existing images (and map to parts)
app.post('/api/vision/identify', upload.single('file'), async (req, res) => {
  try {
    await ensureImageIndexTable();
    let emb = null;
    if (req.body?.gcsUri && req.body.gcsUri.startsWith('gs://')) {
      emb = await embedImageGcs(req.body.gcsUri);
    } else if (req.file && req.file.buffer) {
      emb = await embedImageBytes(req.file.buffer);
    } else {
      return res.status(400).json({ error: 'file or gcsUri required' });
    }
    const k = Math.min(Number(req.body?.k || 8), 50);
    const sql = `
      SELECT *, (embedding <=> $1::vector) AS dist
      FROM public.image_index
      WHERE embedding IS NOT NULL
      ORDER BY embedding <=> $1::vector
      LIMIT ${k};
    `;
    const r = await db.query(sql, [emb]);
    // Fetch part details for those with brand/code
    const items = [];
    for (const row of r.rows) {
      let part = null;
      if (row.brand_norm && row.code_norm) {
        try {
          const p = await db.query(`SELECT * FROM public.relay_specs WHERE brand_norm=$1 AND code_norm=$2 LIMIT 1`, [row.brand_norm, row.code_norm]);
          part = p.rows[0] || null;
        } catch {}
      }
      items.push({ ...row, part });
    }
    res.json({ dim: emb.length, items });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
