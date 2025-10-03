const db = require('../../db');
const { embedText } = require('../utils/embed');
const { specTextSignature } = require('../utils/textsig');

function toVectorLiteral(vec) {
  if (!Array.isArray(vec)) return '[]';
  const safe = vec.map(v => Number.isFinite(v) ? Number(v) : 0);
  return '[' + safe.map(v => v.toFixed(6)).join(',') + ']';
}

async function ensureVectorExt() {
  await db.query(`CREATE EXTENSION IF NOT EXISTS vector;`);
}

async function updateRowEmbedding(specsTable, row) {
  await ensureVectorExt();
  const sig = specTextSignature(row);
  if (!sig) return false;
  const vec = await embedText(sig);
  const vecLit = toVectorLiteral(vec);
  await db.query(`UPDATE public.${specsTable} SET embedding = $1::vector WHERE brand_norm=$2 AND code_norm=$3`, [vecLit, row.brand_norm, row.code_norm]);
  // best-effort index
  try {
    await db.query(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='ix_${specsTable}_embedding_ivf'
        ) THEN
          EXECUTE 'CREATE INDEX ix_${specsTable}_embedding_ivf ON public.${specsTable} USING ivfflat (embedding vector_cosine_ops) WITH (lists = 200)';
        END IF;
      END $$;
    `);
  } catch {}
  return true;
}

module.exports = { updateRowEmbedding };
