// src/tasks/embedFamilies.js
'use strict';
const { Client } = require('pg');
const { GoogleGenAI } = require('@google/genai');

/**
 * 환경:
 *   GOOGLE_GENAI_USE_VERTEXAI=true
 *   GOOGLE_CLOUD_PROJECT=partsplan
 *   GOOGLE_CLOUD_LOCATION=global   // 임베딩은 global 권장
 *   (DB 접속은 DATABASE_URL 사용)
 *
 * 모델:
 *   기본 gemini-embedding-001 (권장)
 *   필요 시 TEXT_EMBED_MODEL=text-embedding-005 또는 text-multilingual-embedding-002
 *   출력 차원은 DB 스키마(vector(768))에 맞춰 768로 고정
 */

const MODEL = process.env.TEXT_EMBED_MODEL || 'gemini-embedding-001';
const OUTPUT_DIM = parseInt(process.env.TEXT_EMBED_DIM || '768', 10);

async function run() {
  const db = new Client({ connectionString: process.env.DATABASE_URL });
  await db.connect();

  const { rows } = await db.query(
    `SELECT family_slug, descriptor_text
       FROM public.component_family_descriptor
      WHERE descriptor_text IS NOT NULL
        AND btrim(descriptor_text) <> ''
        AND (embedding IS NULL)`
  );
  if (rows.length === 0) {
    console.log('[embedFamilies] nothing to embed');
    await db.end(); return;
  }

  // GenAI SDK (Vertex 모드) 사용
  // Node에서 embed는 embedContent(); 다건 입력 가능
  // 참고: https://ai.google.dev/gemini-api/docs/embeddings
  const ai = new GoogleGenAI({});

  // 250개/요청 제한 → 안전하게 200개씩 처리
  const BATCH = 200;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const contents = batch.map(r => r.descriptor_text);

    const resp = await ai.models.embedContent({
      model: MODEL,
      contents,
      // taskType은 검색/분류 안정화를 위해 RETRIEVAL_DOCUMENT 권장
      // 차원은 768로 강제(스키마와 일치)
      config: { taskType: 'RETRIEVAL_DOCUMENT', outputDimensionality: OUTPUT_DIM }
    });

    const embs = resp.embeddings; // [{values: number[]}, ...]
    if (!embs || embs.length !== batch.length) {
      throw new Error(`Embedding response size mismatch: got ${embs?.length}, want ${batch.length}`);
    }

    // pgvector 입력은 '[v1,v2,...]'::vector 형태로 캐스팅
    for (let k = 0; k < batch.length; k++) {
      const fam = batch[k].family_slug;
      const vecLit = '[' + embs[k].values.join(',') + ']';
      await db.query(
        `UPDATE public.component_family_descriptor
            SET embedding = $2::vector
          WHERE family_slug = $1`,
        [fam, vecLit]
      );
      console.log(`[embedFamilies] upserted embedding for ${fam}`);
    }
  }

  await db.end();
}

module.exports = { run };

if (require.main === module) {
  run().catch(err => { console.error(err); process.exit(1); });
}
