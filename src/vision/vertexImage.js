const crypto = require('crypto');
const { Storage } = require('@google-cloud/storage');
let VertexAI = null, MultiModalEmbeddingModel = null;
try {
  ({ VertexAI } = require('@google-cloud/vertexai'));
} catch {}
const storage = new Storage();
const DEFAULT_DIM = Number(process.env.VISION_EMB_DIM || 1408);

function seededEmbedding(buffer, dim=DEFAULT_DIM){
  const hash = crypto.createHash('sha256').update(buffer).digest();
  // Xorshift32 seed from first 4 bytes
  let x = hash.readUInt32LE(0) || 123456789;
  function rnd(){
    // Xorshift
    x ^= x << 13; x ^= x >>> 17; x ^= x << 5; x >>>= 0;
    return (x % 100000) / 100000; // 0..1
  }
  const out = new Array(dim);
  for (let i=0;i<dim;i++){
    out[i] = (rnd() - 0.5) * 0.2; // centered small values
  }
  return out;
}

async function readBytesFromGcs(gcsUri){
  if (!gcsUri || !gcsUri.startsWith('gs://')) throw new Error('gcsUri required (gs://...)');
  const [bucketName, ...rest] = gcsUri.replace('gs://','').split('/');
  const filePath = rest.join('/');
  const file = storage.bucket(bucketName).file(filePath);
  const [bytes] = await file.download();
  return bytes;
}

async function embedImageBytes(bytes){
  // Try Vertex MultiModal embedding first
  if (VertexAI && !process.env.VERTEX_EMBEDDING_MOCK) {
    try {
      const project = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
      const location = process.env.VERTEX_LOCATION || 'us-central1';
      const vertexAI = new VertexAI({ project, location });
      // Late import for types
      const model = vertexAI.getGenerativeModel ? vertexAI.getGenerativeModel({model: 'multimodalembedding'}) : null;
      if (model && model.embedContent) {
        const res = await model.embedContent({
          content: { role: 'user', parts: [{ inlineData: { mimeType: 'image/png', data: Buffer.from(bytes).toString('base64') } }] }
        });
        // New SDK returns res.embedding.values
        const values = res?.embedding?.values || res?.data?.[0]?.embeddings?.imageEmbedding || null;
        if (values && values.length) return values.map(Number);
      } else if (vertexAI.MultiModalEmbeddingModel) {
        // Older surface
        const m = new vertexAI.MultiModalEmbeddingModel({model: 'multimodalembedding'});
        const r = await m.embed({ image: { bytesBase64Encoded: Buffer.from(bytes).toString('base64') } });
        const values = r?.imageEmbedding?.values || r?.data?.[0]?.embeddings?.imageEmbedding;
        if (values && values.length) return values.map(Number);
      }
    } catch (e) {
      console.error('[vision] Vertex embed failed, fallback to mock:', e.message || e);
    }
  }
  return seededEmbedding(bytes);
}

async function embedImageGcs(gcsUri){
  const bytes = await readBytesFromGcs(gcsUri);
  return await embedImageBytes(bytes);
}

module.exports = { embedImageBytes, embedImageGcs, DEFAULT_DIM };
