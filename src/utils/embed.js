const { VertexAI } = require('@google-cloud/vertexai');
const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION = process.env.VERTEX_LOCATION || 'us-central1';
const EMBED_TEXT = process.env.VERTEX_EMBED_TEXT || 'text-embedding-004';

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });

async function embedText(text) {
  const model = vertex.getEmbeddingModel({ model: EMBED_TEXT });
  const resp = await model.embedContent({ content: { parts: [{ text }] } });
  return resp?.embeddings?.[0]?.values || [];
}

module.exports = { embedText };
