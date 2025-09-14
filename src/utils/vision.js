const { VertexAI } = require('@google-cloud/vertexai');
const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION = process.env.VERTEX_LOCATION || 'us-central1';
const MODEL_ID = process.env.VERTEX_VISION_EMBED || 'multimodalembedding@001';

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });

async function embedImage(gcsUri) {
  const model = vertex.getEmbeddingModel({ model: MODEL_ID });
  // Note: embedContent supports image via "fileData"
  const resp = await model.embedContent({
    content: {
      parts: [{ fileData: { fileUri: gcsUri, mimeType: 'image/png' } }]
    }
  });
  const v = resp?.embeddings?.[0]?.values || [];
  return v;
}

module.exports = { embedImage };
