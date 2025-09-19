// relay-worker/src/utils/extract.js
// CommonJS 모듈. Doc AI → 부족하면 Vertex로 PDF 직접 읽기(하이브리드).

const { Storage } = require('@google-cloud/storage');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { VertexAI } = require('@google-cloud/vertexai');

const storage = new Storage();

function parseGsUri(gsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gsUri || ''));
  if (!m) throw new Error(`invalid gcs uri: ${gsUri}`);
  return { bucket: m[1], name: m[2] };
}

async function downloadGcs(gsUri) {
  const { bucket, name } = parseGsUri(gsUri);
  const [buf] = await storage.bucket(bucket).file(name).download();
  return buf;
}

// -------- Document AI (OCR) ----------
async function docaiTextFromBuffer(buf) {
  const project = process.env.DOCAI_PROJECT_ID || process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location = process.env.DOCAI_LOCATION || process.env.DOC_AI_LOCATION || 'us';
  const processorId = process.env.DOCAI_PROCESSOR_ID;
  if (!project || !processorId) return { text: '', pages: [] };

  const client = new DocumentProcessorServiceClient();
  const name = client.processorPath(project, location, processorId);
  const [res] = await client.processDocument({
    name,
    rawDocument: { content: buf, mimeType: 'application/pdf' },
  });

  const doc = res.document;
  const text = String(doc?.text || '');
  const pages = (doc?.pages || []).map((p, i) => ({
    index: i,
    text: (p.layout?.textAnchor?.content) ? String(p.layout.textAnchor.content) : '',
  }));
  return { text, pages };
}

// -------- Vertex(Gemini) PDF → text ----------
async function vertexTextFromBuffer(buf) {
  const project = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location = process.env.VERTEX_LOCATION || 'us-central1';
  if (!project) return '';

  const vertex = new VertexAI({ project, location });
  // 가벼운 모델 권장: flash 계열
  const model = vertex.getGenerativeModel({ model: 'gemini-1.5-flash' });

  const prompt = [
    {
      role: 'user',
      parts: [
        { text: '이 PDF의 전체 본문 텍스트를 페이지 순서대로 뽑아 주세요. 표는 탭으로 구분해서 텍스트로 변환해 주세요.' },
        { inlineData: { mimeType: 'application/pdf', data: buf.toString('base64') } },
      ],
    },
  ];

  const r = await model.generateContent({ contents: prompt });
  const resp = r?.response;
  const out =
    (resp?.candidates?.[0]?.content?.parts || [])
      .map((p) => (p.text ? String(p.text) : ''))
      .join('');
  return out || '';
}

/**
 * PDF 텍스트 보장:
 *  1) DocAI로 시도 → 2) 텍스트가 너무 짧거나 실패하면 Vertex로 보강
 */
async function extractText(gsUri) {
  const buf = await downloadGcs(gsUri);

  let text = '', pages = [];
  try {
    const d = await docaiTextFromBuffer(buf);
    text = String(d.text || '');
    pages = Array.isArray(d.pages) ? d.pages : [];
  } catch (e) {
    // DocAI 실패는 폴백으로 진행
    console.warn('[extractText] DocAI error:', e?.message || e);
  }

  if (!text || text.length < 500) {
    try {
      const t2 = await vertexTextFromBuffer(buf);
      if (t2 && t2.length > (text?.length || 0)) {
        text = t2;
        pages = []; // Vertex는 페이지 분할이 없으니 비워둠
      }
    } catch (e) {
      console.warn('[extractText] Vertex fallback error:', e?.message || e);
    }
  }

  return { text, pages };
}

module.exports = { extractText };
