'use strict';

const express = require('express');
const cors = require('cors');
const multer = require('multer');
const bodyParser = require('body-parser');
const { VertexAI } = require('@google-cloud/vertexai');
const db = require('./src/utils/db');
const { safeJsonParse } = require('./src/utils/safe-json');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '15mb' }));

const upload = multer({ storage: multer.memoryStorage() });

function getModel() {
  const project =
    process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  if (!project) throw new Error('GCP project id not set');
  const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
  const modelId = process.env.GEMINI_MODEL_CLASSIFY || 'gemini-2.5-flash';

  const v = new VertexAI({ project, location });
  return v.getGenerativeModel({ model: modelId });
}

/**
 * POST /api/vision/guess (multipart/form-data: file)
 * - 이미지(부품 사진)로 제조사/품명/부품군 추정
 * - DB에 family_label 보강 + hero 이미지 찾아 부가정보 리턴
 */
app.post('/api/vision/guess', upload.single('file'), async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ ok: false, error: 'file required' });
    }

    const model = getModel();
    const base64 = req.file.buffer.toString('base64');

    const prompt = `
전자부품 이미지를 보고 부품군과 제조사/품명을 추정하세요.
반드시 JSON만 출력하세요. 키는 다음과 같습니다:
- family_slug: 부품군 슬러그(예: relay_power, relay_signal, mosfet, igbt_module, capacitor_mlcc ...) 없으면 'other'
- family_label_ko: 한국어 라벨(예: "전력 릴레이", "신호 릴레이", "MOSFET")
- brand: 제조사명(예: Panasonic, OMRON)
- code: 품명/모델명(예: TQ2-L2-12V, ALDP112)
- confidence: 0~1 사이 숫자(신뢰도)
- rationale: 간단한 이유 한 줄
JSON 외 텍스트 절대 금지.`;

    const resp = await model.generateContent({
      contents: [
        {
          role: 'user',
          parts: [
            { text: prompt },
            {
              inlineData: {
                mimeType: req.file.mimetype || 'image/png',
                data: base64,
              },
            },
          ],
        },
      ],
      generationConfig: {
        responseMimeType: 'application/json',
        responseSchema: {
          type: 'object',
          properties: {
            family_slug: { type: 'string' },
            family_label_ko: { type: 'string' },
            brand: { type: 'string' },
            code: { type: 'string' },
            confidence: { type: 'number' },
            rationale: { type: 'string' },
          },
        },
      },
    });

    // 안전 파싱
    const parts = resp?.response?.candidates?.[0]?.content?.parts || [];
    const raw = parts.map((p) => p.text || '').join('\n');
    const pickJson = (s) => (s.match(/\{[\s\S]*\}/) || [])[0] || '{}';

    let guess = {};
    try {
      guess = safeJsonParse(raw) || {};
    } catch {
      try {
        guess = safeJsonParse(pickJson(raw)) || {};
      } catch {
        guess = {};
      }
    }

    // family label 보강
    if (guess.family_slug && !guess.family_label_ko) {
      const q = await db.query(
        `select display_name
           from public.component_registry
          where family_slug = $1
          limit 1`,
        [guess.family_slug],
      );
      if (q.rows[0]?.display_name) guess.family_label_ko = q.rows[0].display_name;
    }
    if (
      !guess.family_label_ko &&
      (/relay/i.test(guess.code || '') || /relay/i.test(guess.rationale || ''))
    ) {
      guess.family_label_ko = '릴레이';
    }

    // 브랜드/코드 폴백 추출
    if (!guess.brand || !guess.code) {
      const all = [raw, guess.raw].filter(Boolean).join(' ');
      const brand = /panasonic/i.test(all)
        ? 'Panasonic'
        : /omron/i.test(all)
        ? 'Omron'
        : /tyco|te\s*connectivity/i.test(all)
        ? 'TE Connectivity'
        : undefined;
      const m = all.match(/\b([A-Z]{1,5}\d[A-Z0-9\-]{2,})\b/); // 예: TQ2-L2-12V
      guess.brand = guess.brand || brand;
      guess.code = guess.code || (m ? m[1] : undefined);
      if (typeof guess.confidence !== 'number') guess.confidence = 0.6;
    }

    // (선택) brand/code 있으면 근접 항목 찾아 부가정보 리턴
    let nearest = [];
    const b = (guess.brand || '').toString();
    const c = (guess.code || '').toString();

    if (b || c) {
      const q = `
        with p as (select lower($1) b, lower($2) c)
        select 'relay_power_specs' as table,
               brand, code, series, family_slug, datasheet_uri
          from public.relay_power_specs, p
         where (brand_norm = p.b or p.b = '')
            or (code_norm  = p.c or p.c = '')
         limit 20`;
      nearest = (await db.query(q, [b, c])).rows;

      // label + 대표이미지 보강
      const out = [];
      for (const r0 of nearest) {
        const famLabelRow = await db.query(
          `select display_name
             from public.component_registry
            where family_slug = $1
            limit 1`,
          [r0.family_slug],
        );
        const famLabel = famLabelRow.rows[0]?.display_name || null;

        let hero = null;
        if (r0.brand && r0.code) {
          const img = await db.query(
            `select gcs_uri
               from public.image_index
              where brand_norm = lower($1)
                and code_norm  = lower($2)
              order by created_at desc
              limit 1`,
            [r0.brand, r0.code],
          );
          const gcs = img.rows[0]?.gcs_uri || null;
          hero = gcs
            ? gcs.replace(/^gs:\/\//, 'https://storage.googleapis.com/')
            : null;
        }

        out.push({ ...r0, family_label: famLabel, hero_image_url: hero });
      }
      nearest = out;
    }

    res.json({
      ok: true,
      guess: {
        family_slug: guess.family_slug || null,
        family_label: guess.family_label_ko || null,
        brand: guess.brand || null,
        code: guess.code || null,
        confidence:
          typeof guess.confidence === 'number' ? guess.confidence : 0.6,
        rationale: guess.rationale || null,
      },
      nearest,
    });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

module.exports = app;
