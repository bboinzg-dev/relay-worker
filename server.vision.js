
// server.vision.js
'use strict';

const express = require('express');
const cors = require('cors');
const multer = require('multer');
const bodyParser = require('body-parser');
const { VertexAI } = require('@google-cloud/vertexai');
const db = require('./src/utils/db');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '15mb' }));
const upload = multer({ storage: multer.memoryStorage() });

const PROJECT_ID = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL      = process.env.GEMINI_MODEL_CLASSIFY || 'gemini-2.5-flash';

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });
const model  = vertex.getGenerativeModel({ model: MODEL });

// POST /api/vision/guess  (multipart: file)
app.post('/api/vision/guess', upload.single('file'), async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) return res.status(400).json({ error: 'file required' });
    const bytes = req.file.buffer;
    const base64 = bytes.toString('base64');

    // Ask Gemini to extract brand/code/family guess
 const prompt = `전자부품 이미지를 보고 부품군과 제조사/품명을 추정하세요.
 반드시 JSON만 출력하세요. 키는 다음과 같습니다:
 - family_slug: 부품군 슬러그(예: relay_power, relay_signal, mosfet, igbt_module, capacitor_mlcc ...) 없으면 'other'
 - family_label_ko: 사람 친화적인 한국어 라벨(예: "전력 릴레이", "신호 릴레이", "MOSFET")
 - brand: 제조사명(예: Panasonic, OMRON)
 - code: 품명/모델명(예: TQ2-L2-12V, ALDP112)
 - confidence: 0~1 사이 숫자(신뢰도)
 - rationale: 간단한 이유 한 줄
 JSON 외 텍스트 절대 금지.`

 const resp = await model.generateContent({
   contents: [{
     role: 'user',
     parts: [
       { text: prompt },
       { inlineData: { mimeType: req.file.mimetype || 'image/png', data: base64 } }
     ]
   }],
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
         rationale: { type: 'string' }
       }
     }
   }
  });

 // ★ JSON 안전 파싱(코드펜스/잡텍스트 대비)
 // 안전 파싱(혹시 텍스트 섞여도 최대한 복구)
 const parts = resp?.response?.candidates?.[0]?.content?.parts || [];
 const raw = parts.map(p => p.text || '').join('\n');
 const pickJson = (s) => (s.match(/\{[\s\S]*\}/) || [])[0] || '{}';
 let guess = {};
 try { guess = JSON.parse(raw); } catch { try { guess = JSON.parse(pickJson(raw)); } catch { guess = {}; } }

 // family_label 폴백: DB의 display_name 사용
 if (guess.family_slug && !guess.family_label_ko) {
   const q = await db.query(
     `SELECT display_name FROM public.component_registry WHERE family_slug = $1 LIMIT 1`,
     [guess.family_slug]
   );
   if (q.rows[0]?.display_name) guess.family_label_ko = q.rows[0].display_name;
 }
 if (!guess.family_label_ko && /relay/i.test(guess.code || '') || /relay/i.test(guess.rationale || '')) {
   guess.family_label_ko = '릴레이';
 }
 }

 // ★ 폴백 정규식(브랜드/PN을 텍스트에서 뽑기)
 if (!guess.brand || !guess.code) {
   const all = [raw, guess.raw].filter(Boolean).join(' ');
   const brand =
     /panasonic/i.test(all) ? 'Panasonic' :
     /omron/i.test(all)     ? 'Omron'     :
     /tyco|te\s*connectivity/i.test(all) ? 'TE Connectivity' : undefined;
   const m = all.match(/\b([A-Z]{1,5}\d[A-Z0-9\-]{2,})\b/); // 예: TQ2-L2-12V, ATQ223 등
   guess.brand = guess.brand || brand;
   guess.code  = guess.code  || (m ? m[1] : undefined);
   guess.confidence = guess.confidence ?? 0.6;
 }

    // Optional: do a nearest-neighbor by (brand,code) if present
    let nearest = [];
    const brand = (guess.brand || '').toString();
    const code  = (guess.code || '').toString();
    if (brand || code) {
      const q = `
        WITH p AS (
          SELECT lower($1) AS b, lower($2) AS c
        )
        SELECT 'relay_power_specs' AS table, brand, code, series, family_slug, datasheet_uri
          FROM public.relay_power_specs, p
         WHERE (brand_norm = p.b OR p.b = '')
            OR (code_norm = p.c  OR p.c = '')
         LIMIT 20`;
      const r = await db.query(q, [brand, code]);
      nearest = r.rows;
    }

    + res.json({
   ok: true,
   guess: {
     family_slug: guess.family_slug || null,
     family_label: guess.family_label_ko || null,
     brand: guess.brand || null,
     code: guess.code || null,
     confidence: typeof guess.confidence === 'number' ? guess.confidence : 0.6,
     rationale: guess.rationale || null
   },
   nearest
});
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

module.exports = app;
