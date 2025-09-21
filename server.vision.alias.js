// server.vision.alias.js
'use strict';
const express = require('express');
const multer  = require('multer');
const upload  = multer({ storage: multer.memoryStorage() });
const { VertexAI } = require('@google-cloud/vertexai');

const router = express.Router();

/** 폴백: /api/vision/guess */
router.post('/api/vision/guess', upload.single('file'), async (req, res) => {
  try {
    const buf  = req.file?.buffer;
    const mime = req.file?.mimetype || 'image/jpeg';
    if (!buf) return res.status(400).json({ ok:false, error:'file required' });

    const project  = process.env.GCP_PROJECT_ID;
    const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
    const modelId  = process.env.GEMINI_MODEL_CLASSIFY || 'gemini-2.5-flash';

    const v = new VertexAI({ project, location });
    const model = v.getGenerativeModel({ model: modelId });

    const prompt = '사진 속 전자부품의 (family_slug, family_label, brand, code, confidence)만 JSON으로 반환. 불확실하면 null과 낮은 confidence.';
    const resp = await model.generateContent({
      contents: [{ role:'user', parts:[
        { inlineData: { data: buf.toString('base64'), mimeType: mime } },
        { text: prompt }
      ]}],
      generationConfig: { responseMimeType: 'application/json' }
    });

    const parts = resp.response?.candidates?.[0]?.content?.parts ?? [];
    const raw   = parts.map(p => p.text ?? '').join('');
    const guess = JSON.parse(raw || '{}');

    return res.json({ ok:true, guess });
  } catch (e) {
    console.error('[vision/guess]', e);
    return res.status(500).json({ ok:false, error:String(e?.message||e) });
  }
});

module.exports = router;
