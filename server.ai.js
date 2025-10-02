// server.ai.js — Express Router export (최종 경로는 /api/ai/*)
'use strict';
const express = require('express');
const router = express.Router();

// 루트 DB 모듈(필요 시 사용)
const { query } = require('./db');

async function resolveHandler(req, res) {
  try {
    const q = (req.body && req.body.q) || req.query?.q || '';
    if (!q) return res.status(400).json({ ok:false, error:'q required' });

    // TODO: 여기서 Vertex(Gemini) 호출/DB 조회를 넣으세요.
    return res.json({ ok:true, echo:q }); // 일단 경로 확인용 에코
  } catch (e) {
    return res.status(500).json({ ok:false, error:String(e?.message||e) });
  }
}

router.get('/ai/resolve', resolveHandler);
router.post('/ai/resolve', resolveHandler);
router.get('/ai/ping', (_req, res) => res.json({ ok:true }));

module.exports = router;
