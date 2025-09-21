// server.vision.alias.js
'use strict';
const express = require('express');
const multer  = require('multer');
const upload  = multer({ storage: multer.memoryStorage() });

const router = express.Router();

/**
 * 기존 비전 모듈을 최대한 활용합니다.
 * - server.vision가 export한 라우터가 있을 경우, 그 핸들러를 직접 부르거나
 * - 내부에 구현된 similarity 함수로 위임합니다.
 *
 * 아래 handler는 "이미 구현되어 있는" search/similar 엔드포인트가 있을 때
 * 그쪽으로 안전히 위임하는 형태의 예시입니다.
 */

// 1) POST /api/vision/guess  (이미지 바이너리 업로드)
router.post('/api/vision/guess', upload.single('file'), async (req, res, next) => {
  try {
    // 1-A) 만약 기존에 /api/vision/search 가 있다면 같은 형식으로 호출
    req.url  = '/api/vision/search';
    req.body = req.body || {};
    if (req.file) {
      // 기존 핸들러가 buffer를 받는 경우: req.body.imageBuffer, 혹은 그대로 req.file 사용
      req.body.imageBuffer = req.file.buffer;
      req.body.filename = req.file.originalname || 'upload.png';
    }
    return next(); // server.vision 라우터가 mount되어 있으면 이 경로로 흘러감
  } catch (e) {
    console.error('[vision/guess alias]', e);
    return res.status(500).json({ ok:false, error:'vision alias failed' });
  }
});

module.exports = router;
