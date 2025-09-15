// src/routes/manager.js
'use strict';

const express = require('express');
const { v4: uuidv4 } = require('uuid');

const router = express.Router();
router.get('/auth/health', (_req, res) => res.json({ ok: true }));

// 마운트 확인용
router.get('/auth/health', (req, res) => res.json({ ok: true, ts: Date.now() }));

// 회원가입
router.post('/auth/signup', express.json({ limit: '5mb' }), async (req, res) => {
  try {
    const p = req.body || {};
    // TODO: 실제 검증/중복체크/DB 저장 추가
    const token = uuidv4();
    res.json({ ok: true, token, user: { username: p.username, email: p.email } });
  } catch (e) {
    console.error('[signup] error:', e);
    res.status(400).json({ error: 'signup failed', detail: String(e.message || e) });
  }
});

// 로그인
router.post('/auth/login', express.json({ limit: '2mb' }), async (req, res) => {
  try {
    const { usernameOrEmail } = req.body || {};
    // TODO: 실제 인증 로직 추가
    const token = uuidv4();
    res.json({ ok: true, token, user: { id: usernameOrEmail } });
  } catch (e) {
    console.error('[login] error:', e);
    res.status(400).json({ error: 'login failed', detail: String(e.message || e) });
  }
});

module.exports = router;
module.exports.default = router; // ESM import 호환
module.exports.router = router;
