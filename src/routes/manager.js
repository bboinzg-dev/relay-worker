// CommonJS 버전: export default 대신 module.exports 사용
const express = require('express');
const router = express.Router();

// 간단한 건강 체크(선택)
router.get('/auth/_health', (req, res) => res.json({ ok: true }));

// 회원가입
router.post('/auth/signup', async (req, res) => {
  const p = req.body || {};
  // 실제 구현 전까지는 정상 200만 보장 (프론트가 404만 아니면 됨)
  res.json({
    ok: true,
    user: {
      id: 'tmp',
      username: p.username || p.email || '',
      email: p.email || '',
      is_seller_requested: !!p.is_seller_requested,
      profile: p.profile || null,
    },
    token: 'stub-token',
  });
});

// 로그인
router.post('/auth/login', async (req, res) => {
  const p = req.body || {};
  res.json({
    ok: true,
    user: { id: 'tmp', username: p.username || '', email: p.email || '' },
    token: 'stub-token',
  });
});

// (선택) 계정 조회
router.get('/account', (req, res) => res.json({ ok: true }));

module.exports = router;
