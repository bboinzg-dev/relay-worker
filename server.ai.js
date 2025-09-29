// server.ai.js
'use strict';

const express = require('express');
const router = express.Router();
const { VertexAI } = require('@google-cloud/vertexai');
const db = require('./src/utils/db');              // 이미 프로젝트에 있는 DB 유틸 사용
const { pool } = db;

// ───────────────── helpers ─────────────────
function clean(s) { return String(s||'').trim(); }
function toArr(a){ return Array.isArray(a) ? a : []; }

// brand 후보를 쿼리에서 추정 (manufacturer_alias 기반)
async function guessBrandFromText(q) {
  const text = String(q||'').toLowerCase();
  try {
    const { rows } = await pool.query(
      `SELECT brand, alias, aliases FROM public.manufacturer_alias`
    );
    for (const r of rows) {
      const tokens = new Set();
      if (r.brand)  tokens.add(String(r.brand));
      if (r.alias)  tokens.add(String(r.alias));
      if (Array.isArray(r.aliases)) r.aliases.forEach(a => tokens.add(String(a)));
      for (const t of tokens) {
        const s = String(t||'').trim().toLowerCase();
        if (!s || s === 'unknown') continue;
        const re = new RegExp(`(^|[^a-z0-9])${s}([^a-z0-9]|$)`, 'i');
        if (re.test(text)) return r.brand;
      }
    }
  } catch {}
  return null;
}

// --- helper: 코드 prefix로 시리즈/브랜드 유추(예: APAN → Panasonic)
async function brandFromSeriesPrefix(q) {
  const m = String(q||'').toUpperCase().match(/^([A-Z]{3,})\d/);
  if (!m) return null;
  const prefix = m[1];
  try {
    const { rows } = await pool.query(
      `SELECT brand_norm FROM public.manufacturer_series_catalog
       WHERE series_norm = lower($1) LIMIT 1`, [prefix]
    );
    return rows[0]?.brand_norm || null;
  } catch {
    return null;
  }
}

// DB 텍스트 검색 top1 (기존 /parts/search 로직 폴백)
async function searchTop1(q) {
  const text = `%${String(q||'').toLowerCase()}%`;
  // unified view component_specs 우선
  try {
    const { rows } = await pool.query(
      `SELECT brand, code FROM public.component_specs
       WHERE brand_norm LIKE $1 OR code_norm LIKE $1 OR lower(coalesce(display_name,'')) LIKE $1
       ORDER BY updated_at DESC
       LIMIT 1`, [text]
    );
    if (rows[0]) return rows[0];
  } catch {}
  // 릴레이 뷰 폴백
  try {
    const { rows } = await pool.query(
      `SELECT brand, code FROM public.relay_specs
       WHERE brand_norm LIKE $1 OR code_norm LIKE $1 OR lower(display_name) LIKE $1
       ORDER BY updated_at DESC
       LIMIT 1`, [text]
    );
    if (rows[0]) return rows[0];
  } catch {}
  return null;
}

// ───────────────── route ─────────────────
router.get('/api/ai/resolve', async (req, res) => {
  const q = clean(req.query.q);
  if (!q) return res.status(400).json({ ok:false, error:'q required' });

  // 1) Vertex로 brand/codes 추정 (Cloud Run은 ADC 있으므로 OK)
  let brand = null, codes = [];
  try {
    const projectId = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT_ID;
    const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
    const modelId = process.env.GEMINI_MODEL_EXTRACT || 'gemini-2.5-flash';

    const v = new VertexAI({
      project: projectId,
      location,
    });
    const mdl = v.getGenerativeModel({
      model: modelId,
      systemInstruction: { parts: [{ text:
        [
          'Parse an electronics part query.',
          'Return STRICT JSON: {"brand": string|null, "codes": string[]}.',
          'No prose. No extra keys.'
        ].join('\n')
      }]},
      generationConfig: { temperature: 0.2, responseMimeType: 'application/json', maxOutputTokens: 512 },
    });
    const resp = await mdl.generateContent({
      contents: [{ role:'user', parts:[{ text:`query: ${q}` }]}],
    });
    let parsed = {};
    try { parsed = JSON.parse(resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}'); } catch {}
    brand = clean(parsed?.brand);
    codes = toArr(parsed?.codes).map(x => clean(x)).filter(Boolean);
  } catch (e) {
    // Vertex 실패해도 계속 진행(폴백으로 커버)
    console.warn('[ai/resolve] Vertex fail:', e?.message || e);
  }

  // 2) 폴백들
  if (!brand) brand = await guessBrandFromText(q);
  if (!brand) brand = await brandFromSeriesPrefix(q);
  let code = codes[0] || '';

  // 3) DB로 최종 검증/보정
  // 코드가 없거나 DB에 없는 경우: 텍스트 검색 top1로 보정
  if (!code) {
    const top = await searchTop1(q);
    if (top?.brand && top?.code) {
      return res.json({ ok:true, brand: top.brand, code: top.code, source: 'db-top1' });
    }
  }

  // 4) 결과 반환
  if (brand && code) return res.json({ ok:true, brand, code, source: 'ai' });

  // 마지막 폴백: DB top1 한 번 더 시도(브랜드 없이도)
  const top2 = await searchTop1(q);
  if (top2?.brand && top2?.code) return res.json({ ok:true, brand: top2.brand, code: top2.code, source: 'db-top1' });

  return res.status(404).json({ ok:false, error:'cannot resolve' });
});

module.exports = router;
