'use strict';
const express = require('express');
const router = express.Router();
const db = require('../../db');

const GCS_BUCKET = process.env.GCS_BUCKET || '';

function sanitizeId(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, '-')
    .replace(/^-+|-+$/g, '');
}

function applyFallbackUrls(part, familySlug = null) {
  if (!part || !GCS_BUCKET) return part;

  const safeBrand = sanitizeId(part.brand || part.brand_norm);
  const safeCode = sanitizeId(part.code || part.code_norm);
  if (!safeBrand || !safeCode) return part;

  const safeFamily = sanitizeId(familySlug || part.family_slug || '');
  const base = `https://storage.googleapis.com/${GCS_BUCKET}`;

  if (!part.cover) {
    const coverPath = safeFamily
      ? `images/${safeFamily}/${safeBrand}/${safeCode}/cover.png`
      : `images/${safeBrand}/${safeCode}/cover.png`;
    part.cover = `${base}/${coverPath}`;
  }

  if (!part.datasheet_url) {
    const datasheetPath = safeFamily
      ? `datasheets/${safeFamily}/${safeBrand}/${safeCode}/datasheet.pdf`
      : `datasheets/${safeCode}.pdf`;
    part.datasheet_url = `${base}/${datasheetPath}`;
  }

  return part;
}

// 안전한 테이블명 정규식
const isSafeIdent = (s) => /^[a-z0-9_]+$/i.test(s);

// GET /api/parts/detail?brand=...&code=...
router.get('/detail', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand and code required' });

    const b = brand.toLowerCase();
    const c = code.toLowerCase();

    // ① 모든 부품군 테이블 목록 가져오기 (이미 DB에 registry 존재)
    const fams = await db.query(
      `SELECT family_slug, display_name, specs_table
         FROM public.component_registry
        ORDER BY family_slug ASC`
    );

    // ② 각 스펙 테이블에서 brand_norm/code_norm 매칭 검색
    let found = null;
    for (const row of fams.rows) {
      const tbl = row.specs_table.replace(/^public\./,''); // 안전
      if (!isSafeIdent(tbl)) continue;
      const sql =
        `SELECT *, $1::text AS _family_slug, $2::text AS _family_label
           FROM public.${tbl}
          WHERE brand_norm = $3 AND code_norm = $4
          LIMIT 1`;
      const r = await db.query(sql, [row.family_slug, row.display_name ?? row.family_slug, b, c]);
      if (r.rows.length) { found = r.rows[0]; break; }
    }

    if (!found) return res.status(404).json({ ok:false, error:'not found' });

    const part = applyFallbackUrls({ ...found }, found._family_slug);

    // ③ 대표 이미지 1~4장 추출 (image_index 테이블 존재)
    const img = await db.query(
      `SELECT gcs_uri
         FROM public.image_index
        WHERE brand_norm = $1 AND code_norm = $2
        ORDER BY created_at DESC
        LIMIT 4`,
      [b, c]
    );
    const images = img.rows.map(r =>
      r.gcs_uri.startsWith('gs://')
        ? r.gcs_uri.replace(/^gs:\/\//, 'https://storage.googleapis.com/')
        : r.gcs_uri
    );

    return res.json({
      ok: true,
      family_slug: found._family_slug,
      family_label: found._family_label,
      part,           // 스펙 row 전체 반환
      images
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok:false, error:String(e.message || e) });
  }
});

module.exports = router;
