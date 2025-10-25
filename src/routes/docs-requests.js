'use strict';

const express = require('express');
const router = express.Router();
const db = require('../../db');

function parseIntOrDefault(value, defaultValue) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : defaultValue;
}

function getUserId(req) {
  return req.user?.id || req.headers['x-user-id'] || req.query.user_id || null;
}

// [A] 구매자용 - 목록/검색
router.get('/docs-requests', async (req, res) => {
  try {
    const { mine } = req.query;
    const limit = parseIntOrDefault(req.query.limit, 50);
    const offset = parseIntOrDefault(req.query.offset, 0);

    if (mine) {
      const uid = getUserId(req);
      if (!uid) return res.status(401).json({ error: 'signin_required' });
      const { rows } = await db.query(
        `SELECT *
           FROM public.docs_requests
          WHERE requester_user_id = $1
          ORDER BY created_at DESC
          LIMIT $2 OFFSET $3`,
        [uid, limit, offset]
      );
      return res.json(rows);
    }

    const manufacturer = req.query.manufacturer ? String(req.query.manufacturer).trim() : '';
    const partNumber = req.query.partNumber ? String(req.query.partNumber).trim() : '';

    if (manufacturer || partNumber) {
      const m = manufacturer ? `%${manufacturer.toLowerCase()}%` : '%';
      const p = partNumber ? `%${partNumber.toLowerCase()}%` : '%';
      const { rows } = await db.query(
        `SELECT *
           FROM public.docs_requests
          WHERE lower(manufacturer) LIKE $1
            AND lower(part_number) LIKE $2
          ORDER BY created_at DESC
          LIMIT $3 OFFSET $4`,
        [m, p, limit, offset]
      );
      return res.json(rows);
    }

    const { rows } = await db.query(
      `SELECT *
         FROM public.docs_requests
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2`,
      [limit, offset]
    );
    return res.json(rows);
  } catch (err) {
    console.error('[docs-requests] list error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});

// [B] 구매자용 - 생성
router.post('/docs-requests', async (req, res) => {
  try {
    const uid = getUserId(req);
    if (!uid) return res.status(401).json({ error: 'signin_required' });

    const { manufacturer, partNumber, docs, note } = req.body || {};
    if (!manufacturer || !partNumber || !Array.isArray(docs) || docs.length === 0) {
      return res.status(400).json({ error: 'manufacturer, partNumber, docs[] are required' });
    }

    const { rows } = await db.query(
      `INSERT INTO public.docs_requests
        (requester_user_id, manufacturer, part_number, docs, note, status)
       VALUES ($1, $2, $3, $4, $5, 'open')
       RETURNING *`,
      [uid, manufacturer, partNumber, docs, note ?? null]
    );
    const created = rows[0];

    await db.query(
      `INSERT INTO public.docs_request_targets (docs_request_id, target_type, status)
       VALUES ($1, 'brand_code', 'pending')`,
      [created.id]
    );

    return res.status(201).json(created);
  } catch (err) {
    console.error('[docs-requests] create error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});

// [C] 판매자용 - 내게 들어온 자료요청 보기 (뷰 사용)
router.get('/seller/docs-requests', async (req, res) => {
  try {
    const sellerId = getUserId(req) || req.query.seller_id;
    if (!sellerId) return res.status(401).json({ error: 'signin_required' });

    const { rows } = await db.query(
      `SELECT *
         FROM public.vw_docs_requests_for_seller
        WHERE seller_id = $1
        ORDER BY requested_at DESC`,
      [sellerId]
    );
    return res.json(rows);
  } catch (err) {
    console.error('[docs-requests] seller list error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});

// [D] 판매자용 - 응답 등록
router.post('/seller/docs-requests/:targetId/respond', async (req, res) => {
  const sellerId = getUserId(req);
  if (!sellerId) return res.status(401).json({ error: 'signin_required' });

  const { kind, note, promisedDate, fileUrl, fileBlobId } = req.body || {};
  if (!['promise', 'upload'].includes(kind)) {
    return res.status(400).json({ error: 'invalid kind' });
  }

  const client = await db.getPool().connect();
  try {
    await client.query('BEGIN');

    const responseInsert = await client.query(
      `INSERT INTO public.docs_request_responses
        (docs_request_id, target_id, responder_user_id, kind, note, promised_date, file_url, file_blob_id)
       SELECT t.docs_request_id, t.id, $1, $2, $3, $4, $5, $6
         FROM public.docs_request_targets t
        WHERE t.id = $7
        RETURNING *`,
      [sellerId, kind, note ?? null, promisedDate ?? null, fileUrl ?? null, fileBlobId ?? null, req.params.targetId]
    );

    if (responseInsert.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'target_not_found' });
    }

    if (kind === 'promise' && promisedDate) {
      await client.query(
        `UPDATE public.docs_request_targets
            SET status = 'promised', promise_date = $1
          WHERE id = $2`,
        [promisedDate, req.params.targetId]
      );
    } else if (kind === 'upload') {
      await client.query(
        `UPDATE public.docs_request_targets
            SET status = 'responded'
          WHERE id = $1`,
        [req.params.targetId]
      );
    }

    await client.query('COMMIT');
    return res.status(201).json(responseInsert.rows[0]);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[docs-requests] seller respond error:', err);
    return res.status(500).json({ error: 'internal_error' });
  } finally {
    client.release();
  }
});

// [E] 구매계획 입찰 목록
router.get('/plan-bids', async (req, res) => {
  try {
    const prId = req.query.pr_id;
    if (!prId) return res.status(400).json({ error: 'pr_id required' });

    const { rows } = await db.query(
      `SELECT *
         FROM public.vw_purchase_plan_bids
        WHERE purchase_request_id = $1
        ORDER BY created_at DESC`,
      [prId]
    );
    return res.json(rows);
  } catch (err) {
    console.error('[docs-requests] plan bids error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});

module.exports = router;
