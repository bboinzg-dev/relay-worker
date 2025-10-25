'use strict';

const express = require('express');
const router = express.Router();
const db = require('../../db');
const { parseActor, getSellerKeySet } = require('../utils/auth');

function parseIntOrDefault(value, defaultValue) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : defaultValue;
}

function getUserId(req) {
  const keys = getSellerKeySet(req);
  if (keys.length > 0) return keys[0];
  const actor = parseActor(req);
  return actor?.id || null;
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

function coerceNullableNumber(value) {
  if (value === undefined || value === null || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function coerceNullableBoolean(value) {
  if (value === undefined || value === null || value === '') return null;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value === 1;
  if (typeof value === 'string') {
    const lowered = value.toLowerCase();
    if (['true', '1', 'yes', 'y'].includes(lowered)) return true;
    if (['false', '0', 'no', 'n'].includes(lowered)) return false;
  }
  return null;
}

function coerceNullableText(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text.length ? text : null;
}

async function fetchPlanBidDetail(id, sellerKeySet) {
  const params = [id];
  let sellerFilter = '';

  if (Array.isArray(sellerKeySet) && sellerKeySet.length) {
    sellerFilter = `
      AND pb.seller_id IN (
        SELECT match_key FROM (
          SELECT UNNEST($2::text[]) AS match_key
          UNION
          SELECT id::text FROM public.users WHERE id::text = ANY($2::text[]) OR username = ANY($2::text[])
          UNION
          SELECT username FROM public.users WHERE id::text = ANY($2::text[]) OR username = ANY($2::text[])
        ) __seller_keys
      )`;
    params.push(sellerKeySet);
  }

  const baseSql = `SELECT
        pb.id,
        pb.purchase_request_id,
        COALESCE(pb.offer_brand, pr.brand)   AS brand,
        COALESCE(pb.offer_code,  pr.code)    AS code,
        pb.offer_qty,
        pb.offer_qty        AS qty_offer,
        pb.unit_price_cents,
        pb.currency,
        pb.lead_time_days,
        pb.no_parcel,
        pb.has_stock,
        pb.delivery_date,
        pb.quote_valid_until,
        pb.status,
        pb.note,
        pb.created_at,
        pb.updated_at
      FROM public.plan_bids pb
      LEFT JOIN public.purchase_requests pr ON pr.id = pb.purchase_request_id
     WHERE pb.id = $1${sellerFilter}`;

  const { rows } = await db.query(baseSql, params);
  return rows[0] || null;
}

// [E] 구매계획 입찰 목록
router.get('/plan-bids', async (req, res) => {
  try {
    const prId = req.query.pr_id;
    const isMine = String(req.query.mine || '') === '1';

    if (!isMine && !prId) {
      return res.status(400).json({ error: 'pr_id required (or use mine=1)' });
    }

    let sellerKeySet = [];
    if (isMine) {
      sellerKeySet = getSellerKeySet(req);
      if (!sellerKeySet.length) return res.status(401).json({ error: 'signin_required' });
    }

    if (isMine && !prId) {
      const { rows } = await db.query(
        `WITH me AS (
            SELECT UNNEST($1::text[]) AS k
          ), mapped AS (
            SELECT k FROM me
            UNION
            SELECT id::text FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
            UNION
            SELECT username FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
          )
          SELECT
            pb.id,
            pb.purchase_request_id,
            COALESCE(pb.offer_brand, pr.brand)   AS brand,
            COALESCE(pb.offer_code,  pr.code)    AS code,
            pb.offer_qty,
            pb.offer_qty        AS qty_offer,
            pb.unit_price_cents,
            pb.currency,
            pb.lead_time_days,
            pb.no_parcel,
            pb.has_stock,
            pb.delivery_date,
            pb.quote_valid_until,
            pb.status,
            pb.note,
            pb.created_at,
            pb.updated_at
          FROM public.plan_bids pb
          LEFT JOIN public.purchase_requests pr ON pr.id = pb.purchase_request_id
         WHERE pb.seller_id IN (SELECT k FROM mapped)
         ORDER BY pb.created_at DESC
         LIMIT 200`,
        [sellerKeySet]
      );
      return res.json(rows);
    }

    if (isMine) {
      const { rows } = await db.query(
        `WITH me AS (
            SELECT UNNEST($2::text[]) AS k
          ), mapped AS (
            SELECT k FROM me
            UNION
            SELECT id::text FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
            UNION
            SELECT username FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
          )
          SELECT
            pb.id,
            pb.purchase_request_id,
            COALESCE(pb.offer_brand, pr.brand)   AS brand,
            COALESCE(pb.offer_code,  pr.code)    AS code,
            pb.offer_qty,
            pb.offer_qty        AS qty_offer,
            pb.unit_price_cents,
            pb.currency,
            pb.lead_time_days,
            pb.no_parcel,
            pb.has_stock,
            pb.delivery_date,
            pb.quote_valid_until,
            pb.status,
            pb.note,
            pb.created_at,
            pb.updated_at
          FROM public.plan_bids pb
          LEFT JOIN public.purchase_requests pr ON pr.id = pb.purchase_request_id
         WHERE pb.purchase_request_id = $1
           AND pb.seller_id IN (SELECT k FROM mapped)
         ORDER BY pb.created_at DESC`,
        [prId, sellerKeySet]
      );
      return res.json(rows);
    }

    const { rows } = await db.query(
      `SELECT
          pb.id,
          pb.purchase_request_id,
          COALESCE(pb.offer_brand, pr.brand)   AS brand,
          COALESCE(pb.offer_code,  pr.code)    AS code,
          pb.offer_qty,
          pb.unit_price_cents,
          pb.currency,
          pb.lead_time_days,
          pb.no_parcel,
          pb.has_stock,
          pb.delivery_date,
          pb.quote_valid_until,
          pb.status,
          pb.note,
          pb.created_at,
          pb.updated_at
        FROM public.plan_bids pb
        LEFT JOIN public.purchase_requests pr ON pr.id = pb.purchase_request_id
       WHERE pb.purchase_request_id = $1
       ORDER BY pb.created_at DESC`,
      [prId]
    );
    return res.json(rows);
  } catch (err) {
    console.error('[docs-requests] plan bids error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});

// [F] 구매계획 입찰 수정
router.patch('/plan-bids/:id', async (req, res) => {
  try {
    const sellerKeySet = getSellerKeySet(req);
    if (!sellerKeySet.length) return res.status(401).json({ error: 'signin_required' });

    const { id } = req.params;
    if (!id) return res.status(400).json({ error: 'id required' });

    const body = req.body || {};
    let cents = null;

    const rawUnitPriceCents = coerceNullableNumber(body.unit_price_cents);
    if (rawUnitPriceCents !== null) {
      cents = Math.round(rawUnitPriceCents);
    } else {
      const rawUnitPrice = coerceNullableNumber(body.unit_price);
      if (rawUnitPrice !== null) {
        cents = Math.round(rawUnitPrice * 100);
      }
    }

    const params = [
      id,
      coerceNullableNumber(body.offer_qty),
      cents,
      coerceNullableText(body.currency),
      coerceNullableNumber(body.lead_time_days),
      coerceNullableBoolean(body.no_parcel),
      coerceNullableBoolean(body.has_stock),
      coerceNullableText(body.delivery_date),
      coerceNullableText(body.quote_valid_until),
      coerceNullableText(body.offer_brand),
      coerceNullableText(body.offer_code),
      coerceNullableText(body.note),
      sellerKeySet,
    ];

    const { rows } = await db.query(
      `WITH me AS (
          SELECT UNNEST($13::text[]) AS k
        ), mapped AS (
          SELECT k FROM me
          UNION
          SELECT id::text FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
          UNION
          SELECT username FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
        )
        UPDATE public.plan_bids
           SET offer_qty = COALESCE($2, offer_qty),
               unit_price_cents = COALESCE($3, unit_price_cents),
               currency = COALESCE($4, currency),
               lead_time_days = COALESCE($5, lead_time_days),
               no_parcel = COALESCE($6, no_parcel),
               has_stock = COALESCE($7, has_stock),
               delivery_date = COALESCE($8, delivery_date),
               quote_valid_until = COALESCE($9, quote_valid_until),
               offer_brand = COALESCE($10, offer_brand),
               offer_code = COALESCE($11, offer_code),
               note = COALESCE($12, note),
               updated_at = now()
         WHERE id = $1
           AND seller_id IN (SELECT k FROM mapped)
         RETURNING id`,
      params
    );

    if (!rows.length) {
      return res.status(404).json({ error: 'not_found' });
    }

    const detail = await fetchPlanBidDetail(id, sellerKeySet);
    return res.json(detail || rows[0]);
  } catch (err) {
    console.error('[docs-requests] plan bid update error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});

// [G] 구매계획 입찰 삭제
router.delete('/plan-bids/:id', async (req, res) => {
  try {
    const sellerKeySet = getSellerKeySet(req);
    if (!sellerKeySet.length) return res.status(401).json({ error: 'signin_required' });

    const { id } = req.params;
    if (!id) return res.status(400).json({ error: 'id required' });

    const result = await db.query(
      `WITH me AS (
          SELECT UNNEST($2::text[]) AS k
        ), mapped AS (
          SELECT k FROM me
          UNION
          SELECT id::text FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
          UNION
          SELECT username FROM public.users WHERE id::text IN (SELECT k FROM me) OR username IN (SELECT k FROM me)
        )
        DELETE FROM public.plan_bids
         WHERE id = $1
           AND seller_id IN (SELECT k FROM mapped)`,
      [id, sellerKeySet]
    );

    if (!result.rowCount) {
      return res.status(404).json({ error: 'not_found' });
    }

    return res.status(204).end();
  } catch (err) {
    console.error('[docs-requests] plan bid delete error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});

module.exports = router;
