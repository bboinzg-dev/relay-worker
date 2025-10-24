'use strict';

const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const multer = require('multer');
const db = require('./db');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '2mb' }));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });

function parseNumericLike(value) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  return /^-?\d+$/.test(trimmed) ? trimmed : null;
}

function normalizeDocsArray(input) {
  if (!Array.isArray(input)) return [];
  return input.filter((v) => typeof v === 'string' && v.trim().length > 0).map((v) => v.trim());
}

function normalizeTargets(input) {
  if (!Array.isArray(input)) return [];
  const toNullable = (value) => {
    if (value == null) return null;
    const trimmed = String(value).trim();
    return trimmed.length ? trimmed : null;
  };
  return input
    .filter((item) => item && typeof item === 'object' && item.type)
    .map((item) => ({
      type: String(item.type || '').trim(),
      seller_id: toNullable(item.seller_id),
      listing_id: toNullable(item.listing_id),
      plan_bid_id: toNullable(item.plan_bid_id),
      promise_date: toNullable(item.promise_date),
    }))
    .filter((item) => item.type.length > 0);
}

app.post('/api/uploads/file', upload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    if (!file) {
      return res.status(400).json({ error: 'file_required' });
    }
    const inserted = await db.query(
      `INSERT INTO public.file_blobs (content_type, filename, byte_len, data)
       VALUES ($1,$2,$3,$4)
       RETURNING id`,
      [file.mimetype || 'application/octet-stream', file.originalname || 'file', file.buffer?.length || 0, file.buffer],
    );
    const blobId = inserted.rows[0]?.id;
    return res.json({ blob_id: blobId, url: blobId ? `/api/files/${blobId}` : null });
  } catch (e) {
    console.error('[docs] file upload failed', e);
    return res.status(500).json({ error: 'upload_failed' });
  }
});

app.get('/api/files/:id', async (req, res) => {
  try {
    const id = String(req.params.id || '').trim();
    if (!id) return res.status(400).json({ error: 'id_required' });
    const result = await db.query(
      `SELECT content_type, filename, data FROM public.file_blobs WHERE id = $1 LIMIT 1`,
      [id],
    );
    if (!result.rows.length) {
      return res.status(404).json({ error: 'not_found' });
    }
    const row = result.rows[0];
    res.setHeader('Content-Type', row.content_type || 'application/octet-stream');
    res.setHeader('Content-Disposition', `inline; filename="${row.filename || 'file'}"`);
    return res.send(row.data);
  } catch (e) {
    console.error('[docs] file fetch failed', e);
    return res.status(500).json({ error: 'fetch_failed' });
  }
});

app.post('/api/docs/requests', async (req, res) => {
  const body = req.body || {};
  const manufacturerRaw = typeof body.manufacturer === 'string' ? body.manufacturer : null;
  const partNumberRaw = typeof body.part_number === 'string' ? body.part_number : null;
  const manufacturer = manufacturerRaw != null ? manufacturerRaw.trim() : null;
  const partNumber = partNumberRaw != null ? partNumberRaw.trim() : null;
  const normalizedManufacturer = manufacturer && manufacturer.length ? manufacturer : null;
  const normalizedPartNumber = partNumber && partNumber.length ? partNumber : null;
  const docs = normalizeDocsArray(body.docs);
  const targets = normalizeTargets(body.targets);
  const requesterUserId = parseNumericLike(body.requester_user_id);

  const client = await db.getPool().connect();
  try {
    await client.query('BEGIN');

    const insertedRequest = await client.query(
      `INSERT INTO public.docs_requests (requester_user_id, manufacturer, part_number, docs, status)
       VALUES ($1,$2,$3,$4,'open')
       RETURNING id`,
      [requesterUserId, normalizedManufacturer, normalizedPartNumber, docs],
    );

    const requestId = insertedRequest.rows[0]?.id;
    if (!requestId) {
      throw new Error('request_insert_failed');
    }

    for (const target of targets) {
      try {
        await client.query(
          `INSERT INTO public.docs_request_targets
             (docs_request_id, target_type, seller_id, listing_id, plan_bid_id, status, promise_date)
           VALUES ($1,$2,$3,$4,$5,'pending',$6)`,
          [
            requestId,
            target.type,
            target.seller_id,
            target.listing_id,
            target.plan_bid_id,
            target.promise_date,
          ],
        );
      } catch (e) {
        console.error('[docs] insert target failed', e);
        throw e;
      }
    }

    await client.query('COMMIT');
    return res.json({ id: requestId });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[docs] request creation failed', e);
    return res.status(500).json({ error: 'request_creation_failed' });
  } finally {
    client.release();
  }
});

app.get('/api/docs/requests/mine', async (req, res) => {
  try {
    const requesterUserId = parseNumericLike(req.query?.requester_user_id);
    const result = await db.query(
      `SELECT dr.*,
              COALESCE(json_agg(t.*) FILTER (WHERE t.id IS NOT NULL), '[]'::json) AS targets
         FROM public.docs_requests dr
         LEFT JOIN public.docs_request_targets t ON t.docs_request_id = dr.id
        WHERE ($1::bigint IS NULL OR dr.requester_user_id = $1::bigint)
        GROUP BY dr.id
        ORDER BY dr.created_at DESC`,
      [requesterUserId],
    );
    return res.json(result.rows);
  } catch (e) {
    console.error('[docs] fetch mine failed', e);
    return res.status(500).json({ error: 'fetch_failed' });
  }
});

app.get('/api/docs/targets/for-seller/general', async (req, res) => {
  try {
    const sellerId = String(req.query?.seller_id || '').trim();
    if (!sellerId) return res.status(400).json({ error: 'seller_id_required' });
    const result = await db.query(
      `SELECT *
         FROM public.vw_docs_requests_for_seller
        WHERE seller_id = $1 AND listing_id IS NULL
        ORDER BY requested_at DESC`,
      [sellerId],
    );
    return res.json(result.rows);
  } catch (e) {
    console.error('[docs] fetch general targets failed', e);
    return res.status(500).json({ error: 'fetch_failed' });
  }
});

app.get('/api/docs/targets/for-seller/listings', async (req, res) => {
  try {
    const sellerId = String(req.query?.seller_id || '').trim();
    if (!sellerId) return res.status(400).json({ error: 'seller_id_required' });
    const result = await db.query(
      `SELECT *
         FROM public.vw_docs_requests_for_listings
        WHERE seller_id = $1
        ORDER BY requested_at DESC`,
      [sellerId],
    );
    return res.json(result.rows);
  } catch (e) {
    console.error('[docs] fetch listing targets failed', e);
    return res.status(500).json({ error: 'fetch_failed' });
  }
});

app.post('/api/docs/targets/:targetId/respond', upload.single('file'), async (req, res) => {
  const targetId = String(req.params.targetId || '').trim();
  if (!targetId) return res.status(400).json({ error: 'target_id_required' });

  const responderUserId = parseNumericLike(req.body?.responder_user_id);
  const note = typeof req.body?.note === 'string' && req.body.note.trim().length ? req.body.note.trim() : null;
  const promisedDateRaw = req.body?.promised_date;
  const promisedDate = promisedDateRaw != null && String(promisedDateRaw).trim() !== ''
    ? String(promisedDateRaw).trim()
    : null;

  const hasFile = !!req.file;
  const hasPromise = !!promisedDate;

  if (!hasFile && !hasPromise) {
    return res.status(400).json({ error: 'no_payload' });
  }

  const client = await db.getPool().connect();
  let blobId = null;
  let fileUrl = null;
  try {
    await client.query('BEGIN');

    if (hasFile) {
      const insertedBlob = await client.query(
        `INSERT INTO public.file_blobs (content_type, filename, byte_len, data)
         VALUES ($1,$2,$3,$4)
         RETURNING id`,
        [
          req.file.mimetype || 'application/octet-stream',
          req.file.originalname || 'file',
          req.file.buffer?.length || 0,
          req.file.buffer,
        ],
      );
      blobId = insertedBlob.rows[0]?.id;
      fileUrl = blobId ? `/api/files/${blobId}` : null;

      const insertUpload = await client.query(
        `INSERT INTO public.docs_request_responses
           (docs_request_id, target_id, responder_user_id, kind, note, file_blob_id, file_url)
         SELECT t.docs_request_id, t.id, $1, 'upload', $2, $3, $4
           FROM public.docs_request_targets t
          WHERE t.id = $5
         RETURNING id`,
        [responderUserId, note, blobId, fileUrl, targetId],
      );
      if (!insertUpload.rowCount) {
        throw Object.assign(new Error('target_not_found'), { statusCode: 404 });
      }
      await client.query(
        `UPDATE public.docs_request_targets
            SET status = 'responded', updated_at = now()
          WHERE id = $1`,
        [targetId],
      );
    }

    if (hasPromise) {
      const insertPromise = await client.query(
        `INSERT INTO public.docs_request_responses
           (docs_request_id, target_id, responder_user_id, kind, note, promised_date)
         SELECT t.docs_request_id, t.id, $1, 'promise', $2, $3
           FROM public.docs_request_targets t
          WHERE t.id = $4
         RETURNING id`,
        [responderUserId, note, promisedDate, targetId],
      );
      if (!insertPromise.rowCount) {
        throw Object.assign(new Error('target_not_found'), { statusCode: 404 });
      }
      await client.query(
        `UPDATE public.docs_request_targets
            SET status = CASE WHEN status = 'responded' THEN status ELSE 'promised' END,
                promise_date = $2,
                updated_at = now()
          WHERE id = $1`,
        [targetId, promisedDate],
      );
    }

    await client.query('COMMIT');
    return res.json({ ok: true, file_url: fileUrl, blob_id: blobId });
  } catch (e) {
    await client.query('ROLLBACK');
    const status = e?.statusCode || 500;
    console.error('[docs] respond failed', e);
    if (status === 404) {
      return res.status(404).json({ error: 'target_not_found' });
    }
    return res.status(500).json({ error: 'respond_failed' });
  } finally {
    client.release();
  }
});

module.exports = app;
