'use strict';

const express = require('express');
const multer = require('multer');
const { Storage } = require('@google-cloud/storage');
const { CloudTasksClient } = require('@google-cloud/tasks');
const { runAutoIngest } = require('../pipeline/ingestAuto');
const { generateRunId } = require('../utils/run-id');

const router = express.Router();

const upload = multer({ storage: multer.memoryStorage() });
const storage = new Storage();

const BUCKET = process.env.GCS_BUCKET;
if (!BUCKET) console.warn('[files] GCS_BUCKET not set');

function nowSlug() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return (
    d.getUTCFullYear() +
    pad(d.getUTCMonth() + 1) +
    pad(d.getUTCDate()) +
    '-' +
    pad(d.getUTCHours()) +
    pad(d.getUTCMinutes()) +
    pad(d.getUTCSeconds())
  );
}

/**
 * POST /api/files/upload (multipart/form-data)
 *  - file: PDF
 *  - brand / code / series / display_name / family_slug (선택)
 * 동작:
 *  - GCS 업로드
 *  - Cloud Tasks 설정돼 있으면 인입 작업 enqueue
 *  - 없으면 즉시 runAutoIngest 실행
 */
router.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: 'file required' });
    if (!BUCKET) return res.status(500).json({ ok: false, error: 'GCS_BUCKET not set' });

    const originalName = (req.file.originalname || 'datasheet.pdf').replace(/\s+/g, '_');
    const objectName = `datasheets/${Date.now()}_${nowSlug()}__${originalName}`;
    const contentType = req.file.mimetype || 'application/pdf';

    // 1) GCS 업로드
    const bucket = storage.bucket(BUCKET);
    const file = bucket.file(objectName);

    await file.save(req.file.buffer, {
      resumable: false,
      contentType,
      metadata: { cacheControl: 'public, max-age=3600' },
    });

    const gcsUri = `gs://${BUCKET}/${objectName}`;

    // 2) 파라미터 정리
    const payload = {
      gcsUri,
      filename: originalName,
      family_slug: req.body.family_slug || null,
      brand: req.body.brand || null,
      code: req.body.code || null,
      series: req.body.series || null,
      display_name: req.body.display_name || null,
    };

    // 3) Cloud Tasks가 설정되어 있으면 enq, 아니면 즉시 분석
    const queue = process.env.QUEUE_NAME;
    const location = process.env.TASKS_LOCATION;
    const workerUrl = process.env.WORKER_TASK_URL; // e.g. https://worker-xxxx.run.app/api/worker/ingest
    const invokerSA = process.env.TASKS_INVOKER_SA;

    if (queue && location && workerUrl) {
      const client = new CloudTasksClient();
      const parent = client.queuePath(process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT, location, queue);

      const runId = generateRunId();
      const queuePayload = {
        ...payload,
        runId,
        run_id: runId,
        gcs_uri: payload.gcsUri,
      };
      const body = Buffer.from(JSON.stringify(queuePayload)).toString('base64');
      const audience = process.env.WORKER_AUDIENCE || workerUrl;
      const httpRequest = {
        url: workerUrl,
        httpMethod: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
      };
      const task = { httpRequest };
      if (invokerSA) {
        task.httpRequest.oidcToken = { serviceAccountEmail: invokerSA, audience };
      }

      const [resp] = await client.createTask({ parent, task });
      return res.json({ ok: true, gcsUri, enqueued: true, task: resp.name, run_id: runId });
    }

    // fallback: 즉시 실행
    const result = await runAutoIngest(payload);
    return res.json({ ok: true, gcsUri, enqueued: false, result });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

module.exports = router;
