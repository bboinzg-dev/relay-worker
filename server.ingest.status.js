'use strict';
const express = require('express');
const router = express.Router();
const db = require('./src/utils/db'); // 프로젝트 내 db 래퍼

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

async function hasTable(table) {
  const q = `
    select 1 from information_schema.tables
     where table_schema='public' and table_name=$1
  `;
  const { rows } = await db.query(q, [table]);
  return rows.length > 0;
}
async function hasColumn(table, col) {
  const q = `
    select 1 from information_schema.columns
     where table_schema='public' and table_name=$1 and column_name=$2
  `;
  const { rows } = await db.query(q, [table, col]);
  return rows.length > 0;
}

function pickStatusFromRows(rows) {
  // 1) 운영형 스키마(status) 우선
  for (const r of rows) {
    if (r.status) return String(r.status).toUpperCase();
  }
  // 2) 구형(event) 스키마
  if (rows[0]?.event) {
    const ev = String(rows[0].event).toUpperCase();
    if (ev.includes('FAILED')) return 'FAILED';
    if (ev.includes('SUCCEEDED') || ev.includes('DONE')) return 'SUCCEEDED';
    if (ev.includes('PROCESS') || ev.includes('START')) return 'PROCESSING';
  }
  // 3) 시간 기반 추정
  const r0 = rows[0] || {};
  if (r0.finished_at) return 'SUCCEEDED';
  if (r0.started_at && !r0.finished_at) return 'PROCESSING';
  return 'UNKNOWN';
}

async function loadLogsByRunId(runId) {
  // 동적으로 존재하는 컬럼만 SELECT
  const colsWanted = [
    'id','run_id','gcs_uri','status','event','final_table','final_family','final_brand',
    'final_code','final_datasheet','duration_ms','error_message','detail','ts','started_at','finished_at','task_name','retry_count'
  ];
  const present = {};
  for (const c of colsWanted) {
    present[c] = await hasColumn('ingest_run_logs', c);
  }
  const selectList = ['id','run_id']
    .filter(c => present[c])
    .map(c => `"${c}"`)
    .concat(
      colsWanted
        .filter(c => c !== 'id' && c !== 'run_id' && present[c])
        .map(c => `"${c}"`)
    )
    .join(', ');

  // 정렬 컬럼 결정
  const orderBy =
    present['finished_at'] ? '"finished_at" desc nulls last' :
    present['ts']         ? '"ts" desc' :
    present['started_at'] ? '"started_at" desc' :
                            '"id" desc';

  const sql = `
    select ${selectList}
      from public.ingest_run_logs
     where run_id = $1
     order by ${orderBy}
     limit 100
  `;
  const { rows } = await db.query(sql, [runId]);
  return rows;
}

async function loadLogsById(id) {
  // id=UUID가 로그 PK일 수도 있음
  const cols = await db.query(`
    select column_name from information_schema.columns
     where table_schema='public' and table_name='ingest_run_logs'
  `);
  const colset = new Set(cols.rows.map(r => r.column_name));
  const selectList = Array.from(colset).map(c => `"${c}"`).join(', ');

  const { rows } = await db.query(
    `select ${selectList} from public.ingest_run_logs
      where id = $1
      limit 100`, [id]
  );
  return rows;
}

async function loadJobById(id) {
  if (!(await hasTable('ingest_jobs'))) return null;
  const { rows } = await db.query(
    `select id, status, source_type, gcs_pdf_uri, last_error, created_at, updated_at
       from public.ingest_jobs
      where id = $1`, [id]
  );
  return rows[0] || null;
}

router.get('/api/ingest/:key', async (req, res) => {
  const key = String(req.params.key || '').trim();
  if (!key) return res.status(400).json({ ok:false, error:'EMPTY_KEY' });

  try {
    let by = 'job_id';
    let job = null;
    let logs = [];
    let status = 'UNKNOWN';

    if (UUID_RE.test(key)) {
      // 1) run_id 기준
      by = 'run_id';
      logs = await loadLogsByRunId(key);

      // 2) run_id로 없으면 id=UUID 가능성
      if (!logs.length) {
        by = 'log_id';
        const rows = await loadLogsById(key);
        if (rows.length) {
          const r0 = rows[0];
          logs = r0.run_id ? await loadLogsByRunId(r0.run_id) : rows;
          by = r0.run_id ? 'run_id' : 'log_id';
        }
      }
    } else {
      // job id 조회 시도(있을 때만)
      job = await loadJobById(key);
      status = job?.status || 'UNKNOWN';
    }

    // Status 추정
    if (logs.length) {
      status = pickStatusFromRows(logs);
    }

    // 간단 요약
    const counts = { SUCCEEDED:0, FAILED:0, RUNNING:0, OTHER:0 };
    for (const r of logs) {
      const s = (r.status || r.event || '').toString().toUpperCase();
      if (s.includes('SUCCEEDED') || s === 'SUCCEEDED') counts.SUCCEEDED++;
      else if (s.includes('FAILED') || s === 'FAILED') counts.FAILED++;
      else if (s.includes('RUNNING') || s.includes('PROCESS') || s.includes('START')) counts.RUNNING++;
      else counts.OTHER++;
    }

    return res.json({ ok:true, by, key, status, job, counts, logs });
  } catch (e) {
    console.error('[ingest status]', e);
    return res.status(500).json({ ok:false, error:'status_query_failed' });
  }
});

module.exports = router;
