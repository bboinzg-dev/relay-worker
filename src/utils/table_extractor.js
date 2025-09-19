// relay-worker/src/utils/table_extractor.js
'use strict';

/**
 * TYPES/TYPE(S) TABLE 에 "명시적으로 나열된 품번"만 추출하는 범용 추출기
 * - 1순위: Document AI (표 인식)
 * - 2순위: Vertex AI (JSON으로 표 구조만 뽑기)
 * 반환: { rows: [{ type_no, part_no, rated_coil_vdc, notes }], hint: "TYPES"|"ORDERING_INFO"|"" }
 */

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const path = require('path');

// -------- 1) Document AI --------
async function docaiExtractTypes({ gcsUri, projectId, location, processorId }) {
  const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
  const client = new DocumentProcessorServiceClient();

  // GCS에서 PDF 바이너리 다운로드 (단일 파일 처리)
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gcsUri));
  if (!m) throw new Error('INVALID_GCS_URI');
  const [ , bucket, obj ] = m;
  const [buf] = await storage.bucket(bucket).file(obj).download();

  const name = client.processorPath(projectId, location, processorId);
  // rawDocument 로 바로 처리
  const [result] = await client.processDocument({
    name,
    rawDocument: { content: buf, mimeType: 'application/pdf' }
  });

  const doc = result.document;
  const pages = doc?.pages || [];
  const out = [];

  // 각 페이지의 tables[] 순회
  for (const p of pages) {
    for (const t of (p.tables || [])) {
      // 헤더 텍스트 normalize
      const headers = [];
      for (const hr of t.headerRows || []) {
        const cols = [];
        for (const cl of hr.cells || []) cols.push(cl.layout?.textAnchor?.content?.trim() || cl.layout?.text || '');
        headers.push(cols);
      }
      // 가장 긴 헤더 행 기준으로 컬럼명 만들기
      const header = (headers[headers.length-1] || []).map(h => normalizeHeader(h));

      // 바디 파싱
      for (const br of t.bodyRows || []) {
        const cells = [];
        for (const cl of br.cells || []) cells.push(cl.layout?.textAnchor?.content?.trim() || cl.layout?.text || '');
        if (!cells.length) continue;

        // 컬럼명과 매핑
        const row = {};
        for (let i=0;i<Math.min(header.length, cells.length);i++){
          const key = header[i];
          const val = String(cells[i] || '').trim();
          if (!key) continue;
          if (key === 'type_no' || key === 'part_no' || key === 'rated_coil_vdc' || key === 'notes') {
            row[key] = val;
          }
        }

        // 최소 필드가 있으면 row 추가
        if (row.type_no || row.part_no) out.push(row);
      }
    }
  }

  // TYPES/ORDERING 추정 힌트 (간단)
  const hint = (out.length ? 'TYPES' : '');
  return { rows: out, hint };
}

function normalizeHeader(s) {
  const k = String(s || '').toLowerCase().replace(/\s+/g,' ').trim();
  if (/^type\s*no\.?$/.test(k)) return 'type_no';
  if (/^part\s*no\.?$/.test(k)) return 'part_no';
  if (/rated\s*coil\s*voltage/i.test(k) || /coil\s*v(olt)?/i.test(k)) return 'rated_coil_vdc';
  if (/notes?|remark/i.test(k)) return 'notes';
  return '';
}

// -------- 2) Vertex AI (폴백) --------
async function vertexExtractTypes({ callModelJson }, gcsUri) {
  const sys = [
    'Extract explicit part numbers from a catalog table named TYPES / TYPE / TYPES TABLE.',
    'Return strict JSON {"rows":[{"type_no":"","part_no":"","rated_coil_vdc":"","notes":""},...], "hint":"<TYPES|ORDERING_INFO|>"}',
    'Only include part numbers explicitly enumerated in a grid (rows/columns).',
    'If only combinational rules/ordering information with variables exist, return empty list.'
  ].join('\n');

  const usr = JSON.stringify({
    gcs_uri: gcsUri,
    prefer_pages: [1,2,3,4],
    columns: ['Type No.','Part No.','Rated coil voltage','Notes']
  });

  const out = await callModelJson(sys, usr, { maxOutputTokens: 2048 });
  const rows = Array.isArray(out?.rows) ? out.rows : [];
  const norm = rows.map(r => ({
    type_no: String(r?.type_no || '').trim().toUpperCase(),
    part_no: String(r?.part_no || '').trim().toUpperCase(),
    rated_coil_vdc: String(r?.rated_coil_vdc || '').trim(),
    notes: String(r?.notes || '').trim(),
  })).filter(r => r.type_no || r.part_no);

  const hint = String(out?.hint || '').toUpperCase();
  return { rows: norm.slice(0, 300), hint };
}

// -------- 3) 엔트리 포인트 --------
async function extractTypesPreferTable({ gcsUri, projectId, location, processorId, callModelJson }) {
  // 1순위 DocAI
  if (projectId && location && processorId) {
    try {
      const r = await docaiExtractTypes({ gcsUri, projectId, location, processorId });
      if (r?.rows?.length) return { ...r, source: 'docai' };
    } catch (e) {
      console.warn('[types/docai] WARN:', e?.message || e);
    }
  }

  // 2순위 Vertex
  if (callModelJson) {
    try {
      const r = await vertexExtractTypes({ callModelJson }, gcsUri);
      if (r?.rows?.length) return { ...r, source: 'vertex' };
    } catch (e) {
      console.warn('[types/vertex] WARN:', e?.message || e);
    }
  }

  return { rows: [], hint: '', source: null };
}

module.exports = { extractTypesPreferTable };
