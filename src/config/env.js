'use strict';

// 필수: 없으면 바로 throw
function req(name) {
  const v = process.env[name];
  if (!v) throw new Error(`ENV ${name} is required`);
  return v;
}

// 선택: 기본값 허용
function opt(name, def = undefined) {
  const v = process.env[name];
  return (v === undefined || v === '') ? def : v;
}

/** GCS 버킷명만 허용 (gs:// 제거) */
function bucketName(v) {
  if (!v) return v;
  if (v.startsWith('gs://')) return v.replace(/^gs:\/\//, '');
  return v;
}

module.exports = {
  // 프로젝트/리전
  PROJECT_ID:         opt('GCP_PROJECT_ID', opt('GOOGLE_CLOUD_PROJECT')),
  VERTEX_LOCATION:    req('VERTEX_LOCATION'),
  // DocAI (철자 혼선 통일)
  DOCAI_PROJECT_ID:   opt('DOCAI_PROJECT_ID', opt('GCP_PROJECT_ID', opt('GOOGLE_CLOUD_PROJECT'))),
  DOCAI_LOCATION:     opt('DOCAI_LOCATION', 'us'),
  DOCAI_PROCESSOR_ID: req('DOCAI_PROCESSOR_ID'),
  // GCS
  GCS_BUCKET:         bucketName(req('GCS_BUCKET')),            // 업로드/원본
  RESULT_BUCKET:      bucketName(opt('RESULT_BUCKET', req('GCS_BUCKET'))), // 결과 저장(이름)
  DOCAI_OUTPUT_URI:   opt('DOCAI_OUTPUT_BUCKET'),                // gs://… (필요 시)
  // Tasks
  QUEUE_NAME:         req('QUEUE_NAME'),
  TASKS_LOCATION:     req('TASKS_LOCATION'),
  WORKER_TASK_URL:    req('WORKER_TASK_URL'),
  // 추출/기타
  MAX_DOC_PAGES_INLINE: parseInt(opt('MAX_DOC_PAGES_INLINE', '15'), 10),
  GEMINI_MODEL_CLASSIFY: req('GEMINI_MODEL_CLASSIFY'),
  GEMINI_MODEL_EXTRACT:  req('GEMINI_MODEL_EXTRACT'),
  VERTEX_PAGE_PICK_MODEL: opt('VERTEX_PAGE_PICK_MODEL', 'gemini-2.5-flash'),
  VERTEX_EMBED_TEXT:      opt('VERTEX_EMBED_TEXT', 'text-embedding-004'),
};
