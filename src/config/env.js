'use strict';

function req(name) {
  const v = process.env[name];
  if (!v) throw new Error(`ENV ${name} is required`);
  return v;
}
function opt(name, def = undefined) {
  const v = process.env[name];
  return (v === undefined || v === '') ? def : v;
}
function bucketName(v) {
  if (!v) return v;
  if (v.startsWith('gs://')) return v.replace(/^gs:\/\//, '');
  return v;
}

module.exports = {
  PROJECT_ID:          opt('GCP_PROJECT_ID', opt('GOOGLE_CLOUD_PROJECT')),
  VERTEX_LOCATION:     req('VERTEX_LOCATION'),
  DOCAI_PROJECT_ID:    opt('DOCAI_PROJECT_ID', opt('GCP_PROJECT_ID', opt('GOOGLE_CLOUD_PROJECT'))),
  DOCAI_LOCATION:      opt('DOCAI_LOCATION', 'us'),
  DOCAI_PROCESSOR_ID:  req('DOCAI_PROCESSOR_ID'),
  GCS_BUCKET:          bucketName(req('GCS_BUCKET')),
  RESULT_BUCKET:       bucketName(opt('RESULT_BUCKET', req('GCS_BUCKET'))),
  DOCAI_OUTPUT_URI:    opt('DOCAI_OUTPUT_BUCKET'),
  QUEUE_NAME:          req('QUEUE_NAME'),
  TASKS_LOCATION:      req('TASKS_LOCATION'),
  WORKER_TASK_URL:     req('WORKER_TASK_URL'),
  MAX_DOC_PAGES_INLINE: parseInt(opt('MAX_DOC_PAGES_INLINE', '15'), 10),
  GEMINI_MODEL_CLASSIFY: req('GEMINI_MODEL_CLASSIFY'),
  GEMINI_MODEL_EXTRACT:  req('GEMINI_MODEL_EXTRACT'),
  VERTEX_PAGE_PICK_MODEL: opt('VERTEX_PAGE_PICK_MODEL', 'gemini-2.5-flash'),
  VERTEX_EMBED_TEXT:      opt('VERTEX_EMBED_TEXT', 'text-embedding-004'),
};
