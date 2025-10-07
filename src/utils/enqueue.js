'use strict';
const { CloudTasksClient } = require('@google-cloud/tasks');
const client = new CloudTasksClient();

function resolveProject() {
  return process.env.GCP_PROJECT_ID
      || process.env.GOOGLE_CLOUD_PROJECT
      || process.env.PROJECT_ID
      || process.env.PROJECT
      || process.env.GCLOUD_PROJECT;
}
function resolveAudience(url) {
  try { return new URL(url).origin; } catch {
    return process.env.WORKER_AUDIENCE
        || process.env.TASK_AUDIENCE
        || process.env.TASKS_AUDIENCE;
  }
}

/** Cloud Tasks에 OIDC HTTP 태스크 생성 (ENV 동의어 넓게 지원) */
async function enqueueIngest(payload = {}) {
  const project  = resolveProject();
  const location = process.env.TASKS_LOCATION || 'asia-northeast3';
  const queue    = process.env.QUEUE_NAME     || 'ingest-queue';

  const url =
    process.env.WORKER_TASK_URL ||
    process.env.TASK_URL ||
    process.env.TASK_TARGET_URL;

  if (!project || !url) throw new Error(`enqueue config missing: project=${project}, url=${url}`);

  const parent  = client.queuePath(project, location, queue);
  const saEmail =
    process.env.TASKS_INVOKER_SA ||
    process.env.TASKS_INVOKER_SA_EMAIL ||
    process.env.TASKS_SERVICE_ACCOUNT_EMAIL;

  if (!saEmail) throw new Error('TASKS_INVOKER_SA not set');

  const audience = resolveAudience(url);
  const body     = Buffer.from(JSON.stringify(payload)).toString('base64');

  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url,
      headers: { 'Content-Type': 'application/json' },
      body,
      oidcToken: { serviceAccountEmail: saEmail, audience },
    },
  };

  const raw = process.env.TASKS_DISPATCH_DEADLINE || process.env.TASKS_DEADLINE_SEC;
  const parsed = Number.parseFloat(typeof raw === 'string' ? raw.replace(/s$/i, '') : raw);
  const deadlineSeconds = Math.min(
    Math.max(Number.isFinite(parsed) ? parsed : 900, 30),
    1800
  );
  task.dispatchDeadline = { seconds: Math.ceil(deadlineSeconds), nanos: 0 };

  const [resp] = await client.createTask({ parent, task });
  return resp.name;
}

module.exports = { enqueueIngest };
