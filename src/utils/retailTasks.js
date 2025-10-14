const { CloudTasksClient } = require('@google-cloud/tasks');

const client = new CloudTasksClient();

function queuePathFromEnv() {
  const project = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  if (!project) throw new Error('GCP_PROJECT_ID or GOOGLE_CLOUD_PROJECT env not set');
  const location = process.env.TASKS_LOCATION || 'asia-northeast3';
  const queueId =
    process.env.RETAIL_QUEUE_ID ||
    process.env.QUEUE_ID ||
    `projects/${project}/locations/${location}/queues/retail-index`;

  const parent = queueId.startsWith('projects/')
    ? queueId
    : `projects/${project}/locations/${location}/queues/${queueId}`;
  return { parent, project, location };
}

function sanitizeForTaskName(value) {
  return value.replace(/[^A-Za-z0-9_-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '') || 'all';
}

async function enqueueRetailImport({ since = '5m', delaySec = 90 } = {}) {
  const { parent } = queuePathFromEnv();
  const baseUrl = process.env.WORKER_TASK_URL || process.env.WORKER_AUDIENCE || '';
  if (!baseUrl) {
    throw new Error('WORKER_TASK_URL or WORKER_AUDIENCE env not set');
  }
  const url = `${baseUrl.replace(/\/$/, '')}/api/retail/import?since=${encodeURIComponent(since)}`;
  const serviceAccountEmail = process.env.TASKS_INVOKER_SA;

  const slot = Math.floor(Date.now() / 60000);
  const taskName = `${parent}/tasks/retail-import-${sanitizeForTaskName(since)}-${slot}`;

  const httpRequest = {
    httpMethod: 'POST',
    url,
    headers: { 'Content-Type': 'application/json' },
    body: Buffer.from('{}').toString('base64'),
  };

  const audience = process.env.WORKER_AUDIENCE || baseUrl;
  if (serviceAccountEmail) {
    httpRequest.oidcToken = { serviceAccountEmail, audience };
  }

  const task = {
    name: taskName,
    httpRequest,
    scheduleTime: { seconds: Math.floor(Date.now() / 1000) + Number(delaySec || 0) },
  };

  try {
    await client.createTask({ parent, task });
    return { enqueued: true, task: taskName };
  } catch (e) {
    if (String(e.message || e).includes('ALREADY_EXISTS')) {
      return { enqueued: false, deduped: true, task: taskName };
    }
    throw e;
  }
}

module.exports = { enqueueRetailImport };