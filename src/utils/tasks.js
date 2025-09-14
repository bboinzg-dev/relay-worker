const { CloudTasksClient } = require('@google-cloud/tasks');

const PROJECT = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION = process.env.TASKS_LOCATION || process.env.VERTEX_LOCATION || 'us-central1';
const QUEUE = process.env.TASKS_NOTIFY_QUEUE || 'notify-queue';
const HANDLER_URL = process.env.NOTIFY_HANDLER_URL; // e.g., https://worker-xxxxx.run.app/_tasks/notify
const SA_EMAIL = process.env.TASKS_SERVICE_ACCOUNT_EMAIL; // has roles/run.invoker on worker

const client = new CloudTasksClient();

function queuePath() {
  return client.queuePath(PROJECT, LOCATION, QUEUE);
}

async function enqueueNotify(jobId, { scheduleInSec = 0 } = {}) {
  if (!HANDLER_URL) throw new Error('NOTIFY_HANDLER_URL env not set');
  const body = Buffer.from(JSON.stringify({ job_id: jobId })).toString('base64');
  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url: HANDLER_URL,
      headers: { 'content-type': 'application/json' },
      body,
    },
  };
  if (SA_EMAIL) {
    task.httpRequest.oidcToken = { serviceAccountEmail: SA_EMAIL, audience: HANDLER_URL };
  }
  if (scheduleInSec > 0) {
    const ts = new Date(Date.now() + scheduleInSec * 1000);
    task.scheduleTime = { seconds: Math.floor(ts.getTime() / 1000) };
  }
  const [resp] = await client.createTask({ parent: queuePath(), task });
  return resp.name;
}

module.exports = { enqueueNotify };
