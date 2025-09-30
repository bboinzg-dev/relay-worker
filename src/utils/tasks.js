const { CloudTasksClient } = require('@google-cloud/tasks');
const env = require('../config/env');

const PROJECT = env.PROJECT_ID;
const LOCATION = env.TASKS_LOCATION;
const QUEUE = env.NOTIFY_QUEUE_NAME || 'notify-queue';
const HANDLER_URL = env.NOTIFY_HANDLER_URL;
const SA_EMAIL = env.TASKS_SERVICE_ACCOUNT_EMAIL;

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
