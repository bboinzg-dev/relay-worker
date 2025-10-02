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
  try { return new URL(url).origin; } catch { return process.env.WORKER_AUDIENCE || process.env.TASK_AUDIENCE || process.env.TASKS_AUDIENCE; }
}

async function enqueueIngest({ gcsPdfUri, sourceType='manual', dryRun=false }) {
  const project  = resolveProject();
  const location = process.env.TASKS_LOCATION || 'asia-northeast3';
  const queue    = process.env.QUEUE_NAME || 'ingest-queue';

  const url = process.env.WORKER_TASK_URL
           || process.env.TASK_URL
           || process.env.TASK_TARGET_URL;

  if (!project || !url) {
    throw new Error(`enqueue config missing: project=${project}, url=${url}`);
  }

  const parent  = client.queuePath(project, location, queue);
  const saEmail =
    process.env.TASKS_INVOKER_SA
    || process.env.TASKS_INVOKER_SA_EMAIL
    || process.env.TASKS_SERVICE_ACCOUNT_EMAIL
    || process.env.GOOGLE_SERVICE_ACCOUNT
    || process.env.K_SERVICE_ACCOUNT
    || process.env.SERVICE_ACCOUNT_EMAIL
    || process.env.K_SERVICE && `${project}@appspot.gserviceaccount.com`;

  const audience = resolveAudience(url);

  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url,
      headers: { 'Content-Type': 'application/json' },
      body: Buffer.from(JSON.stringify({ gcsPdfUri, sourceType, dryRun })).toString('base64'),
      oidcToken: { serviceAccountEmail: saEmail, audience },
    },
  };

  const [resp] = await client.createTask({ parent, task });
  return resp.name;
}

module.exports = { enqueueIngest };
