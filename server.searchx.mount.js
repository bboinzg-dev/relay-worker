const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const db = require('./src/utils/db');
const { getSignedUrl, canonicalDatasheetPath, moveObject } = require('./src/utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('./src/utils/schema');
const { runAutoIngest } = require('./src/pipeline/ingestAuto');
const authzGlobal = require('./src/mw/authzGlobal');
const { requestLogger, patchDbLogging, logError } = (()=>{ try { return require('./src/utils/logger'); } catch { return { requestLogger: ()=>((req,res,next)=>next()), patchDbLogging: ()=>{}, logError: ()=>{} }; } })();
const { parseActor } = (()=>{ try { return require('./src/utils/auth'); } catch { return { parseActor: ()=>({}) }; } })();
const { notify, findFamilyForBrandCode } = (()=>{ try { return require('./src/utils/notify'); } catch { return { notify: async()=>({}), findFamilyForBrandCode: async()=>null }; } })();

const app = express();
app.use(requestLogger());
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true }));
const upload = multer({ storage: multer.memoryStorage() });

// Global authorization/tenancy guard
app.use(authzGlobal);

// DB logging
try { const { patchDbLogging } = require('./src/utils/logger'); patchDbLogging(require('./src/utils/db')); } catch {}

const PORT = process.env.PORT || 8080;
const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://') ? GCS_BUCKET_URI.replace(/^gs:\/\//,'').split('/')[0] : '';

// --- health/env ---
app.get('/_healthz', (req, res) => res.type('text/plain').send('ok'));
app.get('/_env', (req, res) => {
  res.json({
    node: process.version,
    gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null,
    has_db: !!process.env.DATABASE_URL,
  });
});

// (omit other mounts for brevity in this patch file)

try { const searchX = require('./server.search2'); app.use(searchX); } catch {}

// 404
app.use((req, res) => res.status(404).json({ error: 'not found' }));

// error guard
app.use((err, req, res, next) => {
  try { require('./src/utils/logger').logError(err, { path: req.originalUrl }); } catch {}
  res.status(500).json({ error: 'internal error' });
});

app.listen(PORT, () => {
  console.log(`worker listening on :${PORT}`);
});
