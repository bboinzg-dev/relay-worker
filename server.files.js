const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { getSignedUrl } = require('./src/utils/gcsSignedUrl');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '4mb' }));

app.get('/api/files/signed-url', async (req, res) => {
  try {
    const gcs = (req.query.gcs || '').toString();
    if (!gcs) return res.status(400).json({ error: 'gcs query required' });
    const ttl = Math.max(60, Math.min(3600, parseInt(req.query.ttl || '1200', 10)));
    const out = await getSignedUrl(gcs, { expiresSec: ttl });
    res.setHeader('cache-control', 'private, max-age=300');
    res.json(out);
  } catch (e) {
    console.error(e); res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
