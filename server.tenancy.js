const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { parseActor } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '2mb' }));

app.get('/_whoami', (req, res) => {
  const actor = parseActor(req);
  res.json({ actor, headers: { 'x-actor-id': req.headers['x-actor-id'] || null, 'x-actor-tenant': req.headers['x-actor-tenant'] || null, 'x-actor-roles': req.headers['x-actor-roles'] || null } });
});

module.exports = app;
