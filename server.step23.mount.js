const express = require('express');
const app = express();
try { const admin = require('./server.admin'); app.use(admin); } catch {}
module.exports = app;
