const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const db = require('./src/utils/db');
const authzGlobal = require('./src/mw/authzGlobal');
const { requestLogger } = (()=>{ try { return require('./src/utils/logger'); } catch { return { requestLogger: ()=>((req,res,next)=>next()) }; } })();

const app = express();
app.use(requestLogger());
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));
const upload = multer({ storage: multer.memoryStorage() });

// Global authorization
app.use(authzGlobal);

try { const checkout = require('./server.checkout'); app.use(checkout); } catch {}
try { const orders = require('./server.orders'); app.use(orders); } catch {}
try { const payments = require('./server.payments'); app.use(payments); } catch {}

app.get('/_healthz', (req, res)=>res.type('text/plain').send('ok'));
app.use((req,res)=>res.status(404).json({ error: 'not found' }));
app.listen(process.env.PORT || 8080, ()=>console.log('worker patched mounts (step22)'));
