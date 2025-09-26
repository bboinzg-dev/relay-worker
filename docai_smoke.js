const { batchProcess } = require('./docai_client');
const { extractRelayFields } = require('./docai_parse');
const { Storage } = require('@google-cloud/storage');
const { safeJsonParse } = require('./src/utils/safe-json');
const storage = new Storage();
function parseGs(gs){ const s=gs.replace('gs://',''); const i=s.indexOf('/'); return {bucket:s.slice(0,i), name:s.slice(i+1)}; }
(async ()=>{
  try{
    const input = process.argv[2];
    if(!input){ console.error('Usage: node docai_smoke.js gs://bucket/file.pdf'); process.exit(1); }
    const outPrefix = `gs://${process.env.GCS_BUCKET_DOCAI || 'partsplan-docai-us'}/out/${Date.now()}/`;
    const { outputs } = await batchProcess(input, outPrefix);
    console.log('DocAI outputs:', outputs);
    const first = outputs.find(u => u.endsWith('.json'));
    const {bucket,name} = parseGs(first);
    const [buf] = await storage.bucket(bucket).file(name).download();
    const doc = safeJsonParse(buf.toString('utf8'));
    console.log('Extracted:', extractRelayFields(doc));
  } catch(e){ console.error(e); process.exit(2); }
})();
