'use strict';
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const path = require('path');

const DOCAI_PROJECT_ID   = process.env.DOCAI_PROJECT_ID || process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const DOCAI_LOCATION     = process.env.DOCAI_LOCATION   || 'us';
const DOCAI_PROCESSOR_ID = process.env.DOCAI_PROCESSOR_ID || '';
const DOCAI_OUTPUT_BUCKET= (process.env.DOCAI_OUTPUT_BUCKET || '').replace(/^gs:\/\//,'');
const MAX_INLINE         = Number(process.env.MAX_DOC_PAGES_INLINE || 15);

function isGsUri(u){ return /^gs:\/\//i.test(String(u||'')); }
function gsParse(u){ const m=/^gs:\/\/([^/]+)\/(.+)$/.exec(String(u||'')); if(!m)throw new Error('INVALID_GCS_URI'); return {bucket:m[1],object:m[2]}; }
const joinText = (pages=[]) => pages.map(p=>p.text||'').filter(Boolean).join('\n\n');

async function vertexPickPages(gcsUri){
  try{
    const { callModelJson } = require('./vertex');
    const sys = 'Select up to 12 page numbers that likely contain TYPES / ORDERING / SPEC tables. Return {"pages":[1,3,...]}.';
    const usr = JSON.stringify({ gcs_uri: gcsUri, hint: ['TYPES','ORDERING','SPECIFICATIONS'] });
    const out = await callModelJson(sys, usr, { maxOutputTokens: 1024 });
    return [...new Set((out?.pages||[]).map(n=>Number(n)).filter(n=>n>0))].sort((a,b)=>a-b).slice(0,12);
  }catch(e){ console.warn('[extract] vertexPickPages WARN:', e?.message||e); return []; }
}

async function docaiOnline({ gcsUri, pages=[] }){
  const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
  const client = new DocumentProcessorServiceClient();
  if(!DOCAI_PROCESSOR_ID) throw new Error('DOCAI_PROCESSOR_ID not set');

  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);
  const { bucket, object } = gsParse(gcsUri);
  const [buf] = await storage.bucket(bucket).file(object).download();

  let req;
  try{
    req = { name, rawDocument:{ content:buf, mimeType:'application/pdf' },
            processOptions: pages.length ? { individualPageSelector:{ pages: pages.map(String) } } : {} };
  }catch(_){ req = { name, rawDocument:{ content:buf, mimeType:'application/pdf' } }; }

  const [result] = await client.processDocument(req);
  const doc = result.document;
  const outPages = (doc?.pages||[]).map((p,i)=>({ page: p.pageNumber || (pages[i]||i+1),
                                                  text: p.layout?.textAnchor?.content || p.layout?.text || '' }));
  return { pages: outPages, text: joinText(outPages) };
}

async function docaiBatch({ gcsUri }){
  const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
  const client = new DocumentProcessorServiceClient();
  if(!DOCAI_PROCESSOR_ID) throw new Error('DOCAI_PROCESSOR_ID not set');
  if(!DOCAI_OUTPUT_BUCKET) throw new Error('DOCAI_OUTPUT_BUCKET not set');

  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);
  const outputPrefix = path.posix.join(DOCAI_OUTPUT_BUCKET.replace(/^\//,''), Date.now().toString());
  const [op] = await client.batchProcessDocuments({
    name,
    inputDocuments:{ gcsDocuments:{ documents:[{ gcsUri, mimeType:'application/pdf' }] } },
    documentOutputConfig:{ gcsOutputConfig:{ gcsUri:`gs://${outputPrefix}/` } }
  });
  await op.promise();

  const [files] = await storage.bucket(DOCAI_OUTPUT_BUCKET.split('/')[0])
    .getFiles({ prefix: outputPrefix });
  const jsons = files.filter(f=>f.name.endsWith('.json'));
  const pagesOut=[];
  for(const jf of jsons){
    const [buf]=await jf.download();
    const parsed=JSON.parse(buf.toString('utf8'));
    const doc=parsed.document||parsed;
    pagesOut.push(...(doc.pages||[]).map((p,i)=>({ page:p.pageNumber||i+1,
                                                  text:p.layout?.textAnchor?.content||p.layout?.text||'' })));
  }
  return { pages: pagesOut, text: joinText(pagesOut) };
}

async function extractText(gcsUri){
  if(!isGsUri(gcsUri)) throw new Error('gcsUri must be gs://…');
  const picked = await vertexPickPages(gcsUri);
  if(picked.length && picked.length<=MAX_INLINE){
    try{ return await docaiOnline({ gcsUri, pages: picked }); } catch(e){ console.warn('[extract] online WARN:',e?.message||e); }
  }
  try{ return await docaiBatch({ gcsUri }); } catch(e){ console.warn('[extract] batch WARN:',e?.message||e); }
  return { text:'', pages:[] };
}

module.exports = { extractText };
