'use strict';

const db = require('../utils/db');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');
const { canonicalDatasheetPath, canonicalCoverPath, moveObject } = require('../utils/gcs');
const { extractTypesPreferTable } = require('../utils/table_extractor');
const vx = require('../utils/vertex');
const { chooseBrandCode } = require('../utils/brandcode');

let extractText;
try { ({ extractText } = require('../utils/extract')); } catch { extractText = null; }

function normSlug(s){ return String(s||'').trim().toLowerCase().replace(/[^a-z0-9_]+/g,'_'); }
function safeTmp(uri){ const h=require('crypto').createHash('sha1').update(String(uri||Date.now())).digest('hex').slice(0,6); return `TMP_${h}`.toUpperCase(); }
async function families(){ try{ const r=await db.query('SELECT family_slug FROM public.component_registry ORDER BY 1'); return r.rows.map(x=>x.family_slug); }catch(_){return ['relay_power','relay_signal'];} }
async function blueprint(slug){ try{ const r=await db.query(`SELECT b.fields_json, b.prompt_template, r.specs_table FROM public.component_spec_blueprint b JOIN public.component_registry r USING(family_slug) WHERE b.family_slug=$1 LIMIT 1`,[slug]); return r.rows[0]||null; }catch(_){return null;} }

async function runAutoIngest({ gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null }){
  if (!gcsUri) throw new Error('gcsUri required');
  const tag = `[ingest:${Date.now()}]`;

  // 0) 텍스트(있으면)
  let corpus=''; try{ if (extractText){ const {text,pages}=await extractText(gcsUri); corpus=(text||(pages||[]).map(p=>p.text).join('\n\n')||'').toString(); } } catch(e){ console.warn(tag,'WARN extractText:',e?.message||e); }

  // 1) 브랜드/코드 후보 선택(범용)
  try{
    if ((!brand || !code) && corpus){
      const picked = await chooseBrandCode(corpus);
      brand = brand || picked.brand || '';
      code  = code  || picked.code  || '';
      series = series || picked.series || '';
      display_name = display_name || (series ? `${brand} ${series}`.trim() : brand);
    }
  }catch(e){ console.warn(tag,'WARN chooseBrandCode:',e?.message||e); }

  // 2) 패밀리 분류(없으면)
  try{
    if (!family_slug && vx.classifyFamily && corpus){
      family_slug = await vx.classifyFamily(corpus, await families());
    }
  }catch(e){ console.warn(tag,'WARN classifyFamily:',e?.message||e); }
  family_slug = normSlug(family_slug || 'relay_power');

  // 3) 블루프린트(있으면)
  let fields={}, specs_table=`${family_slug}_specs`, prompt='';
  try{
    const bp = await blueprint(family_slug);
    if (bp){ fields=bp.fields_json||{}; prompt=bp.prompt_template||''; specs_table=bp.specs_table||specs_table; }
  }catch(e){ console.warn(tag,'WARN blueprint:',e?.message||e); }

  // 4) 스펙(있으면)
  let values={};
  try{ if (vx.extractByBlueprint && Object.keys(fields).length){ const o=await vx.extractByBlueprint(gcsUri, fields, prompt); values=o?.values||{}; } } catch(e){ console.warn(tag,'WARN extractByBlueprint:',e?.message||e); }

  // 5) 타입 표 우선 추출 (DocAI→Vertex)
  let list=[]; try{
    const r = await extractTypesPreferTable({
      gcsUri,
      projectId: process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT,
      location: process.env.DOCAI_LOCATION || process.env.DOC_AI_LOCATION || 'us',
      processorId: process.env.DOCAI_PROCESSOR_ID || '',
      callModelJson: vx.callModelJson || null
    });
    list = r.rows || [];
    // “ORDERING” 조합식만 있으면 list는 비게 됨 (정책)
  }catch(e){ console.warn(tag,'WARN typesPreferTable:',e?.message||e); }

  const bucket = (process.env.GCS_BUCKET||'').replace(/^gs:\/\//,'').split('/')[0] || '';

  async function upsertOne(oneCode){
    const b = String(brand||'unknown').trim();
    const c = String(oneCode || code || safeTmp(gcsUri)).trim();

    const datasheet_uri = bucket ? canonicalDatasheetPath(bucket, family_slug, b, c) : null;
    const cover         = bucket ? canonicalCoverPath(bucket, family_slug, b, c) : null;

    const base = {
      brand: b, code: c, brand_norm: b.toLowerCase(), code_norm: c.toLowerCase(),
      series, display_name, family_slug, datasheet_uri, cover, source_gcs_uri: gcsUri, raw_json: { values }
    };

    // ensure table
    try{ await ensureSpecsTable(specs_table, fields); } catch(e){
      console.warn(tag,'WARN ensureSpecsTable:',e?.message||e,'→ relay_power_specs');
      specs_table = 'relay_power_specs'; await ensureSpecsTable(specs_table, {});
    }

    // filter columns
    const filtered={}; try{
      const m=/^(.+)\.(.+)$/.exec(specs_table); const schema=m?m[1]:'public', table=m?m[2]:specs_table;
      const cols=await db.query(`SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,[schema,table]);
      const allow=new Set(cols.rows.map(r=>r.column_name));
      for(const [k,v] of Object.entries({...base,...values})) if(allow.has(k)) filtered[k]=v;
    }catch(e){ console.warn(tag,'WARN allowed cols:',e?.message||e); Object.assign(filtered, base); }

    // upsert
    const row = await upsertByBrandCode(specs_table, filtered).catch(async e=>{
      console.error(tag,'ERROR upsert:',e?.message||e,'→ relay_power_specs');
      specs_table='relay_power_specs'; await ensureSpecsTable(specs_table, {});
      return await upsertByBrandCode(specs_table,{
        brand:base.brand, code:base.code, brand_norm:base.brand_norm, code_norm:base.code_norm,
        family_slug:'relay_power', source_gcs_uri:gcsUri, datasheet_uri:base.datasheet_uri, cover:base.cover,
        series:base.series||null, display_name:base.display_name||null
      });
    });

    // move file
    try{ if (datasheet_uri && /^gs:\/\//i.test(datasheet_uri) && datasheet_uri!==gcsUri) await moveObject(gcsUri, datasheet_uri); } catch(e){ /* warn만 */ }

    return { ok:true, table:specs_table, brand:b, code:c, datasheet_uri, row };
  }

  if (list.length > 0) {
    // TYPES 표에 적힌 품번만 팬아웃
    const seen=new Set(); const results=[];
    for(const r of list){
      const codeCandidate = (r.type_no || r.part_no || '').toUpperCase().trim();
      if (!codeCandidate) continue;
      const sig = `${(brand||'unknown').toLowerCase()}::${codeCandidate}`;
      if (seen.has(sig)) continue; seen.add(sig);
      results.push(await upsertOne(codeCandidate));
    }
    console.log(tag,'OK FANOUT',{count: results.filter(r=>r?.ok).length});
    return { ok:true, fanout:true, count: results.length, results };
  }

  // 단건
  const single = await upsertOne(code);
  console.log(tag,'OK SINGLE',{ brand: single.brand, code: single.code });
  return { ok:true, fanout:false, ...single };
}

module.exports = { runAutoIngest };
