// relay-worker/src/pipeline/ingestAuto.js
'use strict';

const db = require('../utils/db');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject } = require('../utils/gcs');

let extractText;
try { ({ extractText } = require('../utils/extract')); } catch { extractText = null; }

const vx = require('../utils/vertex');
const { chooseBrandCode } = require('../utils/brandcode');

function normSlug(s){ return String(s||'').trim().toLowerCase().replace(/[^a-z0-9_]+/g,'_'); }
function safeTempCode(uri){
  const h = require('crypto').createHash('sha1').update(String(uri||Date.now())).digest('hex').slice(0,6);
  return `TMP_${h}`.toUpperCase();
}

async function loadFamilies(){
  try{
    const r = await db.query('SELECT DISTINCT family_slug FROM public.component_registry ORDER BY 1');
    return r.rows.map(x=>x.family_slug).filter(Boolean);
  }catch(_){ return ['relay_power']; }
}

async function loadBlueprint(family_slug){
  try{
    const r = await db.query(`
      SELECT b.fields_json, b.prompt_template, r.specs_table
      FROM public.component_spec_blueprint b
      JOIN public.component_registry r USING (family_slug)
      WHERE b.family_slug = $1 LIMIT 1`, [family_slug]);
    return r.rows[0] || null;
  }catch(_){ return null; }
}

async function runAutoIngest({ gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null }){
  if (!gcsUri) throw new Error('gcsUri required');
  const tag = `[ingest:${Date.now()}]`;

  // 0) 텍스트 / 코퍼스
  let corpus = '';
  try{
    if (extractText){
      const { text, pages } = await extractText(gcsUri);
      corpus = (text || (pages||[]).map(p=>p.text).join('\n\n') || '').toString();
    }
  }catch(e){ console.warn(tag, 'WARN extractText:', e?.message||e); }

  // 1) 브랜드/코드 후보 선택(범용)
  try{
    if ((!brand || !code) && corpus){
      const picked = await chooseBrandCode(corpus);
      brand  = brand  || picked.brand  || '';
      code   = code   || picked.code   || '';
      series = series || picked.series || '';
      display_name = display_name || (series ? `${brand} ${series}`.trim() : brand);
    }
  }catch(e){ console.warn(tag,'WARN chooseBrandCode:', e?.message||e); }

  // 2) 가족 분류
  try{
    if (!family_slug){
      const families = await loadFamilies();
      if (vx.classifyFamily && corpus){
        family_slug = await vx.classifyFamily(corpus, families);
      }
    }
  }catch(e){ console.warn(tag,'WARN classifyFamily:', e?.message||e); }
  family_slug = normSlug(family_slug || '') || 'relay_power';

  // 3) 블루프린트 로드
  let fields={}, specs_table=`${family_slug}_specs`, prompt_template='';
  try{
    const bp = await loadBlueprint(family_slug);
    if (bp){ fields = bp.fields_json||{}; prompt_template = bp.prompt_template||''; specs_table = bp.specs_table || specs_table; }
  }catch(e){ console.warn(tag,'WARN loadBlueprint:', e?.message||e); }

  // 4) 스펙 추출(있으면)
  let values={};
  try{
    if (vx.extractByBlueprint && Object.keys(fields||{}).length){
      const out = await vx.extractByBlueprint(gcsUri, fields, prompt_template);
      values = out?.values || {};
    }
  }catch(e){ console.warn(tag,'WARN extractByBlueprint:', e?.message||e); }

  // 5) TYPES 표에서 품번 팬아웃(있으면 그것만, 조합식만 있으면 팬아웃 X)
  let partList=[];
  try{
    const out = await vx.extractPartNumbersFromTypes(gcsUri);
    partList = Array.isArray(out?.parts) ? out.parts : [];
    if (out?.table_hint && out.table_hint.toUpperCase().includes('ORDERING')) partList = []; // 조합식만 있으면 팬아웃 X
  }catch(e){ console.warn(tag,'WARN extractPartNumbersFromTypes:', e?.message||e); }

  // 6) 버킷
  const bucketEnv = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//,'');
  const bucket    = bucketEnv.split('/')[0] || '';

  // 7) 단건 업서트 함수
  async function upsertOne(oneCode){
    const normBrand = String(brand||'unknown').trim();
    const normCode  = String(oneCode || code || safeTempCode(gcsUri)).trim();

    // ✅ 경로는 정규화 이후에
    const datasheet_uri = bucket ? canonicalDatasheetPath(bucket, family_slug, normBrand, normCode) : null;
    const cover         = bucket ? canonicalCoverPath(bucket, family_slug, normBrand, normCode) : null;

    const base = {
      brand: normBrand, code: normCode,
      brand_norm: normBrand.toLowerCase(), code_norm: normCode.toLowerCase(),
      series, display_name, family_slug, datasheet_uri, cover,
      source_gcs_uri: gcsUri, raw_json: { values }
    };

    // 테이블 보장
    try{ await ensureSpecsTable(specs_table, fields||{}); }
    catch(e){
      console.warn(tag,'WARN ensureSpecsTable:', e?.message||e,'→ fallback relay_power_specs');
      specs_table='relay_power_specs'; await ensureSpecsTable(specs_table,{});
    }

    // 허용컬럼 필터
    const filtered={};
    try{
      const m=/^(.+)\.(.+)$/.exec(specs_table); const schema=m?m[1]:'public', table=m?m[2]:specs_table;
      const rows=await db.query(`SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,[schema,table]);
      const allowed=new Set(rows.rows.map(r=>r.column_name));
      for(const [k,v] of Object.entries({...base,...(values||{})})) if(allowed.has(k)) filtered[k]=v;
    }catch(e){ console.warn(tag,'WARN allowed cols:', e?.message||e); Object.assign(filtered, base); }

    // 업서트
    let row;
    try{ row=await upsertByBrandCode(specs_table, filtered); }
    catch(e){
      console.error(tag,'ERROR upsert:', e?.message||e,'→ fallback relay_power_specs');
      try{
        specs_table='relay_power_specs'; await ensureSpecsTable(specs_table,{});
        row=await upsertByBrandCode(specs_table,{
          brand:base.brand, code:base.code, brand_norm:base.brand_norm, code_norm:base.code_norm,
          family_slug:'relay_power', source_gcs_uri:gcsUri, datasheet_uri:base.datasheet_uri, cover:base.cover,
          series:base.series||null, display_name:base.display_name||null
        });
      }catch(e2){ console.error(tag,'ERROR upsert fallback:', e2?.message||e2); return { ok:false, error:String(e2?.message||e2) }; }
    }

    try{ if (datasheet_uri && /^gs:\/\//i.test(datasheet_uri) && datasheet_uri!==gcsUri) await moveObject(gcsUri, datasheet_uri); }
    catch(e){ console.warn(tag,'WARN moveObject:', e?.message||e); }

    return { ok:true, table: specs_table, brand: base.brand, code: base.code, datasheet_uri, row };
  }

  // 8) 팬아웃 or 단건
  if (partList.length>0){
    const seen=new Set(), results=[];
    for(const pn of partList){
      const k=`${(brand||'unknown').toLowerCase()}::${pn.toUpperCase()}`;
      if(seen.has(k)) continue; seen.add(k);
      results.push(await upsertOne(pn));
    }
    console.log(tag,'OK FANOUT',{count: results.filter(r=>r?.ok).length});
    return { ok:true, fanout:true, count: results.length, results };
  }

  const single = await upsertOne(code);
  console.log(tag,'OK SINGLE',{ brand: single.brand, code: single.code });
  return { ok:true, fanout:false, ...single };
}

module.exports = { runAutoIngest };
