function getText(doc, layoutRef){
  if(!layoutRef || !doc.text) return null;
  const { textAnchor } = layoutRef;
  if(!textAnchor || !Array.isArray(textAnchor.textSegments)) return null;
  let out='';
  for(const seg of textAnchor.textSegments){
    const start = parseInt(seg.startIndex||0,10);
    const end = parseInt(seg.endIndex,10);
    out += doc.text.substring(start, end);
  }
  return out.trim();
}

function extractKeyMap(doc){
  const map = {};
  for(const p of (doc.pages||[])){
    for(const f of (p.formFields||[])){
      const k = getText(doc, f.fieldName);
      const v = getText(doc, f.fieldValue);
      if(k && v) map[normalizeKey(k)] = v;
    }
  }
  return map;
}
function normalizeKey(k){
  return String(k).toLowerCase().replace(/\s+/g,' ').replace(/[^a-z0-9%./ ]/g,'').trim();
}
function parseNumber(v){
  if(!v) return null;
  const m = String(v).replace(/[, ]/g,'').match(/([0-9]+(?:\.[0-9]+)?)/);
  return m ? Number(m[1]) : null;
}

function extractRelayFields(doc){
  const kv = extractKeyMap(doc);
  const brand = kv['manufacturer'] || kv['brand'] || null;
  const series = kv['series'] || null;
  const code = kv['part number'] || kv['pn'] || kv['model'] || null;
  const contactForm = kv['contact form'] || kv['contact configuration'] || kv['form'] || null;
  const contactRatingA = kv['contact rating'] || kv['contact current'] || null;
  const coilV = kv['coil voltage'] || kv['coil rated voltage'] || null;

  return {
    brand, series, code,
    contact_form: contactForm,
    contact_rating_a: contactRatingA,
    coil_voltage_vdc: parseNumber(coilV),
    dim_l_mm: parseNumber(kv['length'] || kv['dimension l']),
    dim_w_mm: parseNumber(kv['width']  || kv['dimension w']),
    dim_h_mm: parseNumber(kv['height'] || kv['dimension h']),
    contact_rating_text: contactRatingA || null,
    raw_json: doc
  };
}

module.exports = { extractRelayFields };
