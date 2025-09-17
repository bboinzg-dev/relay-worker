// relay-worker/src/utils/family.js
// ✅ CommonJS, 함수 내보내기 명확히
function normalizeFamilySlug(s) {
  const k = (s || '').toString().trim().toLowerCase();
  const map = {
    // relay 계열
    'relay': 'relay_power',
    'power relay': 'relay_power',
    'reed relay': 'relay_reed',
    'automotive relay': 'relay_automotive',
    'car relay': 'relay_automotive',
    'solid state relay': 'relay_ssr',
    'ssr': 'relay_ssr',
    'signal relay': 'relay_signal',
    // 수동소자
    'mlcc': 'capacitor_mlcc',
    'ceramic': 'capacitor_mlcc',
  };
  return map[k] || k;
}

function chooseCanonicalFamilySlug(raw, families = []) {
  const k = (raw || '').toString().trim().toLowerCase();
  const set = new Set(families.map(f => f.family_slug || f)); // 레지스트리 배열/문자열 둘 다 허용
  if (set.has(k)) return k;
  // 키워드 휴리스틱(등록된 가족 중에서만 선택)
  const has = (re) => new RegExp(re, 'i').test(k);
  const pick = (...cands) => cands.find(c => set.has(c)) || null;
  if (has('\\brelay\\b')) {
    if (has('signal|telecom')) return pick('relay_signal','relay_reed','relay_power');
    if (has('\\breed\\b'))     return pick('relay_reed','relay_signal','relay_power');
    if (has('auto|vehicle|car')) return pick('relay_automotive','relay_power');
    if (has('ssr|solid state'))  return pick('relay_ssr');
    return pick('relay_power');
  }
  if (has('mlcc|ceramic'))       return pick('capacitor_mlcc','capacitor_film','capacitor_elec');
  if (has('electrolytic'))       return pick('capacitor_elec','capacitor_film');
  if (has('film'))               return pick('capacitor_film','capacitor_elec');
  if (has('resistor|chip res'))  return pick('resistor_chip');
  if (has('mosfet'))             return pick('mosfet');
  if (has('diode|rectifier|schottky')) return pick('diode_rectifier');
  if (has('\\bigbt\\b|igbt module'))   return pick('igbt_module');
  return null; // 모호하면 선택 안 함
}

module.exports = { normalizeFamilySlug, chooseCanonicalFamilySlug };
