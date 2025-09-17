// 가족 후보 중 최적의 canonical slug 고르기 (레지스트리 목록에 존재하는 것만 반환)
function chooseCanonicalFamilySlug(raw, families = []) {
  const k = (raw || '').toString().trim().toLowerCase();
  const set = new Set(families.map(f => f.family_slug));     // DB에 실제 등록된 가족만

  if (set.has(k)) return k;                                  // 1) 정확 일치

  // 2) 키워드 기반 휴리스틱 (등록된 가족 중에서만 선택)
  const has = r => new RegExp(r, 'i').test(k);
  const pick = (...cands) => cands.find(c => set.has(c)) || null;

  // 릴레이
  if (has('\\brelay\\b')) {
    if (has('signal|telecom')) return pick('relay_signal','relay_reed','relay_power');
    if (has('\\breed\\b'))     return pick('relay_reed','relay_signal','relay_power');
    if (has('auto|vehicle|car')) return pick('relay_automotive','relay_power');
    if (has('ssr|solid state'))  return pick('relay_ssr');
    return pick('relay_power'); // 기본값
  }

  // 수동소자
  if (has('mlcc|ceramic'))       return pick('capacitor_mlcc','capacitor_film','capacitor_elec');
  if (has('electrolytic'))       return pick('capacitor_elec','capacitor_film');
  if (has('film'))               return pick('capacitor_film','capacitor_elec');
  if (has('resistor|chip res'))  return pick('resistor_chip');

  // 반도체
  if (has('mosfet'))             return pick('mosfet');
  if (has('diode|rectifier|schottky')) return pick('diode_rectifier');
  if (has('\\bigbt\\b|igbt module'))   return pick('igbt_module');

  return null; // 매칭 실패 → 안전하게 중단/리뷰
}

module.exports = { chooseCanonicalFamilySlug };
