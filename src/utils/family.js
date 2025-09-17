// normalize family slugs to our canonical ones
function normalizeFamilySlug(s) {
  const k = (s || '').toString().trim().toLowerCase();
  const map = {
    // relay family split
    'relay': 'relay_power',
    'power relay': 'relay_power',
    'reed relay': 'relay_reed',
    'automotive relay': 'relay_automotive',
    'car relay': 'relay_automotive',
    'solid state relay': 'relay_ssr',
    'ssr': 'relay_ssr',
    // you can add more aliases later (e.g., mlcc → capacitor_mlcc)
  };
  return map[k] || k;
}
module.exports = { normalizeFamilySlug };
