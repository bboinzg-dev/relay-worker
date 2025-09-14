function specTextSignature(row = {}) {
  const parts = [];
  for (const k of ['brand','code','series','display_name','family_slug','contact_form','contact_rating_text','mounting_type','package_type']) {
    if (row[k]) parts.push(`${k}:${row[k]}`);
  }
  for (const k of Object.keys(row)) {
    if (/^dim_|_vdc$|_ohm$|_ma$|_v$|_c$/.test(k) && row[k] != null) {
      parts.push(`${k}:${row[k]}`);
    }
  }
  return parts.join(' | ');
}
module.exports = { specTextSignature };
