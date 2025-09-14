async function sendEmail(to, subject, text){
  // 스텁: 실제 SMTP/SendGrid 연결 대신 콘솔 출력
  console.log('[email] to=%s subject=%s\n%s', to, subject, text);
  return { ok: true };
}
async function sendWebhook(url, payload){
  const r = await fetch(url, { method:'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload) });
  if (!r.ok) return { ok: false, error: await r.text() };
  return { ok: true };
}
module.exports = { sendEmail, sendWebhook };
