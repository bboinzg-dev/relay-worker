#!/usr/bin/env bash
set -euo pipefail
shopt -s globstar nullglob

files=$(grep -RIl --include="*.js" --include="*.ts" --include="*.tsx" 'accounts\.role' || true)
if [ -z "$files" ]; then
  echo "[OK] accounts.role 참조 없음 (변경 불필요)"
  exit 0
fi

for f in $files; do
  cp "$f" "$f.bak.accounts-role" || true
  # SELECT 목록에서 accounts.role AS role / a.role AS role 제거
  sed -E -i 's/([,[:space:]])accounts\.role[[:space:]]+AS[[:space:]]+role[[:space:]]*,?/\1/gI' "$f"
  sed -E -i 's/([,[:space:]])a\.role[[:space:]]+AS[[:space:]]+role[[:space:]]*,?/\1/gI' "$f"
  # 단독 참조 제거(콤마/공백 정리)
  sed -E -i 's/([,[:space:]])accounts\.role([,[:space:]]|$)/\1/gI' "$f"
  # 객체 매핑 role: row.role → users 기반으로 유도
  sed -E -i "s/role:\s*([A-Za-z0-9_]+)\.role/role: (\1.is_seller ? 'seller' : 'buyer')/g" "$f"
done

echo "[DIFF] 이후 변경 목록:"
git status --porcelain
