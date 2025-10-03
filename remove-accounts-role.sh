#!/usr/bin/env bash
set -euo pipefail
shopt -s globstar nullglob

MODE="${1:---preview}"
inc=( "*.js" "*.ts" "*.tsx" )

find_files() {
  grep -RIl --include="${inc[0]}" --include="${inc[1]}" --include="${inc[2]}" 'accounts\.role' || true
}

scan() {
  echo "== [SCAN] occurrences of 'accounts.role' =="
  find_files | sed 's/^/  - /'
  echo
  echo "TIP) 위 파일들이 SQL SELECT 목록 혹은 객체매핑에서 role을 참조합니다."
}

apply() {
  local files
  files=$(find_files)
  if [ -z "$files" ]; then
    echo "적용할 항목 없음."; return 0
  fi
  echo "== [APPLY] patching files =="

  for f in $files; do
    cp "$f" "$f.bak.accounts-role" || true
    # 1) SQL SELECT 에서 'accounts.role AS role' 또는 'a.role AS role' 제거
    sed -E -i 's/([,[:space:]])accounts\.role[[:space:]]+AS[[:space:]]+role[[:space:]]*,?/\1/gI' "$f"
    sed -E -i 's/([,[:space:]])a\.role[[:space:]]+AS[[:space:]]+role[[:space:]]*,?/\1/gI' "$f"

    # 2) 혹시 남아있는 'accounts.role' 단독 참조 제거(콤마 처리)
    sed -E -i 's/([,[:space:]])accounts\.role([,[:space:]]|$)/\1/gI' "$f"

    # 3) 객체 매핑에서 'role: xxx.role' → users 기반으로 유도
    sed -E -i "s/role:\s*([A-Za-z0-9_]+)\.role/role: (\1.is_seller ? 'seller' : 'buyer')/g" "$f"
  done

  echo "== [DIFF] 확인 =="
  git diff -- . ':(exclude)*.bak.accounts-role' || true
  echo
  echo "변경사항을 꼭 확인한 뒤 커밋하세요."
}

case "$MODE" in
  --preview) scan ;;
  --apply)   apply ;;
  *) echo "usage: $0 [--preview|--apply]"; exit 2 ;;
esac
