# Step 27 — PDF → PNG 썸네일 렌더 파이프라인

## 개요
- Poppler(`pdftoppm`)을 이용해 데이터시트 1페이지를 **cover.png**로 생성
- 이벤트 타입 `cover_render`를 처리하여 자동 생성
- 수동 렌더 엔드포인트 `/api/render/cover` 제공

## 파일
- `DOCKERFILE_STEP27` — Cloud Run 배포 시 Poppler 포함 예시 Dockerfile
- `src/utils/pdfCover.js` — GCS 다운로드 → pdftoppm → GCS 업로드
- `server.render.js` — 수동 호출 REST
- `server.tasks.v2.js` — 이벤트 큐에 `cover_render` 처리기 추가

## 마운트 예시 (server.js)
```js
app.use(require('./server.render'));
app.use(require('./server.tasks.v2')); // 기존 server.tasks.js와 택1
```

## 사용 예시
```bash
# 수동 렌더
curl -s -X POST "$SITE/api/proxy/render/cover" -H 'content-type: application/json'     -d '{"brand":"omron","code":"g2r-1a","gcsPdfUri":"gs://partsplan-docai-us/datasheets/g2r-1a.pdf"}' | jq .

# 이벤트 큐 처리(v2 핸들러)
curl -s -X POST "$SITE/api/proxy/tasks/process-events-v2?limit=20" | jq .
```
