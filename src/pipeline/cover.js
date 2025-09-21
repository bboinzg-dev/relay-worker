// src/pipeline/cover.js
'use strict';

const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const { storage } = require('../utils/gcs');

// 안전한 GCS URI 파서
function parseGcsUri(gcsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gcsUri || ''));
  if (!m) throw new Error('INVALID_GCS_URI');
  return { bucket: m[1], name: m[2] };
}

// family/brand/code로 커버 이미지 목적지 GCS 경로 생성
function canonicalCoverPath(targetBucket, family, brand, code) {
  if (!targetBucket) throw new Error('TARGET_BUCKET_REQUIRED');
  const safe = (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, '_');
  const fam = safe(family || 'unknown');
  const b   = safe(brand  || 'unknown');
  const c   = safe(code   || 'unknown');
  return `gs://${targetBucket}/covers/${fam}/${b}/${c}.png`;
}

/**
 * pdfimages로 1~2페이지 이미지 추출해 가장 큰 PNG를 GCS에 업로드
 * 실패/미설치/이미지 없음 등 모든 경우 null 반환 (기존 기능 보존)
 */
async function tryExtractCover(gcsPdfUri, { family, brand, code, targetBucket }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);

    // /tmp 작업 디렉터리 준비
    const tmpDir = path.join(os.tmpdir(), `cover-${Date.now()}`);
    await fs.mkdir(tmpDir, { recursive: true });

    // PDF 다운
    const localPdf = path.join(tmpDir, 'doc.pdf');
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(localPdf, buf);

    // pdfimages 실행 (poppler-utils)
    // 설치되어 있지 않으면 execFile 에러 → catch 로 넘어가고 null 반환
    await execFileP('pdfimages', ['-f', '1', '-l', '2', '-png', localPdf, path.join(tmpDir, 'img')]);

    // 추출 파일 스캔
    const files = (await fs.readdir(tmpDir)).filter((f) => /^img-\d+-\d+\.png$/i.test(f));
    if (!files.length) return null;

    // 가장 큰 PNG 선택
    let pick = null, size = -1;
    for (const f of files) {
      const st = await fs.stat(path.join(tmpDir, f));
      if (st.size > size) { pick = f; size = st.size; }
    }
    if (!pick) return null;

    // 업로드 목적지
    const dst = canonicalCoverPath(targetBucket, family, brand, code);
    const { bucket: dbkt, name: dname } = parseGcsUri(dst);

    // 업로드 (단발성 single-request 업로드: resumable=false)
    await storage.bucket(dbkt).upload(path.join(tmpDir, pick), {
      destination: dname,
      resumable: false,
    });

    return dst;
  } catch (_e) {
    // pdfimages 미설치, 권한, 이미지 없음 등 모든 상황은 조용히 무시
    return null;
  }
}

module.exports = { tryExtractCover, parseGcsUri, canonicalCoverPath };
