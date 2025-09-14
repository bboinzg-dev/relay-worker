import express from "express";
import authRouter from "./routes/manager.js"; // ✅ 실제 라우터가 있는 파일


const app = express();
app.use(express.json());
app.use(authRouter); // ← 추가

// ...기존 listen 유지
