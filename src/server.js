import express from "express";
import authRouter from "./routes/manager.js"; // ← 여기만 바꾸기

const app = express();
app.use(express.json());
app.use(authRouter); // ← 추가

// ...기존 listen 유지
