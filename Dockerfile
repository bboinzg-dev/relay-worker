# Dockerfile
FROM node:20-slim

# pdfimages (poppler-utils) is used for cover extraction. If the package is not available,
# the pipeline still works (cover extraction is skipped).
RUN apt-get update \
 && apt-get install -y --no-install-recommends poppler-utils \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV NODE_ENV=production PORT=8080
ENV VERTEX_LOCATION=asia-northeast3
ENV GOOGLE_GENAI_USE_VERTEXAI=true
ENV GEMINI_MODEL_CLASSIFY=gemini-2.5-flash
ENV GEMINI_MODEL_EXTRACT=gemini-2.5-flash
ENV ALLOW_MINIMAL_INSERT=1

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .
EXPOSE 8080
CMD ["node","server.js"]
