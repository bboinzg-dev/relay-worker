 FROM node:20-slim
 RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils && rm -rf /var/lib/apt/lists/*
 WORKDIR /app
 ENV NODE_ENV=production PORT=8080
 COPY package*.json ./
 RUN npm ci --omit=dev
 COPY . .
 EXPOSE 8080
 CMD ["node","server.js"]
