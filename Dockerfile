FROM node:20-slim

# yt-dlp butuh python3 + ffmpeg (ffmpeg dipakai buat gabungin video+audio kualitas terbaik)
# zip dipakai buat bundling foto slideshow (TikTok/IG) jadi satu file .zip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip ffmpeg zip curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install yt-dlp versi terbaru langsung dari pip (auto update tiap build image)
RUN pip3 install --no-cache-dir --break-system-packages -U yt-dlp

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV PORT=3000
EXPOSE 3000

CMD ["node", "server.js"]
