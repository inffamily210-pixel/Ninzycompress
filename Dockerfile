FROM node:20-slim

# yt-dlp butuh python3 + ffmpeg (ffmpeg dipakai buat gabungin video+audio kualitas terbaik)
# zip dipakai buat bundling foto slideshow (TikTok/IG) jadi satu file .zip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip ffmpeg zip curl unzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install yt-dlp versi terbaru langsung dari pip (auto update tiap build image)
RUN pip3 install --no-cache-dir --break-system-packages -U yt-dlp

# Deno: JS runtime yang dipakai yt-dlp buat mecahin tantangan signature
# YouTube (n-challenge). Tanpa ini, ekstraksi YouTube gampang gagal/throttle
# meski Node.js sudah ada di image ini — yt-dlp defaultnya cuma otomatis
# pakai Deno, runtime lain (termasuk Node) harus di-flag manual.
ENV DENO_INSTALL=/usr/local
RUN curl -fsSL https://deno.land/install.sh | sh -s -- -y
ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV PORT=3000
EXPOSE 3000

# Update yt-dlp tiap server nyala (bukan cuma pas build image) — YouTube
# sering ubah sistem mereka, jadi yt-dlp juga sering rilis patch. Ini
# nambah ~beberapa detik di awal startup, tapi bikin kita gak ketinggalan
# patch cuma karena belum sempat redeploy manual.
CMD ["sh", "-c", "pip3 install --no-cache-dir --break-system-packages -U yt-dlp --quiet || true; node server.js"]
