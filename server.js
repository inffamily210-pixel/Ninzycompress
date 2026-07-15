/**
 * NinzyDownloader Backend
 * ------------------------
 * Backend ringan yang membungkus yt-dlp (Python) untuk ambil info & download
 * video TikTok / YouTube. Didesain untuk dijalankan di Railway (bukan Vercel),
 * karena butuh proses child_process (yt-dlp + ffmpeg) yang tidak cocok untuk
 * serverless function Vercel.
 *
 * Endpoint:
 *   POST /api/info      { url }                -> info video + pilihan kualitas
 *   GET  /api/download   ?url=...&quality=...   -> stream file video/audio
 *   GET  /api/health                            -> cek server & yt-dlp hidup
 */

const express = require('express');
const cors = require('cors');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const app = express();
app.use(express.json());

// ── CORS ──────────────────────────────────────────────────────────────────
// Ganti '*' dengan domain Vercel NinzyCompress kamu kalau mau lebih aman,
// misal: ['https://ninzycompress.vercel.app']
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*')
  .split(',')
  .map(s => s.trim());

app.use(cors({
  origin: ALLOWED_ORIGINS.includes('*') ? true : ALLOWED_ORIGINS,
  methods: ['GET', 'POST', 'OPTIONS'],
  // Content-Disposition gak termasuk header "aman" default di CORS, jadi
  // browser diam-diam nge-block akses ke sana kecuali diizinkan eksplisit
  // di sini. Tanpa ini, nama file hasil download selalu fallback ke nama
  // generik walaupun server udah ngirim nama yang bener.
  exposedHeaders: ['Content-Disposition', 'X-Batch-Results']
}));

const PORT = process.env.PORT || 3000;
const YTDLP_BIN = process.env.YTDLP_BIN || 'yt-dlp';
const PROCESS_TIMEOUT_MS = 3 * 60 * 1000; // 3 menit maksimum per proses

// ── Cookies opsional (buat Instagram/Facebook yang kadang butuh login) ────
// Kalau kamu punya masalah "login required" khusus IG/FB, taruh file
// cookies (format Netscape, hasil export dari extension "Get cookies.txt")
// bernama cookies.txt di folder yang sama dengan server.js, lalu redeploy.
// Kalau file tidak ada, opsi ini otomatis diskip (tidak error).
const COOKIES_FILE = path.join(__dirname, 'cookies.txt');
function cookieArgs() {
  return fs.existsSync(COOKIES_FILE) ? ['--cookies', COOKIES_FILE] : [];
}

// ── Rate limit sederhana (per IP, in-memory) ────────────────────────────
const rateMap = new Map();
function rateLimit(req, res, next) {
  const ip = req.ip || req.headers['x-forwarded-for'] || 'unknown';
  const now = Date.now();
  const windowMs = 60 * 1000;
  const maxReq = 12;
  const entry = rateMap.get(ip) || { count: 0, start: now };
  if (now - entry.start > windowMs) {
    entry.count = 0;
    entry.start = now;
  }
  entry.count++;
  rateMap.set(ip, entry);
  if (entry.count > maxReq) {
    return res.status(429).json({ success: false, error: 'Terlalu banyak permintaan, coba lagi sebentar lagi.' });
  }
  next();
}

// Batch jauh lebih berat (bisa sampai 5x proses yt-dlp per request), jadi
// limitnya lebih ketat dari endpoint biasa supaya server gak kelebihan beban.
const batchRateMap = new Map();
function batchRateLimit(req, res, next) {
  const ip = req.ip || req.headers['x-forwarded-for'] || 'unknown';
  const now = Date.now();
  const windowMs = 5 * 60 * 1000; // 5 menit
  const maxReq = 3;
  const entry = batchRateMap.get(ip) || { count: 0, start: now };
  if (now - entry.start > windowMs) {
    entry.count = 0;
    entry.start = now;
  }
  entry.count++;
  batchRateMap.set(ip, entry);
  if (entry.count > maxReq) {
    return res.status(429).json({ success: false, error: 'Terlalu banyak batch download, tunggu beberapa menit dulu.' });
  }
  next();
}

// ── Helpers ──────────────────────────────────────────────────────────────
function isSupportedUrl(url) {
  try {
    const u = new URL(url);
    const host = u.hostname.replace(/^www\./, '');
    return /(^|\.)tiktok\.com$/.test(host) ||
           /(^|\.)youtube\.com$/.test(host) ||
           /(^|\.)youtu\.be$/.test(host) ||
           /(^|\.)vt\.tiktok\.com$/.test(host) ||
           /(^|\.)instagram\.com$/.test(host) ||
           /(^|\.)facebook\.com$/.test(host) ||
           /(^|\.)fb\.watch$/.test(host);
  } catch {
    return false;
  }
}

function detectPlatform(url) {
  if (/tiktok\.com/i.test(url)) return 'TikTok';
  if (/youtube\.com|youtu\.be/i.test(url)) return 'YouTube';
  if (/instagram\.com/i.test(url)) return 'Instagram';
  if (/facebook\.com|fb\.watch/i.test(url)) return 'Facebook';
  return 'Video';
}

// 1234567 -> "1.2 jt", 45900 -> "45.9 rb"
function formatCount(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return null;
  if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1).replace(/\.0$/, '') + ' M';
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, '') + ' jt';
  if (n >= 1_000) return (n / 1_000).toFixed(1).replace(/\.0$/, '') + ' rb';
  return String(n);
}

const ID_MONTHS = ['Jan','Feb','Mar','Apr','Mei','Jun','Jul','Agu','Sep','Okt','Nov','Des'];
// yt-dlp format upload_date: "20260716" -> "16 Jul 2026"
function formatUploadDate(d) {
  if (!d || typeof d !== 'string' || d.length !== 8) return null;
  const y = d.slice(0, 4), m = parseInt(d.slice(4, 6), 10) - 1, day = parseInt(d.slice(6, 8), 10);
  if (!ID_MONTHS[m]) return null;
  return `${day} ${ID_MONTHS[m]} ${y}`;
}

function runYtDlp(args, { timeoutMs = PROCESS_TIMEOUT_MS } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(YTDLP_BIN, args, { windowsHide: true });
    let stdout = '';
    let stderr = '';
    const killer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error('TIMEOUT'));
    }, timeoutMs);

    child.stdout.on('data', d => { stdout += d.toString(); });
    child.stderr.on('data', d => { stderr += d.toString(); });

    child.on('error', err => {
      clearTimeout(killer);
      reject(err);
    });

    child.on('close', code => {
      clearTimeout(killer);
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(new Error(stderr.trim().split('\n').pop() || `yt-dlp keluar dengan kode ${code}`));
      }
    });
  });
}

function friendlyError(rawMessage) {
  const msg = (rawMessage || '').toLowerCase();
  if (msg.includes('timeout')) return 'Server terlalu lama merespons, coba lagi.';
  if (msg.includes('private') || msg.includes('login required') || msg.includes('rate-limit') || msg.includes('restricted video')) return 'Konten ini privat atau butuh login (sering terjadi di Instagram/Facebook untuk konten tertentu).';
  if (msg.includes('unavailable') || msg.includes('not available') || msg.includes('404')) return 'Video tidak ditemukan atau sudah dihapus.';
  if (msg.includes('geo') || msg.includes('country')) return 'Video ini dibatasi wilayah (geo-blocked).';
  if (msg.includes('unsupported url') || msg.includes('is not a valid url')) return 'Link tidak dikenali. Pastikan link TikTok/YouTube/Instagram/Facebook valid.';
  if (msg.includes('sign in') || msg.includes('confirm you')) return 'Video butuh verifikasi akun, tidak bisa diunduh otomatis.';
  return 'Gagal memproses video. Coba link lain atau ulangi beberapa saat lagi.';
}

// Format selector per tingkat kualitas. Semua punya fallback chain (pakai "/")
// supaya kalau kualitas tertentu tidak tersedia, otomatis turun ke yang ada
// -- tujuannya supaya tidak pernah gagal total karena format tidak ketemu.
const QUALITY_FORMATS = {
  best: 'bv*+ba/b',
  '1080': 'bv*[height<=1080]+ba/b[height<=1080]/b',
  '720': 'bv*[height<=720]+ba/b[height<=720]/b',
  '480': 'bv*[height<=480]+ba/b[height<=480]/b',
  audio: 'ba/b',
  audio_opus: 'ba/b',
  // Preview: prioritaskan format yang video+audio-nya udah nyatu (progresif)
  // biar gak perlu proses merge ffmpeg — respons lebih cepat buat sekadar
  // pratinjau sebelum download beneran.
  preview: 'best[height<=480][acodec!=none][vcodec!=none]/best[height<=480]/best'
};

// ── Routes ───────────────────────────────────────────────────────────────
app.get('/api/health', async (req, res) => {
  try {
    const { stdout } = await runYtDlp(['--version'], { timeoutMs: 15000 });
    res.json({ success: true, ytdlpVersion: stdout.trim() });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post('/api/info', rateLimit, async (req, res) => {
  const { url } = req.body || {};
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ success: false, error: 'URL wajib diisi.' });
  }
  if (!isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'Link harus dari TikTok atau YouTube.' });
  }

  try {
    const { stdout } = await runYtDlp([
      '-j', '--no-warnings', '--no-playlist', '--socket-timeout', '20', ...cookieArgs(), url
    ], { timeoutMs: 45000 });

    const firstLine = stdout.split('\n').find(l => l.trim().startsWith('{'));
    if (!firstLine) throw new Error('Tidak bisa membaca info video.');
    const info = JSON.parse(firstLine);

    // Post foto/slideshow (umum di TikTok, kadang IG) muncul sebagai
    // "entries" berisi gambar-gambar, bukan satu video biasa.
    const isPhotoSet = Array.isArray(info.entries) && info.entries.length > 0;

    const maxHeight = Math.max(
      0,
      ...((info.formats || [])
        .map(f => f.height || 0)
        .filter(h => Number.isFinite(h)))
    );

    let qualities;
    if (isPhotoSet) {
      qualities = [{ label: `📸 Download Semua Foto (${info.entries.length}) — ZIP`, value: 'photos' }];
    } else {
      qualities = [{ label: '🎬 Kualitas Terbaik', value: 'best' }];
      if (maxHeight >= 1080) qualities.push({ label: '1080p', value: '1080' });
      if (maxHeight >= 720) qualities.push({ label: '720p', value: '720' });
      if (maxHeight >= 480 || maxHeight === 0) qualities.push({ label: '480p', value: '480' });
      qualities.push({ label: '🎵 Audio (MP3)', value: 'audio' });
      qualities.push({ label: '🎧 Audio (Opus)', value: 'audio_opus' });

      // Subtitle cuma ditawarin kalau videonya beneran punya subtitle
      // (manual atau auto-generated) — biar gak nampilin opsi yang bakal gagal.
      const hasSubs = (info.subtitles && Object.keys(info.subtitles).length > 0) ||
                      (info.automatic_captions && Object.keys(info.automatic_captions).length > 0);
      if (hasSubs) qualities.push({ label: '📝 Subtitle (.srt)', value: 'subtitle' });
    }

    res.json({
      success: true,
      title: info.title || (isPhotoSet ? 'Postingan Foto' : 'Video'),
      thumbnail: info.thumbnail || (info.thumbnails && info.thumbnails.length ? info.thumbnails.at(-1).url : ''),
      duration: info.duration || 0,
      uploader: info.uploader || info.channel || '',
      platform: detectPlatform(url),
      isPhotoSet,
      photoCount: isPhotoSet ? info.entries.length : 0,
      viewCount: formatCount(info.view_count),
      likeCount: formatCount(info.like_count),
      commentCount: formatCount(info.comment_count),
      uploadDate: formatUploadDate(info.upload_date),
      qualities
    });
  } catch (e) {
    res.status(500).json({ success: false, error: friendlyError(e.message) });
  }
});

app.get('/api/download', rateLimit, async (req, res) => {
  const { url, quality = 'best' } = req.query;
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ success: false, error: 'URL wajib diisi.' });
  }
  if (!isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'Link harus dari TikTok atau YouTube.' });
  }
  if (quality !== 'photos' && quality !== 'subtitle' && !QUALITY_FORMATS[quality]) {
    return res.status(400).json({ success: false, error: 'Pilihan kualitas tidak valid.' });
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ninzydl-'));
  const isPhotos = quality === 'photos';
  const isSubtitle = quality === 'subtitle';
  const isPreview = quality === 'preview';
  const isAudio = quality === 'audio' || quality === 'audio_opus';
  const audioFormat = quality === 'audio_opus' ? 'opus' : 'mp3';

  const args = [
    '--no-warnings', '--no-part', '--restrict-filenames',
    '--socket-timeout', '20'
  ];

  if (isPhotos) {
    // Slideshow foto: ambil SEMUA gambar dalam post ini (bukan playlist
    // eksternal), lalu nanti di-zip jadi satu file.
    args.push('--yes-playlist', '-o', path.join(tmpDir, '%(playlist_index|1)s.%(ext)s'));
  } else if (isSubtitle) {
    // Cuma subtitle-nya aja, gak download videonya. Ambil manual dulu kalau
    // ada, fallback ke auto-generated. Prioritas Indonesia + Inggris.
    args.push(
      '--no-playlist', '--skip-download',
      '--write-subs', '--write-auto-subs',
      '--sub-langs', 'id,en,id-ID,en-US,en-orig',
      '--convert-subs', 'srt',
      '-o', path.join(tmpDir, '%(id)s.%(ext)s')
    );
  } else if (isAudio) {
    args.push('--no-playlist', '-o', path.join(tmpDir, '%(id)s.%(ext)s'));
    args.push('-x', '--audio-format', audioFormat, '-f', QUALITY_FORMATS[quality]);
  } else {
    args.push('--no-playlist', '-o', path.join(tmpDir, '%(id)s.%(ext)s'));
    args.push('--merge-output-format', 'mp4', '-f', QUALITY_FORMATS[quality]);
  }
  args.push(...cookieArgs(), url);

  const cleanup = () => {
    fs.rm(tmpDir, { recursive: true, force: true }, () => {});
  };

  try {
    await runYtDlp(args, isPhotos ? { timeoutMs: PROCESS_TIMEOUT_MS } : undefined);

    if (isPhotos) {
      const files = fs.readdirSync(tmpDir).filter(f => !f.startsWith('.'));
      if (!files.length) throw new Error('Foto tidak ditemukan di post ini.');

      const zipPath = path.join(tmpDir, 'photos.zip');
      await new Promise((resolve, reject) => {
        const zip = spawn('zip', ['-j', zipPath, ...files.map(f => path.join(tmpDir, f))], { windowsHide: true });
        zip.on('error', reject);
        zip.on('close', code => code === 0 ? resolve() : reject(new Error('Gagal membuat file ZIP.')));
      });

      const stat = fs.statSync(zipPath);
      const safeName = `ninzy_foto_${crypto.randomBytes(3).toString('hex')}.zip`;
      res.setHeader('Content-Type', 'application/zip');
      res.setHeader('Content-Length', stat.size);
      res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);

      const stream = fs.createReadStream(zipPath);
      stream.pipe(res);
      stream.on('close', cleanup);
      stream.on('error', () => { cleanup(); if (!res.headersSent) res.status(500).end(); });
      return;
    }

    if (isSubtitle) {
      const files = fs.readdirSync(tmpDir).filter(f => f.endsWith('.srt'));
      if (!files.length) throw new Error('Video ini tidak punya subtitle yang bisa diunduh.');

      if (files.length === 1) {
        const filePath = path.join(tmpDir, files[0]);
        const stat = fs.statSync(filePath);
        const safeName = `ninzy_subtitle_${crypto.randomBytes(3).toString('hex')}.srt`;
        res.setHeader('Content-Type', 'application/x-subrip');
        res.setHeader('Content-Length', stat.size);
        res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);
        const stream = fs.createReadStream(filePath);
        stream.pipe(res);
        stream.on('close', cleanup);
        stream.on('error', () => { cleanup(); if (!res.headersSent) res.status(500).end(); });
        return;
      }

      // Lebih dari 1 bahasa subtitle ketemu → bundling jadi ZIP
      const zipPath = path.join(tmpDir, 'subtitles.zip');
      await new Promise((resolve, reject) => {
        const zip = spawn('zip', ['-j', zipPath, ...files.map(f => path.join(tmpDir, f))], { windowsHide: true });
        zip.on('error', reject);
        zip.on('close', code => code === 0 ? resolve() : reject(new Error('Gagal membuat file ZIP.')));
      });
      const stat = fs.statSync(zipPath);
      const safeName = `ninzy_subtitle_${crypto.randomBytes(3).toString('hex')}.zip`;
      res.setHeader('Content-Type', 'application/zip');
      res.setHeader('Content-Length', stat.size);
      res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);
      const stream = fs.createReadStream(zipPath);
      stream.pipe(res);
      stream.on('close', cleanup);
      stream.on('error', () => { cleanup(); if (!res.headersSent) res.status(500).end(); });
      return;
    }

    const files = fs.readdirSync(tmpDir).filter(f => !f.startsWith('.'));
    if (!files.length) throw new Error('File hasil download tidak ditemukan.');
    const filePath = path.join(tmpDir, files[0]);
    const ext = path.extname(files[0]).replace('.', '') || (isAudio ? audioFormat : 'mp4');
    const stat = fs.statSync(filePath);

    const safeName = `ninzy_${crypto.randomBytes(3).toString('hex')}.${ext}`;
    const contentType = isAudio
      ? (audioFormat === 'opus' ? 'audio/opus' : 'audio/mpeg')
      : (ext === 'webm' ? 'video/webm' : 'video/mp4');

    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Length', stat.size);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Content-Disposition', `${isPreview ? 'inline' : 'attachment'}; filename="${safeName}"`);

    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
    stream.on('close', cleanup);
    stream.on('error', () => { cleanup(); if (!res.headersSent) res.status(500).end(); });
  } catch (e) {
    cleanup();
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: friendlyError(e.message) });
    } else {
      res.end();
    }
  }
});

const BATCH_MAX_URLS = 5;

app.post('/api/batch-download', batchRateLimit, async (req, res) => {
  const { urls, quality = 'best' } = req.body || {};
  const cleanUrls = Array.isArray(urls) ? urls.map(u => (u || '').trim()).filter(Boolean) : [];

  if (!cleanUrls.length) {
    return res.status(400).json({ success: false, error: 'Daftar link kosong.' });
  }
  if (cleanUrls.length > BATCH_MAX_URLS) {
    return res.status(400).json({ success: false, error: `Maksimal ${BATCH_MAX_URLS} link per batch.` });
  }
  for (const u of cleanUrls) {
    if (!isSupportedUrl(u)) {
      return res.status(400).json({ success: false, error: `Link tidak didukung: ${u}` });
    }
  }
  if (!['best', '1080', '720', '480', 'audio', 'audio_opus'].includes(quality)) {
    return res.status(400).json({ success: false, error: 'Pilihan kualitas tidak valid untuk batch.' });
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ninzybatch-'));
  const cleanup = () => fs.rm(tmpDir, { recursive: true, force: true }, () => {});
  const isAudio = quality === 'audio' || quality === 'audio_opus';
  const audioFormat = quality === 'audio_opus' ? 'opus' : 'mp3';
  const results = []; // { url, ok, error? } — dikirim balik lewat header biar frontend bisa nampilin ringkasan

  try {
    for (let i = 0; i < cleanUrls.length; i++) {
      const u = cleanUrls[i];
      const itemDir = path.join(tmpDir, `item${i + 1}`);
      fs.mkdirSync(itemDir);

      const args = [
        '--no-warnings', '--no-playlist', '--no-part', '--restrict-filenames',
        '--socket-timeout', '20',
        '-o', path.join(itemDir, `%(title).60s.%(ext)s`)
      ];
      if (isAudio) {
        args.push('-x', '--audio-format', audioFormat, '-f', QUALITY_FORMATS[quality]);
      } else {
        args.push('--merge-output-format', 'mp4', '-f', QUALITY_FORMATS[quality] || QUALITY_FORMATS.best);
      }
      args.push(...cookieArgs(), u);

      try {
        // Timeout per-item lebih pendek biar total batch gak kelamaan kalau
        // salah satu link bermasalah.
        await runYtDlp(args, { timeoutMs: 90000 });
        const files = fs.readdirSync(itemDir).filter(f => !f.startsWith('.'));
        if (files.length) results.push({ url: u, ok: true });
        else results.push({ url: u, ok: false, error: 'File tidak ditemukan' });
      } catch (e) {
        results.push({ url: u, ok: false, error: friendlyError(e.message) });
      }
    }

    const allFiles = [];
    for (let i = 0; i < cleanUrls.length; i++) {
      const itemDir = path.join(tmpDir, `item${i + 1}`);
      if (fs.existsSync(itemDir)) {
        fs.readdirSync(itemDir).forEach(f => allFiles.push(path.join(itemDir, f)));
      }
    }
    if (!allFiles.length) throw new Error('Semua link di batch ini gagal diproses.');

    const zipPath = path.join(tmpDir, 'batch.zip');
    await new Promise((resolve, reject) => {
      const zip = spawn('zip', ['-j', zipPath, ...allFiles], { windowsHide: true });
      zip.on('error', reject);
      zip.on('close', code => code === 0 ? resolve() : reject(new Error('Gagal membuat file ZIP.')));
    });

    const stat = fs.statSync(zipPath);
    const safeName = `ninzy_batch_${crypto.randomBytes(3).toString('hex')}.zip`;
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Length', stat.size);
    res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);
    // Ringkasan hasil tiap link, biar frontend bisa kasih tau mana yang gagal
    res.setHeader('X-Batch-Results', Buffer.from(JSON.stringify(results)).toString('base64'));

    const stream = fs.createReadStream(zipPath);
    stream.pipe(res);
    stream.on('close', cleanup);
    stream.on('error', () => { cleanup(); if (!res.headersSent) res.status(500).end(); });
  } catch (e) {
    cleanup();
    if (!res.headersSent) {
      res.status(500).json({ success: false, error: friendlyError(e.message), results });
    } else {
      res.end();
    }
  }
});

app.listen(PORT, () => {
  console.log(`NinzyDownloader backend jalan di port ${PORT}`);
});
