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
  methods: ['GET', 'POST', 'OPTIONS']
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
  audio_opus: 'ba/b'
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

    const maxHeight = Math.max(
      0,
      ...((info.formats || [])
        .map(f => f.height || 0)
        .filter(h => Number.isFinite(h)))
    );

    const qualities = [{ label: '🎬 Kualitas Terbaik', value: 'best' }];
    if (maxHeight >= 1080) qualities.push({ label: '1080p', value: '1080' });
    if (maxHeight >= 720) qualities.push({ label: '720p', value: '720' });
    if (maxHeight >= 480 || maxHeight === 0) qualities.push({ label: '480p', value: '480' });
    qualities.push({ label: '🎵 Audio (MP3)', value: 'audio' });
    qualities.push({ label: '🎧 Audio (Opus)', value: 'audio_opus' });

    res.json({
      success: true,
      title: info.title || 'Video',
      thumbnail: info.thumbnail || (info.thumbnails && info.thumbnails.length ? info.thumbnails.at(-1).url : ''),
      duration: info.duration || 0,
      uploader: info.uploader || info.channel || '',
      platform: detectPlatform(url),
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
  if (!QUALITY_FORMATS[quality]) {
    return res.status(400).json({ success: false, error: 'Pilihan kualitas tidak valid.' });
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ninzydl-'));
  const isAudio = quality === 'audio' || quality === 'audio_opus';
  const audioFormat = quality === 'audio_opus' ? 'opus' : 'mp3';

  const args = [
    '--no-warnings', '--no-playlist', '--no-part', '--restrict-filenames',
    '--socket-timeout', '20',
    '-o', path.join(tmpDir, '%(id)s.%(ext)s')
  ];

  if (isAudio) {
    args.push('-x', '--audio-format', audioFormat, '-f', QUALITY_FORMATS[quality]);
  } else {
    args.push('--merge-output-format', 'mp4', '-f', QUALITY_FORMATS[quality]);
  }
  args.push(...cookieArgs(), url);

  const cleanup = () => {
    fs.rm(tmpDir, { recursive: true, force: true }, () => {});
  };

  try {
    await runYtDlp(args);

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
    res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);

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

app.listen(PORT, () => {
  console.log(`NinzyDownloader backend jalan di port ${PORT}`);
});
