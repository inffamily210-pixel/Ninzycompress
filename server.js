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
const admin = require('firebase-admin');

// ── Firebase Admin SDK (server-side, punya akses penuh, TIDAK terikat
// Firestore Security Rules) ────────────────────────────────────────────
// Dipakai buat operasi sensitif (kasih/cabut premium, konfirmasi
// pembayaran, ban user, redeem kode) supaya gak bisa lagi ditulis langsung
// dari browser client seperti sebelumnya.
//
// Setup di Railway → Variables:
//   FIREBASE_SERVICE_ACCOUNT = isi lengkap file JSON service account
//   (Firebase Console → Project Settings → Service Accounts → Generate new
//   private key), di-paste utuh sebagai satu baris JSON.
let fsDb = null;
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    fsDb = admin.firestore();
    console.log('[firebase-admin] Terhubung ke Firestore.');
  } else {
    console.warn('[firebase-admin] FIREBASE_SERVICE_ACCOUNT belum diset — endpoint admin/premium akan nonaktif.');
  }
} catch (e) {
  console.error('[firebase-admin] Gagal init:', e.message);
}
function requireFirebaseAdmin(req, res, next) {
  if (!fsDb) {
    return res.status(500).json({ success: false, error: 'Server belum terhubung ke Firebase (FIREBASE_SERVICE_ACCOUNT belum diset di Railway).' });
  }
  next();
}

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

// ── Admin PIN (server-side only, TIDAK PERNAH dikirim ke browser) ─────────
// Set ini di Railway → Variables → ADMIN_PIN. Kalau gak diset, endpoint
// verifikasi PIN otomatis nonaktif (return error, bukan lolos gratis).
const ADMIN_PIN = process.env.ADMIN_PIN || null;
// Opsional: kalau diisi eksplisit, token tetap valid walau server restart.
// Kalau gak diisi, otomatis random tiap kali server nyala (aman, cuma efek
// sampingnya: user perlu masukin PIN ulang tiap Railway redeploy).
const ADMIN_TOKEN_SECRET = process.env.ADMIN_TOKEN_SECRET || crypto.randomBytes(32).toString('hex');
const ADMIN_TOKEN_TTL_MS = 2 * 60 * 60 * 1000; // token unlock berlaku 2 jam

function signAdminToken(expiresAt) {
  const sig = crypto.createHmac('sha256', ADMIN_TOKEN_SECRET).update(String(expiresAt)).digest('hex');
  return `${expiresAt}.${sig}`;
}
function verifyAdminToken(token) {
  if (!token || typeof token !== 'string' || !token.includes('.')) return false;
  const [expiresAtStr, sig] = token.split('.');
  const expiresAt = Number(expiresAtStr);
  if (!expiresAt || !sig || Date.now() > expiresAt) return false;
  const expected = crypto.createHmac('sha256', ADMIN_TOKEN_SECRET).update(String(expiresAt)).digest('hex');
  try {
    return sig.length === expected.length && crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected));
  } catch {
    return false;
  }
}

// Rate limit KETAT khusus buat percobaan PIN — proteksi brute-force.
const adminPinRateMap = new Map();
function adminPinRateLimit(req, res, next) {
  const ip = req.ip || req.headers['x-forwarded-for'] || 'unknown';
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const maxReq = 5;
  const entry = adminPinRateMap.get(ip) || { count: 0, start: now };
  if (now - entry.start > windowMs) { entry.count = 0; entry.start = now; }
  entry.count++;
  adminPinRateMap.set(ip, entry);
  if (entry.count > maxReq) {
    return res.status(429).json({ success: false, error: 'Terlalu banyak percobaan PIN salah. Coba lagi 15 menit lagi.' });
  }
  next();
}
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

// ── YouTube "Sign in to confirm you're not a bot" ─────────────────────────
// Sebagian besar kasus ini sebenarnya soal yt-dlp gak punya JS runtime buat
// mecahin tantangan signature YouTube (lihat instalasi Deno di Dockerfile),
// bukan soal client spoofing. Biarin yt-dlp pakai daftar client bawaannya
// sendiri (yang sudah otomatis multi-fallback) — jangan dipaksa satu client
// karena itu malah bisa lebih gampang gagal dibanding default-nya.
function ytClientArgs(url) {
  return [];
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
           /(^|\.)fb\.watch$/.test(host) ||
           /(^|\.)twitter\.com$/.test(host) ||
           /(^|\.)x\.com$/.test(host) ||
           /(^|\.)reddit\.com$/.test(host) ||
           /(^|\.)redd\.it$/.test(host) ||
           /(^|\.)soundcloud\.com$/.test(host) ||
           /(^|\.)twitch\.tv$/.test(host);
  } catch {
    return false;
  }
}

function detectPlatform(url) {
  if (/tiktok\.com/i.test(url)) return 'TikTok';
  if (/youtube\.com|youtu\.be/i.test(url)) return 'YouTube';
  if (/instagram\.com/i.test(url)) return 'Instagram';
  if (/facebook\.com|fb\.watch/i.test(url)) return 'Facebook';
  if (/twitter\.com|x\.com/i.test(url)) return 'Twitter/X';
  if (/reddit\.com|redd\.it/i.test(url)) return 'Reddit';
  if (/soundcloud\.com/i.test(url)) return 'SoundCloud';
  if (/twitch\.tv/i.test(url)) return 'Twitch';
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

  const PLAYLIST_CAP = 5; // dibatasi biar konsisten sama limit batch & gak berat di server gratisan
  const isPlaylistUrl = /youtube\.com/i.test(url) && /[?&]list=/.test(url);

  if (isPlaylistUrl) {
    try {
      const { stdout } = await runYtDlp([
        '--flat-playlist', '-j', '--no-warnings', '--playlist-end', String(PLAYLIST_CAP),
        '--socket-timeout', '20', ...cookieArgs(), ...ytClientArgs(url), url
      ], { timeoutMs: 45000 });

      const entries = stdout.split('\n')
        .filter(l => l.trim().startsWith('{'))
        .map(l => JSON.parse(l));

      if (!entries.length) throw new Error('Playlist kosong atau tidak bisa dibaca.');

      return res.json({
        success: true,
        isPlaylist: true,
        platform: 'YouTube',
        playlistTitle: entries[0].playlist_title || entries[0].playlist || 'Playlist YouTube',
        entries: entries.map(e => ({
          id: e.id,
          title: e.title || 'Video',
          thumbnail: e.thumbnails && e.thumbnails.length ? e.thumbnails.at(-1).url : '',
          url: `https://www.youtube.com/watch?v=${e.id}`
        })),
        capped: entries.length >= PLAYLIST_CAP
      });
    } catch (e) {
      return res.status(500).json({ success: false, error: friendlyError(e.message) });
    }
  }

  try {
    const { stdout } = await runYtDlp([
      '-j', '--no-warnings', '--no-playlist', '--socket-timeout', '20', ...cookieArgs(), ...ytClientArgs(url), url
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
    let subtitleLanguages = [];
    if (isPhotoSet) {
      qualities = [{ label: `📸 Download Semua Foto (${info.entries.length}) — ZIP`, value: 'photos' }];
    } else {
      qualities = [{ label: '🎬 Kualitas Terbaik', value: 'best' }];
      if (maxHeight >= 1080) qualities.push({ label: '1080p', value: '1080' });
      if (maxHeight >= 720) qualities.push({ label: '720p', value: '720' });
      if (maxHeight >= 480 || maxHeight === 0) qualities.push({ label: '480p', value: '480' });
      qualities.push({ label: '🎵 Audio (MP3)', value: 'audio' });
      qualities.push({ label: '🎧 Audio (Opus)', value: 'audio_opus' });

      // Kumpulin SEMUA bahasa subtitle yang beneran ada di video ini —
      // manual dulu (kualitas lebih bagus), baru auto-generated. Auto-
      // generated dibatasi ke bahasa umum aja (bukan semua ratusan hasil
      // auto-translate YouTube) biar daftarnya gak kepanjangan.
      if (info.subtitles) {
        for (const lang of Object.keys(info.subtitles)) {
          subtitleLanguages.push({ lang, auto: false });
        }
      }
      const commonAutoLangs = ['id', 'en', 'ja', 'ko', 'zh-Hans', 'zh-Hant', 'es', 'ar', 'hi', 'pt', 'fr', 'de', 'ru', 'th', 'vi'];
      if (info.automatic_captions) {
        for (const lang of commonAutoLangs) {
          if (info.automatic_captions[lang] && !subtitleLanguages.find(s => s.lang === lang)) {
            subtitleLanguages.push({ lang, auto: true });
          }
        }
        // Bahasa asli video (biasanya paling akurat auto-generated-nya)
        // kadang codenya spesifik & gak masuk daftar umum di atas.
        const origKey = Object.keys(info.automatic_captions).find(k => k.endsWith('-orig'));
        if (origKey && !subtitleLanguages.find(s => s.lang === origKey)) {
          subtitleLanguages.push({ lang: origKey, auto: true });
        }
      }
    }

    const hasThumbnail = !!(info.thumbnail || (info.thumbnails && info.thumbnails.length));
    if (hasThumbnail) qualities.push({ label: '🖼️ Thumbnail (Gambar)', value: 'thumbnail' });

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
      description: (info.description || '').slice(0, 3000),
      subtitleLanguages,
      qualities
    });
  } catch (e) {
    res.status(500).json({ success: false, error: friendlyError(e.message) });
  }
});

app.get('/api/download', rateLimit, async (req, res) => {
  const { url, quality = 'best', trimStart, trimEnd, subLang, skipSponsor } = req.query;
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ success: false, error: 'URL wajib diisi.' });
  }
  if (!isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'Link harus dari TikTok atau YouTube.' });
  }
  if (quality !== 'photos' && quality !== 'subtitle' && quality !== 'thumbnail' && !QUALITY_FORMATS[quality]) {
    return res.status(400).json({ success: false, error: 'Pilihan kualitas tidak valid.' });
  }

  // Trim durasi: cuma berlaku buat download video/audio biasa, bukan buat
  // foto/subtitle/thumbnail yang gak punya konsep "durasi".
  const ts = Number(trimStart), te = Number(trimEnd);
  const hasTrim = Number.isFinite(ts) && Number.isFinite(te) && ts >= 0 && te > ts &&
                  !['photos', 'subtitle', 'thumbnail', 'preview'].includes(quality);

  // SponsorBlock: cuma masuk akal buat YouTube (data komunitasnya cuma ada
  // di sana), dan cuma buat video/audio biasa sama kayak trim.
  const hasSponsorSkip = skipSponsor === '1' && /youtube\.com|youtu\.be/i.test(url) &&
                         !['photos', 'subtitle', 'thumbnail', 'preview'].includes(quality);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ninzydl-'));
  const isPhotos = quality === 'photos';
  const isSubtitle = quality === 'subtitle';
  const isThumbnail = quality === 'thumbnail';
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
  } else if (isThumbnail) {
    args.push(
      '--no-playlist', '--skip-download',
      '--write-thumbnail', '--convert-thumbnails', 'jpg',
      '-o', path.join(tmpDir, '%(id)s.%(ext)s')
    );
  } else if (isSubtitle) {
    // Cuma subtitle-nya aja, gak download videonya. Kalau user milih bahasa
    // spesifik, pakai itu; kalau enggak, fallback ke daftar default lama.
    const safeSubLang = (typeof subLang === 'string' && /^[a-zA-Z0-9,-]{1,40}$/.test(subLang))
      ? subLang
      : 'id,en,id-ID,en-US,en-orig';
    args.push(
      '--no-playlist', '--skip-download',
      '--write-subs', '--write-auto-subs',
      '--sub-langs', safeSubLang,
      '--convert-subs', 'srt',
      '-o', path.join(tmpDir, '%(id)s.%(ext)s')
    );
  } else if (isAudio) {
    args.push('--no-playlist', '-o', path.join(tmpDir, '%(id)s.%(ext)s'));
    args.push('-x', '--audio-format', audioFormat, '-f', QUALITY_FORMATS[quality]);
    if (hasSponsorSkip) args.push('--sponsorblock-remove', 'sponsor,selfpromo,interaction,intro,outro');
    if (hasTrim) args.push('--download-sections', `*${ts}-${te}`);
    if (hasTrim || hasSponsorSkip) args.push('--force-keyframes-at-cuts');
  } else {
    args.push('--no-playlist', '-o', path.join(tmpDir, '%(id)s.%(ext)s'));
    args.push('--merge-output-format', 'mp4', '-f', QUALITY_FORMATS[quality]);
    if (hasSponsorSkip) args.push('--sponsorblock-remove', 'sponsor,selfpromo,interaction,intro,outro');
    if (hasTrim) args.push('--download-sections', `*${ts}-${te}`);
    if (hasTrim || hasSponsorSkip) args.push('--force-keyframes-at-cuts');
  }
  args.push(...cookieArgs(), ...ytClientArgs(url), url);

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

    if (isThumbnail) {
      const files = fs.readdirSync(tmpDir).filter(f => !f.startsWith('.'));
      if (!files.length) throw new Error('Thumbnail tidak ditemukan.');
      const filePath = path.join(tmpDir, files[0]);
      const stat = fs.statSync(filePath);
      const safeName = `ninzy_thumbnail_${crypto.randomBytes(3).toString('hex')}.jpg`;
      res.setHeader('Content-Type', 'image/jpeg');
      res.setHeader('Content-Length', stat.size);
      res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);
      const stream = fs.createReadStream(filePath);
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
      args.push(...cookieArgs(), ...ytClientArgs(u), u);

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

app.post('/api/admin/verify-pin', adminPinRateLimit, (req, res) => {
  if (!ADMIN_PIN) {
    return res.status(500).json({ success: false, error: 'ADMIN_PIN belum diset di server (Railway → Variables).' });
  }
  const { pin } = req.body || {};
  if (!pin || typeof pin !== 'string') {
    return res.status(400).json({ success: false, error: 'PIN wajib diisi.' });
  }
  let match = false;
  try {
    match = pin.length === ADMIN_PIN.length && crypto.timingSafeEqual(Buffer.from(pin), Buffer.from(ADMIN_PIN));
  } catch {
    match = false;
  }
  if (!match) {
    return res.status(401).json({ success: false, error: 'PIN salah.' });
  }
  const expiresAt = Date.now() + ADMIN_TOKEN_TTL_MS;
  res.json({ success: true, token: signAdminToken(expiresAt), expiresAt });
});

app.post('/api/admin/verify-token', (req, res) => {
  const { token } = req.body || {};
  res.json({ success: verifyAdminToken(token) });
});

app.listen(PORT, () => {
  console.log(`NinzyDownloader backend jalan di port ${PORT}`);
});
