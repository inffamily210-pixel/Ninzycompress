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
const https = require('https');

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

// Config downloader — toggle nyala/mati, platform aktif, pesan maintenance.
// Disimpan di Firestore (adminSystem/downloaderConfig) biar kepakai
// langsung sama frontend Vercel DAN dicek di backend ini sendiri sebelum
// proses download jalan — bukan cuma dicek di frontend (yang bisa di-
// bypass kalau orang tau endpoint API-nya langsung).
const DOWNLOADER_CONFIG_DOC = 'downloaderConfig';
const DEFAULT_DOWNLOADER_CONFIG = {
  enabled: true,
  maintenanceMessage: null, // null = gak ada pesan, string = tampilin banner ini di card downloader
  platforms: {
    TikTok: true, YouTube: true, Instagram: true, Facebook: true,
    'Twitter/X': true, Reddit: true, SoundCloud: true, Twitch: true,
    Pinterest: true
  }
};

// Cache ringan (60 detik) biar gak query Firestore di SETIAP request
// download — toggle admin emang gak butuh reaktif dalam hitungan
// milidetik, delay maks 1 menit buat efeknya kepakai itu wajar.
let downloaderConfigCache = null;
let downloaderConfigCacheAt = 0;
async function getDownloaderConfig() {
  if (!fsDb) return DEFAULT_DOWNLOADER_CONFIG;
  if (downloaderConfigCache && Date.now() - downloaderConfigCacheAt < 60_000) return downloaderConfigCache;
  try {
    const doc = await fsDb.collection('adminSystem').doc(DOWNLOADER_CONFIG_DOC).get();
    downloaderConfigCache = doc.exists ? { ...DEFAULT_DOWNLOADER_CONFIG, ...doc.data() } : DEFAULT_DOWNLOADER_CONFIG;
    downloaderConfigCacheAt = Date.now();
    return downloaderConfigCache;
  } catch (e) {
    // Fail-open: Firestore bermasalah gak boleh diam-diam matiin downloader
    // buat semua orang.
    return DEFAULT_DOWNLOADER_CONFIG;
  }
}

// Dipanggil di awal /api/info dan /api/download — return null kalau boleh
// lanjut, atau { statusCode, error } kalau harus ditolak.
async function checkDownloaderEnabled(url) {
  const config = await getDownloaderConfig();
  if (config.enabled === false) {
    return { statusCode: 503, error: config.maintenanceMessage || 'Downloader lagi dimatikan sementara oleh admin.' };
  }
  const platform = detectPlatformForLog(url);
  if (platform && config.platforms && config.platforms[platform] === false) {
    return { statusCode: 503, error: `Platform ${platform} lagi dinonaktifkan sementara. Coba platform lain dulu ya.` };
  }
  return null;
}

// ── Request logging (buat admin panel: live log, statistik, monitoring) ──
// Dua lapis:
// 1. In-memory ring buffer — cepat, gak nunggu network Firestore, dipakai
//    buat "live log" & angka real-time di admin panel. Ilang kalau server
//    restart (wajar, ini bukan sumber kebenaran jangka panjang).
// 2. Firestore — ringkasan per-request ditulis ke sana (fire-and-forget,
//    gak nge-block response ke user), jadi histori tetep ada meski server
//    restart, buat statistik harian/mingguan di panel admin.
const LOG_BUFFER_MAX = 500;
const requestLogBuffer = []; // array of { id, ts, method, path, platform, quality, statusCode, durationMs, bytesOut, error }

function logRequest(entry) {
  const record = { id: crypto.randomUUID(), ts: Date.now(), ...entry };
  requestLogBuffer.push(record);
  if (requestLogBuffer.length > LOG_BUFFER_MAX) requestLogBuffer.shift();

  // Tulis ke Firestore tanpa nunggu (fire-and-forget) — kalau gagal (misal
  // Firebase belum di-setup), gak boleh sampai ganggu response ke user.
  if (fsDb) {
    fsDb.collection('downloaderLogs').add({
      ...entry,
      ts: admin.firestore.FieldValue.serverTimestamp()
    }).catch(() => { /* logging gak boleh bikin request utama gagal */ });
  }
  return record;
}

// Coba tebak platform dari URL query/body buat keperluan statistik —
// dipanggil di middleware sebelum handler tau URL-nya (jadi duplikasi
// ringan dari detectPlatform, didefinisikan lebih awal di sini).
function detectPlatformForLog(str) {
  if (!str || typeof str !== 'string') return null;
  if (/tiktok\.com/i.test(str)) return 'TikTok';
  if (/youtube\.com|youtu\.be/i.test(str)) return 'YouTube';
  if (/instagram\.com/i.test(str)) return 'Instagram';
  if (/facebook\.com|fb\.watch/i.test(str)) return 'Facebook';
  if (/twitter\.com|x\.com/i.test(str)) return 'Twitter/X';
  if (/reddit\.com/i.test(str)) return 'Reddit';
  if (/soundcloud\.com/i.test(str)) return 'SoundCloud';
  if (/twitch\.tv/i.test(str)) return 'Twitch';
  if (/pinterest\.|pin\.it/i.test(str)) return 'Pinterest';
  return null;
}

// Middleware: nyatet SETIAP request yang lewat /api/* — durasi, status code,
// ukuran response (bandwidth), platform yang di-hit. Dipasang sebelum semua
// route lain biar kecatet semua tanpa perlu diulang manual di tiap handler.
app.use((req, res, next) => {
  if (!req.path.startsWith('/api/')) return next();
  const startedAt = Date.now();
  let bytesOut = 0;
  const originalWrite = res.write.bind(res);
  const originalEnd = res.end.bind(res);
  res.write = (chunk, ...args) => {
    if (chunk) bytesOut += Buffer.byteLength(chunk);
    return originalWrite(chunk, ...args);
  };
  res.end = (chunk, ...args) => {
    if (chunk) bytesOut += Buffer.byteLength(chunk);
    return originalEnd(chunk, ...args);
  };

  res.on('finish', () => {
    const urlGuess = (req.query && req.query.url) || (req.body && req.body.url) || (req.body && req.body.profileUrl) || null;
    logRequest({
      method: req.method,
      path: req.path,
      platform: detectPlatformForLog(urlGuess),
      quality: (req.query && req.query.quality) || (req.body && req.body.quality) || null,
      statusCode: res.statusCode,
      durationMs: Date.now() - startedAt,
      bytesOut,
      ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress || null
    });
  });
  next();
});

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

// Middleware: wajib bawa token admin yang valid (Authorization: Bearer <token>)
// buat semua endpoint yang ngubah data sensitif. Token ini didapat dari
// /api/admin/verify-pin, jadi cuma orang yang tau PIN yang bisa dapet token.
function requireAdminToken(req, res, next) {
  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
  if (!verifyAdminToken(token)) {
    return res.status(401).json({ success: false, error: 'Sesi admin gak valid/expired, login PIN ulang.' });
  }
  next();
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

function sha256Hex(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}

// ── Push Notification (Web Push / VAPID) ──────────────────────────────────
// Upgrade dari Notification API biasa (butuh tab kebuka) ke Push API asli:
// notifikasi tetap masuk walau app-nya ketutup total, karena yang "bangunin"
// browser itu push service (FCM dkk di level OS/browser), bukan tab yang
// masih jalan. Server cuma ngirim payload terenkripsi ke endpoint push
// service — gak peduli tab-nya kebuka atau nggak.
//
// Setup di Railway → Variables:
//   VAPID_PUBLIC_KEY  = (lihat pesan Claude — sudah digenerate, tinggal pakai)
//   VAPID_PRIVATE_KEY = (idem — JANGAN disebar/commit ke git)
//   VAPID_SUBJECT     = mailto:email-kamu@gmail.com (kontak buat push service kalau ada masalah)
const webpush = require('web-push');
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || null;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || null;
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:admin@ninzycompress.com';
const PUSH_ENABLED = !!(VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY);
if (PUSH_ENABLED) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
  console.log('[push] VAPID siap, push notification aktif.');
} else {
  console.warn('[push] VAPID_PUBLIC_KEY/VAPID_PRIVATE_KEY belum diset — endpoint push nonaktif dulu.');
}

// subId = sha256(endpoint) push subscription. Dipakai sebagai ID dokumen
// Firestore biar lookup pas kirim push tinggal 1x get(), dan biar gak perlu
// nyimpen endpoint mentah (yang lumayan panjang) di banyak tempat lain
// (creatorWatch.subscriberSubIds cukup nyimpen subId, bukan subscription utuh).
async function sendPushToSubId(subId, payload) {
  if (!PUSH_ENABLED || !fsDb) return { ok: false, reason: 'push_disabled' };
  const ref = fsDb.collection('pushSubscriptions').doc(subId);
  const snap = await ref.get();
  if (!snap.exists) return { ok: false, reason: 'not_found' };
  try {
    await webpush.sendNotification(snap.data().subscription, JSON.stringify(payload));
    ref.set({ lastSentAt: Date.now() }, { merge: true }).catch(() => {});
    return { ok: true };
  } catch (e) {
    // 404/410 dari push service artinya subscription-nya udah gak valid lagi
    // (uninstall, permission dicabut, dst) — bersihin dari Firestore sekalian.
    if (e.statusCode === 404 || e.statusCode === 410) {
      ref.delete().catch(() => {});
      return { ok: false, reason: 'expired' };
    }
    return { ok: false, reason: e.message };
  }
}

// Preset notifikasi — SENGAJA gak nerima title/body bebas dari client, biar
// /api/push/send gak jadi "kirim teks apa aja ke device sendiri" yang gampang
// disalahgunakan. Teksnya dikontrol terpusat di sini; tinggal tambah key baru
// kalau mau bikin jenis notifikasi lain (reward spin siap diklaim, dll).
const PUSH_PRESETS = {
  'compress-done': (data) => ({
    title: '✅ Kompresi selesai',
    body: (data && data.filename) ? `${data.filename} siap diunduh` : 'Video kamu siap diunduh',
    url: '/'
  }),
  'test': () => ({
    title: '🔔 Notifikasi percobaan',
    body: 'Kalau ini muncul, push notification kamu udah aktif!',
    url: '/'
  })
};

// ── Creator Watch (alert upload baru dari creator favorit) ────────────────
// SENGAJA dipisah dari /api/creator-videos yang udah ada: di sini cuma butuh
// 1 video TERBARU buat dibandingin sama lastVideoId tersimpan, jadi pakai
// --playlist-end 1 biar cron-nya ringan & cepat — gak perlu thumbnail/durasi
// dst kayak /api/creator-videos yang memang buat ditampilin ke user.
async function fetchLatestVideoForCreator(uploaderUrl) {
  const { stdout } = await runYtDlp([
    '--flat-playlist', '-j', '--no-warnings', '--playlist-end', '1',
    '--socket-timeout', '15', ...cookieArgs(), ...ytClientArgs(uploaderUrl), uploaderUrl
  ], { timeoutMs: 30000 });
  const entry = stdout.split('\n')
    .map(l => l.trim())
    .filter(l => l.startsWith('{'))
    .map(l => { try { return JSON.parse(l); } catch (e) { return null; } })
    .find(e => e && e.id);
  if (!entry) return null;
  const platform = detectPlatform(uploaderUrl);
  let videoUrl = null;
  if (entry.webpage_url) videoUrl = entry.webpage_url;
  else if (entry.url && /^https?:\/\//.test(entry.url)) videoUrl = entry.url;
  else if (platform === 'YouTube') videoUrl = `https://www.youtube.com/watch?v=${entry.id}`;
  return { id: entry.id, title: entry.title || 'Video baru', url: videoUrl };
}

const CREATOR_WATCH_INTERVAL_MS = (parseInt(process.env.CREATOR_WATCH_INTERVAL_MINUTES, 10) || 30) * 60 * 1000;

// Loop cron utama: jalan tiap CREATOR_WATCH_INTERVAL_MS, cek SEMUA creator
// yang punya minimal 1 subscriber, SATU-SATU (sequential + jeda, BUKAN
// Promise.all paralel) — biar server gratisan Railway gak keberatan nge-spawn
// banyak proses yt-dlp bersamaan (alasan sama kayak batchRateLimit di atas).
async function runCreatorWatchCheck() {
  if (!fsDb) return;
  let snap;
  try {
    snap = await fsDb.collection('creatorWatch').get();
  } catch (e) {
    console.error('[creatorWatch] Gagal ambil daftar creator:', e.message);
    return;
  }
  const docs = snap.docs.filter(d => Array.isArray(d.data().subscriberSubIds) && d.data().subscriberSubIds.length > 0);
  if (!docs.length) return;
  console.log(`[creatorWatch] Cek ${docs.length} creator...`);
  for (const doc of docs) {
    const data = doc.data();
    try {
      const latest = await fetchLatestVideoForCreator(data.uploaderUrl);
      if (latest) {
        const isNew = data.lastVideoId && latest.id !== data.lastVideoId;
        if (isNew) {
          console.log(`[creatorWatch] Upload baru dari ${data.name}: ${latest.title}`);
          for (const subId of data.subscriberSubIds) {
            sendPushToSubId(subId, {
              title: `📢 ${data.name} upload baru`,
              body: latest.title,
              url: latest.url || '/'
            }).then(r => {
              if (!r.ok) console.warn(`[creatorWatch] Gagal kirim push ke ${subId}: ${r.reason}`);
            }).catch(e => console.error(`[creatorWatch] Error kirim push ke ${subId}:`, e.message));
          }
        }
        await doc.ref.set({ lastVideoId: latest.id, lastVideoTitle: latest.title, lastCheckedAt: Date.now() }, { merge: true });
      }
    } catch (e) {
      console.warn(`[creatorWatch] Gagal cek ${data.name}:`, e.message);
    }
    // Jeda antar-creator biar gak nge-spawn semua proses yt-dlp bersamaan.
    await new Promise(r => setTimeout(r, 2000));
  }
}

// ── API Access untuk developer (fitur #113 di showcase, sebelumnya cuma UI) ─
// Key TIDAK disimpan mentah — yang disimpan cuma hash SHA-256-nya, dan ID
// dokumen Firestore = hash itu sendiri (biar validasi tinggal 1x get() by ID).
// Plaintext key CUMA ditampilin sekali pas baru dibuat, persis kayak GitHub
// Personal Access Token — kalau ke-skip/ilang, user harus generate key baru.
const API_KEY_PREFIX = 'ninzy_live_';
const API_KEY_MAX_PER_USER = 3;
const API_KEY_RATE_PER_MIN = parseInt(process.env.API_KEY_RATE_PER_MIN, 10) || 60;

function generateApiKey() {
  return API_KEY_PREFIX + crypto.randomBytes(24).toString('hex');
}

// Verifikasi identitas user PREMIUM dari Firebase ID token — BUKAN dari email
// yang dikirim client mentah-mentah kayak /api/redeem-code (itu cukup buat
// tempel kode aktivasi, tapi buat nerbitin API key produksi harusnya lebih
// ketat). Konsekuensinya: user WAJIB login pakai Google (signInWithPopup) buat
// fitur ini, karena cuma login itu yang menghasilkan ID token yang bisa
// diverifikasi cryptographically lewat admin.auth().verifyIdToken(). Login
// email/password lokal (sistem lama app ini, localStorage-only) gak
// menghasilkan Firebase ID token sama sekali, jadi belum bisa dipakai di sini.
async function getVerifiedPremiumEmail(req) {
  const authHeader = req.headers.authorization || '';
  const idToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
  if (!idToken) return { error: 'Login dengan Google dulu buat akses fitur ini.', status: 401 };
  if (!fsDb) return { error: 'Server belum terhubung ke Firebase.', status: 500 };
  let decoded;
  try {
    decoded = await admin.auth().verifyIdToken(idToken);
  } catch (e) {
    return { error: 'Sesi login gak valid, coba login ulang.', status: 401 };
  }
  const email = (decoded.email || '').toLowerCase().trim();
  if (!email) return { error: 'Akun Google kamu gak punya email.', status: 400 };
  const snap = await fsDb.collection('users').doc(email).get();
  const premiumExpiry = snap.exists ? (snap.data().premiumExpiry || 0) : 0;
  if (premiumExpiry <= Date.now()) {
    return { error: 'API Access khusus member Premium. Upgrade dulu ya.', status: 403 };
  }
  return { email };
}

const apiKeyRateMap = new Map();
// Middleware buat endpoint /api/v1/* — beda dari rateLimit (per-IP) di atas,
// karena integrasi developer itu wajar jalan dari 1 server/IP yang sama
// terus-menerus. Yang mau dibatasi di sini per KEY, bukan per IP.
async function requireApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  if (!key || typeof key !== 'string') {
    return res.status(401).json({ success: false, error: 'Header X-API-Key wajib diisi. Buat key di Admin Panel → 🔌 API Access.' });
  }
  if (!fsDb) {
    return res.status(500).json({ success: false, error: 'Server belum terhubung ke Firebase.' });
  }
  const hash = sha256Hex(key);
  let snap;
  try {
    snap = await fsDb.collection('apiKeys').doc(hash).get();
  } catch (e) {
    return res.status(500).json({ success: false, error: 'Gagal validasi API key.' });
  }
  if (!snap.exists || snap.data().revoked) {
    return res.status(401).json({ success: false, error: 'API key gak valid atau sudah di-revoke.' });
  }
  // Rate limit per-key, in-memory (reset kalau server restart — sama kayak
  // limiter lain di file ini, bukan cuma buat endpoint ini).
  const now = Date.now();
  const windowMs = 60 * 1000;
  const entry = apiKeyRateMap.get(hash) || { count: 0, start: now };
  if (now - entry.start > windowMs) { entry.count = 0; entry.start = now; }
  entry.count++;
  apiKeyRateMap.set(hash, entry);
  if (entry.count > API_KEY_RATE_PER_MIN) {
    return res.status(429).json({ success: false, error: `Rate limit ${API_KEY_RATE_PER_MIN} request/menit terlampaui.` });
  }
  // Catat pemakaian — fire-and-forget, gak boleh nge-block response ke developer.
  snap.ref.set({ lastUsedAt: now, requestCount: admin.firestore.FieldValue.increment(1) }, { merge: true }).catch(() => {});
  req.apiKeyOwner = snap.data().ownerEmail;
  next();
}

// ── Helpers ──────────────────────────────────────────────────────────────
// Beberapa short-link (paling umum: pin.it dari Pinterest) kadang gagal
// di-resolve langsung sama yt-dlp — extractor Pinterest-nya kadang gak
// ngikutin redirect short-link dengan bener (bug lama yt-dlp, bukan sesuatu
// yang bisa kita perbaiki dari sisi ini). Solusinya: kita resolve
// redirect-nya SENDIRI dulu di sini (murni baca header HTTP "Location",
// SAMA SEKALI GAK ngambil/nyimpen isi halaman), baru kirim URL hasil akhir
// (yang udah pinterest.com/pin/... lengkap) ke yt-dlp.
function resolveRedirectUrl(url, maxRedirects = 5) {
  return new Promise((resolve) => {
    if (maxRedirects <= 0) return resolve(url); // kehabisan hop, pakai apa adanya

    let target;
    try { target = new URL(url); } catch { return resolve(url); }

    const req = https.request({
      hostname: target.hostname,
      path: target.pathname + target.search,
      method: 'HEAD',
      timeout: 8000,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; NinzyDownloader/1.0)' }
    }, (res) => {
      const location = res.headers.location;
      if (res.statusCode >= 300 && res.statusCode < 400 && location) {
        // Location kadang relatif, kadang URL penuh — resolve relatif
        // terhadap URL saat ini kalau perlu.
        let nextUrl;
        try { nextUrl = new URL(location, target).toString(); } catch { return resolve(url); }
        res.resume(); // buang body-nya (harusnya emang kosong buat HEAD), biar koneksi bisa ditutup bersih
        resolve(resolveRedirectUrl(nextUrl, maxRedirects - 1));
      } else {
        res.resume();
        resolve(target.toString()); // gak ada redirect lagi, ini URL final-nya
      }
    });

    req.on('error', () => resolve(url)); // gagal resolve, pakai URL asli aja — biar yt-dlp yang coba sendiri
    req.on('timeout', () => { req.destroy(); resolve(url); });
    req.end();
  });
}

// Domain-domain yang short-link-nya diketahui suka gagal di-resolve
// langsung sama yt-dlp — cuma domain INI yang bakal di-expand manual dulu,
// biar gak nambahin latency network buat platform lain yang emang udah
// jalan normal tanpa perlu resolve tambahan.
const SHORTLINK_DOMAINS = ['pin.it'];
async function expandShortlinkIfNeeded(url) {
  try {
    const host = new URL(url).hostname.replace(/^www\./, '');
    if (SHORTLINK_DOMAINS.includes(host)) {
      return await resolveRedirectUrl(url);
    }
  } catch { /* URL gak valid, biarin aja gagal natural di validasi selanjutnya */ }
  return url;
}

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
           /(^|\.)twitch\.tv$/.test(host) ||
           /(^|\.)pinterest\.com$/.test(host) ||
           /(^|\.)pin\.it$/.test(host);
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
  if (/pinterest\.|pin\.it/i.test(url)) return 'Pinterest';
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

// yt-dlp ngasih nama codec mentah yang teknis banget, misal "avc1.640028"
// atau "av01.0.05M.08" — user awam gak butuh angka profile/level di
// belakangnya, cukup nama codec-nya aja (H.264, AV1, VP9, dst).
function dlSimplifyCodecName(raw) {
  if (!raw || raw === 'none') return null;
  const c = raw.toLowerCase();
  if (c.startsWith('avc1') || c.startsWith('h264')) return 'H.264';
  if (c.startsWith('av01') || c.startsWith('av1')) return 'AV1';
  if (c.startsWith('vp9') || c.startsWith('vp09')) return 'VP9';
  if (c.startsWith('vp8')) return 'VP8';
  if (c.startsWith('hev1') || c.startsWith('hvc1') || c.startsWith('h265')) return 'H.265 (HEVC)';
  if (c.startsWith('mp4a') || c.startsWith('aac')) return 'AAC';
  if (c.startsWith('opus')) return 'Opus';
  if (c.startsWith('mp3')) return 'MP3';
  if (c.startsWith('vorbis')) return 'Vorbis';
  // Gak dikenali — kasih balik apa adanya (dipotong biar gak kepanjangan)
  // daripada nyembunyiin info sama sekali.
  return raw.split('.')[0].toUpperCase();
}

// Sederhanain rasio widthxheight ke bentuk umum kayak "16:9", "9:16", "1:1"
// pakai GCD, dengan toleransi kecil ke rasio standar biar gak muncul angka
// aneh kayak "1071:601" buat video yang sebenernya 16:9.
function dlSimplifyAspectRatio(width, height) {
  if (!width || !height) return null;
  const COMMON_RATIOS = [
    [16, 9], [9, 16], [4, 3], [3, 4], [1, 1], [21, 9], [4, 5], [5, 4]
  ];
  const actual = width / height;
  for (const [w, h] of COMMON_RATIOS) {
    if (Math.abs(actual - w / h) < 0.02) return `${w}:${h}`;
  }
  const gcd = (a, b) => (b === 0 ? a : gcd(b, a % b));
  const g = gcd(width, height);
  return `${width / g}:${height / g}`;
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
  // Post foto/slideshow TikTok/IG gak punya format video sama sekali (cuma
  // audio + gambar terpisah), jadi kalau user coba download pakai pilihan
  // kualitas video, yt-dlp bakal bilang "Requested format is not available".
  if (msg.includes('requested format is not available') || msg.includes('no video formats')) {
    return 'Postingan ini kemungkinan foto/slideshow, bukan video — coba cek ulang link-nya lalu pakai opsi "Download Semua Foto" atau "Jadikan 1 Video".';
  }
  // Dukungan yt-dlp buat TikTok photo mode/album itu memang belum konsisten
  // (bukan bug di server ini — issue terbuka di yt-dlp sendiri, statusnya
  // "wontfix"). Kalau error-nya nunjuk-nunjuk ke pola ini, kasih tau jujur
  // daripada pesan generik yang bikin user pikir link-nya salah ketik.
  if (msg.includes('no formats found') || msg.includes("couldn't find") || (msg.includes('tiktok') && msg.includes('photo'))) {
    return 'TikTok kadang gak konsisten buat postingan foto/album tertentu — bukan link-nya salah, tapi keterbatasan dari extractor yt-dlp buat jenis postingan ini. Coba lagi beberapa saat, atau screenshot manual kalau mendesak.';
  }
  return 'Gagal memproses video. Coba link lain atau ulangi beberapa saat lagi.';
}

// Format selector per tingkat kualitas. Semua punya fallback chain (pakai "/")
// supaya kalau kualitas tertentu tidak tersedia, otomatis turun ke yang ada
// -- tujuannya supaya tidak pernah gagal total karena format tidak ketemu.
//
// CATATAN soal "8K": diminta di daftar fitur, tapi gak dimasukin di sini.
// Alasannya: gak ada satupun platform yang didukung tool ini (TikTok,
// YouTube, IG, FB, Twitter/X, Reddit, SoundCloud, Twitch) yang nyediain
// upload asli di atas 4K — nawarin pilihan "8K" yang gak pernah bisa
// kepenuhi cuma bakal bikin user nyoba, gagal, dan mikir tool-nya rusak.
// Cap tertinggi realistis ya 2160p (4K), dan itu pun cuma buat video yang
// sumbernya emang di-upload 4K (YouTube kebanyakan).
const QUALITY_FORMATS = {
  best: 'bv*+ba/b',
  '2160': 'bv*[height<=2160]+ba/b[height<=2160]/b',
  '1440': 'bv*[height<=1440]+ba/b[height<=1440]/b',
  '1080': 'bv*[height<=1080]+ba/b[height<=1080]/b',
  '720': 'bv*[height<=720]+ba/b[height<=720]/b',
  '480': 'bv*[height<=480]+ba/b[height<=480]/b',
  '360': 'bv*[height<=360]+ba/b[height<=360]/b',
  '240': 'bv*[height<=240]+ba/b[height<=240]/b',
  '144': 'bv*[height<=144]+ba/b[height<=144]/b',
  audio: 'ba/b',
  audio_opus: 'ba/b',
  audio_m4a: 'ba[ext=m4a]/ba/b',
  audio_aac: 'ba/b',
  // Preview: prioritaskan format yang video+audio-nya udah nyatu (progresif)
  // biar gak perlu proses merge ffmpeg — respons lebih cepat buat sekadar
  // pratinjau sebelum download beneran.
  preview: 'best[height<=480][acodec!=none][vcodec!=none]/best[height<=480]/best',
  preview_audio: 'ba/b'
};

// ── Routes ───────────────────────────────────────────────────────────────
// Waktu proses ini mulai jalan — dipakai buat hitung uptime di /api/health.
const SERVER_START_TIME = Date.now();

// Auto-cleanup file sementara SAAT STARTUP — cleanup normal udah ada di tiap
// endpoint (fs.rm setelah stream selesai/gagal, lihat fungsi cleanup() di
// /api/download dan runBatchDownload), tapi itu gak kepanggil kalau
// container mati mendadak (crash/OOM/restart paksa) SEBELUM request selesai.
// Ini jaring pengaman tambahan: bersihin folder tmp sisa dari proses
// sebelumnya begitu server baru nyala, biar disk gak numpuk pelan-pelan
// tiap kali ada crash yang gak sempet cleanup sendiri.
(function cleanupStaleTempFolders() {
  try {
    const tmpRoot = os.tmpdir();
    const entries = fs.readdirSync(tmpRoot);
    const stalePrefixes = ['ninzydl-', 'ninzybatch-'];
    let cleaned = 0;
    entries.forEach(name => {
      if (stalePrefixes.some(p => name.startsWith(p))) {
        fs.rm(path.join(tmpRoot, name), { recursive: true, force: true }, () => {});
        cleaned++;
      }
    });
    if (cleaned > 0) console.log(`[startup-cleanup] Membersihkan ${cleaned} folder sementara sisa dari sesi sebelumnya.`);
  } catch (e) {
    console.warn('[startup-cleanup] Gagal cek folder tmp:', e.message);
  }
})();

// ── Status per-platform (BEDA dari toggle admin di DEFAULT_DOWNLOADER_CONFIG) ──
// Toggle admin itu manual, ON/OFF yang di-set orang. Ini beda: TES BENERAN
// tiap platform masih bisa di-extract yt-dlp saat ini juga — soalnya
// extractor bisa rusak sewaktu-waktu kalau platform sumbernya ubah struktur
// halaman, meski gak ada yang matiin manual. Dites pakai 1 URL contoh
// publik yang stabil per-platform, dengan --simulate (SAMA SEKALI GAK
// download filenya, cuma coba extract metadata) + timeout pendek.
const PLATFORM_TEST_URLS = {
  TikTok: 'https://www.tiktok.com/@tiktok/video/7106593422490878762', // video resmi akun TikTok
  YouTube: 'https://www.youtube.com/watch?v=jNQXAC9IVRw', // "Me at the zoo" — video pertama YouTube, gak bakal dihapus
  Instagram: 'https://www.instagram.com/p/C0hZ1qNoqZ0/', // post publik, format valid
  Facebook: 'https://www.facebook.com/facebook/videos/10154947350226729/', // video resmi halaman Facebook
  'Twitter/X': 'https://twitter.com/Twitter/status/1445078208190291973',
  Reddit: 'https://www.reddit.com/r/videos/comments/1b4qty7/', // Reddit redirect otomatis walau slug judul dihilangkan, URL ini tetap valid
  SoundCloud: 'https://soundcloud.com/soundcloud/soundcloud-2024',
  Twitch: 'https://www.twitch.tv/twitch', // channel URL — kalau offline, yt-dlp biasanya tetap bisa baca metadata channel (bukan realtime stream), jadi tetap valid buat tes
  Pinterest: 'https://www.pinterest.com/pinterest/official-pinterest-tips/' // board publik Pinterest resmi
};
// CATATAN PENTING: ID yang "kependekan"/gak lengkap (kayak Reddit di atas)
// TETAP bakal dianggap gagal-tapi-sehat asal pesan errornya jelas soal
// "postingan gak ketemu", BUKAN soal extractor-nya sendiri gak ngerti
// format URL platform itu. Itu bedanya "extractor rusak" vs "video contoh
// kebetulan gak ada" — lihat isExtractorHealthyError() di bawah buat
// pembagian pastinya.
function isExtractorHealthyError(errMsg) {
  const msg = (errMsg || '').toLowerCase();
  // Ini pola error yang nunjukin extractor-nya JALAN NORMAL, cuma video
  // contohnya yang emang gak ada/dihapus/private/lagi-offline — bukan
  // extractor rusak.
  return /not available|unavailable|404|private|removed|does not exist|no longer exists|offline|is not currently live/.test(msg);
}

let platformStatusCache = null;
let platformStatusCacheAt = 0;
const PLATFORM_STATUS_CACHE_MS = 5 * 60 * 1000; // 5 menit — tes ini agak berat (banyak network call), gak perlu dites ulang tiap detik

async function testPlatform(name, url) {
  const startedAt = Date.now();
  try {
    await runYtDlp([
      '--simulate', '--no-warnings', '--socket-timeout', '10',
      ...cookieArgs(), ...ytClientArgs(url), url
    ], { timeoutMs: 20000 });
    return { platform: name, healthy: true, checkedAt: Date.now(), latencyMs: Date.now() - startedAt };
  } catch (e) {
    const healthyDespiteError = isExtractorHealthyError(e.message);
    return {
      platform: name,
      healthy: healthyDespiteError,
      checkedAt: Date.now(),
      latencyMs: Date.now() - startedAt,
      note: healthyDespiteError ? 'Video contoh gak tersedia, tapi extractor jalan normal' : (e.message === 'TIMEOUT' ? 'Timeout' : 'Extractor gagal proses')
    };
  }
}

async function getPlatformStatus() {
  if (platformStatusCache && Date.now() - platformStatusCacheAt < PLATFORM_STATUS_CACHE_MS) {
    return platformStatusCache;
  }
  // Semua platform dites PARALEL (bukan satu-satu) biar total waktu tunggu
  // gak numpuk — 9 platform x 20 detik timeout kalau serial bisa 3 menit,
  // paralel maksimal ~20 detik aja (waktu platform paling lambat).
  const results = await Promise.all(
    Object.entries(PLATFORM_TEST_URLS).map(([name, url]) => testPlatform(name, url))
  );
  platformStatusCache = results;
  platformStatusCacheAt = Date.now();
  return results;
}

// Endpoint PUBLIK (read-only, gak expose apa-apa yang sensitif) — dipanggil
// frontend buat nampilin badge aktif/gak aktif per platform.
app.get('/api/platform-status', rateLimit, async (req, res) => {
  try {
    const results = await getPlatformStatus();
    res.json({ success: true, platforms: results, cachedAt: platformStatusCacheAt });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/api/health', async (req, res) => {
  const requestReceivedAt = Date.now();
  try {
    const { stdout } = await runYtDlp(['--version'], { timeoutMs: 15000 });
    const uptimeSeconds = Math.floor((Date.now() - SERVER_START_TIME) / 1000);
    res.json({
      success: true,
      ytdlpVersion: stdout.trim(),
      uptimeSeconds,
      // RAILWAY_REGION di-set otomatis sama platform Railway kalau di-deploy
      // di sana — bisa null kalau dijalanin lokal/platform lain. JUJUR: ini
      // nunjukin di mana SATU server ini di-host, bukan pilihan beberapa
      // server buat dipilih user (karena emang cuma ada 1 server).
      region: process.env.RAILWAY_REGION || null,
      serverProcessingMs: Date.now() - requestReceivedAt
    });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

async function handleInfoRequest(req, res) {
  let { url } = req.body || {};
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ success: false, error: 'URL wajib diisi.' });
  }
  if (!isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'Link harus dari TikTok, YouTube, Instagram, Facebook, Twitter/X, Reddit, SoundCloud, Twitch, atau Pinterest.' });
  }
  // Expand short-link (pin.it dll) ke URL penuh dulu — beberapa extractor
  // yt-dlp gagal ngikutin redirect short-link sendiri.
  url = await expandShortlinkIfNeeded(url);

  const blockedCheck = await checkDownloaderEnabled(url);
  if (blockedCheck) {
    return res.status(blockedCheck.statusCode).json({ success: false, error: blockedCheck.error });
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
    // --no-playlist cuma perlu buat YouTube (biar link video-di-dalam-
    // playlist gak ke-treat sebagai playlist penuh). Untuk TikTok/Instagram,
    // JANGAN pasang --no-playlist: post foto/slideshow di platform itu
    // di-extract yt-dlp lewat mode "playlist" gambar — kalau dipaksa
    // --no-playlist, entries fotonya gak pernah muncul dan post itu malah
    // salah kebaca sebagai video biasa (yang ujungnya gagal pas didownload
    // karena isinya gambar diam, bukan video).
    const noPlaylistArg = /youtube\.com|youtu\.be/i.test(url) ? ['--no-playlist'] : [];
    const isTikTokUrl = /tiktok\.com/i.test(url);
    let stdout;
    try {
      ({ stdout } = await runYtDlp([
        '-j', '--no-warnings', ...noPlaylistArg, '--socket-timeout', '20', ...cookieArgs(), ...ytClientArgs(url), url
      ], { timeoutMs: 45000 }));
    } catch (firstError) {
      // TikTok photo mode/album kadang gagal total di percobaan pertama
      // (dukungan yt-dlp buat jenis postingan ini emang belum konsisten —
      // issue terbuka di yt-dlp sendiri). --ignore-no-formats-error bikin
      // yt-dlp tetap coba ambil METADATA-nya aja meski gak nemu format video
      // yang bisa didownload — kadang ini cukup buat "menyelamatkan" info
      // dasar (title, entries foto) walau percobaan normal gagal total.
      if (!isTikTokUrl) throw firstError;
      ({ stdout } = await runYtDlp([
        '-j', '--no-warnings', '--ignore-no-formats-error', ...noPlaylistArg,
        '--socket-timeout', '20', ...cookieArgs(), ...ytClientArgs(url), url
      ], { timeoutMs: 45000 }));
    }

    const firstLine = stdout.split('\n').find(l => l.trim().startsWith('{'));
    if (!firstLine) throw new Error('Tidak bisa membaca info video.');
    const info = JSON.parse(firstLine);

    // Post foto/slideshow (umum di TikTok, kadang IG) muncul sebagai
    // "entries" berisi gambar-gambar, bukan satu video biasa.
    let isPhotoSet = Array.isArray(info.entries) && info.entries.length > 0;

    const maxHeight = Math.max(
      0,
      ...((info.formats || [])
        .map(f => f.height || 0)
        .filter(h => Number.isFinite(h)))
    );

    // Cari format video+audio (atau video-only kalau gak ada gabungan) yang
    // paling representatif buat height tertentu — dipakai buat estimasi
    // ukuran file per pilihan kualitas dan buat "Analisis Video" (codec,
    // bitrate, fps, dst) dari kualitas terbaik yang tersedia.
    function bestFormatForHeight(targetHeight) {
      const candidates = (info.formats || []).filter(f => f.vcodec && f.vcodec !== 'none' && f.height);
      if (!candidates.length) return null;
      // Cari yang paling deket ke targetHeight tanpa ngelebihin (biar cocok
      // sama apa yang bakal beneran di-download), fallback ke yang paling
      // tinggi kalau gak ada yang <= target.
      const notExceeding = candidates.filter(f => f.height <= targetHeight);
      const pool = notExceeding.length ? notExceeding : candidates;
      return pool.reduce((best, f) => (f.height > best.height ? f : best), pool[0]);
    }

    // Estimasi ukuran file total (video+audio) buat height tertentu, dalam
    // MB. filesize (exact) diprioritaskan; filesize_approx (dari bitrate x
    // durasi, kadang gak persis) jadi fallback. Ukuran audio ditambahin dari
    // format audio terbaik yang tersedia, karena kualitas video biasanya
    // di-download bareng track audio terpisah (bv*+ba).
    const bestAudioFormat = (info.formats || [])
      .filter(f => f.acodec && f.acodec !== 'none' && (!f.vcodec || f.vcodec === 'none'))
      .reduce((best, f) => {
        const size = f.filesize || f.filesize_approx || 0;
        const bestSize = best ? (best.filesize || best.filesize_approx || 0) : -1;
        return size > bestSize ? f : best;
      }, null);
    const audioSizeBytes = bestAudioFormat ? (bestAudioFormat.filesize || bestAudioFormat.filesize_approx || 0) : 0;

    function estimateSizeMb(targetHeight) {
      const f = bestFormatForHeight(targetHeight);
      if (!f) return null;
      const videoBytes = f.filesize || f.filesize_approx || 0;
      // Kalau format itu udah video+audio nyatu (progresif), gak usah
      // ditambah lagi ukuran audio terpisah.
      const alreadyHasAudio = f.acodec && f.acodec !== 'none';
      const totalBytes = videoBytes + (alreadyHasAudio ? 0 : audioSizeBytes);
      if (!totalBytes) return null;
      return Math.round((totalBytes / (1024 * 1024)) * 10) / 10;
    }

    // "Analisis Video" ditampilin dari format kualitas TERTINGGI yang
    // tersedia (yang dipakai kalau user pilih "Kualitas Terbaik") — itu yang
    // paling relevan buat user liat sebelum mutusin mau download apa gak.
    const topFormat = bestFormatForHeight(maxHeight || 99999);
    const videoAnalysis = topFormat ? {
      resolution: (topFormat.width && topFormat.height) ? `${topFormat.width}x${topFormat.height}` : null,
      aspectRatio: (topFormat.width && topFormat.height) ? dlSimplifyAspectRatio(topFormat.width, topFormat.height) : null,
      fps: topFormat.fps || null,
      videoCodec: dlSimplifyCodecName(topFormat.vcodec),
      audioCodec: dlSimplifyCodecName((bestAudioFormat && bestAudioFormat.acodec) || topFormat.acodec),
      videoBitrateKbps: topFormat.vbr ? Math.round(topFormat.vbr) : (topFormat.tbr ? Math.round(topFormat.tbr) : null),
      audioBitrateKbps: bestAudioFormat && bestAudioFormat.abr ? Math.round(bestAudioFormat.abr) : (topFormat.abr ? Math.round(topFormat.abr) : null),
      dynamicRange: topFormat.dynamic_range || null // "SDR" / "HDR10" / dst, kalau extractor-nya expose
    } : null;

    // Pengaman tambahan: kalau bukan photo-set (gak ada entries) TAPI juga
    // gak ada satupun format yang punya video track (semua vcodec='none'),
    // ini hampir pasti post foto yang gagal kebaca lewat entries -- jangan
    // tawarin opsi kualitas video yang pasti gagal, treat aja sebagai foto.
    const hasAnyVideoFormat = (info.formats || []).some(f => f.vcodec && f.vcodec !== 'none');
    if (!isPhotoSet && !hasAnyVideoFormat && !isPlaylistUrl) {
      isPhotoSet = true;
    }

    let qualities;
    let subtitleLanguages = [];
    if (isPhotoSet) {
      const photoCount = (info.entries && info.entries.length) || 1;
      if (photoCount > 1) {
        qualities = [
          { label: `📸 Download Semua Foto (${photoCount}) — ZIP`, value: 'photos' },
          { label: '🎞️ Jadikan 1 Video (Slideshow + Musik)', value: 'photos_video' }
        ];
      } else {
        // Foto tunggal (pin Pinterest biasa, atau post gambar tunggal
        // platform lain) — "ZIP buat 1 file" kesannya aneh, dan opsi
        // slideshow gak relevan buat 1 gambar doang.
        qualities = [{ label: '🖼️ Download Foto', value: 'photos' }];
      }
    } else {
      // Helper: tempelin estimasi ukuran ke label kalau ketemu, misal
      // "1080p" jadi "1080p (~24.3 MB)" — biar user tau ukurannya SEBELUM
      // klik download, gak perlu nebak-nebak dulu.
      const withSize = (label, height) => {
        const mb = estimateSizeMb(height);
        return { label: mb ? `${label} (~${mb} MB)` : label, sizeMb: mb };
      };

      const bestSize = estimateSizeMb(maxHeight || 99999);
      qualities = [{ label: bestSize ? `🎬 Kualitas Terbaik (~${bestSize} MB)` : '🎬 Kualitas Terbaik', value: 'best', sizeMb: bestSize }];
      // Cuma nawarin tingkat kualitas yang beneran ada di video ini (gak
      // ada gunanya nawarin 1080p buat video yang sumbernya cuma 480p —
      // bakal ke-upscale palsu atau malah gagal format-nya).
      if (maxHeight >= 2160) qualities.push({ value: '2160', ...withSize('2160p (4K)', 2160) });
      if (maxHeight >= 1440) qualities.push({ value: '1440', ...withSize('1440p (2K)', 1440) });
      if (maxHeight >= 1080) qualities.push({ value: '1080', ...withSize('1080p', 1080) });
      if (maxHeight >= 720) qualities.push({ value: '720', ...withSize('720p', 720) });
      if (maxHeight >= 480 || maxHeight === 0) qualities.push({ value: '480', ...withSize('480p', 480) });
      if (maxHeight >= 360) qualities.push({ value: '360', ...withSize('360p', 360) });
      if (maxHeight >= 240) qualities.push({ value: '240', ...withSize('240p', 240) });
      if (maxHeight >= 144) qualities.push({ value: '144', ...withSize('144p (hemat kuota)', 144) });
      const audioMb = audioSizeBytes ? Math.round((audioSizeBytes / (1024 * 1024)) * 10) / 10 : null;
      qualities.push({ label: audioMb ? `🎵 Audio (MP3) (~${audioMb} MB)` : '🎵 Audio (MP3)', value: 'audio', sizeMb: audioMb });
      qualities.push({ label: '🎧 Audio (Opus)', value: 'audio_opus' });
      qualities.push({ label: '🎼 Audio (M4A)', value: 'audio_m4a' });

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

    // Musik latar (TikTok/IG/YT Shorts sering nempelin metadata lagu resmi
    // di sini) — kalau ketemu, tawarin download lagunya doang secara
    // terpisah dari video utuhnya.
    const musicTitle = info.track || (info.alt_title && info.alt_title !== info.title ? info.alt_title : null);
    const musicArtist = info.artist || info.creator || null;
    const hasMusic = !isPhotoSet && !!(musicTitle || musicArtist);
    if (hasMusic) {
      qualities.push({
        label: `🎶 Download Lagu Ini — ${[musicTitle, musicArtist].filter(Boolean).join(' · ').slice(0, 40)}`,
        value: 'music'
      });
    }

    // Live/upcoming: kasih tau di awal biar user gak nunggu proses gagal
    // percuma — video live yang masih berlangsung gak selalu bisa langsung
    // di-download utuh, dan yang "upcoming" jelas belum ada filenya.
    const liveStatus = info.live_status || null; // is_live | is_upcoming | was_live | post_live | not_live
    const isLive = liveStatus === 'is_live' || liveStatus === 'post_live';
    const isUpcoming = liveStatus === 'is_upcoming';

    res.json({
      success: true,
      title: info.title || (isPhotoSet ? 'Postingan Foto' : 'Video'),
      thumbnail: info.thumbnail || (info.thumbnails && info.thumbnails.length ? info.thumbnails.at(-1).url : ''),
      duration: info.duration || 0,
      uploader: info.uploader || info.channel || '',
      uploaderUrl: info.uploader_url || info.channel_url || null,
      isVerified: !!(info.channel_is_verified || info.uploader_verified || info.is_verified || info.artist_verified),
      platform: detectPlatform(url),
      isPhotoSet,
      photoCount: isPhotoSet ? ((info.entries && info.entries.length) || 1) : 0,
      viewCount: formatCount(info.view_count),
      likeCount: formatCount(info.like_count),
      commentCount: formatCount(info.comment_count),
      repostCount: formatCount(info.repost_count),
      uploadDate: formatUploadDate(info.upload_date),
      description: (info.description || '').slice(0, 3000),
      ageRestricted: !!(info.age_limit && info.age_limit >= 18),
      isLive,
      isUpcoming,
      concurrentViewers: isLive ? formatCount(info.concurrent_view_count) : null,
      music: hasMusic ? { title: musicTitle, artist: musicArtist, album: info.album || null } : null,
      subtitleLanguages,
      qualities,
      videoAnalysis: isPhotoSet ? null : videoAnalysis
    });
  } catch (e) {
    res.status(500).json({ success: false, error: friendlyError(e.message) });
  }
}
app.post('/api/info', rateLimit, handleInfoRequest);
app.post('/api/v1/info', requireApiKey, handleInfoRequest);

async function handleDownloadRequest(req, res) {
  let { url, quality = 'best', trimStart, trimEnd, subLang, skipSponsor, format } = req.query;
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ success: false, error: 'URL wajib diisi.' });
  }
  if (!isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'Link harus dari TikTok, YouTube, Instagram, Facebook, Twitter/X, Reddit, SoundCloud, Twitch, atau Pinterest.' });
  }
  // Expand short-link (pin.it dll) ke URL penuh dulu — sama seperti /api/info.
  url = await expandShortlinkIfNeeded(url);
  const blockedCheck = await checkDownloaderEnabled(url);
  if (blockedCheck) {
    return res.status(blockedCheck.statusCode).json({ success: false, error: blockedCheck.error });
  }
  if (quality !== 'photos' && quality !== 'photos_video' && quality !== 'subtitle' && quality !== 'thumbnail' && quality !== 'music' && !QUALITY_FORMATS[quality]) {
    return res.status(400).json({ success: false, error: 'Pilihan kualitas tidak valid.' });
  }
  // Container video: cuma relevan buat download video biasa (bukan audio,
  // foto, subtitle, atau thumbnail — itu semua punya ekstensi tetap sendiri).
  const VALID_VIDEO_FORMATS = ['mp4', 'webm', 'mkv'];
  const videoFormat = VALID_VIDEO_FORMATS.includes(format) ? format : null;
  if (format && !videoFormat) {
    return res.status(400).json({ success: false, error: 'Format video gak didukung. Pilih MP4, WEBM, atau MKV.' });
  }

  // Trim durasi: cuma berlaku buat download video/audio biasa, bukan buat
  // foto/subtitle/thumbnail yang gak punya konsep "durasi".
  const ts = Number(trimStart), te = Number(trimEnd);
  const hasTrim = Number.isFinite(ts) && Number.isFinite(te) && ts >= 0 && te > ts &&
                  !['photos', 'photos_video', 'subtitle', 'thumbnail', 'preview', 'preview_audio'].includes(quality);

  // SponsorBlock: cuma masuk akal buat YouTube (data komunitasnya cuma ada
  // di sana), dan cuma buat video/audio biasa sama kayak trim.
  const hasSponsorSkip = skipSponsor === '1' && /youtube\.com|youtu\.be/i.test(url) &&
                         !['photos', 'photos_video', 'subtitle', 'thumbnail', 'preview', 'preview_audio'].includes(quality);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ninzydl-'));
  const isPhotos = quality === 'photos';
  const isPhotosVideo = quality === 'photos_video';
  const isSubtitle = quality === 'subtitle';
  const isThumbnail = quality === 'thumbnail';
  const isPreview = quality === 'preview' || quality === 'preview_audio';
  const isPreviewAudio = quality === 'preview_audio';
  const isMusic = quality === 'music';
  const isAudio = quality === 'audio' || quality === 'audio_opus' || quality === 'audio_m4a' || isMusic || isPreviewAudio;
  const audioFormat = quality === 'audio_opus' ? 'opus' : (quality === 'audio_m4a' || isPreviewAudio ? 'm4a' : 'mp3');

  const args = [
    '--no-warnings', '--no-part', '--restrict-filenames',
    '--socket-timeout', '20'
  ];

  if (isPhotos || isPhotosVideo) {
    // Slideshow foto: ambil SEMUA gambar dalam post ini (bukan playlist
    // eksternal). Buat photos_video, kita juga butuh audio latarnya biar
    // bisa digabung jadi 1 video utuh.
    args.push('--yes-playlist', '-o', path.join(tmpDir, '%(playlist_index|1)s.%(ext)s'));
    if (isPhotosVideo) {
      args.push('--write-thumbnail'); // fallback kalau audio gak ke-extract terpisah
    }
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
    args.push('-x', '--audio-format', audioFormat, '-f', QUALITY_FORMATS[quality] || QUALITY_FORMATS.audio);
    if (isMusic) args.push('--embed-thumbnail', '--embed-metadata'); // biar file lagu punya cover art + judul/artis pas dibuka di player musik
    if (hasSponsorSkip) args.push('--sponsorblock-remove', 'sponsor,selfpromo,interaction,intro,outro');
    if (hasTrim) args.push('--download-sections', `*${ts}-${te}`);
    if (hasTrim || hasSponsorSkip) args.push('--force-keyframes-at-cuts');
  } else {
    args.push('--no-playlist', '-o', path.join(tmpDir, '%(id)s.%(ext)s'));
    const outputFormat = videoFormat || 'mp4';
    args.push('--merge-output-format', outputFormat, '-f', QUALITY_FORMATS[quality]);
    // --remux-video ganti container tanpa re-encode ulang video/audio-nya
    // (cepat, gak makan resource server) — cukup buat kebanyakan kasus.
    // MKV paling fleksibel nampung codec apa aja, WEBM/MP4 kadang perlu
    // yt-dlp fallback ke recode otomatis kalau kombinasi codec-nya emang
    // gak didukung container itu, tapi itu ditangani yt-dlp sendiri.
    if (videoFormat) args.push('--remux-video', videoFormat);
    if (hasSponsorSkip) args.push('--sponsorblock-remove', 'sponsor,selfpromo,interaction,intro,outro');
    if (hasTrim) args.push('--download-sections', `*${ts}-${te}`);
    if (hasTrim || hasSponsorSkip) args.push('--force-keyframes-at-cuts');
  }
  args.push(...cookieArgs(), ...ytClientArgs(url), url);

  const cleanup = () => {
    fs.rm(tmpDir, { recursive: true, force: true }, () => {});
  };

  try {
    await runYtDlp(args, (isPhotos || isPhotosVideo) ? { timeoutMs: PROCESS_TIMEOUT_MS } : undefined);

    if (isPhotosVideo) {
      const allFiles = fs.readdirSync(tmpDir).filter(f => !f.startsWith('.'));
      const imageFiles = allFiles
        .filter(f => /\.(jpe?g|png|webp)$/i.test(f))
        .sort((a, b) => (parseInt(a) || 0) - (parseInt(b) || 0));
      if (!imageFiles.length) throw new Error('Foto tidak ditemukan di post ini.');

      // Cari file audio (musik latar TikTok/IG) kalau ada — yt-dlp kadang
      // narik audio slideshow sebagai file audio terpisah (m4a/mp3/webm).
      const audioFile = allFiles.find(f => /\.(m4a|mp3|opus|webm|aac)$/i.test(f) && !imageFiles.includes(f));

      const DURATION_PER_PHOTO = 2.5; // detik per foto kalau gak ada audio buat nentuin panjang video
      const listPath = path.join(tmpDir, 'slideshow.txt');
      const listContent = imageFiles.map(f =>
        `file '${path.join(tmpDir, f).replace(/'/g, "'\\''")}'\nduration ${DURATION_PER_PHOTO}`
      ).join('\n') + `\nfile '${path.join(tmpDir, imageFiles[imageFiles.length - 1]).replace(/'/g, "'\\''")}'`;
      fs.writeFileSync(listPath, listContent);

      const outPath = path.join(tmpDir, 'slideshow_output.mp4');
      const ffmpegArgs = [
        '-y', '-f', 'concat', '-safe', '0', '-i', listPath
      ];
      if (audioFile) ffmpegArgs.push('-i', path.join(tmpDir, audioFile));
      ffmpegArgs.push(
        '-vf', "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
        '-c:v', 'libx264', '-r', '30'
      );
      if (audioFile) ffmpegArgs.push('-c:a', 'aac', '-shortest');
      ffmpegArgs.push(outPath);

      await new Promise((resolve, reject) => {
        const ff = spawn('ffmpeg', ffmpegArgs, { windowsHide: true });
        let stderr = '';
        ff.stderr.on('data', d => { stderr += d.toString(); });
        ff.on('error', reject);
        ff.on('close', code => code === 0 ? resolve() : reject(new Error('Gagal menggabungkan foto jadi video: ' + stderr.trim().split('\n').pop())));
      });

      const stat = fs.statSync(outPath);
      const safeName = `ninzy_slideshow_${crypto.randomBytes(3).toString('hex')}.mp4`;
      res.setHeader('Content-Type', 'video/mp4');
      res.setHeader('Content-Length', stat.size);
      res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);
      const stream = fs.createReadStream(outPath);
      stream.pipe(res);
      stream.on('close', cleanup);
      stream.on('error', () => { cleanup(); if (!res.headersSent) res.status(500).end(); });
      return;
    }

    if (isPhotos) {
      const files = fs.readdirSync(tmpDir).filter(f => !f.startsWith('.'));
      if (!files.length) throw new Error('Foto tidak ditemukan di post ini.');

      // Foto tunggal (pin Pinterest, atau post gambar tunggal platform lain)
      // — kirim langsung file-nya, gak usah di-ZIP. User gak perlu extract
      // ZIP cuma buat 1 gambar.
      if (files.length === 1) {
        const filePath = path.join(tmpDir, files[0]);
        const ext = path.extname(files[0]).replace('.', '') || 'jpg';
        const stat = fs.statSync(filePath);
        const safeName = `ninzy_foto_${crypto.randomBytes(3).toString('hex')}.${ext}`;
        const contentType = ext === 'png' ? 'image/png' : ext === 'webp' ? 'image/webp' : 'image/jpeg';
        res.setHeader('Content-Type', contentType);
        res.setHeader('Content-Length', stat.size);
        res.setHeader('Content-Disposition', `attachment; filename="${safeName}"`);
        const stream = fs.createReadStream(filePath);
        stream.pipe(res);
        stream.on('close', cleanup);
        stream.on('error', () => { cleanup(); if (!res.headersSent) res.status(500).end(); });
        return;
      }

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

    const safeName = `ninzy_${isMusic ? 'lagu' : ''}${crypto.randomBytes(3).toString('hex')}.${ext}`;
    const contentType = isAudio
      ? (audioFormat === 'opus' ? 'audio/opus' : audioFormat === 'm4a' ? 'audio/mp4' : 'audio/mpeg')
      : (ext === 'webm' ? 'video/webm' : ext === 'mkv' ? 'video/x-matroska' : 'video/mp4');

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
}
app.get('/api/download', rateLimit, handleDownloadRequest);
app.get('/api/v1/download', requireApiKey, handleDownloadRequest);

const BATCH_MAX_URLS = 5;

// Logika inti download banyak link sekaligus lalu dibundling jadi ZIP.
// Diekstrak jadi fungsi terpisah supaya bisa dipakai baik dari
// /api/batch-download (link manual dari user) maupun dari
// /api/creator-download-all (link hasil scan profil/channel).
async function runBatchDownload(cleanUrls, quality, res) {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ninzybatch-'));
  const cleanup = () => fs.rm(tmpDir, { recursive: true, force: true }, () => {});
  const isAudio = quality === 'audio' || quality === 'audio_opus' || quality === 'audio_m4a';
  const audioFormat = quality === 'audio_opus' ? 'opus' : (quality === 'audio_m4a' ? 'm4a' : 'mp3');
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
}

app.post('/api/batch-download', batchRateLimit, async (req, res) => {
  const { urls, quality = 'best' } = req.body || {};
  let cleanUrls = Array.isArray(urls) ? urls.map(u => (u || '').trim()).filter(Boolean) : [];

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
  // Expand short-link (pin.it dll) di semua URL batch sekaligus, paralel.
  cleanUrls = await Promise.all(cleanUrls.map(u => expandShortlinkIfNeeded(u)));
  if (!['best', '2160', '1440', '1080', '720', '480', '360', '240', '144', 'audio', 'audio_opus', 'audio_m4a'].includes(quality)) {
    return res.status(400).json({ success: false, error: 'Pilihan kualitas tidak valid untuk batch.' });
  }

  await runBatchDownload(cleanUrls, quality, res);
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

// ── Kode Aktivasi Premium (activationCodes collection) ────────────────────
// PENTING: ini yang benerin celah paling kritis — sebelumnya redeem cuma
// ngebaca angka dari format kode ("NINZY-30-xxx" = 30 hari) tanpa pernah
// cek ke server apakah kode itu beneran pernah di-generate admin. Sekarang
// kode WAJIB ada & valid di Firestore sebelum premium diberikan.

app.post('/api/admin/create-activation-code', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const { code, days, maxUses, buyerName, buyerWa, note, price } = req.body || {};
  if (!code || typeof code !== 'string' || !/^[A-Z0-9-]{4,40}$/.test(code)) {
    return res.status(400).json({ success: false, error: 'Format kode tidak valid.' });
  }
  const d = Number(days);
  if (!Number.isFinite(d) || d <= 0 || d > 3650) {
    return res.status(400).json({ success: false, error: 'Jumlah hari tidak valid.' });
  }
  const mu = Number(maxUses) || 1;

  try {
    const ref = fsDb.collection('activationCodes').doc(code);
    const existing = await ref.get();
    if (existing.exists) {
      return res.status(409).json({ success: false, error: 'Kode ini sudah pernah dibuat sebelumnya, pakai kode lain.' });
    }
    await ref.set({
      code, days: d, maxUses: mu, usedCount: 0, active: true,
      buyerName: buyerName || '', buyerWa: buyerWa || '', note: note || '', price: price || '',
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });
    res.json({ success: true, code, days: d, maxUses: mu });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal menyimpan kode: ' + e.message });
  }
});

app.post('/api/admin/toggle-activation-code', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const { code, active } = req.body || {};
  if (!code) return res.status(400).json({ success: false, error: 'Kode wajib diisi.' });
  try {
    await fsDb.collection('activationCodes').doc(code).update({ active: !!active });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal update kode: ' + e.message });
  }
});

app.get('/api/admin/activation-codes', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  try {
    const snap = await fsDb.collection('activationCodes').orderBy('createdAt', 'desc').limit(200).get();
    const codes = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    res.json({ success: true, codes });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal ambil daftar kode: ' + e.message });
  }
});

app.post('/api/redeem-code', rateLimit, requireFirebaseAdmin, async (req, res) => {
  const { email, code: rawCode } = req.body || {};
  const code = (rawCode || '').trim().toUpperCase();
  const cleanEmail = (email || '').trim().toLowerCase();

  if (!cleanEmail || !cleanEmail.includes('@')) {
    return res.status(400).json({ success: false, error: 'Kamu harus login dulu buat redeem kode.' });
  }
  if (!code) {
    return res.status(400).json({ success: false, error: 'Masukkan kode aktivasi dulu.' });
  }

  try {
    const codeRef = fsDb.collection('activationCodes').doc(code);
    const userRef = fsDb.collection('users').doc(cleanEmail);

    // Transaction: pastikan kode gak bisa dipakai 2x barengan (race condition)
    const result = await fsDb.runTransaction(async (tx) => {
      const codeSnap = await tx.get(codeRef);
      if (!codeSnap.exists) {
        throw new Error('Kode aktivasi tidak ditemukan. Cek kembali penulisan kodenya.');
      }
      const c = codeSnap.data();
      if (!c.active) {
        throw new Error('Kode ini sudah tidak aktif.');
      }
      const maxUses = c.maxUses || 1;
      const usedCount = c.usedCount || 0;
      if (usedCount >= maxUses) {
        throw new Error(`Kode ini sudah mencapai batas ${maxUses} pengguna.`);
      }

      const userSnap = await tx.get(userRef);
      const currentExpiry = (userSnap.exists && userSnap.data().premiumExpiry) || 0;
      const base = currentExpiry > Date.now() ? currentExpiry : Date.now();
      const newExpiry = base + c.days * 86400000;
      const totalDaysLeft = Math.ceil((newExpiry - Date.now()) / 86400000);

      tx.update(codeRef, { usedCount: usedCount + 1 });
      tx.set(userRef, {
        premiumExpiry: newExpiry, premiumDays: totalDaysLeft,
        premiumSetAt: Date.now(), premiumSetBy: 'redeem-code'
      }, { merge: true });

      return { days: c.days, newExpiry, totalDaysLeft };
    });

    res.json({ success: true, ...result });
  } catch (e) {
    res.status(400).json({ success: false, error: e.message || 'Gagal redeem kode.' });
  }
});

// ── Premium (admin) ────────────────────────────────────────────────────
app.post('/api/admin/grant-premium', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const email = (req.body?.email || '').trim().toLowerCase();
  const days = Number(req.body?.days);
  if (!email || !email.includes('@')) return res.status(400).json({ success: false, error: 'Email tidak valid.' });
  if (!Number.isFinite(days) || days <= 0 || days > 3650) return res.status(400).json({ success: false, error: 'Jumlah hari tidak valid.' });
  try {
    const ref = fsDb.collection('users').doc(email);
    const snap = await ref.get();
    const existing = snap.exists ? (snap.data().premiumExpiry || 0) : 0;
    const base = existing > Date.now() ? existing : Date.now();
    const expiry = base + days * 86400000;
    const totalDays = Math.round((expiry - Date.now()) / 86400000);
    await ref.set({ premiumExpiry: expiry, premiumDays: totalDays, premiumSetAt: Date.now(), premiumSetBy: 'admin' }, { merge: true });
    res.json({ success: true, expiry, totalDays, extended: existing > Date.now() });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal memberi premium: ' + e.message });
  }
});

app.post('/api/admin/revoke-premium', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const email = (req.body?.email || '').trim().toLowerCase();
  if (!email || !email.includes('@')) return res.status(400).json({ success: false, error: 'Email tidak valid.' });
  try {
    await fsDb.collection('users').doc(email).set({ premiumExpiry: 0, premiumSetBy: 'admin-revoke' }, { merge: true });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal mencabut premium: ' + e.message });
  }
});

// ── Payment (admin) ────────────────────────────────────────────────────
app.post('/api/admin/confirm-payment', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const { paymentId, status, email, days } = req.body || {};
  if (!paymentId || !['approved', 'rejected'].includes(status)) {
    return res.status(400).json({ success: false, error: 'Data tidak lengkap/valid.' });
  }
  try {
    await fsDb.collection('payments').doc(paymentId).update({ status, processedAt: Date.now() });

    let premiumResult = null;
    if (status === 'approved' && email && Number(days) > 0) {
      const cleanEmail = email.trim().toLowerCase();
      const ref = fsDb.collection('users').doc(cleanEmail);
      const snap = await ref.get();
      const existing = snap.exists ? (snap.data().premiumExpiry || 0) : 0;
      const base = existing > Date.now() ? existing : Date.now();
      const expiry = base + Number(days) * 86400000;
      const totalDays = Math.round((expiry - Date.now()) / 86400000);
      await ref.set({ premiumExpiry: expiry, premiumDays: totalDays, premiumSetAt: Date.now(), premiumSetBy: 'admin-payment' }, { merge: true });
      premiumResult = { expiry, totalDays, extended: existing > Date.now() };
    }
    res.json({ success: true, premium: premiumResult });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal proses payment: ' + e.message });
  }
});

// ── Moderasi (admin) ───────────────────────────────────────────────────
app.post('/api/admin/ban-user', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const email = (req.body?.email || '').trim().toLowerCase();
  const reason = (req.body?.reason || '').trim() || 'Tidak ada alasan';
  if (!email || !email.includes('@')) return res.status(400).json({ success: false, error: 'Email tidak valid.' });
  try {
    await fsDb.collection('bannedUsers').doc(email).set({ email, reason, bannedAt: Date.now() });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal ban user: ' + e.message });
  }
});

app.post('/api/admin/unban-user', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const email = (req.body?.email || '').trim().toLowerCase();
  if (!email) return res.status(400).json({ success: false, error: 'Email tidak valid.' });
  try {
    await fsDb.collection('bannedUsers').doc(email).delete();
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal unban user: ' + e.message });
  }
});

app.post('/api/admin/warn-user', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const email = (req.body?.email || '').trim().toLowerCase();
  const reason = (req.body?.reason || '').trim() || 'Tidak ada alasan';
  if (!email || !email.includes('@')) return res.status(400).json({ success: false, error: 'Email tidak valid.' });
  try {
    const ref = fsDb.collection('userWarnings').doc(email);
    const doc = await ref.get();
    const count = (doc.exists ? (doc.data().count || 0) : 0) + 1;
    await ref.set({ email, count, lastReason: reason, lastAt: Date.now() }, { merge: true });
    res.json({ success: true, count });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal memberi warning: ' + e.message });
  }
});

// ── Kupon Diskon Pembayaran (beda dari kode aktivasi — ini persen-based,
// dipakai pas checkout QRIS/GoPay) ─────────────────────────────────────
app.post('/api/admin/create-discount-coupon', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const { code, percent, maxUses, days } = req.body || {};
  const cleanCode = (code || '').trim().toUpperCase();
  if (!cleanCode || !/^[A-Z0-9-]{2,40}$/.test(cleanCode)) return res.status(400).json({ success: false, error: 'Format kode tidak valid.' });
  const p = Number(percent);
  if (!Number.isFinite(p) || p <= 0 || p > 100) return res.status(400).json({ success: false, error: 'Persentase tidak valid.' });
  try {
    await fsDb.collection('coupons').doc(cleanCode).set({
      code: cleanCode, percent: p, maxUses: Number(maxUses) || 50, usedCount: 0,
      active: true, expiresAt: Date.now() + (Number(days) || 30) * 86400000, createdAt: Date.now()
    });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal membuat kupon: ' + e.message });
  }
});

app.post('/api/admin/toggle-discount-coupon', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const { code, active } = req.body || {};
  if (!code) return res.status(400).json({ success: false, error: 'Kode wajib diisi.' });
  try {
    await fsDb.collection('coupons').doc(code).update({ active: !!active });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal update kupon: ' + e.message });
  }
});

// Profil pembuat video (followers, avatar, bio) — proses TERPISAH dari info
// video karena butuh "mengunjungi" halaman profil, bukan halaman video.
// Dukungannya beda-beda tiap platform (yt-dlp gak selalu dapet semua field
// buat semua situs), jadi ini best-effort: field yang gak ketemu ya null aja.
app.get('/api/creator-profile', rateLimit, async (req, res) => {
  const { url } = req.query;
  if (!url || typeof url !== 'string' || !isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'URL profil tidak valid.' });
  }
  try {
    // Field avatar/nama channel itu beda-beda letaknya tergantung gimana
    // yt-dlp resolve URL profil (playlist-items 0 vs 1, tergantung versi &
    // extractor platform). Daripada nebak satu cara yang "pasti benar",
    // kita coba beberapa strategi dan VALIDASI hasilnya sebelum dipakai —
    // supaya gak ketuker sama thumbnail video individual kayak yang
    // sempet kejadian.
    async function tryFetchProfile(playlistItems) {
      const { stdout } = await runYtDlp([
        '--dump-single-json', '--playlist-items', playlistItems,
        '--no-warnings', '--socket-timeout', '15', ...cookieArgs(), ...ytClientArgs(url), url
      ], { timeoutMs: 25000 });
      return JSON.parse(stdout.split('\n').find(l => l.trim().startsWith('{')) || '{}');
    }

    // "Objek video individual" dicirikan oleh _type "video" (atau kosong)
    // DAN gak punya entries/playlist_count sama sekali. Objek channel/user
    // ditandai _type "playlist"/"multi_video"/"url" ATAU punya
    // entries/playlist_count, meskipun cuma 0-1 entri yang di-resolve.
    function looksLikeChannelObject(data) {
      if (!data || typeof data !== 'object') return false;
      if (data._type && data._type !== 'video') return true;
      if (Array.isArray(data.entries)) return true;
      if (Number.isFinite(data.playlist_count)) return true;
      return false;
    }

    // Avatar creator biasanya ditandai id spesifik ("avatar_uncropped",
    // "avatar_larger", dst) di array thumbnails milik objek channel.
    //
    // allowVideoLikeObject: TikTok itu kasus khusus — dari source code
    // extractor resminya, avatar creator DIAMBIL dengan cara resolve video
    // PERTAMA di profil itu (videos[0].author.avatar_larger), lalu hasilnya
    // dipetakan ke field "thumbnail" level-root. Artinya buat TikTok, objek
    // hasil resolve 1 video BISA jadi sumber avatar yang valid meskipun
    // "keliatan" kayak objek video individual (gak ada _type playlist atau
    // entries) — beda dari platform lain yang biasanya avatar cuma valid
    // kalau objeknya jelas-jelas level-channel.
    function pickAvatar(data, { allowVideoLikeObject = false } = {}) {
      const thumbs = data.thumbnails || [];
      const tagged = thumbs.find(t => t.id && /avatar/i.test(t.id));
      if (tagged) return tagged.url;
      if (!thumbs.length) return null;
      if (!allowVideoLikeObject && !looksLikeChannelObject(data)) return null;

      const fallbackUrl = thumbs.at(-1).url;
      // Sanity check: kalau thumbnail "channel" ini persis sama dengan
      // thumbnail video pertama di entries, itu tandanya yt-dlp sebenernya
      // ngasih kita thumbnail VIDEO (bukan avatar asli) — mending gak usah
      // ditampilin sama sekali daripada nampilin video-cover sebagai foto
      // profil (kayak yang sempet kejadian).
      const firstEntry = Array.isArray(data.entries) ? data.entries[0] : null;
      const firstEntryThumb = firstEntry && (
        firstEntry.thumbnail ||
        (Array.isArray(firstEntry.thumbnails) && firstEntry.thumbnails.length ? firstEntry.thumbnails.at(-1).url : null)
      );
      if (firstEntryThumb && firstEntryThumb === fallbackUrl) return null;

      // Sanity check kedua, khusus mode allowVideoLikeObject: kalau
      // thumbnail yang mau dipakai ternyata SAMA PERSIS dengan thumbnail
      // video utama objek itu sendiri (data.thumbnail), itu juga tanda kuat
      // ini cover video, bukan avatar — video biasanya nunjuk thumbnail
      // videonya sendiri sebagai data.thumbnail (elemen terakhir array).
      if (allowVideoLikeObject && data.thumbnail && data.thumbnail === fallbackUrl && thumbs.length <= 1) {
        return null;
      }

      return fallbackUrl;
    }

    const isTikTok = /tiktok\.com/i.test(url);

    let data = await tryFetchProfile('0');
    let avatar = pickAvatar(data);
    let name = data.channel || data.uploader || data.title || null;

    // Retry pakai 1 item kalau avatar belum ketemu (dengan 0 item beberapa
    // extractor emang gak bisa narik avatar sama sekali — perlu minimal 1
    // video di-resolve). TikTok SELALU butuh retry ini karena avatar-nya
    // emang cuma bisa didapet dari resolve video pertama (lihat penjelasan
    // di pickAvatar), jadi allowVideoLikeObject di-set true khusus platform
    // ini biar avatar-nya gak keblokir validasi "harus keliatan channel".
    if (!avatar) {
      const retryData = await tryFetchProfile('1');
      const retryAvatar = pickAvatar(retryData, { allowVideoLikeObject: isTikTok });
      if (retryAvatar) {
        avatar = retryAvatar;
        // retryData terbukti punya avatar valid — pakai itu juga sebagai
        // sumber field lain (followers/bio/name) kalau field itu kosong di
        // data awal, karena kemungkinan besar retryData lebih lengkap.
        if (!name) name = retryData.channel || retryData.uploader || retryData.title || null;
        if (!data.channel_follower_count && retryData.channel_follower_count) data = retryData;
      } else if (!name) {
        name = retryData.channel || retryData.uploader || retryData.title || null;
      }
    }

    const totalVideos = Number.isFinite(data.playlist_count) ? data.playlist_count
      : (Number.isFinite(data.n_entries) ? data.n_entries : null);

    if (!name && !avatar) {
      throw new Error('Profil kosong / gak ketemu.');
    }

    res.json({
      success: true,
      name,
      handle: data.uploader_id || data.channel_id || null,
      avatar,
      followers: formatCount(data.channel_follower_count),
      bio: data.description ? data.description.slice(0, 600) : null,
      isVerified: !!(data.channel_is_verified || data.uploader_verified || data.is_verified || data.artist_verified),
      totalVideos: totalVideos !== null ? formatCount(totalVideos) : null,
      externalLinks: Array.isArray(data.webpage_url_domain) ? null : (data.channel_url || data.uploader_url || null),
      url
    });
  } catch (e) {
    res.status(404).json({ success: false, error: 'Info profil gak tersedia buat platform ini.' });
  }
});

// Video-video lain dari creator yang sama ("video terkait" ATAU, dengan
// limit lebih besar + mode all=1, "download semua dari akun ini"). Sama
// seperti creator-profile, ini "mengunjungi" halaman channel/profil (bukan
// halaman video tunggal), pakai --flat-playlist biar cepat (gak buka tiap
// video satu-satu).
const CREATOR_VIDEOS_CAP = 8; // mode "video terkait" (ringkas)
const CREATOR_VIDEOS_ALL_CAP = 30; // mode "download semua" (dibatasi biar server gratisan gak keberatan)
app.get('/api/creator-videos', rateLimit, async (req, res) => {
  const { url, all } = req.query;
  if (!url || typeof url !== 'string' || !isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'URL profil tidak valid.' });
  }
  const isAllMode = all === '1';
  const cap = isAllMode ? CREATOR_VIDEOS_ALL_CAP : CREATOR_VIDEOS_CAP;
  try {
    const { stdout } = await runYtDlp([
      '--flat-playlist', '-j', '--no-warnings', '--playlist-end', String(cap),
      '--socket-timeout', isAllMode ? '25' : '15', ...cookieArgs(), ...ytClientArgs(url), url
    ], { timeoutMs: isAllMode ? 60000 : 30000 });

    const entries = stdout.split('\n')
      .filter(l => l.trim().startsWith('{'))
      .map(l => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean)
      // Beberapa extractor nyelipin entry non-video (misal info channel itu
      // sendiri) di baris pertama flat-playlist — buang yang gak punya id.
      .filter(e => e.id);

    if (!entries.length) {
      return res.status(404).json({ success: false, error: 'Gak ada video lain yang ketemu dari creator ini.' });
    }

    const platform = detectPlatform(url);
    // flat-playlist ngasih "url"/"webpage_url" yang formatnya beda-beda tiap
    // extractor (kadang link penuh, kadang cuma id) — fallback ke link
    // YouTube standar kalau memang platform-nya YouTube dan yang lain kosong.
    const resolveVideoUrl = (e) => {
      if (e.webpage_url) return e.webpage_url;
      if (e.url && /^https?:\/\//.test(e.url)) return e.url;
      if (platform === 'YouTube' && e.id) return `https://www.youtube.com/watch?v=${e.id}`;
      return null;
    };

    res.json({
      success: true,
      platform,
      capped: entries.length >= cap,
      videos: entries.slice(0, cap).map(e => ({
        id: e.id,
        title: e.title || 'Video',
        thumbnail: e.thumbnails && e.thumbnails.length ? e.thumbnails.at(-1).url : (e.thumbnail || ''),
        duration: e.duration || 0,
        viewCount: formatCount(e.view_count),
        url: resolveVideoUrl(e)
      })).filter(v => v.url)
    });
  } catch (e) {
    res.status(404).json({ success: false, error: 'Video terkait gak tersedia buat platform ini.' });
  }
});

// Download semua video dari 1 akun sekaligus, dibundling jadi ZIP. Ini versi
// "berat" dari batch-download (mirip alurnya) tapi sumber daftar link-nya
// dari profil/channel, bukan dari input manual user.
const CREATOR_DOWNLOAD_ALL_MAX = 15; // dibatasi lebih ketat dari list (30) karena tiap video beneran di-download, bukan cuma dibaca metadatanya
app.post('/api/creator-download-all', batchRateLimit, async (req, res) => {
  const { profileUrl, quality = 'best' } = req.body || {};
  if (!profileUrl || typeof profileUrl !== 'string' || !isSupportedUrl(profileUrl)) {
    return res.status(400).json({ success: false, error: 'URL profil tidak valid.' });
  }
  if (!['best', '2160', '1440', '1080', '720', '480', '360', '240', '144', 'audio', 'audio_opus', 'audio_m4a'].includes(quality)) {
    return res.status(400).json({ success: false, error: 'Pilihan kualitas tidak valid.' });
  }
  try {
    const { stdout } = await runYtDlp([
      '--flat-playlist', '-j', '--no-warnings', '--playlist-end', String(CREATOR_DOWNLOAD_ALL_MAX),
      '--socket-timeout', '20', ...cookieArgs(), ...ytClientArgs(profileUrl), profileUrl
    ], { timeoutMs: 40000 });

    const platform = detectPlatform(profileUrl);
    const entries = stdout.split('\n')
      .filter(l => l.trim().startsWith('{'))
      .map(l => { try { return JSON.parse(l); } catch { return null; } })
      .filter(e => e && e.id);

    const urls = entries.map(e => {
      if (e.webpage_url) return e.webpage_url;
      if (e.url && /^https?:\/\//.test(e.url)) return e.url;
      if (platform === 'YouTube') return `https://www.youtube.com/watch?v=${e.id}`;
      return null;
    }).filter(Boolean).slice(0, CREATOR_DOWNLOAD_ALL_MAX);

    if (!urls.length) {
      return res.status(404).json({ success: false, error: 'Gak ada video yang ketemu buat di-download dari akun ini.' });
    }

    // Reuse logika batch-download yang udah ada (download tiap URL satu-satu,
    // bundle jadi ZIP) supaya gak duplikasi kode penanganan proses/cleanup.
    await runBatchDownload(urls, quality, res);
  } catch (e) {
    res.status(500).json({ success: false, error: friendlyError(e.message) });
  }
});

// ── Admin: monitoring downloader ──────────────────────────────────────────

// Live log — ambil dari in-memory buffer (cepat, real-time, gak nunggu
// Firestore). limit dibatasi biar respons tetep ringan.
app.get('/api/admin/downloader-logs', requireAdminToken, (req, res) => {
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 100));
  const logs = requestLogBuffer.slice(-limit).reverse(); // terbaru duluan
  res.json({ success: true, logs, bufferSize: requestLogBuffer.length });
});

// Statistik — dihitung dari in-memory buffer (kejadian sejak server
// terakhir nyala). JUJUR soal keterbatasan: ini BUKAN statistik permanen
// dari awal waktu, karena buffer-nya kebatas 500 entri terakhir dan ke-reset
// tiap restart server. Buat histori lebih panjang, data mentahnya juga
// ketulis ke Firestore collection "downloaderLogs" (bisa diquery manual dari
// Firebase Console kalau butuh rentang waktu lebih jauh).
app.get('/api/admin/downloader-stats', requireAdminToken, (req, res) => {
  const logs = requestLogBuffer;
  const now = Date.now();
  const last1h = logs.filter(l => now - l.ts < 60 * 60 * 1000);
  const last24h = logs.filter(l => now - l.ts < 24 * 60 * 60 * 1000);

  function summarize(subset) {
    const total = subset.length;
    const errors = subset.filter(l => l.statusCode >= 400).length;
    const totalBytes = subset.reduce((sum, l) => sum + (l.bytesOut || 0), 0);
    const avgDurationMs = total ? Math.round(subset.reduce((sum, l) => sum + (l.durationMs || 0), 0) / total) : 0;

    const byPlatform = {};
    subset.forEach(l => {
      const p = l.platform || 'Lainnya';
      byPlatform[p] = (byPlatform[p] || 0) + 1;
    });

    const byEndpoint = {};
    subset.forEach(l => {
      byEndpoint[l.path] = (byEndpoint[l.path] || 0) + 1;
    });

    // Grafik error: hitung error per-jam buat 24 jam terakhir (dipakai
    // frontend buat gambar grafik batang sederhana).
    return {
      total, errors,
      errorRate: total ? Math.round((errors / total) * 1000) / 10 : 0,
      totalBytesOut: totalBytes,
      avgDurationMs,
      byPlatform,
      byEndpoint
    };
  }

  // Grafik error per jam (24 jam terakhir) — array 24 angka, index 0 = 23
  // jam lalu, index 23 = jam sekarang.
  const errorByHour = Array(24).fill(0);
  const totalByHour = Array(24).fill(0);
  last24h.forEach(l => {
    const hoursAgo = Math.floor((now - l.ts) / (60 * 60 * 1000));
    const idx = 23 - Math.min(23, hoursAgo);
    totalByHour[idx]++;
    if (l.statusCode >= 400) errorByHour[idx]++;
  });

  res.json({
    success: true,
    serverUptimeSeconds: Math.floor((Date.now() - SERVER_START_TIME) / 1000),
    last1h: summarize(last1h),
    last24h: summarize(last24h),
    errorByHour,
    totalByHour,
    bufferedSince: logs.length ? new Date(logs[0].ts).toISOString() : null
  });
});

// Config downloader — toggle nyala/mati, platform aktif, pesan maintenance.
app.get('/api/admin/downloader-config', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  try {
    // Baca langsung dari Firestore (bukan cache) buat endpoint admin —
    // admin harus selalu liat data terbaru pas buka panel, cache 60 detik
    // cuma buat jalur publik yang lebih sering di-hit.
    const doc = await fsDb.collection('adminSystem').doc(DOWNLOADER_CONFIG_DOC).get();
    res.json({ success: true, config: doc.exists ? { ...DEFAULT_DOWNLOADER_CONFIG, ...doc.data() } : DEFAULT_DOWNLOADER_CONFIG });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post('/api/admin/downloader-config', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  const { enabled, maintenanceMessage, platforms } = req.body || {};
  const update = {};
  if (typeof enabled === 'boolean') update.enabled = enabled;
  if (maintenanceMessage === null || typeof maintenanceMessage === 'string') update.maintenanceMessage = maintenanceMessage || null;
  if (platforms && typeof platforms === 'object') update.platforms = platforms;
  if (!Object.keys(update).length) {
    return res.status(400).json({ success: false, error: 'Gak ada perubahan yang dikirim.' });
  }
  try {
    await fsDb.collection('adminSystem').doc(DOWNLOADER_CONFIG_DOC).set(update, { merge: true });
    downloaderConfigCache = null; // invalidate cache biar perubahan langsung kepakai, gak nunggu 60 detik
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// Endpoint PUBLIK (tanpa admin token) — dipanggil frontend Vercel buat tau
// downloader lagi aktif apa nggak, dan platform mana yang dibatasi
// sementara, SEBELUM user coba cek video. Read-only, gak expose data
// sensitif, jadi aman diakses tanpa token.
app.get('/api/downloader-config', rateLimit, async (req, res) => {
  const config = await getDownloaderConfig();
  res.json({ success: true, config });
});

// ── Push Notification: public key, subscribe, unsubscribe, kirim ──────────
app.get('/api/push/public-key', (req, res) => {
  if (!PUSH_ENABLED) return res.status(503).json({ success: false, error: 'Push notification belum dikonfigurasi di server.' });
  res.json({ success: true, publicKey: VAPID_PUBLIC_KEY });
});

app.post('/api/push/subscribe', rateLimit, requireFirebaseAdmin, async (req, res) => {
  if (!PUSH_ENABLED) return res.status(503).json({ success: false, error: 'Push notification belum dikonfigurasi di server.' });
  const { subscription, email } = req.body || {};
  if (!subscription || !subscription.endpoint || !subscription.keys) {
    return res.status(400).json({ success: false, error: 'Data subscription gak lengkap.' });
  }
  const subId = sha256Hex(subscription.endpoint);
  try {
    const ref = fsDb.collection('pushSubscriptions').doc(subId);
    const existing = await ref.get();
    await ref.set({
      subscription,
      email: (email || '').toLowerCase().trim() || null,
      userAgent: (req.headers['user-agent'] || '').slice(0, 300),
      updatedAt: Date.now(),
      ...(existing.exists ? {} : { createdAt: Date.now() })
    }, { merge: true });
    res.json({ success: true, subId });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal simpan subscription.' });
  }
});

app.post('/api/push/unsubscribe', rateLimit, requireFirebaseAdmin, async (req, res) => {
  const { endpoint } = req.body || {};
  if (!endpoint) return res.status(400).json({ success: false, error: 'Endpoint wajib diisi.' });
  try {
    await fsDb.collection('pushSubscriptions').doc(sha256Hex(endpoint)).delete();
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal hapus subscription.' });
  }
});

// Relay dari client buat notifikasi yang triggernya kejadian di BROWSER
// (kompresi FFmpeg.wasm selesai, dll — bukan sesuatu yang backend ini bisa
// tau sendiri). "type" dibatasi ke PUSH_PRESETS di atas, bukan title/body
// bebas, biar endpoint ini gak jadi celah spam-notif ke diri sendiri.
app.post('/api/push/send', rateLimit, async (req, res) => {
  if (!PUSH_ENABLED) return res.status(503).json({ success: false, error: 'Push notification belum dikonfigurasi di server.' });
  const { endpoint, type, data } = req.body || {};
  if (!endpoint || !PUSH_PRESETS[type]) {
    return res.status(400).json({ success: false, error: 'endpoint atau type gak valid.' });
  }
  const result = await sendPushToSubId(sha256Hex(endpoint), PUSH_PRESETS[type](data));
  if (!result.ok) {
    return res.status(result.reason === 'not_found' ? 404 : 500).json({ success: false, error: 'Gagal kirim push (' + result.reason + ').' });
  }
  res.json({ success: true });
});

// ── Creator Watch: subscribe/unsubscribe alert upload baru dari favorit ───
app.post('/api/creator-watch/subscribe', rateLimit, requireFirebaseAdmin, async (req, res) => {
  const { uploaderUrl, name, platform, avatar, endpoint } = req.body || {};
  if (!uploaderUrl || !isSupportedUrl(uploaderUrl) || !endpoint) {
    return res.status(400).json({ success: false, error: 'Data creator/endpoint gak lengkap atau gak valid.' });
  }
  const subId = sha256Hex(endpoint);
  try {
    const subSnap = await fsDb.collection('pushSubscriptions').doc(subId).get();
    if (!subSnap.exists) {
      return res.status(400).json({ success: false, error: 'Aktifkan push notification dulu sebelum subscribe creator.' });
    }
    const ref = fsDb.collection('creatorWatch').doc(sha256Hex(uploaderUrl));
    const existing = await ref.get();
    if (!existing.exists) {
      // Seed lastVideoId dari video TERBARU SAAT INI, biar user gak langsung
      // dapet notif "upload baru" buat video yang sebenernya udah lama ada.
      let seed = null;
      try { seed = await fetchLatestVideoForCreator(uploaderUrl); } catch (e) {}
      await ref.set({
        uploaderUrl, name: name || 'Creator', platform: platform || 'Unknown', avatar: avatar || null,
        lastVideoId: seed ? seed.id : null,
        lastVideoTitle: seed ? seed.title : null,
        lastCheckedAt: Date.now(),
        createdAt: Date.now(),
        subscriberSubIds: [subId]
      });
    } else {
      await ref.set({ subscriberSubIds: admin.firestore.FieldValue.arrayUnion(subId) }, { merge: true });
    }
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal subscribe creator watch.' });
  }
});

app.post('/api/creator-watch/unsubscribe', rateLimit, requireFirebaseAdmin, async (req, res) => {
  const { uploaderUrl, endpoint } = req.body || {};
  if (!uploaderUrl || !endpoint) return res.status(400).json({ success: false, error: 'Data gak lengkap.' });
  try {
    const ref = fsDb.collection('creatorWatch').doc(sha256Hex(uploaderUrl));
    await ref.set({ subscriberSubIds: admin.firestore.FieldValue.arrayRemove(sha256Hex(endpoint)) }, { merge: true });
    const after = await ref.get();
    if (after.exists && (!after.data().subscriberSubIds || !after.data().subscriberSubIds.length)) {
      await ref.delete();
    }
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal unsubscribe creator watch.' });
  }
});

// ── API Access developer: generate / list / revoke key (fitur #113) ───────
app.post('/api/developer/keys', rateLimit, requireFirebaseAdmin, async (req, res) => {
  const auth = await getVerifiedPremiumEmail(req);
  if (auth.error) return res.status(auth.status).json({ success: false, error: auth.error });
  try {
    const existing = await fsDb.collection('apiKeys').where('ownerEmail', '==', auth.email).where('revoked', '==', false).get();
    if (existing.size >= API_KEY_MAX_PER_USER) {
      return res.status(400).json({ success: false, error: `Maksimal ${API_KEY_MAX_PER_USER} API key aktif. Revoke salah satu dulu.` });
    }
    const plainKey = generateApiKey();
    const hash = sha256Hex(plainKey);
    await fsDb.collection('apiKeys').doc(hash).set({
      ownerEmail: auth.email,
      label: ((req.body || {}).label || 'Default').slice(0, 60),
      prefix: plainKey.slice(0, API_KEY_PREFIX.length + 6),
      createdAt: Date.now(),
      lastUsedAt: null,
      revoked: false,
      requestCount: 0
    });
    // plainKey CUMA muncul di response ini — gak pernah disimpan mentah di Firestore.
    res.json({ success: true, key: plainKey });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal buat API key.' });
  }
});

app.get('/api/developer/keys', rateLimit, requireFirebaseAdmin, async (req, res) => {
  const auth = await getVerifiedPremiumEmail(req);
  if (auth.error) return res.status(auth.status).json({ success: false, error: auth.error });
  try {
    const snap = await fsDb.collection('apiKeys').where('ownerEmail', '==', auth.email).get();
    const keys = snap.docs.map(d => {
      const v = d.data();
      return {
        id: d.id, label: v.label, prefix: v.prefix + '••••••••',
        createdAt: v.createdAt, lastUsedAt: v.lastUsedAt || null,
        revoked: !!v.revoked, requestCount: v.requestCount || 0
      };
    }).sort((a, b) => b.createdAt - a.createdAt);
    res.json({ success: true, keys });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal ambil daftar API key.' });
  }
});

app.post('/api/developer/keys/:keyId/revoke', rateLimit, requireFirebaseAdmin, async (req, res) => {
  const auth = await getVerifiedPremiumEmail(req);
  if (auth.error) return res.status(auth.status).json({ success: false, error: auth.error });
  try {
    const ref = fsDb.collection('apiKeys').doc(req.params.keyId);
    const snap = await ref.get();
    if (!snap.exists || snap.data().ownerEmail !== auth.email) {
      return res.status(404).json({ success: false, error: 'API key gak ditemukan.' });
    }
    await ref.set({ revoked: true }, { merge: true });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: 'Gagal revoke API key.' });
  }
});

// ── Admin: oversight API key & Creator Watch (pantau abuse/usage) ─────────
app.get('/api/admin/api-keys', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  try {
    const snap = await fsDb.collection('apiKeys').orderBy('createdAt', 'desc').limit(200).get();
    const keys = snap.docs.map(d => {
      const v = d.data();
      return { id: d.id, ownerEmail: v.ownerEmail, label: v.label, prefix: v.prefix, createdAt: v.createdAt, lastUsedAt: v.lastUsedAt || null, revoked: !!v.revoked, requestCount: v.requestCount || 0 };
    });
    res.json({ success: true, keys });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post('/api/admin/api-keys/:keyId/revoke', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  try {
    await fsDb.collection('apiKeys').doc(req.params.keyId).set({ revoked: true }, { merge: true });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/api/admin/creator-watch', requireAdminToken, requireFirebaseAdmin, async (req, res) => {
  try {
    const snap = await fsDb.collection('creatorWatch').get();
    const creators = snap.docs.map(d => {
      const v = d.data();
      return { id: d.id, name: v.name, platform: v.platform, uploaderUrl: v.uploaderUrl, subscriberCount: (v.subscriberSubIds || []).length, lastVideoTitle: v.lastVideoTitle, lastCheckedAt: v.lastCheckedAt };
    });
    res.json({ success: true, creators, intervalMinutes: CREATOR_WATCH_INTERVAL_MS / 60000 });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`NinzyDownloader backend jalan di port ${PORT}`);

  // ── Auto-restart kalau server nge-hang ──────────────────────────────────
  // JUJUR soal apa ini dan apa BUKAN: ini BUKAN "restart worker individual
  // tanpa nge-down-in website" — Railway di sini jalan 1 process tunggal,
  // gak ada worker pool. Yang ini lakuin: self-check tiap beberapa menit
  // sekali, dan kalau proses ini kedeteksi gak sehat (event loop macet
  // total, ditandai dari watchdog gak sempet jalan tepat waktu), keluar
  // dari process dengan exit code error. Railway otomatis restart container
  // yang exit dengan error (restart policy bawaan platform) — jadi total
  // downtime-nya cuma waktu Railway boot ulang container (~beberapa detik),
  // BUKAN restart worker mulus tanpa downtime sama sekali.
  const WATCHDOG_INTERVAL_MS = 60 * 1000;
  const WATCHDOG_MAX_DELAY_MS = 20 * 1000; // kalau macet lebih dari ini, event loop dianggap hang
  let lastTick = Date.now();
  setInterval(() => {
    const now = Date.now();
    const delay = now - lastTick - WATCHDOG_INTERVAL_MS;
    lastTick = now;
    if (delay > WATCHDOG_MAX_DELAY_MS) {
      console.error(`[watchdog] Event loop macet ${delay}ms, keluar dari proses biar Railway restart container.`);
      process.exit(1);
    }
  }, WATCHDOG_INTERVAL_MS);

  // ── Cron Creator Watch ────────────────────────────────────────────────
  // Cek pertama sengaja ditunda dikit (2 menit) biar gak numpuk sama proses
  // startup lain, habis itu berulang tiap CREATOR_WATCH_INTERVAL_MS.
  if (fsDb) {
    setTimeout(() => {
      runCreatorWatchCheck().catch((e) => console.error('[creatorWatch] Error gak ketangkep:', e.message));
      setInterval(() => {
        runCreatorWatchCheck().catch((e) => console.error('[creatorWatch] Error gak ketangkep:', e.message));
      }, CREATOR_WATCH_INTERVAL_MS);
    }, 2 * 60 * 1000);
    console.log(`[creatorWatch] Cron aktif, cek tiap ${CREATOR_WATCH_INTERVAL_MS / 60000} menit.`);
  }
});
