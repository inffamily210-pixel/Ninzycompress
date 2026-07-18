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
  // Post foto/slideshow TikTok/IG gak punya format video sama sekali (cuma
  // audio + gambar terpisah), jadi kalau user coba download pakai pilihan
  // kualitas video, yt-dlp bakal bilang "Requested format is not available".
  if (msg.includes('requested format is not available') || msg.includes('no video formats')) {
    return 'Postingan ini kemungkinan foto/slideshow, bukan video — coba cek ulang link-nya lalu pakai opsi "Download Semua Foto" atau "Jadikan 1 Video".';
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
    // --no-playlist cuma perlu buat YouTube (biar link video-di-dalam-
    // playlist gak ke-treat sebagai playlist penuh). Untuk TikTok/Instagram,
    // JANGAN pasang --no-playlist: post foto/slideshow di platform itu
    // di-extract yt-dlp lewat mode "playlist" gambar — kalau dipaksa
    // --no-playlist, entries fotonya gak pernah muncul dan post itu malah
    // salah kebaca sebagai video biasa (yang ujungnya gagal pas didownload
    // karena isinya gambar diam, bukan video).
    const noPlaylistArg = /youtube\.com|youtu\.be/i.test(url) ? ['--no-playlist'] : [];
    const { stdout } = await runYtDlp([
      '-j', '--no-warnings', ...noPlaylistArg, '--socket-timeout', '20', ...cookieArgs(), ...ytClientArgs(url), url
    ], { timeoutMs: 45000 });

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
      qualities = [
        { label: `📸 Download Semua Foto (${photoCount}) — ZIP`, value: 'photos' },
        { label: '🎞️ Jadikan 1 Video (Slideshow + Musik)', value: 'photos_video' }
      ];
    } else {
      qualities = [{ label: '🎬 Kualitas Terbaik', value: 'best' }];
      // Cuma nawarin tingkat kualitas yang beneran ada di video ini (gak
      // ada gunanya nawarin 1080p buat video yang sumbernya cuma 480p —
      // bakal ke-upscale palsu atau malah gagal format-nya).
      if (maxHeight >= 2160) qualities.push({ label: '2160p (4K)', value: '2160' });
      if (maxHeight >= 1440) qualities.push({ label: '1440p (2K)', value: '1440' });
      if (maxHeight >= 1080) qualities.push({ label: '1080p', value: '1080' });
      if (maxHeight >= 720) qualities.push({ label: '720p', value: '720' });
      if (maxHeight >= 480 || maxHeight === 0) qualities.push({ label: '480p', value: '480' });
      if (maxHeight >= 360) qualities.push({ label: '360p', value: '360' });
      if (maxHeight >= 240) qualities.push({ label: '240p', value: '240' });
      if (maxHeight >= 144) qualities.push({ label: '144p (hemat kuota)', value: '144' });
      qualities.push({ label: '🎵 Audio (MP3)', value: 'audio' });
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
      qualities
    });
  } catch (e) {
    res.status(500).json({ success: false, error: friendlyError(e.message) });
  }
});

app.get('/api/download', rateLimit, async (req, res) => {
  const { url, quality = 'best', trimStart, trimEnd, subLang, skipSponsor, format } = req.query;
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ success: false, error: 'URL wajib diisi.' });
  }
  if (!isSupportedUrl(url)) {
    return res.status(400).json({ success: false, error: 'Link harus dari TikTok atau YouTube.' });
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
                  !['photos', 'photos_video', 'subtitle', 'thumbnail', 'preview'].includes(quality);

  // SponsorBlock: cuma masuk akal buat YouTube (data komunitasnya cuma ada
  // di sana), dan cuma buat video/audio biasa sama kayak trim.
  const hasSponsorSkip = skipSponsor === '1' && /youtube\.com|youtu\.be/i.test(url) &&
                         !['photos', 'photos_video', 'subtitle', 'thumbnail', 'preview'].includes(quality);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ninzydl-'));
  const isPhotos = quality === 'photos';
  const isPhotosVideo = quality === 'photos_video';
  const isSubtitle = quality === 'subtitle';
  const isThumbnail = quality === 'thumbnail';
  const isPreview = quality === 'preview';
  const isMusic = quality === 'music';
  const isAudio = quality === 'audio' || quality === 'audio_opus' || quality === 'audio_m4a' || isMusic;
  const audioFormat = quality === 'audio_opus' ? 'opus' : (quality === 'audio_m4a' ? 'm4a' : 'mp3');

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
});

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

app.listen(PORT, () => {
  console.log(`NinzyDownloader backend jalan di port ${PORT}`);
});
