"use strict";

const express = require("express");
const multer = require("multer");
const cors = require("cors");
const { execFile } = require("node:child_process");
const path = require("node:path");
const fs = require("node:fs");
const os = require("node:os");
const crypto = require("node:crypto");
const sharp = require("sharp");
const { conversionQueue } = require("./queue");
const { config: redisConfig } = require("./redisConfig");
const { printBanner } = require("./utils/banner");

const logger = require("pino")({
  level: "info",
  base: { service: "api" },
  serializers: { err: require("pino").stdSerializers.err },
  transport: {
    target: "pino-pretty",
    options: { colorize: true },
  },
});

// ── Process Tracking System ────────────────────────────────────────────────
const activeProcesses = new Set();
const PROCESS_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

function trackExecFile(bin, args, options, callback) {
  if (typeof options === "function") {
    callback = options;
    options = {};
  }
  const child = execFile(bin, args, options || {}, (err, stdout, stderr) => {
    activeProcesses.delete(child);
    if (callback) {
      try {
        callback(err, stdout, stderr);
      } catch (cbErr) {
        logger.error({ cbErr, bin }, "Error in trackExecFile callback");
      }
    }
  });
  child.startTime = Date.now();
  child.bin = bin;
  activeProcesses.add(child);
  return child;
}

// Watchdog: kill processes exceeding timeout
setInterval(() => {
  const now = Date.now();
  for (const child of activeProcesses) {
    if (now - child.startTime > PROCESS_TIMEOUT_MS) {
      logger.warn(
        { bin: child.bin, pid: child.pid },
        "Killing stuck process due to timeout",
      );
      child.kill("SIGKILL");
      activeProcesses.delete(child);
    }
  }
}, 30000);

// ── Cache System ───────────────────────────────────────────────────────────
const CACHE_DIR = path.join(__dirname, "convertify-cache");
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

const cacheMap = new Map(); // hash -> filename

async function loadCache() {
  try {
    const files = await fs.promises.readdir(CACHE_DIR);
    for (const f of files) {
      const hash = f.split(".")[0];
      cacheMap.set(hash, f);
    }
    logger.info({ count: files.length }, "Cache loaded from disk");
  } catch (err) {
    logger.error({ err }, "Failed to load cache");
  }
}
loadCache();

async function getFileHash(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const input = fs.createReadStream(filePath);
    input.on("data", (data) => hash.update(data));
    input.on("end", () => resolve(hash.digest("hex")));
    input.on("error", reject);
  });
}

async function serveFromCache(res, hash, safeBase, outputExt, mimeType) {
  const cachedFile = cacheMap.get(hash);
  if (cachedFile) {
    const cachedPath = path.join(CACHE_DIR, cachedFile);
    if (fs.existsSync(cachedPath)) {
      const stat = await fs.promises.stat(cachedPath);
      res.setHeader("Content-Type", mimeType);
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${safeBase}_cached.${outputExt}"`,
      );
      res.setHeader("Content-Length", stat.size);
      res.setHeader("X-Cache", "HIT");
      fs.createReadStream(cachedPath).pipe(res);
      return true;
    }
  }
  return false;
}

async function storeInCache(hash, filePath, outputExt) {
  const cacheName = `${hash}.${outputExt}`;
  const cachePath = path.join(CACHE_DIR, cacheName);
  try {
    await fs.promises.copyFile(filePath, cachePath);
    cacheMap.set(hash, cacheName);
  } catch (err) {
    logger.error({ err, hash }, "Failed to store in cache");
  }
}

let path7za;
try {
  path7za = require("7zip-bin").path7za;
} catch (err) {
  logger.warn(
    { err: err.message },
    "7zip-bin import failed, falling back to system '7z' binary.",
  );
  path7za = "7z";
}

// ── Startup binary probe ───────────────────────────────────────────────────
trackExecFile("ffmpeg", ["-version"], (err) => {
  if (err) {
    logger.error("ffmpeg not found on PATH. Video conversions will fail.");
  }
});

// Ensure the bundled 7za binary is executable (npm may strip the +x bit)
try {
  const stats = fs.statSync(path7za);
  if (!(stats.mode & 0o100)) {
    fs.chmodSync(path7za, 0o700);
  }
} catch (err) {
  logger.debug({ err: err.message }, "Best-effort chmod failed");
}
trackExecFile(path7za, ["--help"], (err) => {
  if (err) {
    logger.error(
      { err: err.code || err.message },
      "7za binary not usable. Archive conversions will fail.",
    );
  }
});

// ── Compression binary probes ──────────────────────────────────────────────
for (const bin of ["gs", "pngquant", "optipng", "jpegoptim"]) {
  trackExecFile(bin, ["--version"], (err) => {
    if (err)
      logger.warn(
        { bin },
        "Binary not found on PATH. Compression for affected formats will fail.",
      );
  });
}

const app = express();
const PORT = process.env.PORT || 3000;

// ── Dedicated temp directory with startup cleanup ──────────────────────────
const TEMP_DIR = path.join(os.tmpdir(), "convertify-tmp");
if (fs.existsSync(TEMP_DIR)) {
  // Clean orphaned temp files from previous crashes at startup
  fs.promises.readdir(TEMP_DIR).then(async (files) => {
    await Promise.all(
      files.map((f) =>
        fs.promises
          .rm(path.join(TEMP_DIR, f), { recursive: true, force: true })
          .catch(() => {}),
      ),
    );
    console.log(
      `[TEMP] Cleaned ${files.length} orphaned files from temp directory`,
    );
  });
} else {
  fs.mkdirSync(TEMP_DIR, { recursive: true });
}

// ── Request timeout (30 minutes; generous for large files) ─────────────────
const REQUEST_TIMEOUT_MS = 30 * 60 * 1000;
app.use((req, res, next) => {
  res.setTimeout(REQUEST_TIMEOUT_MS, () => {
    res.status(408).json({ error: "Request timed out." });
  });
  next();
});

// ── Rate limiting (in-memory, per-IP sliding window) ───────────────────────
const RATE_LIMIT = {
  windowMs: 60 * 1000, // 1 minute window
  maxRequests: 30, // max requests per IP per window
  convertMax: 10, // stricter limit for conversion endpoints
};

const rateMap = new Map(); // ip -> { conversions: number, total: number, resetAt: number }

setInterval(
  () => {
    const now = Date.now();
    for (const [ip, entry] of rateMap) {
      if (now > entry.resetAt) rateMap.delete(ip);
    }
  },
  5 * 60 * 1000,
).unref();

function rateLimit(req, res, next) {
  const ip = req.ip || req.connection.remoteAddress;
  const now = Date.now();
  let entry = rateMap.get(ip);
  if (!entry || now > entry.resetAt) {
    entry = { conversions: 0, total: 0, resetAt: now + RATE_LIMIT.windowMs };
    rateMap.set(ip, entry);
  }
  entry.total++;
  const isConvert = req.method === "POST";
  if (isConvert) entry.conversions++;
  if (
    entry.total > RATE_LIMIT.maxRequests ||
    (isConvert && entry.conversions > RATE_LIMIT.convertMax)
  ) {
    return res
      .status(429)
      .json({ error: "Too many requests. Please try again in a moment." });
  }
  next();
}
app.use(rateLimit);

app.use(cors());
app.use(express.static(path.join(__dirname, "public")));
app.use(express.json({ limit: "1mb" })); // Guard against oversized JSON payloads

// ── Format lists ───────────────────────────────────────────────────────────
const VIDEO_EXTS = [
  ".3g2",
  ".3gp",
  ".3gpp",
  ".avi",
  ".cavs",
  ".dv",
  ".dvr",
  ".flv",
  ".m2ts",
  ".m4v",
  ".mkv",
  ".mod",
  ".mov",
  ".mp4",
  ".mpeg",
  ".mpg",
  ".mts",
  ".mxf",
  ".ogg",
  ".ts",
  ".vob",
  ".webm",
  ".wmv",
];
const AUDIO_EXTS = [
  ".aac",
  ".ac3",
  ".aif",
  ".aifc",
  ".aiff",
  ".amr",
  ".au",
  ".caf",
  ".flac",
  ".m4a",
  ".m4b",
  ".mp3",
  ".oga",
  ".ogg",
  ".voc",
  ".wav",
  ".weba",
  ".wma",
];
const IMAGE_EXTS = [
  ".jpg",
  ".jpeg",
  ".png",
  ".webp",
  ".gif",
  ".bmp",
  ".tiff",
  ".tif",
  ".avif",
  ".heic",
  ".heif",
  ".ico",
  ".jfif",
  ".ppm",
  ".tga",
];
const ARCHIVE_EXTS = [
  ".zip",
  ".tar",
  ".tar.gz",
  ".tgz",
  ".tar.bz2",
  ".7z",
  ".rar",
];

// Document extensions and their conversion targets
// LibreOffice handles office formats; pandoc handles markup/text
const DOC_EXTS = [
  ".docx",
  ".doc",
  ".odt",
  ".rtf",
  ".txt",
  ".html",
  ".htm",
  ".xlsx",
  ".xls",
  ".ods",
  ".csv",
  ".pptx",
  ".ppt",
  ".odp",
  ".pdf",
  ".epub",
  ".md",
  ".odd",
  ".odg",
  ".pub",
  ".xps",
  ".psd",
  ".psb",
  ".eps",
  ".ps",
];

// What each input extension can be converted TO via LibreOffice
const LO_FORMATS = {
  // Writer (word processing)
  docx: ["pdf", "odt", "rtf", "txt", "html", "docx"],
  doc: ["pdf", "odt", "rtf", "txt", "html", "docx"],
  odt: ["pdf", "docx", "rtf", "txt", "html"],
  rtf: ["pdf", "docx", "odt", "txt", "html"],
  // Calc (spreadsheet)
  xlsx: ["pdf", "ods", "csv", "xlsx"],
  xls: ["pdf", "ods", "csv", "xlsx"],
  ods: ["pdf", "xlsx", "csv"],
  csv: ["pdf", "xlsx", "ods"],
  // Impress (presentation)
  pptx: ["pdf", "odp", "pptx"],
  ppt: ["pdf", "odp", "pptx"],
  odp: ["pdf", "pptx"],
  // Office formats
  odd: ["pdf"],
  odg: ["pdf"],
  pub: ["pdf"],
  xps: ["pdf"],
  // Adobe/PostScript
  psd: ["pdf", "png", "jpg"],
  psb: ["pdf", "png", "jpg"],
  eps: ["pdf", "png", "jpg"],
  ps: ["pdf", "png", "jpg"],
};

// Pandoc handles markup/text conversions
const PANDOC_FORMATS = {
  md: ["docx", "pdf", "html", "odt", "rtf", "txt", "epub"],
  html: ["docx", "pdf", "odt", "rtf", "txt", "md", "epub"],
  htm: ["docx", "pdf", "odt", "rtf", "txt", "md", "epub"],
  txt: ["docx", "pdf", "html", "odt", "rtf", "md"],
  epub: ["docx", "pdf", "html", "odt", "txt", "md"],
};

// Compute all doc extensions for multer filtering
const ALL_DOC_EXTS = new Set(DOC_EXTS);
const ALL_EXTS = new Set([
  ...VIDEO_EXTS,
  ...IMAGE_EXTS,
  ...DOC_EXTS,
  ...ARCHIVE_EXTS,
  ...AUDIO_EXTS,
]);

// gif is deliberately kept in both lists: video→gif uses ffmpeg palette gen,
// image→gif uses sharp. Routing is determined by the *input* file type.
const VIDEO_OUTPUTS = new Set([
  "mp4",
  "webm",
  "mkv",
  "avi",
  "mov",
  "gif",
  "m4v",
]);
const AUDIO_OUTPUTS = new Set(["mp3", "wav", "m4a", "flac", "ogg", "aac"]);
const IMAGE_OUTPUTS = new Set([
  "jpg",
  "png",
  "webp",
  "avif",
  "gif",
  "bmp",
  "tiff",
  "heic",
  "ico",
  "tga",
  "ppm",
]);
const ARCHIVE_OUTPUTS = new Set(["zip", "tar", "tar.gz", "7z"]);

// ── FFmpeg Hardware acceleration cache ─────────────────────────────────────
const FFmpegHW = {
  h264_videotoolbox: false,
  hevc_videotoolbox: false,
  h264_nvenc: false,
  hevc_nvenc: false,
};
trackExecFile("ffmpeg", ["-encoders"], (err, stdout) => {
  if (!err && stdout) {
    if (stdout.includes("h264_videotoolbox")) FFmpegHW.h264_videotoolbox = true;
    if (stdout.includes("hevc_videotoolbox")) FFmpegHW.hevc_videotoolbox = true;
    if (stdout.includes("h264_nvenc")) FFmpegHW.h264_nvenc = true;
    if (stdout.includes("hevc_nvenc")) FFmpegHW.hevc_nvenc = true;
  }
});

// ── Exporting functions for Worker ─────────────────────────────────────────
module.exports = {
  convertVideoToVideo,
  convertVideoToAudio,
  convertAudioToAudio,
  convertImage,
  convertWithLibreOffice,
  convertWithPandoc,
  compressPdf,
  compressPng,
  compressJpg,
  getType,
  mimeFor,
  safeUnlink,
  sanitizeFilename,
  TEMP_DIR,
  path7za,
  detectMediaStreams,
  getFileHash,
  storeInCache,
  serveFromCache,
};

// ── Concurrency limiter (REMOVED: Now handled by BullMQ) ───────────────────
// Legacy semaphore logic removed to avoid confusion

// ── Multer ─────────────────────────────────────────────────────────────────
const upload = multer({
  dest: TEMP_DIR,
  limits: { fileSize: 1024 * 1024 * 1024 }, // 1 GB
  fileFilter: (req, file, cb) => {
    const name = file.originalname.toLowerCase();
    let ext = path.extname(name);
    if (name.endsWith(".tar.gz")) ext = ".tar.gz";
    else if (name.endsWith(".tar.bz2")) ext = ".tar.bz2";

    // Basic MIME integrity check against extension categories
    const mime = file.mimetype || "";
    if (
      VIDEO_EXTS.includes(ext) &&
      !mime.startsWith("video/") &&
      !mime.includes("octet-stream") &&
      !mime.includes("ogg") &&
      !mime.includes("application/")
    ) {
      return cb(new Error("MIME type mismatch for video format."));
    }
    if (
      AUDIO_EXTS.includes(ext) &&
      !mime.startsWith("audio/") &&
      !mime.includes("octet-stream") &&
      !mime.includes("ogg") &&
      !mime.includes("video/") &&
      !mime.includes("application/")
    ) {
      return cb(new Error("MIME type mismatch for audio format."));
    }

    if (ALL_EXTS.has(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`Unsupported file type: ${ext || "(no extension)"}`));
    }
  },
});

// ── Compression multer (50 MB cap; pdf/png/jpg only) ──────────────────────
const COMPRESS_MAX_BYTES = 50 * 1024 * 1024;

const uploadCompress = multer({
  dest: TEMP_DIR,
  limits: { fileSize: COMPRESS_MAX_BYTES },
  fileFilter: (req, file, cb) => {
    const name = file.originalname.toLowerCase();
    const ext = path.extname(name);
    const mime = file.mimetype || "";

    // Security: Validate both extension and MIME type
    if (ext === ".pdf" && mime === "application/pdf") return cb(null, true);
    if (ext === ".png" && mime === "image/png") return cb(null, true);
    if ((ext === ".jpg" || ext === ".jpeg") && mime === "image/jpeg")
      return cb(null, true);

    cb(
      new Error(
        `Invalid or mismatched format for compression: ${ext || "(none)"} [${mime}]`,
      ),
    );
  },
});

// ── Helpers ────────────────────────────────────────────────────────────────
function getType(filename) {
  const name = filename.toLowerCase();
  let ext = path.extname(name);
  if (name.endsWith(".tar.gz")) ext = ".tar.gz";
  else if (name.endsWith(".tar.bz2")) ext = ".tar.bz2";

  if (AUDIO_EXTS.includes(ext)) return "audio";
  if (VIDEO_EXTS.includes(ext)) return "video";
  if (IMAGE_EXTS.includes(ext)) return "image";
  if (ALL_DOC_EXTS.has(ext)) return "document";
  if (ARCHIVE_EXTS.includes(ext)) return "archive";
  return null;
}

function mimeFor(fmt) {
  const map = {
    mp4: "video/mp4",
    webm: "video/webm",
    mkv: "video/x-matroska",
    avi: "video/x-msvideo",
    mov: "video/quicktime",
    gif: "image/gif",
    mp3: "audio/mpeg",
    wav: "audio/wav",
    aac: "audio/aac",
    jpg: "image/jpeg",
    jpeg: "image/jpeg",
    png: "image/png",
    webp: "image/webp",
    avif: "image/avif",
    bmp: "image/bmp",
    tiff: "image/tiff",
    heic: "image/heic",
    m4v: "video/x-m4v",
    m4a: "audio/mp4",
    ico: "image/x-icon",
    tga: "image/x-tga",
    ppm: "image/x-portable-pixmap",
    odd: "application/vnd.oasis.opendocument.graphics",
    odg: "application/vnd.oasis.opendocument.graphics",
    pub: "application/x-mspublisher",
    xps: "application/oxps",
    psd: "image/vnd.adobe.photoshop",
    psb: "image/vnd.adobe.photoshop",
    eps: "application/postscript",
    ps: "application/postscript",
    // Document types
    pdf: "application/pdf",
    docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    doc: "application/msword",
    odt: "application/vnd.oasis.opendocument.text",
    rtf: "application/rtf",
    txt: "text/plain",
    html: "text/html",
    htm: "text/html",
    xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    xls: "application/vnd.ms-excel",
    ods: "application/vnd.oasis.opendocument.spreadsheet",
    csv: "text/csv",
    pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ppt: "application/vnd.ms-powerpoint",
    odp: "application/vnd.oasis.opendocument.presentation",
    md: "text/markdown",
    epub: "application/epub+zip",
    // Archive types
    zip: "application/zip",
    tar: "application/x-tar",
    tgz: "application/gzip",
    "tar.gz": "application/gzip",
    "tar.bz2": "application/x-bzip2",
    rar: "application/vnd.rar",
    "7z": "application/x-7z-compressed",
  };
  return map[fmt] || "application/octet-stream";
}

/**
 * Sanitize a user-supplied base name for use in Content-Disposition.
 * Strips path separators and characters that could escape the quoted string.
 */
function sanitizeFilename(name) {
  return path
    .basename(name)
    .replaceAll(/[/\\]/g, "_") // path separators
    .replaceAll('"', "'") // double-quote would break the header value
    .replaceAll(/[\x00-\x1f]/g, ""); // control characters
}

async function safeUnlink(filePath) {
  try {
    await fs.promises.unlink(filePath);
  } catch (err) {
    // Intentionally ignored, but logged at debug level for traceability
    logger.debug(`[safeUnlink] Skip/Fail: ${filePath}`, err.message);
  }
}

// ── Media conversion (uses execFile to avoid shell injection) ───────────────
function detectMediaStreams(inputPath) {
  return new Promise((resolve) => {
    trackExecFile(
      "ffprobe",
      [
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "json",
        inputPath,
      ],
      (err, stdout) => {
        if (err) return resolve({ hasVideo: false, hasAudio: false });
        try {
          const info = JSON.parse(stdout);
          const streams = info.streams || [];
          const hasVideo = streams.some((s) => s.codec_type === "video");
          const hasAudio = streams.some((s) => s.codec_type === "audio");
          resolve({ hasVideo, hasAudio });
        } catch (e) {
          logger.error("[detectMediaStreams parse error]", e.message);
          resolve({ hasVideo: false, hasAudio: false });
        }
      },
    );
  });
}

function checkDuration(inputPath, streamType) {
  return new Promise((resolve, reject) => {
    trackExecFile(
      "ffprobe",
      [
        "-v",
        "error",
        "-select_streams",
        `${streamType}:0`,
        "-show_entries",
        "stream=duration",
        "-of",
        "csv=p=0",
        inputPath,
      ],
      (err, stdout) => {
        if (err) return resolve();
        const lines = stdout.split("\n").map((s) => s.trim());
        const durationItem = lines.find((line) => line.match(/^[\d.]+$/));
        if (durationItem && Number.parseFloat(durationItem) > 7200) {
          return reject(
            new Error(
              `${streamType === "v" ? "Video" : "Audio"} duration exceeds the 2 hour maximum limit`,
            ),
          );
        }
        resolve();
      },
    );
  });
}

function parseFfmpegError(err, stderr, typeName) {
  logger.error(`[${typeName} error]`, err);
  let errMsg = "Unsupported or corrupted media format";
  const errStr = (stderr || "").toLowerCase();
  if (errStr.includes("invalid data")) errMsg = "Corrupted file data";
  else if (errStr.includes("codec not supported"))
    errMsg = "Unsupported codec profile";
  else if (errStr.includes("moov atom not found"))
    errMsg = "Corrupted file layout (moov atom missing)";
  else if (errStr.includes("error opening input"))
    errMsg = "File path or access error";
  return new Error(errMsg);
}

function convertAudioToAudio(inputPath, outputPath, outputFmt, opts, onChild) {
  return checkDuration(inputPath, "a").then(() => {
    let args;
    const bitrate = String(Number.parseInt(opts.audioBitrate, 10) || 192);
    if (outputFmt === "mp3") {
      args = [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-acodec",
        "libmp3lame",
        "-ab",
        `${bitrate}k`,
        "-ar",
        "44100",
        outputPath,
      ];
    } else if (outputFmt === "wav") {
      args = ["-y", "-i", inputPath, "-vn", "-acodec", "pcm_s16le", outputPath];
    } else if (outputFmt === "aac" || outputFmt === "m4a") {
      args = [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        outputPath,
      ];
    } else if (outputFmt === "flac") {
      args = ["-y", "-i", inputPath, "-vn", "-c:a", "flac", outputPath];
    } else if (outputFmt === "ogg") {
      args = ["-y", "-i", inputPath, "-vn", "-c:a", "libvorbis", outputPath];
    } else {
      args = ["-y", "-i", inputPath, "-vn", outputPath];
    }

    logger.debug("[convertAudioToAudio start]", args);
    const { promise, child } = execFileWithTracker("ffmpeg", args);
    if (onChild) onChild(child);
    return promise.catch((err) => {
      throw parseFfmpegError(err.err, err.stderr, "convertAudioToAudio");
    });
  });
}

function convertVideoToAudio(inputPath, outputPath, outputFmt, opts, onChild) {
  return checkDuration(inputPath, "v").then(() => {
    let args;
    const bitrate = String(Number.parseInt(opts.audioBitrate, 10) || 192);
    if (outputFmt === "mp3") {
      args = [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-map",
        "0:a?",
        "-acodec",
        "libmp3lame",
        "-ab",
        `${bitrate}k`,
        "-ar",
        "44100",
        outputPath,
      ];
    } else if (outputFmt === "wav") {
      args = [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-map",
        "0:a?",
        "-acodec",
        "pcm_s16le",
        outputPath,
      ];
    } else if (outputFmt === "aac" || outputFmt === "m4a") {
      args = [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-map",
        "0:a?",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        outputPath,
      ];
    } else if (outputFmt === "flac") {
      args = [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-map",
        "0:a?",
        "-c:a",
        "flac",
        outputPath,
      ];
    } else if (outputFmt === "ogg") {
      args = [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-map",
        "0:a?",
        "-c:a",
        "libvorbis",
        outputPath,
      ];
    } else {
      args = ["-y", "-i", inputPath, "-vn", "-map", "0:a?", outputPath];
    }

    logger.debug("[convertVideoToAudio start]", args);
    const { promise, child } = execFileWithTracker("ffmpeg", args);
    if (onChild) onChild(child);
    return promise.catch((err) => {
      throw parseFfmpegError(err.err, err.stderr, "convertVideoToAudio");
    });
  });
}

function convertVideoToVideo(inputPath, outputPath, outputFmt, opts, onChild) {
  return checkDuration(inputPath, "v").then(() => {
    let args;
    if (outputFmt === "gif") {
      const fps = String(Number.parseInt(opts.gifFps, 10) || 10);
      const scale = String(Number.parseInt(opts.gifScale, 10) || 480);
      const vf = `fps=${fps},scale=${scale}:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse`;
      args = ["-y", "-i", inputPath, "-vf", vf, "-loop", "0", outputPath];
    } else {
      const crf = String(Number.parseInt(opts.crf, 10) || 23);

      let encoder_h264 = "libx264";
      if (FFmpegHW.h264_nvenc) encoder_h264 = "h264_nvenc";
      else if (FFmpegHW.h264_videotoolbox) encoder_h264 = "h264_videotoolbox";

      if (outputFmt === "webm") {
        args = [
          "-y",
          "-i",
          inputPath,
          "-c:v",
          "libvpx-vp9",
          "-crf",
          crf,
          "-b:v",
          "0",
          "-c:a",
          "libopus",
          outputPath,
        ];
      } else if (outputFmt === "m4v") {
        args = [
          "-y",
          "-i",
          inputPath,
          "-c:v",
          encoder_h264,
          "-crf",
          "23",
          "-preset",
          "fast",
          "-c:a",
          "aac",
          "-b:a",
          "192k",
          outputPath,
        ];
      } else {
        args = [
          "-y",
          "-i",
          inputPath,
          "-c:v",
          encoder_h264,
          "-crf",
          crf,
          "-preset",
          "fast",
          "-c:a",
          "aac",
          "-b:a",
          "192k",
          outputPath,
        ];
      }

      let w = Number.parseInt(opts.width, 10);
      let h = Number.parseInt(opts.height, 10);

      if (!w && !h) {
        args.splice(-1, 0, "-vf", "scale='min(1920,iw)':-2");
      } else {
        w = w || -2;
        h = h || -2;
        args.splice(-1, 0, "-vf", `scale=${w}:${h}`);
      }
    }

    logger.debug("[convertVideoToVideo start]", args);
    const { promise, child } = execFileWithTracker("ffmpeg", args);
    if (onChild) onChild(child);
    return promise.catch((err) => {
      throw parseFfmpegError(err.err, err.stderr, "convertVideoToVideo");
    });
  });
}

// ── Image conversion ────────────────────────────────────────────────────────
async function convertImage(inputPath, outputPath, outputFmt, opts, onChild) {
  logger.debug("[convertImage start]", inputPath, "->", outputPath);
  try {
    const quality = Number.parseInt(opts.imageQuality, 10) || 90;
    let s = sharp(inputPath, { failOn: "none" });
    if (opts.width || opts.height) {
      s = s.resize(
        opts.width ? Number.parseInt(opts.width, 10) : null,
        opts.height ? Number.parseInt(opts.height, 10) : null,
        { fit: "inside", withoutEnlargement: true },
      );
    }
    const fmt = outputFmt === "jpg" ? "jpeg" : outputFmt;
    if (fmt === "jpeg") s = s.jpeg({ quality });
    else if (fmt === "png")
      s = s.png({ compressionLevel: Math.round((100 - quality) / 11) });
    else if (fmt === "webp") s = s.webp({ quality });
    else if (fmt === "avif") s = s.avif({ quality });
    else if (fmt === "heic") s = s.heif({ compression: "hevc", quality });
    else if (fmt === "gif") s = s.gif();
    else if (fmt === "bmp") s = s.bmp();
    else if (fmt === "tiff") s = s.tiff({ quality });
    else s = s.toFormat(fmt);

    await s.toFile(outputPath);
    logger.debug("[convertImage done via sharp]", outputPath);
  } catch (err) {
    logger.debug("[convertImage sharp fallback to magick]", err.message);
    try {
      let args = [inputPath];
      if (opts.width || opts.height) {
        const w = Number.parseInt(opts.width, 10);
        const h = Number.parseInt(opts.height, 10);
        args.push("-resize", `${w || ""}x${h || ""}>`);
      }
      args.push(outputPath);

      const { promise, child } = execFileWithTracker("magick", args);
      if (onChild) onChild(child);
      await promise;
      logger.debug("[convertImage done via magick]", outputPath);
    } catch (magickErr) {
      logger.debug(
        "[convertImage magick fallback to ffmpeg]",
        magickErr.message,
      );
      try {
        let args = ["-y", "-i", inputPath];
        if (opts.width || opts.height) {
          const w = Number.parseInt(opts.width, 10) || -2;
          const h = Number.parseInt(opts.height, 10) || -2;
          args.push("-vf", `scale=${w}:${h}`);
        }
        if (outputFmt === "jpg" || outputFmt === "jpeg") args.push("-q:v", "2");
        args.push(outputPath);

        const { promise, child } = execFileWithTracker("ffmpeg", args);
        if (onChild) onChild(child);
        await promise;
        logger.debug("[convertImage done via ffmpeg]", outputPath);
      } catch (fallbackErr) {
        logger.error("[convertImage final failure]", fallbackErr.message);
        throw new Error(
          "Image conversion failed. Format not supported or file corrupt.",
        );
      }
    }
  }
}

// ── Document conversion via LibreOffice ─────────────────────────────────────
async function convertWithLibreOffice(
  inputPath,
  outputDir,
  outputFmt,
  onChild,
) {
  try {
    const { promise: preflight } = execFileWithTracker("soffice", [
      "--version",
    ]);
    await preflight.catch(() => {
      throw new Error(
        "soffice not found: LibreOffice is not installed or not in PATH.",
      );
    });
  } catch (preflightErr) {
    logger.error("[convertWithLibreOffice preflight error]", preflightErr);
    throw new Error(
      "soffice not found: LibreOffice is not installed or not in PATH.",
    );
  }

  // Give each conversion its own isolated LO profile to prevent lock contention
  const profilePath = path.join(TEMP_DIR, `lo_${crypto.randomUUID()}`);
  const profileDir = `file://${profilePath}`;
  const args = [
    `-env:UserInstallation=${profileDir}`,
    "--headless",
    "--norestore",
    "--nofirststartwizard",
    "--convert-to",
    outputFmt,
    "--outdir",
    outputDir,
    inputPath,
  ];

  logger.debug("[convertWithLibreOffice start]", args);

  let stdout, stderr;
  try {
    const { promise, child } = execFileWithTracker("soffice", args, {
      timeout: 3 * 60 * 1000,
    });
    if (onChild) onChild(child);
    const res = await promise;
    stdout = res.stdout;
    stderr = res.stderr;
  } catch (err) {
    logger.error("[convertWithLibreOffice error]", err.err || err);
    if (err.stdout) logger.error("[convertWithLibreOffice stdout]", err.stdout);
    if (err.stderr) logger.error("[convertWithLibreOffice stderr]", err.stderr);
    throw new Error(
      "LibreOffice conversion failed. The file may be corrupt or password-protected.",
    );
  } finally {
    // Clean up profile directory immediately
    await fs.promises
      .rm(profilePath, { recursive: true, force: true })
      .catch(() => {});
  }

  // Verify the expected output file exists
  const expectedOutput = path.join(
    outputDir,
    `${path.basename(inputPath, path.extname(inputPath))}.${outputFmt}`,
  );
  if (!fs.existsSync(expectedOutput)) {
    const verifyErr = new Error(
      `LibreOffice finished but no .${outputFmt} file was produced.`,
    );
    logger.error("[convertWithLibreOffice error]", verifyErr);
    if (stdout) logger.error("[convertWithLibreOffice stdout]", stdout);
    if (stderr) logger.error("[convertWithLibreOffice stderr]", stderr);
    throw verifyErr;
  }

  logger.debug("[convertWithLibreOffice done]", expectedOutput);
}

// ── Document conversion via Pandoc ──────────────────────────────────────────
async function convertWithPandoc(
  inputPath,
  outputPath,
  inputFmt,
  outputFmt,
  onChild,
) {
  const fmtMap = { md: "markdown", htm: "html", txt: "plain" };
  const from = fmtMap[inputFmt] || inputFmt;
  const to = fmtMap[outputFmt] || outputFmt;
  const args = ["-f", from, "-t", to, "-o", outputPath, inputPath];
  logger.debug("[convertWithPandoc start]", args);

  const { promise, child } = execFileWithTracker("pandoc", args, {
    timeout: 2 * 60 * 1000,
  });
  if (onChild) onChild(child);

  try {
    await promise;
    logger.debug("[convertWithPandoc done]", outputPath);
  } catch (err) {
    logger.error("[convertWithPandoc error]", err.err || err);
    throw new Error("Pandoc conversion failed.");
  }
}

// ── Compression helpers ─────────────────────────────────────────────────────

// Wrap execFile to allow passing the child process back for cleanup tracking
function execFileWithTracker(bin, args, options) {
  let child;
  const promise = new Promise((resolve, reject) => {
    child = trackExecFile(bin, args, options, (err, stdout, stderr) => {
      if (err) {
        const msg = err.message || "Command failed without message";
        const wrapped = new Error(msg);
        wrapped.err = err;
        wrapped.stdout = stdout;
        wrapped.stderr = stderr;
        return reject(wrapped);
      }
      resolve({ stdout, stderr });
    });
  });
  // Attach a dummy catch to prevent unhandled rejection if the caller is slow
  promise.catch(() => {});
  return { promise, child };
}

// PDF: Ghostscript — rewrites PDF with preset quality, removes metadata
const VALID_PDF_PRESETS = new Set(["screen", "ebook", "printer"]);

async function compressPdf(inputPath, outputPath, onChild, opts = {}) {
  const preset = VALID_PDF_PRESETS.has(opts.preset) ? opts.preset : "ebook";
  const args = [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    `-dPDFSETTINGS=/${preset}`,
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
    "-dDetectDuplicateImages=true",
    "-dCompressFonts=true",
    "-dRemoveAllMetadata=true",
    `-sOutputFile=${outputPath}`,
    inputPath,
  ];
  logger.debug("[compressPdf start] preset=%s", preset);
  const { promise, child } = execFileWithTracker("gs", args, {
    timeout: 5 * 60 * 1000,
  });
  if (onChild) onChild(child);
  try {
    await promise;
  } catch (error_) {
    logger.error(
      "[compressPdf error]",
      error_.err ? error_.err.message : error_.message,
    );
    throw new Error("Invalid file content or compression preset failed.");
  }
  logger.debug("[compressPdf done]", outputPath);
}

// PNG: pngquant (lossy palette) → optipng (lossless deflate + strip metadata)
async function compressPng(inputPath, outputPath, onChild, opts = {}) {
  const quality = Math.min(
    100,
    Math.max(1, Number.parseInt(opts.quality, 10) || 80),
  );
  const qualMin = Math.max(0, quality - 15);
  const workDir = await fs.promises.mkdtemp(path.join(TEMP_DIR, "cpress-png-"));
  try {
    const quantized = path.join(workDir, "q.png");

    // Pass 1: lossy palette reduction
    const { promise: p1, child: c1 } = execFileWithTracker(
      "pngquant",
      [
        `--quality=${qualMin}-${quality}`,
        "--force",
        "--output",
        quantized,
        "--",
        inputPath,
      ],
      { timeout: 2 * 60 * 1000 },
    );
    if (onChild) onChild(c1);
    await p1.catch((error_) => {
      if (error_?.err?.code !== 99) throw error_.err ?? error_;
    });

    const passOneFile = fs.existsSync(quantized) ? quantized : inputPath;

    // Pass 2: lossless deflation
    const { promise: p2, child: c2 } = execFileWithTracker(
      "optipng",
      ["-o2", "-strip", "all", "-out", outputPath, passOneFile],
      { timeout: 2 * 60 * 1000 },
    );
    if (onChild) onChild(c2);
    await p2.catch((error_) => {
      throw error_.err;
    });
  } catch (err) {
    logger.error("[compressPng error]", err.message);
    throw new Error("Unsupported format or PNG optimization failed.");
  } finally {
    await fs.promises
      .rm(workDir, { recursive: true, force: true })
      .catch(() => {});
  }
  logger.debug("[compressPng done]", outputPath);
}

// JPG: jpegoptim — quality reduction, progressive encoding, strip all metadata
async function compressJpg(inputPath, outputPath, onChild, opts = {}) {
  const quality = Math.min(
    100,
    Math.max(1, Number.parseInt(opts.quality, 10) || 75),
  );
  const workDir = await fs.promises.mkdtemp(path.join(TEMP_DIR, "cpress-jpg-"));
  try {
    const workFile = path.join(workDir, "input.jpg");
    await fs.promises.copyFile(inputPath, workFile);

    const { promise, child } = execFileWithTracker(
      "jpegoptim",
      [`--max=${quality}`, "--strip-all", "--all-progressive", workFile],
      { timeout: 2 * 60 * 1000 },
    );
    if (onChild) onChild(child);
    await promise.catch((error_) => {
      throw error_.err;
    });

    await fs.promises.copyFile(workFile, outputPath);
  } catch (err) {
    logger.error("[compressJpg error]", err.message);
    throw new Error("Invalid file content or JPG compression failed.");
  } finally {
    await fs.promises
      .rm(workDir, { recursive: true, force: true })
      .catch(() => {});
  }
  logger.debug("[compressJpg done]", outputPath);
}

// ── Shared compression route handler ───────────────────────────────────────
async function handleCompress(req, res, outputExt, mimeType) {
  if (!req.file) return res.status(400).json({ error: "No file uploaded." });

  const startTime = Date.now();
  const inputPath = req.file.path;
  const rawBase = path.basename(
    req.file.originalname,
    path.extname(req.file.originalname),
  );
  const safeBase = sanitizeFilename(rawBase) || "compressed";

  try {
    const fileHash = await getFileHash(inputPath);
    const finalHash = crypto
      .createHash("sha256")
      .update(fileHash + outputExt + JSON.stringify(req.body))
      .digest("hex");

    if (await serveFromCache(res, finalHash, safeBase, outputExt, mimeType)) {
      await safeUnlink(inputPath).catch(() => {});
      logger.info(
        {
          type: "compress",
          status: "success",
          format: outputExt,
          duration: Date.now() - startTime,
          cache: "hit",
        },
        "Served from cache",
      );
      return;
    }

    const job = await conversionQueue.add(
      "compress",
      {
        inputPath,
        outputPath: path.join(
          TEMP_DIR,
          `${req.file.filename}_out.${outputExt}`,
        ),
        outputFmt: outputExt,
        options: req.body,
        fileType: "compress",
        safeBase,
        mimeType,
        finalHash,
      },
      {
        timeout: 5 * 60 * 1000,
        attempts: 2,
      },
    );

    res.json({
      jobId: job.id,
      message: "Compression started",
      status: "waiting",
    });
  } catch (err) {
    logger.error(
      {
        type: "compress",
        status: "fail",
        format: outputExt,
        duration: Date.now() - startTime,
        error: err.message,
      },
      "Compression enqueue failed",
    );
    if (!res.headersSent)
      res.status(500).json({ error: "Failed to queue job" });
    await safeUnlink(inputPath).catch(() => {});
  }
}

// ── Routes ──────────────────────────────────────────────────────────────────
app.get("/formats", (req, res) => {
  // Build doc format map for the client: ext → available output formats
  const docFormats = {};
  for (const [ext, outs] of Object.entries(LO_FORMATS)) docFormats[ext] = outs;
  for (const [ext, outs] of Object.entries(PANDOC_FORMATS)) {
    docFormats[ext] = [...new Set([...(docFormats[ext] || []), ...outs])];
  }

  res.json({
    video: {
      inputs: VIDEO_EXTS,
      outputs: [...new Set([...VIDEO_OUTPUTS, ...AUDIO_OUTPUTS])],
    },
    audio: { inputs: AUDIO_EXTS, outputs: [...AUDIO_OUTPUTS] },
    image: { inputs: IMAGE_EXTS, outputs: [...IMAGE_OUTPUTS] },
    document: { inputs: DOC_EXTS, outputs: docFormats },
    archive: { inputs: ARCHIVE_EXTS, outputs: [...ARCHIVE_OUTPUTS] },
    // Compression capability — separate from conversion formats
    compress: {
      pdf: {
        accepts: [".pdf"],
        presets: ["screen", "ebook", "printer"],
        default: { preset: "ebook" },
      },
      png: { accepts: [".png"], quality: { min: 1, max: 100, default: 80 } },
      jpg: {
        accepts: [".jpg", ".jpeg"],
        quality: { min: 1, max: 100, default: 75 },
      },
      maxFileSizeBytes: COMPRESS_MAX_BYTES,
    },
  });
});

// ── Compression routes ──────────────────────────────────────────────────────
// Body params: quality (1-100, jpg/png), preset (screen|ebook|printer, pdf)

app.post("/compress/pdf", uploadCompress.single("file"), (req, res) =>
  handleCompress(req, res, "pdf", "application/pdf"),
);

app.post("/compress/png", uploadCompress.single("file"), (req, res) =>
  handleCompress(req, res, "png", "image/png"),
);

app.post("/compress/jpg", uploadCompress.single("file"), (req, res) =>
  handleCompress(req, res, "jpg", "image/jpeg"),
);

// ── Archive conversion route ────────────────────────────────────────────────
app.post("/convert-archive", upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file uploaded." });

  const inputPath = req.file.path;
  const rawName = req.file.originalname.toLowerCase();
  let inputExt = path.extname(rawName).slice(1);
  if (rawName.endsWith(".tar.gz") || rawName.endsWith(".tgz"))
    inputExt = "tar.gz";
  else if (rawName.endsWith(".tar.bz2")) inputExt = "tar.bz2";

  const outputFmt = (req.body.outputFormat || "")
    .toLowerCase()
    .replace(/^\./, "");
  if (!outputFmt) {
    await safeUnlink(inputPath);
    return res.status(400).json({ error: "No output format specified." });
  }

  const rawBase = path.basename(
    req.file.originalname,
    path.extname(req.file.originalname),
  );
  const safeBase = sanitizeFilename(rawBase) || "converted";

  try {
    const fileHash = await getFileHash(inputPath);
    const finalHash = crypto
      .createHash("sha256")
      .update(fileHash + outputFmt + JSON.stringify(req.body))
      .digest("hex");

    if (
      await serveFromCache(
        res,
        finalHash,
        safeBase,
        outputFmt,
        mimeFor(outputFmt),
      )
    ) {
      await safeUnlink(inputPath).catch(() => {});
      return;
    }

    const job = await conversionQueue.add(
      "convert",
      {
        inputPath,
        outputPath: path.join(
          TEMP_DIR,
          `out_${crypto.randomUUID()}.${outputFmt}`,
        ),
        outputFmt,
        options: req.body,
        fileType: "archive",
        inputExt,
        safeBase,
        finalHash,
      },
      {
        timeout: 10 * 60 * 1000, // 10 minutes for archives
        attempts: 2,
      },
    );

    res.json({
      jobId: job.id,
      message: "Archive conversion started",
      status: "waiting",
    });
  } catch (err) {
    logger.error(
      { type: "archive", error: err.message },
      "Archive conversion enqueue failed",
    );
    res.status(500).json({ error: "Failed to queue job" });
    await safeUnlink(inputPath).catch(() => {});
  }
});

// ── Document conversion route ───────────────────────────────────────────────
app.post("/convert-doc", upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file uploaded." });

  const inputPath = req.file.path;
  const inputExt = path.extname(req.file.originalname).toLowerCase().slice(1);
  const outputFmt = (req.body.outputFormat || "")
    .toLowerCase()
    .replace(/^\./, "");

  if (!outputFmt) {
    await safeUnlink(inputPath);
    return res.status(400).json({ error: "No output format specified." });
  }

  // Validate output format is supported for this input
  const loOuts = LO_FORMATS[inputExt] || [];
  const pandocOuts = PANDOC_FORMATS[inputExt] || [];
  const allOuts = [...new Set([...loOuts, ...pandocOuts])];
  if (!allOuts.includes(outputFmt)) {
    await safeUnlink(inputPath);
    return res
      .status(400)
      .json({ error: `Cannot convert .${inputExt} to .${outputFmt}` });
  }

  const rawBase = path.basename(
    req.file.originalname,
    path.extname(req.file.originalname),
  );
  const safeBase = sanitizeFilename(rawBase) || "converted";

  try {
    const fileHash = await getFileHash(inputPath);
    const finalHash = crypto
      .createHash("sha256")
      .update(fileHash + outputFmt + JSON.stringify(req.body))
      .digest("hex");

    if (
      await serveFromCache(
        res,
        finalHash,
        safeBase,
        outputFmt,
        mimeFor(outputFmt),
      )
    ) {
      await safeUnlink(inputPath).catch(() => {});
      return;
    }

    const job = await conversionQueue.add(
      "convert",
      {
        inputPath,
        outputPath: path.join(TEMP_DIR, `${req.file.filename}.${outputFmt}`),
        outputFmt,
        options: req.body,
        fileType: "document",
        inputExt,
        safeBase,
        finalHash,
      },
      {
        timeout: 5 * 60 * 1000,
        attempts: 2,
      },
    );

    res.json({
      jobId: job.id,
      message: "Document conversion started",
      status: "waiting",
    });
  } catch (err) {
    logger.error(
      { type: "document", error: err.message },
      "Document conversion enqueue failed",
    );
    res.status(500).json({ error: "Failed to queue job" });
    await safeUnlink(inputPath).catch(() => {});
  }
});

app.post("/convert", upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file uploaded." });

  const inputPath = req.file.path;
  const fileType = getType(req.file.originalname);
  const outputFmt = (req.body.outputFormat || "")
    .toLowerCase()
    .replace(/^\./, "");

  if (!outputFmt) {
    await safeUnlink(inputPath);
    return res.status(400).json({ error: "No output format specified." });
  }

  const rawBase = path.basename(
    req.file.originalname,
    path.extname(req.file.originalname),
  );
  const safeBase = sanitizeFilename(rawBase) || "converted";

  try {
    const fileHash = await getFileHash(inputPath);
    const finalHash = crypto
      .createHash("sha256")
      .update(fileHash + outputFmt + JSON.stringify(req.body))
      .digest("hex");

    if (
      await serveFromCache(
        res,
        finalHash,
        safeBase,
        outputFmt,
        mimeFor(outputFmt),
      )
    ) {
      await safeUnlink(inputPath).catch(() => {});
      return;
    }

    let streamInfo = null;
    if (fileType === "video" || fileType === "audio") {
      streamInfo = await detectMediaStreams(inputPath);
      if (!streamInfo.hasVideo && !streamInfo.hasAudio) {
        throw new Error("Invalid or unsupported media stream");
      }
    }

    const job = await conversionQueue.add(
      "convert",
      {
        inputPath,
        outputPath: path.join(TEMP_DIR, `${req.file.filename}.${outputFmt}`),
        outputFmt,
        options: req.body,
        fileType,
        safeBase,
        streamInfo,
        finalHash,
      },
      {
        timeout: 15 * 60 * 1000, // 15 minutes for media
        attempts: 2,
      },
    );

    res.json({
      jobId: job.id,
      message: "Conversion started",
      status: "waiting",
    });
  } catch (err) {
    logger.error(
      { type: "conversion", error: err.message },
      "Conversion enqueue failed",
    );
    res.status(500).json({ error: err.message || "Failed to queue job" });
    await safeUnlink(inputPath).catch(() => {});
  }
});

app.get("/status/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await conversionQueue.getJob(jobId);

    if (!job) {
      return res.status(404).json({ error: "Job not found" });
    }

    const jobState = await job.getState();
    const response = { jobId: job.id, status: jobState };

    if (jobState === "completed") {
      response.result = job.returnvalue;
    } else if (jobState === "failed") {
      response.error = job.failedReason;
    }

    res.json(response);
  } catch (err) {
    logger.error({ err }, "Error fetching job status");
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/download/:jobId", async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await conversionQueue.getJob(jobId);

    if (!job || (await job.getState()) !== "completed") {
      return res
        .status(404)
        .json({ error: "File not found or processing not complete." });
    }

    const { outputPath, outputFilename } = job.returnvalue;

    if (!fs.existsSync(outputPath)) {
      return res
        .status(404)
        .json({ error: "File has been removed from server." });
    }

    const mime = mimeFor(path.extname(outputFilename).slice(1));
    res.setHeader("Content-Type", mime);
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${outputFilename}"`,
    );

    const stream = fs.createReadStream(outputPath);
    stream.pipe(res);

    stream.on("close", async () => {
      // Cleanup after download
      await safeUnlink(outputPath).catch(() => {});
      // Also cleanup the input file if it's still there
      if (job.data.inputPath) {
        await safeUnlink(job.data.inputPath).catch(() => {});
      }
      // Remove job from queue
      await job.remove();
    });
  } catch (err) {
    logger.error({ err }, "Error in download endpoint");
    res.status(500).json({ error: "Internal server error" });
  }
});

if (require.main === module) {
  const server = app.listen(PORT, async () => {
    const additionalTips = [];

    // Checking Redis configuration
    let redisStatus = "disconnected";
    try {
      const client = await conversionQueue.client;
      if (client) {
        redisStatus =
          client.status || (client.connected ? "ready" : "disconnected");
        const maxMemoryPolicy = await client.config("GET", "maxmemory-policy");

        // ioredis returns an object { "maxmemory-policy": "value" } or an array ["maxmemory-policy", "value"]
        const policyValue = Array.isArray(maxMemoryPolicy)
          ? maxMemoryPolicy[1]
          : maxMemoryPolicy["maxmemory-policy"];

        if (policyValue && policyValue !== "noeviction") {
          additionalTips.push(
            `Eviction policy is "${policyValue}". It should be "noeviction" for BullMQ stability.`,
          );
        }
      }
    } catch (e) {
      // If client check fails, it remains 'disconnected'
      logger.warn(
        { err: e.message },
        "Redis status check failed for startup banner",
      );
    }

    printBanner("Convertify API", {
      port: PORT,
      redis: {
        status: redisStatus,
        host: redisConfig.host,
        port: redisConfig.port,
      },
      ffmpegHW: FFmpegHW,
      cacheCount: cacheMap.size,
      additionalTips,
    });
  });

  // ── Graceful shutdown ──────────────────────────────────────────────────────
  const shutdown = () => {
    console.log("[SHUTDOWN] Received signal, shutting down.");
    server.close();
    for (const child of activeProcesses) {
      try {
        child.kill("SIGKILL");
      } catch {}
    }
    fs.promises
      .rm(TEMP_DIR, { recursive: true, force: true })
      .catch(() => {})
      .finally(() => {
        console.log("[SHUTDOWN] Done.");
        process.exit(0);
      });
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
