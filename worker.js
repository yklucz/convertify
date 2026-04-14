"use strict";

// Import required modules
const { Worker } = require("bullmq");
const { config } = require("./redisConfig");
const { execFile } = require("node:child_process");
const path = require("node:path");
const fs = require("node:fs");
const archiver = require("archiver");
const pino = require("pino");
const crypto = require("node:crypto");
const os = require("node:os");
const { printBanner } = require("./utils/banner");

// Import conversion functions from server.js
const {
  convertVideoToVideo,
  convertVideoToAudio,
  convertAudioToAudio,
  convertImage,
  convertWithLibreOffice,
  convertWithPandoc,
  compressPdf,
  compressPng,
  compressJpg,
  safeUnlink,
  TEMP_DIR,
  path7za,
  storeInCache,
} = require("./server");

// Logger setup
const logger = pino({
  level: "info",
  base: { service: "worker" },
  serializers: { err: pino.stdSerializers.err },
  transport: {
    target: "pino-pretty",
    options: { colorize: true },
  },
});

// Process tracking for child processes
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

// Cleanup function
async function cleanup(inputPath, outputPath, currentChild) {
  if (currentChild && !currentChild.killed) currentChild.kill("SIGKILL");
  await safeUnlink(inputPath).catch(() => {});
  await safeUnlink(outputPath).catch(() => {});
}

// Worker setup - processes conversion jobs
const worker = new Worker(
  "conversion",
  async (job) => {
    let {
      inputPath,
      outputPath,
      outputFmt,
      options,
      fileType,
      streamInfo,
      inputExt,
      finalHash,
    } = job.data;

    let currentChild = null;
    let finished = false;
    const startTime = Date.now();

    logger.info(
      { jobId: job.id, type: fileType, format: outputFmt },
      "Job started",
    );

    try {
      // Dispatch to appropriate conversion function based on fileType
      if (fileType === "video" || fileType === "audio") {
        const hasVideo = streamInfo?.hasVideo ?? false;
        const hasAudio = streamInfo?.hasAudio ?? false;
        const isVideoOut = ["mp4", "webm", "mkv", "avi", "mov", "gif"].includes(
          outputFmt,
        );
        const isAudioOut = ["mp3", "wav", "m4a", "flac", "ogg", "aac"].includes(
          outputFmt,
        );

        if (hasVideo && isVideoOut) {
          await convertVideoToVideo(
            inputPath,
            outputPath,
            outputFmt,
            options,
            (child) => {
              currentChild = child;
            },
          );
        } else if (hasVideo && isAudioOut) {
          await convertVideoToAudio(
            inputPath,
            outputPath,
            outputFmt,
            options,
            (child) => {
              currentChild = child;
            },
          );
        } else if (!hasVideo && hasAudio && isAudioOut) {
          await convertAudioToAudio(
            inputPath,
            outputPath,
            outputFmt,
            options,
            (child) => {
              currentChild = child;
            },
          );
        } else {
          throw new Error("Unsupported media conversion mapping");
        }
      } else if (fileType === "image") {
        await convertImage(
          inputPath,
          outputPath,
          outputFmt,
          options,
          (child) => {
            currentChild = child;
          },
        );
      } else if (fileType === "document") {
        const inputExt = path.extname(inputPath).toLowerCase().slice(1);
        const pandocOuts =
          {
            md: ["docx", "pdf", "html", "odt", "rtf", "txt", "epub"],
            html: ["docx", "pdf", "odt", "rtf", "txt", "md", "epub"],
            htm: ["docx", "pdf", "odt", "rtf", "txt", "md", "epub"],
            txt: ["docx", "pdf", "html", "odt", "rtf", "md"],
            epub: ["docx", "pdf", "html", "odt", "txt", "md"],
          }[inputExt] || [];
        const loOuts =
          {
            docx: ["pdf", "odt", "rtf", "txt", "html", "docx"],
            doc: ["pdf", "odt", "rtf", "txt", "html", "docx"],
            odt: ["pdf", "docx", "rtf", "txt", "html"],
            rtf: ["pdf", "docx", "odt", "txt", "html"],
            xlsx: ["pdf", "ods", "csv", "xlsx"],
            xls: ["pdf", "ods", "csv", "xlsx"],
            ods: ["pdf", "xlsx", "csv"],
            csv: ["pdf", "xlsx", "ods"],
            pptx: ["pdf", "odp", "pptx"],
            ppt: ["pdf", "odp", "pptx"],
            odp: ["pdf", "pptx"],
          }[inputExt] || [];

        const usePandoc =
          pandocOuts.includes(outputFmt) && !loOuts.includes(outputFmt);

        if (usePandoc) {
          await convertWithPandoc(
            inputPath,
            outputPath,
            path.parse(inputPath).name,
            outputFmt,
            (child) => {
              currentChild = child;
            },
          );
        } else {
          await convertWithLibreOffice(
            inputPath,
            path.dirname(outputPath),
            outputFmt,
            (child) => {
              currentChild = child;
            },
          );
          outputPath = path.join(
            path.dirname(outputPath),
            `${path.basename(inputPath, path.extname(inputPath))}.${outputFmt}`,
          );
        }
      } else if (fileType === "compress") {
        // Handle compression based on file extension
        const ext = path.extname(inputPath).toLowerCase();
        if (ext === ".pdf") {
          await compressPdf(inputPath, outputPath, options, (child) => {
            currentChild = child;
          });
        } else if (ext === ".png") {
          await compressPng(inputPath, outputPath, options, (child) => {
            currentChild = child;
          });
        } else if (ext === ".jpg" || ext === ".jpeg") {
          await compressJpg(inputPath, outputPath, options, (child) => {
            currentChild = child;
          });
        } else {
          throw new Error(`Unsupported compression format: ${ext}`);
        }
      } else if (fileType === "archive") {
        const extractDir = path.join(TEMP_DIR, `ext_${crypto.randomUUID()}`);
        const renamedInput = path.join(
          TEMP_DIR,
          `${path.basename(inputPath)}_${crypto.randomUUID()}.${inputExt}`,
        );

        try {
          await fs.promises.rename(inputPath, renamedInput);
          await fs.promises.mkdir(extractDir, { recursive: true });

          // Extraction
          await new Promise((resolve, reject) => {
            currentChild = trackExecFile(
              path7za,
              ["x", renamedInput, `-o${extractDir}`, "-y"],
              (err) => {
                if (err) reject(new Error("Archive extraction failed."));
                else resolve();
              },
            );
          });

          // Archiving
          await new Promise((resolve, reject) => {
            if (outputFmt === "7z") {
              currentChild = trackExecFile(
                path7za,
                ["a", outputPath, `${extractDir}/*`],
                (err) => {
                  if (err) reject(new Error("Failed to create 7z archive."));
                  else resolve();
                },
              );
            } else {
              const outputStream = fs.createWriteStream(outputPath);
              const archiveFormat = outputFmt === "tar.gz" ? "tar" : outputFmt;
              const archive = archiver(archiveFormat, {
                gzip: outputFmt === "tar.gz",
                zlib: { level: 9 },
              });
              outputStream.on("close", resolve);
              archive.on("error", reject);
              archive.pipe(outputStream);
              archive.directory(extractDir, false);
              archive.finalize();
            }
          });
        } finally {
          await safeUnlink(renamedInput).catch(() => {});
          await fs.promises
            .rm(extractDir, { recursive: true, force: true })
            .catch(() => {});
        }
      } else {
        throw new Error(`Unsupported file type: ${fileType}`);
      }

      if (finished) return;

      // Verify output file exists
      const stat = await fs.promises.stat(outputPath);
      if (!stat || stat.size === 0) {
        throw new Error("Conversion produced an empty file.");
      }

      // Store in cache if finalHash is provided
      if (finalHash) {
        await storeInCache(finalHash, outputPath, outputFmt);
      }

      logger.info(
        {
          jobId: job.id,
          type: fileType,
          format: outputFmt,
          duration: Date.now() - startTime,
          fileSize: stat.size,
        },
        "Job completed successfully",
      );

      // Return result for job completion
      return {
        outputPath: outputPath,
        outputFilename: path.basename(outputPath),
        fileSize: stat.size,
      };
    } catch (err) {
      if (!finished) {
        finished = true;
        await cleanup(inputPath, outputPath, currentChild);
      }
      logger.error(
        {
          jobId: job.id,
          type: fileType,
          format: outputFmt,
          duration: Date.now() - startTime,
          error: err.message,
        },
        "Job failed",
      );
      throw err; // Re-throw to let BullMQ handle failure
    }
  },
  {
    connection: config,
    concurrency: os.cpus().length || 4, // Process jobs in parallel based on CPU availability
  },
);

// Worker event listeners
worker.on("completed", (job) => {
  logger.info({ jobId: job.id }, "Job marked as completed");
});

worker.on("failed", (job, err) => {
  logger.error({ jobId: job.id, failedReason: err.message }, "Job failed");
});

worker.on("error", (err) => {
  logger.error({ err }, "Worker error");
});

// Graceful shutdown
function shutdown() {
  logger.info("[WORKER] Received shutdown signal");
  worker.close();
  // Kill any active child processes
  for (const child of activeProcesses) {
    try {
      child.kill("SIGKILL");
    } catch {}
  }
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

// Log worker startup
// FFmpeg Hardware detection in worker
const FFmpegHW = {
  h264_videotoolbox: false,
  hevc_videotoolbox: false,
  h264_nvenc: false,
  hevc_nvenc: false,
};
execFile("ffmpeg", ["-encoders"], (err, stdout) => {
  if (!err && stdout) {
    if (stdout.includes("h264_videotoolbox")) FFmpegHW.h264_videotoolbox = true;
    if (stdout.includes("hevc_videotoolbox")) FFmpegHW.hevc_videotoolbox = true;
    if (stdout.includes("h264_nvenc")) FFmpegHW.h264_nvenc = true;
    if (stdout.includes("hevc_nvenc")) FFmpegHW.hevc_nvenc = true;
  }

  printBanner("Convertify Worker", {
    redis: {
      status: worker.isRunning() ? "ready" : "connecting",
      host: config.host,
      port: config.port,
    },
    ffmpegHW: FFmpegHW,
    minimal: true,
  });
});
