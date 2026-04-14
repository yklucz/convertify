# Convertify

A local file converter for video, audio, images, documents, and archives. All processing happens on your machine — nothing is uploaded to any external service.

## Features

- **Video** — convert between MP4, WebM, MKV, AVI, MOV, GIF, M4V, and more
- **Audio** — convert between MP3, WAV, M4A, FLAC, OGG, AAC, and more
- **Image** — convert between JPG, PNG, WEBP, AVIF, GIF, BMP, TIFF, HEIC, ICO, and more
- **Document** — convert DOCX, XLSX, PPTX, ODT, RTF, TXT, HTML, MD, EPUB, PSD, EPS via LibreOffice and Pandoc
- **Archive** — convert between ZIP, TAR, TAR.GZ, and 7Z
- **Compress** — reduce file size for PDF (Ghostscript), PNG (pngquant + optipng), and JPG (jpegoptim)
- **Conversion cache** — repeated conversions of the same file + options are served instantly from disk
- **Queue-based processing** — jobs run through BullMQ backed by Redis; parallel workers scale to available CPU cores
- **Hardware acceleration** — auto-detects Apple VideoToolbox and NVIDIA NVENC when available

## Requirements

### Node.js dependencies

```bash
npm install
```

### System binaries

| Binary | Purpose | Install |
|---|---|---|
| `ffmpeg` | Video and audio conversion | `brew install ffmpeg` |
| `pandoc` | Markup/text document conversion | `brew install pandoc` |
| LibreOffice (`soffice`) | Office document conversion | `brew install --cask libreoffice` |
| `gs` (Ghostscript) | PDF compression | `brew install ghostscript` |
| `pngquant` | PNG lossy compression | `brew install pngquant` |
| `optipng` | PNG lossless optimization | `brew install optipng` |
| `jpegoptim` | JPG compression | `brew install jpegoptim` |

If `soffice` is not found after installing LibreOffice:

```bash
echo 'export PATH="/Applications/LibreOffice.app/Contents/MacOS:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Redis

BullMQ requires a running Redis instance. Configure the connection in `redisConfig.js` or via environment variables:

```env
REDIS_HOST=
REDIS_PORT=
REDIS_USERNAME=
REDIS_PASSWORD=
REDIS_DB=0
```

Set the Redis eviction policy to `noeviction` for stable queue behavior:

```bash
redis-cli config set maxmemory-policy noeviction
```

## Project structure

```
convertify/
├── server.js          # Express API + conversion logic
├── worker.js          # BullMQ worker process
├── queue.js           # BullMQ queue definition
├── redisConfig.js     # Redis connection config
├── utils/
│   └── banner.js      # Startup banner
└── public/
    └── index.html     # Frontend UI
```

## Running

Start both the API server and the worker in separate terminals:

```bash
# Terminal 1 — API server
node server.js

# Terminal 2 — Worker
node worker.js

# Run all in one terminal
npm run all
```

Open [http://localhost:3000](http://localhost:3000).

The server defaults to port `3000`. Override with:

```bash
PORT=8080 node server.js
```

## Limits

| Type | Limit |
|---|---|
| Upload size | 1 GB |
| Compress upload | 50 MB |
| Job timeout (media) | 15 minutes |
| Job timeout (documents) | 5 minutes |
| Rate limit | 30 requests / minute per IP, max 10 conversions |

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP server port |
| `REDIS_HOST` | `127.0.0.1` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_USERNAME` | `default` | Redis username |
| `REDIS_PASSWORD` | _(empty)_ | Redis password |
| `REDIS_DB` | `0` | Redis database index |