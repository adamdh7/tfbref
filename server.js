const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const { S3Client, DeleteObjectCommand, DeleteObjectsCommand, HeadObjectCommand, GetObjectCommand, ListObjectsV2Command, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const mongoose = require('mongoose');

process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
});

process.on('unhandledRejection', (reason) => {
    console.error('Unhandled Rejection:', reason);
});

const PORT = process.env.PORT || 10000;
const MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024 - 5 * 1024 * 1024;
const MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const LIMIT_R2_BYTES = 7516192768;
const DOWNLOAD_URL_TTL_SECONDS = 3600;
const UPLOAD_URL_TTL_SECONDS = 3600;
const PENDING_UPLOAD_TTL_MS = 30 * 60 * 1000;
const PENDING_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;
const R2_STATUS_SYNC_INTERVAL_MS = 5 * 60 * 1000;

const R2_BASE_URL = 'https://pub-e2d76735e9dd42f2af664d9e64599ca6.r2.dev';
const ICON_URL = 'https://adamdh7.org/adamdh7.png';
const R2_BUCKET = 'bref';

const s3 = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT || 'https://49bdcdc6f29c08eda8bb7bcb8db9e27f.r2.cloudflarestorage.com',
    maxAttempts: 5,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID || 'f0f6afdccc64b458f4d86110918e11ce',
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || 'de5455c6af1e858d598d94d0de10717493133998d8e9cff54110311f744b266c'
    }
});

mongoose.connect(process.env.MONGO_URI || 'mongodb+srv://adamdh7:Tchengy1@botadamdh7.lo27bbm.mongodb.net/brefs?appName=brefs')
    .then(() => console.log('[MONGO] CONNECTED'))
    .catch(error => console.error(`[MONGO] CONNECTION_ERROR error=${error.message || error}`));

const statusSchema = new mongoose.Schema({
    key: { type: String, unique: true },
    totalSize: { type: Number, default: 0 },
    limitBytes: { type: Number, default: LIMIT_R2_BYTES }
}, { versionKey: false, collection: 'statuses' });
const StatusModel = mongoose.model('Status', statusSchema);

const pendingUploadSchema = new mongoose.Schema({
    _id: { type: String, required: true },
    expiresAt: { type: Date, required: true }
}, { versionKey: false, collection: 'upload_pending' });
const PendingUploadModel = mongoose.model('PendingUpload', pendingUploadSchema);

async function ensureGlobalStatus() {
    const status = await StatusModel.findOneAndUpdate(
        { key: 'global' },
        { $set: { limitBytes: LIMIT_R2_BYTES }, $unset: { lastWipe: 1 }, $setOnInsert: { totalSize: 0, key: 'global' } },
        { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true }
    ).lean();
    return status;
}

async function listR2Objects() {
    const objects = [];
    let continuationToken;
    let page = 0;
    console.log(`[R2] LIST_START bucket=${R2_BUCKET}`);

    try {
        do {
            page += 1;
            console.log(`[R2] LIST_PAGE_START page=${page}`);
            const command = new ListObjectsV2Command({
                Bucket: R2_BUCKET,
                ContinuationToken: continuationToken
            });
            const response = await s3.send(command);
            for (const object of response.Contents || []) {
                if (!object.Key) continue;
                objects.push({
                    key: String(object.Key),
                    size: Number(object.Size || 0),
                    lastModified: object.LastModified ? new Date(object.LastModified) : null
                });
            }
            console.log(`[R2] LIST_PAGE_OK page=${page} objects=${(response.Contents || []).length} truncated=${Boolean(response.IsTruncated)}`);
            continuationToken = response.IsTruncated ? String(response.NextContinuationToken || '') : '';
        } while (continuationToken);
        console.log(`[R2] LIST_DONE objects=${objects.length} pages=${page}`);
        return objects;
    } catch (error) {
        console.error(`[R2] LIST_ERROR page=${page} error=${error.message || error}`);
        throw error;
    }
}

async function syncStatusFromR2() {
    console.log('[R2] STATUS_SYNC_START');
    try {
        const objects = await listR2Objects();
        const totalSize = objects.reduce((sum, object) => sum + Math.max(0, Number(object.size || 0)), 0);
        await StatusModel.updateOne(
            { key: 'global' },
            { $set: { totalSize, limitBytes: LIMIT_R2_BYTES }, $setOnInsert: { key: 'global' } },
            { upsert: true }
        );
        console.log(`[R2] STATUS_SYNC totalBytes=${totalSize} limitBytes=${LIMIT_R2_BYTES} objects=${objects.length}`);
        console.log('[R2] STATUS_SYNC_DONE');
        return { totalSize, objects };
    } catch (error) {
        console.error(`[R2] STATUS_SYNC_FAILED error=${error.message || error}`);
        throw error;
    }
}

async function wipeEntireR2Bucket(reason) {
    console.log(`[R2] WIPE_REQUEST reason=${reason}`);
    const objects = await listR2Objects();
    console.log(`[R2] WIPE_START reason=${reason} objects=${objects.length}`);

    for (let index = 0; index < objects.length; index += 1000) {
        const batch = objects.slice(index, index + 1000);
        if (!batch.length) continue;
        const response = await s3.send(new DeleteObjectsCommand({
            Bucket: R2_BUCKET,
            Delete: {
                Objects: batch.map(object => ({ Key: object.key })),
                Quiet: false
            }
        }));
        const deleted = Array.isArray(response.Deleted) ? response.Deleted.length : 0;
        const errors = Array.isArray(response.Errors) ? response.Errors.length : 0;
        console.log(`[R2] WIPE_BATCH index=${Math.floor(index / 1000) + 1} requested=${batch.length} deleted=${deleted} errors=${errors}`);
        if (errors) console.error(`[R2] WIPE_ERRORS ${JSON.stringify(response.Errors)}`);
    }

    await PendingUploadModel.deleteMany({});
    await ensureGlobalStatus();
    await StatusModel.updateOne({ key: 'global' }, { $set: { totalSize: 0, limitBytes: LIMIT_R2_BYTES } });
    console.log('[R2] WIPE_DONE totalBytes=0 pending=0');
}

async function cleanupExpiredPendingUploads() {
    const now = new Date();
    const pending = await PendingUploadModel.find({ expiresAt: { $lte: now } }).limit(500).lean();
    if (!pending.length) return;

    console.log(`[UPLOAD] PENDING_CLEANUP_START count=${pending.length}`);
    for (const item of pending) {
        const token = String(item._id || '');
        if (!token) continue;
        try {
            await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: token }));
            console.log(`[UPLOAD] PENDING_R2_DELETE token=${token} result=ok`);
        } catch (error) {
            const code = String(error && (error.name || error.Code || error.code) || '').toLowerCase();
            if (!code.includes('nosuchkey') && !code.includes('notfound')) {
                console.error(`[UPLOAD] PENDING_R2_DELETE token=${token} result=error error=${error.message || error}`);
            } else {
                console.log(`[UPLOAD] PENDING_R2_DELETE token=${token} result=missing`);
            }
        }
        await PendingUploadModel.deleteOne({ _id: token });
        console.log(`[UPLOAD] PENDING_REMOVED token=${token}`);
    }

    await syncStatusFromR2();
    console.log('[UPLOAD] PENDING_CLEANUP_DONE');
}

async function cleanupOldR2Objects() {
    const cutoff = Date.now() - MAX_AGE_MS;
    const objects = await listR2Objects();
    let removed = 0;

    for (const object of objects) {
        if (!object.lastModified || object.lastModified.getTime() >= cutoff) continue;
        try {
            await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: object.key }));
            removed += 1;
            console.log(`[R2] OLD_OBJECT_DELETE key=${object.key} result=ok`);
        } catch (error) {
            console.error(`[R2] OLD_OBJECT_DELETE key=${object.key} result=error error=${error.message || error}`);
        }
    }

    if (removed) await syncStatusFromR2();
}

async function dropLegacyFileCollection() {
    try {
        const collections = await mongoose.connection.db.listCollections({ name: 'files' }).toArray();
        if (collections.length) {
            await mongoose.connection.db.collection('files').drop();
            console.log('[MONGO] LEGACY_FILES_COLLECTION_DROPPED');
        }
    } catch (error) {
        console.error(`[MONGO] LEGACY_FILES_COLLECTION_DROP_ERROR error=${error.message || error}`);
    }
}

mongoose.connection.once('open', async () => {
    try {
        await ensureGlobalStatus();
        await StatusModel.deleteMany({ key: { $ne: 'global' } });
        await dropLegacyFileCollection();
        await syncStatusFromR2();
        console.log('[MONGO] STORAGE_STATE_READY');
    } catch (error) {
        console.error(`[MONGO] STORAGE_STATE_INIT_ERROR error=${error.message || error}`);
    }
});

function genToken() {
    const chars = '0123456789';
    let t = '';
    for (let i = 0; i < 7; i++) t += chars[Math.floor(Math.random() * chars.length)];
    return t;
}

function safeFileName(name) {
    const ext = path.extname(name || '');
    const base = path.basename(name || '', ext);
    const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 120);
    const safeExt = ext.replace(/[^a-zA-Z0-9.]/g, '');
    return (safeBase + safeExt) || 'file';
}

function contentTypeFromName(filename) {
    const ext = path.extname(filename).toLowerCase();
    const map = {
        '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif',
        '.webp': 'image/webp', '.bmp': 'image/bmp', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
        '.tiff': 'image/tiff', '.heic': 'image/heic', '.heif': 'image/heif', '.avif': 'image/avif',
        '.mp4': 'video/mp4', '.webm': 'video/webm', '.mov': 'video/quicktime', '.quicktime': 'video/quicktime',
        '.mkv': 'video/x-matroska', '.avi': 'video/x-msvideo', '.wmv': 'video/x-ms-wmv', '.flv': 'video/x-flv',
        '.m4v': 'video/x-m4v', '.3gp': 'video/3gpp', '.ts': 'video/mp2t', '.ogv': 'video/ogg',
        '.mpeg': 'video/mpeg', '.mpg': 'video/mpeg', '.m2ts': 'video/mp2t', '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav', '.ogg': 'audio/ogg', '.m4a': 'audio/mp4', '.flac': 'audio/flac',
        '.aac': 'audio/aac', '.opus': 'audio/opus', '.mid': 'audio/midi', '.midi': 'audio/midi',
        '.pdf': 'application/pdf', '.doc': 'application/msword', '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.xls': 'application/vnd.ms-excel', '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.ppt': 'application/vnd.ms-powerpoint', '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        '.odt': 'application/vnd.oasis.opendocument.text', '.rtf': 'application/rtf', '.txt': 'text/plain',
        '.html': 'text/html', '.htm': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
        '.mjs': 'application/javascript', '.json': 'application/json', '.xml': 'application/xml',
        '.csv': 'text/csv', '.md': 'text/markdown', '.yaml': 'text/yaml', '.yml': 'text/yaml',
        '.zip': 'application/zip', '.rar': 'application/vnd.rar', '.7z': 'application/x-7z-compressed',
        '.tar': 'application/x-tar', '.gz': 'application/gzip', '.woff': 'font/woff', '.woff2': 'font/woff2',
        '.ttf': 'font/ttf', '.otf': 'font/otf', '.epub': 'application/epub+zip',
        '.apk': 'application/vnd.android.package-archive', '.exe': 'application/vnd.microsoft.portable-executable',
        '.bin': 'application/octet-stream'
    };
    return map[ext] || 'application/octet-stream';
}

function isImageFile(filename) {
    return ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.svg', '.avif', '.heic', '.heif', '.tiff', '.ico'].includes(path.extname(filename).toLowerCase());
}

function isVideoFile(filename) {
    return ['.mp4', '.webm', '.m4v', '.mov', '.ogg', '.ogv', '.quicktime', '.mkv', '.avi', '.wmv', '.flv', '.3gp', '.ts', '.mpeg', '.mpg', '.m2ts'].includes(path.extname(filename).toLowerCase());
}

function escapeHtml(text) {
    return String(text).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function buildContentDisposition(filename) {
    const original = String(filename || 'file');
    const fallback = safeFileName(original).replace(/["\\]/g, '_') || 'file';
    return `attachment; filename="${fallback}"; filename*=UTF-8''${encodeURIComponent(original)}`;
}

async function buildPresignedDownloadUrl(token, filename, mimeType) {
    const command = new GetObjectCommand({
        Bucket: R2_BUCKET,
        Key: token,
        ResponseContentDisposition: buildContentDisposition(filename),
        ResponseContentType: mimeType || contentTypeFromName(filename),
        ResponseCacheControl: 'no-store'
    });
    return await getSignedUrl(s3, command, { expiresIn: DOWNLOAD_URL_TTL_SECONDS });
}

async function buildPresignedMediaUrl(token, filename, mimeType) {
    const command = new GetObjectCommand({
        Bucket: R2_BUCKET,
        Key: token,
        ResponseContentDisposition: `inline; filename="${safeFileName(filename)}"; filename*=UTF-8''${encodeURIComponent(filename)}`,
        ResponseContentType: mimeType || contentTypeFromName(filename),
        ResponseCacheControl: 'public, max-age=3600'
    });
    return await getSignedUrl(s3, command, { expiresIn: DOWNLOAD_URL_TTL_SECONDS });
}

function isEmbeddedMediaRequest(req) {
    const dest = String(req.get('sec-fetch-dest') || '').toLowerCase();
    return dest === 'video' || dest === 'audio' || dest === 'image' || dest === 'object' || !!(req.get('range') && dest !== 'document');
}

function buildUiRestrictionScript() {
    return `<script>
(function(){
    function preventBrowserActions(event){
        if(event.cancelable) event.preventDefault();
    }
    document.addEventListener('contextmenu', preventBrowserActions, {passive:false});
    document.addEventListener('selectstart', preventBrowserActions, {passive:false});
    document.addEventListener('dragstart', preventBrowserActions, {passive:false});
    document.addEventListener('copy', preventBrowserActions, {passive:false});
    document.addEventListener('cut', preventBrowserActions, {passive:false});
    document.addEventListener('paste', preventBrowserActions, {passive:false});
    document.addEventListener('dblclick', preventBrowserActions, {passive:false});
    document.addEventListener('keydown', function(event){
        const key = String(event.key || '').toLowerCase();
        if (event.ctrlKey || event.metaKey || key === 'f12' || key === 'f5' || key === 'f11' || key === '+' || key === '-' || key === '=') {
            if (event.cancelable) event.preventDefault();
            event.stopPropagation();
        }
    }, {passive:false});
    document.addEventListener('wheel', function(event){
        if ((event.ctrlKey || event.metaKey) && event.cancelable) {
            event.preventDefault();
        }
    }, {passive:false});
    document.addEventListener('touchstart', function(event){
        if (event.touches && event.touches.length > 1 && event.cancelable) {
            event.preventDefault();
        }
    }, {passive:false});
    document.addEventListener('touchmove', function(event){
        if (event.touches && event.touches.length > 1 && event.cancelable) {
            event.preventDefault();
        }
    }, {passive:false});
    document.addEventListener('gesturestart', preventBrowserActions, {passive:false});
    document.addEventListener('gesturechange', preventBrowserActions, {passive:false});
    document.addEventListener('gestureend', preventBrowserActions, {passive:false});
})();
</script>`;
}

function buildForceDownloadHtml(filename, downloadUrl) {
    const safeTitle = escapeHtml(filename);
    const safeUrl = escapeHtml(downloadUrl);
    return `<!doctype html>
<html lang="ht">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta name="theme-color" content="#000000">
<title>${safeTitle}</title>
<link rel="icon" type="image/png" href="${ICON_URL}">
<style>
html,body{margin:0;padding:0;width:100%;height:100%;height:100dvh;background:#000;color:#fff;display:flex;align-items:center;justify-content:center;font-family:sans-serif;text-align:center;overflow:hidden;overscroll-behavior:none;touch-action:none;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;}
*{box-sizing:border-box;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;}
a{color:#fff;text-decoration:underline;font-size:18px;margin-top:15px;display:inline-block;touch-action:manipulation;}
</style>
</head>
<body>
<div>
    <p>Telechajman an ta sipoze kòmanse….</p>
    <a id="dl" href="${safeUrl}" download="${safeTitle}">Si telechajman an pako kòmanse klike la…</a>
</div>
${buildUiRestrictionScript()}
<script>
window.addEventListener('load', function(){
    var a = document.getElementById('dl');
    if (a) {
        setTimeout(function(){
            try { a.click(); } catch(e) {}
        }, 0);
    }
});
</script>
</body>
</html>`;
}

function buildLightweightViewerHtml(title, mediaUrl, downloadUrl, isVideo, mimeType) {
    const safeTitle = escapeHtml(title);
    const safeMediaUrl = escapeHtml(mediaUrl);
    const safeDownloadUrl = escapeHtml(downloadUrl);
    const safeMime = escapeHtml(mimeType || contentTypeFromName(title));
    const mediaBlock = isVideo
        ? `<video id="media-element" data-src="${safeMediaUrl}" preload="metadata" playsinline controls></video>`
        : `<img id="media-element" data-src="${safeMediaUrl}" alt="${safeTitle}" decoding="async">`;

    return `<!doctype html>
<html lang="ht">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta name="theme-color" content="#000000">
<title>${safeTitle}</title>
<link rel="icon" type="image/png" href="${ICON_URL}">
<link rel="shortcut icon" type="image/png" href="${ICON_URL}">
<link rel="apple-touch-icon" href="${ICON_URL}">
<style>
html,body{margin:0;width:100%;height:100%;height:100dvh;overflow:hidden;background:#000;overscroll-behavior:none;touch-action:none;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;}
*{box-sizing:border-box;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;}
.wrap{display:flex;align-items:center;justify-content:center;width:100%;height:100%;height:100dvh;position:relative;overflow:hidden;touch-action:none;}
img,video,audio{max-width:100%;max-height:100%;object-fit:contain;outline:none;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;touch-action:none;}
.download-btn{position:fixed;left:50%;bottom:30px;transform:translateX(-50%);z-index:9999;display:none;background:rgba(255,255,255,0.2);width:56px;height:56px;border-radius:50%;align-items:center;justify-content:center;color:#fff;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.3);transition:opacity 0.3s ease, transform 0.2s ease;cursor:pointer;text-decoration:none;touch-action:manipulation;}
.download-btn:active{transform:translateX(-50%) scale(0.9);}
.download-btn svg{width:24px;height:24px;fill:currentColor;}
</style>
</head>
<body>
<div class="wrap">${mediaBlock}</div>
<a id="download-btn" class="download-btn" href="${safeDownloadUrl}" download="${safeTitle}">
    <svg viewBox="0 0 24 24"><path d="M5 20h14v-2H5v2zM19 9h-4V3H9v6H5l7 7 7-7z"/></svg>
</a>
${buildUiRestrictionScript()}
<script>
(function(){
    const mediaEl = document.getElementById('media-element');
    const btn = document.getElementById('download-btn');
    const mediaUrl = mediaEl ? mediaEl.getAttribute('data-src') : '';
    const downloadUrl = "${safeDownloadUrl}";
    const mimeType = "${safeMime}";
    const isVideo = ${isVideo ? 'true' : 'false'};
    let activated = false;

    function downloadNow(){
        if (activated) return;
        activated = true;
        window.location.replace(downloadUrl);
    }
    function showDownloadButton(){
        if (btn) btn.style.display = 'flex';
    }
    function activateMedia(){
        if (!mediaEl || !mediaUrl) return downloadNow();
        if (isVideo) {
            let canPlay = '';
            try { canPlay = mediaEl.canPlayType(mimeType); } catch(e) {}
            if (!canPlay) return downloadNow();

            mediaEl.addEventListener('loadedmetadata', showDownloadButton, {once:true});
            mediaEl.addEventListener('loadeddata', showDownloadButton, {once:true});
            mediaEl.addEventListener('canplay', showDownloadButton, {once:true});
            mediaEl.addEventListener('error', downloadNow, {once:true});
            mediaEl.src = mediaUrl;
            mediaEl.load();
            const playPromise = mediaEl.play();
            if (playPromise && typeof playPromise.catch === 'function') {
                playPromise.catch(showDownloadButton);
            }
            return;
        }
        mediaEl.addEventListener('load', showDownloadButton, {once:true});
        mediaEl.addEventListener('error', downloadNow, {once:true});
        mediaEl.src = mediaUrl;
    }
    window.addEventListener('load', function(){ setTimeout(activateMedia, 0); }, {once:true});
    if (btn) {
        btn.addEventListener('click', function(event){ event.stopPropagation(); });
    }
})();
</script>
</body>
</html>`;
}

function buildCustomPlayerHtml(title, targetUrl, fullUrl, mimeType, downloadUrl) {
    const pageTitle = escapeHtml(title);
    const safeTargetUrl = escapeHtml(targetUrl);
    const safeFullUrl = escapeHtml(fullUrl);
    const safeMime = escapeHtml(mimeType || 'video/mp4');
    const safeDownloadUrl = escapeHtml(downloadUrl);

    return `<!doctype html>
<html lang="ht">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover"/>
<title>${pageTitle}</title>
<meta property="og:type" content="video.other" />
<meta property="og:title" content="${pageTitle}" />
<meta property="og:description" content="Gade ${pageTitle} sou Adam_D’H7" />
<meta property="og:video" content="${safeTargetUrl}" />
<meta property="og:video:secure_url" content="${safeTargetUrl}" />
<meta property="og:video:type" content="${safeMime}" />
<meta property="og:video:width" content="1280" />
<meta property="og:video:height" content="720" />
<meta property="og:image" content="${ICON_URL}" />
<meta name="twitter:card" content="player" />
<meta name="twitter:site" content="@adam_dh7" />
<meta name="twitter:title" content="${pageTitle}" />
<meta name="twitter:description" content="Gade ${pageTitle} sou Adam_D’H7" />
<meta name="twitter:player" content="${safeFullUrl}" />
<meta name="twitter:player:width" content="1280" />
<meta name="twitter:player:height" content="720" />
<meta name="twitter:player:stream" content="${safeTargetUrl}" />
<meta name="twitter:player:stream:content_type" content="${safeMime}" />
<meta name="twitter:image" content="${ICON_URL}" />
<link rel="manifest" href="manifest.json" />
<meta name="theme-color" content="#000000" />
<meta name="mobile-web-app-capable" content="yes" />
<meta name="apple-mobile-web-app-capable" content="yes" />
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />
<meta name="apple-mobile-web-app-title" content="${pageTitle}" />
<link rel="apple-touch-icon" href="${ICON_URL}" />
<style>
:root{--bg:#000;--muted:#9aa0a6;--accent:#fff;--seek-height:8px;}
*{box-sizing:border-box;user-select:none;-webkit-user-select:none;-ms-user-select:none;-moz-user-select:none;-webkit-touch-callout:none;}
html,body{width:100%;height:100%;height:100dvh;margin:0;padding:0;background:var(--bg);color:var(--accent);font-family:Inter,system-ui,Arial,sans-serif;overflow:hidden;overscroll-behavior:none;touch-action:none;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;}
.wrap,.video-card,.controls-wrap,.inside-mini-controls{user-select:none;-webkit-user-select:none;-ms-user-select:none;-moz-user-select:none;-webkit-touch-callout:none;}
.wrap{position:absolute;inset:0;width:100%;height:100%;height:100dvh;margin:0;padding:0;overflow:hidden;}
.wrap.css-fullscreen{position:fixed !important;inset:0 !important;width:100vw !important;height:100vh !important;height:100dvh !important;z-index:99999 !important;background:#000;overflow:hidden !important;}
.video-card{position:absolute;inset:0;border-radius:0;overflow:hidden;background:#000;width:100%;height:100%;touch-action:none;}
video{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;z-index:1;background:#000;transition:filter .08s linear;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;touch-action:none;}
.controls-wrap{position:absolute;left:12px;right:12px;bottom:12px;pointer-events:none;z-index:12;transition:opacity .25s ease;filter:drop-shadow(0px 2px 8px rgba(0,0,0,0.8));}
.time-row{display:flex;align-items:center;gap:12px;padding:8px 12px;color:var(--muted);font-size:14px;justify-content:center;}
.time{width:82px;text-align:center;font-variant-numeric:tabular-nums;text-shadow:0px 2px 4px rgba(0,0,0,0.9);}
.progress{flex:1;max-width:820px;display:flex;align-items:center;}
.seek{position:relative;height:var(--seek-height);width:100%;border-radius:999px;cursor:pointer;background:rgba(255,255,255,0.22);touch-action:none;}
.fill{position:absolute;left:0;top:0;height:100%;width:0%;border-radius:999px;background:linear-gradient(90deg,rgba(255,255,255,0.95),rgba(255,255,255,0.55));pointer-events:none;}
.thumb{position:absolute;top:50%;transform:translate(-50%,-50%);width:18px;height:18px;border-radius:50%;background:#fff;box-shadow:0px 1px 4px rgba(0,0,0,0.8);pointer-events:auto;touch-action:none;}
.controls-hidden{opacity:0;pointer-events:none !important;}
.controls-visible{opacity:1;pointer-events:auto;}
.inside-mini-controls{position:absolute;left:0;right:0;top:50%;transform:translateY(-50%);z-index:13;display:flex;justify-content:space-between;align-items:center;padding:0 15%;pointer-events:none;transition:opacity .25s ease;}
.inside-hidden{opacity:0;pointer-events:none !important;}
.inside-visible{opacity:1;pointer-events:auto;}
.inside-item{display:flex;align-items:center;background:none;border:none;flex:1;pointer-events:auto;}
#backContainer{justify-content:flex-start;}
#playContainer{justify-content:center;}
#forwardContainer{justify-content:flex-end;}
.inside-item .mini-btn{width:55px;height:55px;border-radius:50%;border:none;background:transparent;color:var(--accent);font-weight:700;font-size:26px;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;-webkit-tap-highlight-color:transparent;filter:drop-shadow(0px 3px 6px rgba(0,0,0,0.9));-webkit-appearance:none;appearance:none;padding:0;touch-action:manipulation;}
.inside-item .mini-btn:disabled{pointer-events:none;}
.inside-item .mini-btn:active{transform:scale(0.85);}
.landscape-btn{margin-left:8px;border:none;background:transparent;color:var(--muted);width:34px;height:34px;cursor:pointer;filter:drop-shadow(0px 2px 4px rgba(0,0,0,0.9));display:flex;align-items:center;justify-content:center;padding:0;-webkit-appearance:none;appearance:none;touch-action:manipulation;}
.landscape-btn:hover{color:var(--accent);}
.landscape-btn svg{width:24px;height:24px;}
.swipe-indicator-left,.swipe-indicator-right{position:absolute;top:50%;transform:translateY(-50%);color:var(--accent);display:flex;flex-direction:column;align-items:center;gap:8px;z-index:20;pointer-events:none;opacity:0;transition:opacity .15s ease;font-weight:700;font-size:16px;text-shadow:0px 2px 8px rgba(0,0,0,0.9);}
.swipe-indicator-left{left:40px;}
.swipe-indicator-right{right:40px;}
.swipe-indicator-left.show,.swipe-indicator-right.show{opacity:1;}
.swipe-icon{width:26px;height:26px;}
.spinner-overlay{position:absolute;inset:0;display:flex;justify-content:center;align-items:center;z-index:15;pointer-events:none;}
.spinner{animation:rotate 2s linear infinite;width:50px;height:50px;filter:drop-shadow(0px 2px 6px rgba(0,0,0,0.8));}
.spinner .path{stroke:var(--accent);stroke-linecap:round;animation:dash 1.5s ease-in-out infinite;}
@keyframes rotate{100%{transform:rotate(360deg);}}
@keyframes dash{0%{stroke-dasharray:1,150;stroke-dashoffset:0;}50%{stroke-dasharray:90,150;stroke-dashoffset:-35;}100%{stroke-dasharray:90,150;stroke-dashoffset:-124;}}
#errorOverlay{display:none;position:absolute;inset:0;background:#000;z-index:50;justify-content:center;align-items:center;flex-direction:column;}
.error-container{position:relative;width:100%;height:100%;display:flex;justify-content:center;align-items:center;}
.error-image-wrapper{position:relative;width:100%;max-width:100%;aspect-ratio:16/9;display:flex;justify-content:center;align-items:center;}
.error-image-wrapper img{width:100%;height:100%;object-fit:contain;user-select:none;-webkit-user-select:none;-webkit-touch-callout:none;pointer-events:none;}
#errTxt{position:absolute;bottom:5%;width:100%;text-align:center;color:grey;font-size:clamp(12px,2vw,16px);z-index:10000;font-weight:bold;text-shadow:0 1px 2px #000;}
</style>
</head>
<body>
<div class="wrap" id="mainWrap">
    <div class="video-card" id="card">
        <video id="video" preload="metadata" playsinline data-src="${safeTargetUrl}">
            <source data-src="${safeTargetUrl}" type="${safeMime}">
        </video>
        <div id="errorOverlay">
            <div class="error-container">
                <div class="error-image-wrapper">
                    <img src="https://adamdh7.org/asset/nwa.png" alt="Erè" />
                    <div id="errTxt"></div>
                </div>
            </div>
        </div>
        <div class="spinner-overlay" id="spinnerContainer" style="display:flex;">
            <svg class="spinner" viewBox="0 0 50 50"><circle class="path" cx="25" cy="25" r="20" fill="none" stroke-width="5"></circle></svg>
        </div>
        <div class="inside-mini-controls inside-visible" id="insideMini">
            <div class="inside-item" id="backContainer"><button class="mini-btn" id="insideBack">-10</button></div>
            <div class="inside-item" id="playContainer"><button class="mini-btn" id="insidePlay">❚❚</button></div>
            <div class="inside-item" id="forwardContainer"><button class="mini-btn" id="insideForward">+10</button></div>
        </div>
        <div class="controls-wrap controls-visible" id="controlsWrap">
            <div class="time-row" id="timeRow">
                <div class="time" id="current">0:00</div>
                <div class="progress">
                    <div class="seek" id="seekBar" tabindex="0" role="slider" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0" aria-disabled="false">
                        <div class="fill" id="fill"></div>
                        <div class="thumb" id="thumb" aria-hidden="true"></div>
                    </div>
                </div>
                <div class="time" id="duration">0:00</div>
                <button class="landscape-btn" id="landscapeBtn"></button>
            </div>
        </div>
        <div class="swipe-indicator-left" id="swipeIndicatorLeft" aria-hidden="true">
            <div class="swipe-icon" id="swipeIconLeft"></div>
            <div class="swipe-value" id="swipeValueLeft">0%</div>
        </div>
        <div class="swipe-indicator-right" id="swipeIndicatorRight" aria-hidden="true">
            <div class="swipe-icon" id="swipeIconRight"></div>
            <div class="swipe-value" id="swipeValueRight">0%</div>
        </div>
    </div>
</div>
${buildUiRestrictionScript()}
<script>
(function(){
    const mainWrap = document.getElementById('mainWrap');
    const card = document.getElementById('card');
    const video = document.getElementById('video');
    const source = video.querySelector('source');
    const insidePlay = document.getElementById('insidePlay');
    const insideForward = document.getElementById('insideForward');
    const insideBack = document.getElementById('insideBack');
    const backContainer = document.getElementById('backContainer');
    const playContainer = document.getElementById('playContainer');
    const forwardContainer = document.getElementById('forwardContainer');
    const spinnerContainer = document.getElementById('spinnerContainer');
    const seekBar = document.getElementById('seekBar');
    const fill = document.getElementById('fill');
    const thumb = document.getElementById('thumb');
    const currentEl = document.getElementById('current');
    const durationEl = document.getElementById('duration');
    const landscapeBtn = document.getElementById('landscapeBtn');
    const controlsWrap = document.getElementById('controlsWrap');
    const insideMini = document.getElementById('insideMini');
    const swipeIndicatorLeft = document.getElementById('swipeIndicatorLeft');
    const swipeIconLeft = document.getElementById('swipeIconLeft');
    const swipeValueLeft = document.getElementById('swipeValueLeft');
    const swipeIndicatorRight = document.getElementById('swipeIndicatorRight');
    const swipeIconRight = document.getElementById('swipeIconRight');
    const swipeValueRight = document.getElementById('swipeValueRight');
    const errorOverlay = document.getElementById('errorOverlay');
    const errTxt = document.getElementById('errTxt');

    const mediaUrl = video.getAttribute('data-src') || '';
    const mimeType = "${safeMime}";
    const downloadUrl = "${safeDownloadUrl}";

    const svgBrightness = '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" width="26" height="26"><path d="M12 4V2M12 22v-2M4.93 4.93L3.51 3.51M20.49 20.49l-1.42-1.42M4 12H2M22 12h-2M4.93 19.07l-1.42 1.42M20.49 3.51l-1.42 1.42M12 8a4 4 0 100 8 4 4 0 000-8z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
    const svgVolume = '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" width="26" height="26"><path d="M11 5L6 9H2v6h4l5 4V5z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M19 9a5 5 0 010 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
    const svgFullscreenEnter = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:24px;height:24px;"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"></path></svg>';
    const svgFullscreenExit = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:24px;height:24px;"><path d="M8 3v3a2 2 0 0 1-2 2H3m18 0h-3a2 2 0 0 1-2-2V3m0 18v-3a2 2 0 0 1 2-2h3M3 16h3a2 2 0 0 1 2 2v3"></path></svg>';
    const svgReplay = '<svg viewBox="0 0 24 24" fill="currentColor" style="width:34px;height:34px;"><path d="M12 5V1L7 6l5 5V7c3.31 0 6 2.69 6 6s-2.69 6-6 6-6-2.69-6-6H4c0 4.42 3.58 8 8 8s8-3.58 8-8-3.58-8-8-8z"/></svg>';

    landscapeBtn.innerHTML = svgFullscreenEnter;
    const cornerRatio = 0.25;
    let hideTimeout = null;
    let isCssFullscreen = false;
    let isEndedState = false;
    let mediaActivated = false;
    let controlsAreVisible = true;
    let currentBrightness = 1;
    let currentVolume = 1;

    function isIOSDevice() {
        return /iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
    }

    function formatTime(sec){
        sec = Math.floor(sec) || 0;
        const h = Math.floor(sec / 3600);
        const m = Math.floor((sec % 3600) / 60);
        const s = sec % 60;
        if(h > 0) return h + ":" + String(m).padStart(2,'0') + ":" + String(s).padStart(2,'0');
        return m + ":" + String(s).padStart(2,'0');
    }

    function isCustomFullscreenCard(){
        return document.fullscreenElement === card || document.webkitFullscreenElement === card || isCssFullscreen;
    }

    function isWideVideo() {
        return video.videoWidth > 0 && video.videoHeight > 0 && (video.videoWidth / video.videoHeight) > 1.2;
    }

    function unmuteVideo() {
        if (video.muted) video.muted = false;
    }

    function areControlsVisible() {
        return controlsAreVisible && controlsWrap.classList.contains('controls-visible') && insideMini.classList.contains('inside-visible');
    }

    function setControlsInteraction(enabled) {
        controlsAreVisible = enabled;
        insidePlay.disabled = !enabled;
        insideForward.disabled = !enabled;
        insideBack.disabled = !enabled;
        landscapeBtn.disabled = !enabled;
        seekBar.tabIndex = enabled ? 0 : -1;
        seekBar.setAttribute('aria-disabled', enabled ? 'false' : 'true');
        controlsWrap.style.pointerEvents = enabled ? 'auto' : 'none';
        insideMini.style.pointerEvents = enabled ? 'auto' : 'none';
    }

    function updateUIForTime(time) {
        currentEl.textContent = formatTime(time);
        const dur = video.duration || 1;
        let pct = (time / dur) * 100;
        pct = Math.max(0, Math.min(100, pct));
        fill.style.width = pct + '%';
        thumb.style.left = pct + '%';
        seekBar.setAttribute('aria-valuenow', Math.floor(time || 0));
    }

    function clearEndedState() {
        if (isEndedState) {
            isEndedState = false;
            forwardContainer.style.visibility = 'visible';
            updatePlayIcon();
            resetHideTimer();
        }
    }

    function updatePlayIcon(){
        if (isEndedState) return;
        insidePlay.textContent = video.paused ? '▶︎' : '❚❚';
    }

    async function togglePlay(){
        if (!areControlsVisible()) return;
        unmuteVideo();
        if (isEndedState) {
            clearEndedState();
            video.currentTime = 0;
            try { await video.play(); } catch(e) {}
            return;
        }
        try {
            if(video.paused){
                video.muted = false;
                await video.play();
            } else {
                video.pause();
            }
        } catch(e) {}
        updatePlayIcon();
    }

    insidePlay.addEventListener('click', function(e){
        e.stopPropagation();
        if (!areControlsVisible()) return;
        togglePlay();
        resetHideTimer();
    });

    insideForward.addEventListener('click', function(e){
        e.stopPropagation();
        if (!areControlsVisible()) return;
        unmuteVideo();
        const targetTime = Math.min(video.duration || 0, video.currentTime + 10);
        video.currentTime = targetTime;
        updateUIForTime(targetTime);
        resetHideTimer();
    });

    insideBack.addEventListener('click', function(e){
        e.stopPropagation();
        if (!areControlsVisible()) return;
        unmuteVideo();
        clearEndedState();
        const targetTime = Math.max(0, video.currentTime - 10);
        video.currentTime = targetTime;
        updateUIForTime(targetTime);
        resetHideTimer();
    });

    let scrubbing = false;
    let wasPlayingBeforeScrub = false;

    function timeFromClientX(clientX){
        const r = seekBar.getBoundingClientRect();
        let p = (clientX - r.left) / r.width;
        p = Math.max(0, Math.min(1, p));
        return (video.duration || 0) * p;
    }

    function startScrub(clientX){
        if (!areControlsVisible()) return;
        unmuteVideo();
        clearEndedState();
        wasPlayingBeforeScrub = !video.paused;
        try { video.pause(); } catch(e) {}
        scrubbing = true;
        const t = timeFromClientX(clientX);
        video.currentTime = t;
        updateUIForTime(t);
        resetHideTimer();
    }

    function moveScrub(clientX){
        if (!scrubbing) return;
        if (!areControlsVisible()) return endScrub();
        const t = timeFromClientX(clientX);
        video.currentTime = t;
        updateUIForTime(t);
        resetHideTimer();
    }

    function endScrub(){
        if (!scrubbing) return;
        scrubbing = false;
        if (wasPlayingBeforeScrub && areControlsVisible()) {
            try { video.play().catch(function(){}); } catch(e) {}
        }
        resetHideTimer();
    }

    seekBar.addEventListener('mousedown', function(e){
        if (!areControlsVisible()) return;
        e.preventDefault();
        e.stopPropagation();
        startScrub(e.clientX);
    });

    seekBar.addEventListener('touchstart', function(e){
        if (!areControlsVisible()) return;
        e.preventDefault();
        e.stopPropagation();
        if (e.touches && e.touches[0]) startScrub(e.touches[0].clientX);
    }, {passive:false});

    thumb.addEventListener('pointerdown', function(e){
        if (!areControlsVisible()) return;
        e.preventDefault();
        e.stopPropagation();
        startScrub(e.clientX);
    });

    window.addEventListener('mousemove', function(e){ moveScrub(e.clientX); });
    window.addEventListener('mouseup', endScrub);
    window.addEventListener('touchmove', function(e){
        if (scrubbing && e.touches && e.touches[0]) {
            if (e.cancelable) e.preventDefault();
            moveScrub(e.touches[0].clientX);
        }
    }, {passive:false});
    window.addEventListener('touchend', endScrub);
    window.addEventListener('pointermove', function(e){
        if (scrubbing && e.pointerType) moveScrub(e.clientX);
    });
    window.addEventListener('pointerup', endScrub);

    video.addEventListener('timeupdate', function(){
        if (!scrubbing) updateUIForTime(video.currentTime);
    });

    video.addEventListener('loadedmetadata', function(){
        durationEl.textContent = formatTime(video.duration || 0);
        seekBar.setAttribute('aria-valuemax', Math.floor(video.duration || 0));
        currentVolume = typeof video.volume === 'number' ? video.volume : 1;
        hideBuffering();
    });

    video.addEventListener('loadeddata', hideBuffering);
    video.addEventListener('canplay', hideBuffering);
    video.addEventListener('play', function(){
        clearEndedState();
        updatePlayIcon();
    });
    video.addEventListener('pause', function(){
        updatePlayIcon();
        hideBuffering();
    });
    video.addEventListener('ended', function(){
        isEndedState = true;
        insidePlay.innerHTML = svgReplay;
        forwardContainer.style.visibility = 'hidden';
        showAll();
        if (hideTimeout) clearTimeout(hideTimeout);
    });

    function showAll(){
        controlsWrap.classList.remove('controls-hidden');
        controlsWrap.classList.add('controls-visible');
        insideMini.classList.remove('inside-hidden');
        insideMini.classList.add('inside-visible');
        setControlsInteraction(true);
        resetHideTimer();
    }

    function hideAll(){
        controlsWrap.classList.remove('controls-visible');
        controlsWrap.classList.add('controls-hidden');
        insideMini.classList.remove('inside-visible');
        insideMini.classList.add('inside-hidden');
        setControlsInteraction(false);
        if (hideTimeout) clearTimeout(hideTimeout);
    }

    function toggleAll(){
        if (areControlsVisible()) hideAll();
        else showAll();
    }

    function resetHideTimer() {
        if (hideTimeout) clearTimeout(hideTimeout);
        if (isEndedState || !areControlsVisible()) return;
        hideTimeout = setTimeout(hideAll, 4000);
    }

    function isOverControls(target){
        return !!(target && target.closest && (target.closest('.controls-wrap') || target.closest('.inside-mini-controls') || target.closest('.inside-item') || target.closest('.seek')));
    }

    card.addEventListener('click', function(e){
        if (isOverControls(e.target)) return;
        hideSwipeIndicatorsDirectly();
        toggleAll();
    });

    window.addEventListener('mousemove', function(){
        if (areControlsVisible()) resetHideTimer();
    });

    [insidePlay, insideForward, insideBack, seekBar, landscapeBtn].forEach(function(el){
        if (el) el.addEventListener('click', function(e){ e.stopPropagation(); });
    });

    function enterLandscapeMode(){
        if (!areControlsVisible()) return;
        unmuteVideo();
        const wasPlaying = !video.paused;
        if (isIOSDevice() && video.webkitEnterFullscreen) {
            video.webkitEnterFullscreen();
            if (wasPlaying) video.play().catch(function(){});
            return;
        }
        const fsPromise = card.requestFullscreen ? card.requestFullscreen() : (card.webkitRequestFullscreen ? card.webkitRequestFullscreen() : Promise.reject(new Error("No FS")));
        fsPromise.then(function(){
            if (isWideVideo() && screen.orientation && screen.orientation.lock) {
                screen.orientation.lock('landscape').catch(function(){});
            }
            card.classList.add('landscape-mode');
            if (wasPlaying) video.play().catch(function(){});
            showAll();
        }).catch(function(){
            if (video.webkitEnterFullscreen) {
                video.webkitEnterFullscreen();
            } else {
                mainWrap.classList.add('css-fullscreen');
                isCssFullscreen = true;
                landscapeBtn.innerHTML = svgFullscreenExit;
            }
            card.classList.add('landscape-mode');
            if (wasPlaying) video.play().catch(function(){});
            showAll();
        });
    }

    function exitLandscapeMode(){
        if (!areControlsVisible()) return;
        const wasPlaying = !video.paused;
        if (isCssFullscreen) {
            mainWrap.classList.remove('css-fullscreen');
            isCssFullscreen = false;
            landscapeBtn.innerHTML = svgFullscreenEnter;
        } else {
            try {
                if (screen.orientation && screen.orientation.unlock) {
                    try { screen.orientation.unlock(); } catch(_) {}
                }
                if (document.exitFullscreen) {
                    document.exitFullscreen().catch(function(){});
                } else if (document.webkitExitFullscreen) {
                    document.webkitExitFullscreen();
                }
            } catch(err) {}
        }
        card.classList.remove('landscape-mode');
        if (wasPlaying) video.play().catch(function(){});
        showAll();
    }

    landscapeBtn.addEventListener('click', function(e){
        e.stopPropagation();
        if (!areControlsVisible()) return;
        if (!isCustomFullscreenCard()) enterLandscapeMode();
        else exitLandscapeMode();
    });

    document.addEventListener('fullscreenchange', function(){
        if (document.fullscreenElement === card) {
            card.classList.add('landscape-mode');
            landscapeBtn.innerHTML = svgFullscreenExit;
            showAll();
        } else {
            card.classList.remove('landscape-mode');
            landscapeBtn.innerHTML = svgFullscreenEnter;
            isCssFullscreen = false;
            mainWrap.classList.remove('css-fullscreen');
            showAll();
        }
    });

    let gesture = null;
    let indicatorTimeoutLeft = null;
    let indicatorTimeoutRight = null;
    let startTouchX = 0;
    let startTouchY = 0;
    let isVerticalSwipe = false;
    let swipeDirectionDetermined = false;
    let gestureTouchIgnored = false;

    function showSwipeIndicator(kind, percent){
        if (kind === 'brightness') {
            swipeIconLeft.innerHTML = svgBrightness;
            swipeValueLeft.textContent = percent + '%';
            swipeIndicatorLeft.classList.add('show');
            if (indicatorTimeoutLeft) clearTimeout(indicatorTimeoutLeft);
            indicatorTimeoutLeft = setTimeout(function(){ swipeIndicatorLeft.classList.remove('show'); }, 800);
        } else {
            swipeIconRight.innerHTML = svgVolume;
            swipeValueRight.textContent = percent + '%';
            swipeIndicatorRight.classList.add('show');
            if (indicatorTimeoutRight) clearTimeout(indicatorTimeoutRight);
            indicatorTimeoutRight = setTimeout(function(){ swipeIndicatorRight.classList.remove('show'); }, 800);
        }
    }

    function hideSwipeIndicatorsDirectly(){
        swipeIndicatorLeft.classList.remove('show');
        swipeIndicatorRight.classList.remove('show');
        if (indicatorTimeoutLeft) clearTimeout(indicatorTimeoutLeft);
        if (indicatorTimeoutRight) clearTimeout(indicatorTimeoutRight);
    }

    function clamp(v, a = 0, b = 1) { return Math.max(a, Math.min(b, v)); }

    function startLandscapeGesture(clientX, clientY){
        if (!areControlsVisible() || !isCustomFullscreenCard()) {
            gesture = null;
            return;
        }
        const rect = card.getBoundingClientRect();
        const relX = clientX - rect.left;
        const limit = rect.width * cornerRatio;

        if (relX <= limit) {
            gesture = { type:'brightness', startY:clientY, initialValue:currentBrightness };
            showSwipeIndicator('brightness', Math.round(gesture.initialValue * 100));
        } else if (relX >= rect.width - limit) {
            currentVolume = typeof video.volume === 'number' ? video.volume : 1;
            gesture = { type:'volume', startY:clientY, initialValue:currentVolume };
            showSwipeIndicator('volume', Math.round(gesture.initialValue * 100));
        } else {
            gesture = null;
        }
    }

    function moveLandscapeGesture(clientY){
        if (!gesture) return;
        if (!areControlsVisible() || !isCustomFullscreenCard()) {
            gesture = null;
            hideSwipeIndicatorsDirectly();
            return;
        }
        const rect = card.getBoundingClientRect();
        const delta = gesture.startY - clientY;
        const pct = delta / (rect.height * 0.7);
        let newVal = clamp(gesture.initialValue + pct, 0, 1);

        if (gesture.type === 'brightness') {
            currentBrightness = Math.max(0.05, newVal);
            video.style.filter = 'brightness(' + currentBrightness + ')';
            showSwipeIndicator('brightness', Math.round(currentBrightness * 100));
        } else {
            currentVolume = newVal;
            video.volume = currentVolume;
            showSwipeIndicator('volume', Math.round(currentVolume * 100));
        }
    }

    function endLandscapeGesture(){
        if (!gesture) return;
        gesture = null;
    }

    card.addEventListener('touchstart', function(e){
        if (!areControlsVisible() || !isCustomFullscreenCard() || isOverControls(e.target) || (e.touches && e.touches.length !== 1)) {
            gesture = null;
            gestureTouchIgnored = true;
            return;
        }
        gestureTouchIgnored = false;
        const t = e.touches[0];
        startTouchX = t.clientX;
        startTouchY = t.clientY;
        isVerticalSwipe = false;
        swipeDirectionDetermined = false;
    }, {passive:true});

    card.addEventListener('touchmove', function(e){
        if (gestureTouchIgnored || !areControlsVisible() || !isCustomFullscreenCard() || !e.touches.length) return;
        const t = e.touches[0];
        const diffX = t.clientX - startTouchX;
        const diffY = t.clientY - startTouchY;

        if (!swipeDirectionDetermined) {
            if (Math.abs(diffY) > 15 || Math.abs(diffX) > 15) {
                swipeDirectionDetermined = true;
                if (Math.abs(diffY) > Math.abs(diffX) * 1.5) {
                    isVerticalSwipe = true;
                    startLandscapeGesture(startTouchX, startTouchY);
                } else {
                    isVerticalSwipe = false;
                    gesture = null;
                }
            }
        }
        if (isVerticalSwipe && gesture) {
            if (e.cancelable) e.preventDefault();
            moveLandscapeGesture(t.clientY);
        }
    }, {passive:false});

    card.addEventListener('touchend', function(){ endLandscapeGesture(); gestureTouchIgnored = false; }, {passive:true});
    card.addEventListener('touchcancel', function(){ endLandscapeGesture(); gestureTouchIgnored = false; }, {passive:true});

    function showBuffering(){
        if (backContainer) backContainer.style.visibility = 'hidden';
        if (playContainer) playContainer.style.visibility = 'hidden';
        if (forwardContainer) forwardContainer.style.visibility = 'hidden';
        spinnerContainer.style.display = 'flex';
    }

    function hideBuffering(){
        if (backContainer) backContainer.style.visibility = 'visible';
        if (playContainer) playContainer.style.visibility = 'visible';
        if (forwardContainer) forwardContainer.style.visibility = isEndedState ? 'hidden' : 'visible';
        spinnerContainer.style.display = 'none';
    }

    video.addEventListener('loadstart', showBuffering);
    video.addEventListener('waiting', showBuffering);
    video.addEventListener('seeking', showBuffering);
    video.addEventListener('stalled', function(){ if (video.readyState < 3) showBuffering(); });
    video.addEventListener('playing', hideBuffering);
    video.addEventListener('seeked', hideBuffering);
    video.addEventListener('canplay', hideBuffering);
    video.addEventListener('canplaythrough', hideBuffering);
    video.addEventListener('pause', hideBuffering);
    if (video.readyState >= 3) hideBuffering();

    function showUnsupportedDownload(){
        if (mediaActivated) return;
        mediaActivated = true;
        hideBuffering();
        window.location.replace(downloadUrl);
    }

    function showError(msg){
        if (errTxt) errTxt.textContent = msg;
        if (errorOverlay) errorOverlay.style.display = 'flex';
        hideBuffering();
        hideAll();
    }

    video.addEventListener('error', function(){
        const mediaError = video.error;
        if (mediaError && (mediaError.code === 3 || mediaError.code === 4)) return showUnsupportedDownload();
        if (mediaError && mediaError.code === 2) return showError('Erè koneksyon (oubyen rezo a pa la).');
        showUnsupportedDownload();
    });

    function activateVideo(){
        if (mediaActivated) return;
        let canPlay = '';
        try { canPlay = video.canPlayType(mimeType); } catch(e) {}
        if (!canPlay) return showUnsupportedDownload();

        mediaActivated = true;
        if (source) source.src = source.getAttribute('data-src') || mediaUrl;
        video.src = mediaUrl;
        video.load();
        tryAutoplay();
    }

    async function tryAutoplay(){
        try {
            video.muted = false;
            await video.play();
        } catch(e) {
            try {
                video.muted = true;
                await video.play();
            } catch(_) {}
        }
        updatePlayIcon();
    }

    document.addEventListener('click', unmuteVideo);
    document.addEventListener('touchstart', unmuteVideo, {passive:true});
    showAll();
    window.addEventListener('load', function(){ setTimeout(activateVideo, 0); }, {once:true});
    window.__player = { video:video };
})();
</script>
</body>
</html>`;
}

function sendUnknown(req, res) {
    if (req.headers.accept && req.headers.accept.includes('text/html')) {
        res.status(404).send('<!doctype html><html lang="ht"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no"><title>Paj sa pa ekziste</title></head><body style="margin:0;background:#000;color:#fff;text-align:center;padding:50px;font-family:sans-serif;overflow:hidden;user-select:none;-webkit-user-select:none;"><h1>Paj sa pa ekziste</h1><script>setTimeout(function(){ window.close(); window.history.back(); }, 1500);</script></body></html>');
    } else {
        res.status(404).send('Paj sa pa ekziste');
    }
}

async function getRemoteObjectMeta(token) {
    const cleanToken = String(token || '').trim();
    if (!cleanToken) return null;
    console.log(`[R2] HEAD_START token=${cleanToken}`);
    try {
        const head = await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: cleanToken }));
        const result = {
            exists: true,
            contentType: head.ContentType || null,
            contentLength: head.ContentLength || null,
            metadata: head.Metadata || {},
            lastModified: head.LastModified ? new Date(head.LastModified).toISOString() : null
        };
        console.log(`[R2] HEAD_OK token=${cleanToken} size=${result.contentLength || 0} contentType=${JSON.stringify(result.contentType)} metadataToken=${JSON.stringify(result.metadata.token || '')}`);
        return result;
    } catch (err) {
        const code = String(err && (err.name || err.Code || err.code) || '');
        console.error(`[R2] HEAD_ERROR token=${cleanToken} code=${code} error=${err.message || err}`);
        return null;
    }
}

function safeDecodeURIComponent(value) {
    try {
        return decodeURIComponent(value);
    } catch (err) {
        return value;
    }
}

async function ensureMappingFromR2(token) {
    const cleanToken = String(token || '').trim();
    if (!cleanToken) return null;

    const meta = await getRemoteObjectMeta(cleanToken);
    if (!meta || !meta.exists) return null;

    const metadata = meta.metadata || {};
    const metadataToken = String(metadata.token || '').trim();
    const originalNameEncoded = String(metadata.originalname || '').trim();
    const safeOriginalValue = String(metadata.safeoriginal || '').trim();

    if (!metadataToken || metadataToken !== cleanToken || !originalNameEncoded || !safeOriginalValue) {
        console.error(`[R2] METADATA_INVALID token=${cleanToken}`);
        return null;
    }

    const originalName = safeDecodeURIComponent(originalNameEncoded);
    const safeOriginal = safeFileName(safeOriginalValue);

    return {
        token: metadataToken,
        originalName: originalName || safeOriginal,
        safeOriginal: safeOriginal || 'file',
        size: Number(meta.contentLength || 0),
        mime: meta.contentType || contentTypeFromName(safeOriginal),
        createdAt: meta.lastModified || null
    };
}

function buildRemoteUrl(remotePath) {
    return `${R2_BASE_URL}/${encodeURIComponent(String(remotePath))}`;
}

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.options('*', cors());
app.use((req, res, next) => {
    if (req.path.startsWith('/upload')) {
        console.log(`[HTTP] REQUEST method=${req.method} path=${req.path} origin=${JSON.stringify(req.get('origin') || '')} contentType=${JSON.stringify(req.get('content-type') || '')}`);
        res.on('finish', () => {
            console.log(`[HTTP] RESPONSE method=${req.method} path=${req.path} status=${res.statusCode}`);
        });
    }
    next();
});

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range');
    res.header('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length, Content-Type, Content-Disposition');
    next();
});

app.use(compression({
    filter: (req, res) => {
        try {
            if (req && req.path && req.path.startsWith('/TF-')) return false;
        } catch(e) {}
        return compression.filter(req, res);
    }
}));

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), { index: 'index.html' }));

async function generateUniqueUploadToken() {
    for (let attempt = 0; attempt < 20; attempt += 1) {
        const token = genToken();
        const exists = await getRemoteObjectMeta(token);
        if (!exists) return token;
    }
    throw Object.assign(new Error('Unable to generate a unique upload token'), { status: 503 });
}

async function handleUploadPresign(req, res) {
    try {
        const body = req.body && typeof req.body === 'object' ? req.body : {};
        const originalName = String(body.filename || body.originalname || req.get('x-filename') || '').trim();
        const size = Number(body.size || req.get('x-file-size') || 0);
        const contentType = String(body.contentType || req.get('x-content-type') || '').trim() || contentTypeFromName(originalName);

        console.log(`[UPLOAD] PRESIGN_REQUEST filename=${JSON.stringify(originalName)} size=${size} contentType=${JSON.stringify(contentType)}`);

        if (!originalName) return res.status(400).json({ error: 'filename is required' });
        if (!Number.isSafeInteger(size) || size <= 0) return res.status(400).json({ error: 'valid file size is required' });
        if (size > MAX_FILE_SIZE) return res.status(413).json({ error: 'file too large', maximumBytes: MAX_FILE_SIZE });

        const current = await syncStatusFromR2();
        console.log(`[UPLOAD] CAPACITY_CHECK usedBytes=${current.totalSize} requestedBytes=${size} limitBytes=${LIMIT_R2_BYTES}`);

        if (current.totalSize + size > LIMIT_R2_BYTES) {
            console.log(`[UPLOAD] LIMIT_EXCEEDED usedBytes=${current.totalSize} requestedBytes=${size} projectedBytes=${current.totalSize + size} limitBytes=${LIMIT_R2_BYTES}`);
            await wipeEntireR2Bucket('capacity');
            console.log(`[UPLOAD] CAPACITY_RESET_DONE requestedBytes=${size}`);
        } else {
            console.log(`[UPLOAD] CAPACITY_AVAILABLE projectedBytes=${current.totalSize + size} limitBytes=${LIMIT_R2_BYTES}`);
        }

        const token = await generateUniqueUploadToken();
        const safeOriginal = safeFileName(originalName);
        const expiresAt = new Date(Date.now() + PENDING_UPLOAD_TTL_MS);
        const metadata = {
            originalname: encodeURIComponent(originalName),
            safeoriginal: safeOriginal,
            token
        };

        console.log(`[UPLOAD] TOKEN_CREATED token=${token} safeoriginal=${JSON.stringify(safeOriginal)}`);

        await PendingUploadModel.create({
            _id: token,
            expiresAt
        });
        console.log(`[UPLOAD] PENDING_CREATED token=${token} expiresAt=${expiresAt.toISOString()}`);

        try {
            const command = new PutObjectCommand({
                Bucket: R2_BUCKET,
                Key: token,
                ContentType: contentType,
                Metadata: metadata
            });

            console.log(`[UPLOAD] PRESIGN_SIGN_START token=${token}`);
            const uploadUrl = await getSignedUrl(s3, command, {
                expiresIn: UPLOAD_URL_TTL_SECONDS,
                signableHeaders: new Set(['content-type']),
                unhoistableHeaders: new Set([
                    'x-amz-meta-originalname',
                    'x-amz-meta-safeoriginal',
                    'x-amz-meta-token'
                ])
            });
            const signedHeaders = (() => {
                try {
                    return new URL(uploadUrl).searchParams.get('X-Amz-SignedHeaders') || '';
                } catch (error) {
                    return '';
                }
            })();
            console.log(`[UPLOAD] PRESIGNED_SIGNED_HEADERS token=${token} headers=${JSON.stringify(signedHeaders)}`);
            const origin = (process.env.BASE_URL || 'https://bref.adamdh7.org').replace(/\/+$/, '');
            const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;

            console.log(`[UPLOAD] PRESIGNED_URL_CREATED token=${token} ttlSeconds=${UPLOAD_URL_TTL_SECONDS}`);

            return res.json({
                token,
                method: 'PUT',
                uploadUrl,
                expiresIn: UPLOAD_URL_TTL_SECONDS,
                headers: {
                    'Content-Type': contentType,
                    'x-amz-meta-originalname': metadata.originalname,
                    'x-amz-meta-safeoriginal': metadata.safeoriginal,
                    'x-amz-meta-token': metadata.token
                },
                url: `${origin}${sharePath}`,
                sharePath
            });
        } catch (error) {
            try {
                await PendingUploadModel.deleteOne({ _id: token });
                console.log(`[UPLOAD] PRESIGN_PENDING_ROLLBACK token=${token} result=ok`);
            } catch (rollbackError) {
                console.error(`[UPLOAD] PRESIGN_PENDING_ROLLBACK token=${token} result=error error=${rollbackError.message || rollbackError}`);
            }
            console.error(`[UPLOAD] PRESIGN_ERROR token=${token} error=${error.message || error}`);
            throw error;
        }
    } catch (error) {
        const status = Number(error.status || 500);
        console.error(`[UPLOAD] PRESIGN_FAILED status=${status} error=${error.message || error}`);
        return res.status(status).json({ error: error.message || 'upload presign failed' });
    }
}

app.post('/upload/presign', handleUploadPresign);
app.post('/upload', handleUploadPresign);

app.post('/upload/confirm', async (req, res) => {
    try {
        const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
        const bodyKeys = Object.keys(body);
        const token = String(body.token || body.tfid || req.query.token || '').trim();

        console.log(`[UPLOAD] CONFIRM_REQUEST token=${token || 'missing'} keys=${bodyKeys.join(',') || 'none'}`);

        if (!token) return res.status(400).json({ error: 'token is required' });
        if (bodyKeys.length && (bodyKeys.length !== 1 || !['token', 'tfid'].includes(bodyKeys[0]))) return res.status(400).json({ error: 'only token is allowed' });

        const pending = await PendingUploadModel.findById(token).lean();
        if (!pending) {
            console.log(`[UPLOAD] CONFIRM_REJECTED token=${token} reason=not_pending`);
            return res.status(409).json({ error: 'upload is not pending' });
        }

        const meta = await getRemoteObjectMeta(token);
        console.log(`[UPLOAD] CONFIRM_R2_HEAD token=${token} exists=${Boolean(meta && meta.exists)}`);

        if (!meta || !meta.exists) {
            await PendingUploadModel.deleteOne({ _id: token });
            console.log(`[UPLOAD] CONFIRM_REJECTED token=${token} reason=object_missing`);
            return res.status(404).json({ error: 'uploaded file not found in R2' });
        }

        const metadata = meta.metadata || {};
        if (String(metadata.token || '').trim() !== token) {
            try {
                await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: token }));
                console.log(`[UPLOAD] INVALID_METADATA_R2_DELETE token=${token} result=ok`);
            } catch (deleteError) {
                console.error(`[UPLOAD] INVALID_METADATA_R2_DELETE token=${token} result=error error=${deleteError.message || deleteError}`);
            }
            const removed = await PendingUploadModel.deleteOne({ _id: token });
            console.log(`[UPLOAD] INVALID_METADATA_PENDING_DELETE token=${token} deleted=${removed.deletedCount || 0}`);
            console.log(`[UPLOAD] CONFIRM_REJECTED token=${token} reason=metadata_token_mismatch`);
            return res.status(400).json({ error: 'invalid R2 metadata' });
        }

        if (!metadata.originalname || !metadata.safeoriginal) {
            try {
                await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: token }));
                console.log(`[UPLOAD] INCOMPLETE_METADATA_R2_DELETE token=${token} result=ok`);
            } catch (deleteError) {
                console.error(`[UPLOAD] INCOMPLETE_METADATA_R2_DELETE token=${token} result=error error=${deleteError.message || deleteError}`);
            }
            const removed = await PendingUploadModel.deleteOne({ _id: token });
            console.log(`[UPLOAD] INCOMPLETE_METADATA_PENDING_DELETE token=${token} deleted=${removed.deletedCount || 0}`);
            console.log(`[UPLOAD] CONFIRM_REJECTED token=${token} reason=metadata_incomplete`);
            return res.status(400).json({ error: 'invalid R2 metadata' });
        }

        const size = Number(meta.contentLength || 0);
        if (!Number.isSafeInteger(size) || size <= 0 || size > MAX_FILE_SIZE) {
            try {
                await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: token }));
                console.log(`[UPLOAD] INVALID_SIZE_R2_DELETE token=${token} result=ok`);
            } catch (deleteError) {
                console.error(`[UPLOAD] INVALID_SIZE_R2_DELETE token=${token} result=error error=${deleteError.message || deleteError}`);
            }
            const removed = await PendingUploadModel.deleteOne({ _id: token });
            console.log(`[UPLOAD] INVALID_SIZE_PENDING_DELETE token=${token} deleted=${removed.deletedCount || 0}`);
            console.log(`[UPLOAD] CONFIRM_REJECTED token=${token} reason=invalid_size size=${size}`);
            return res.status(400).json({ error: 'invalid uploaded file size' });
        }

        const status = await syncStatusFromR2();
        const pendingDelete = await PendingUploadModel.deleteOne({ _id: token });
        console.log(`[UPLOAD] PENDING_CONFIRMED_REMOVED token=${token} deleted=${pendingDelete.deletedCount || 0}`);

        const originalName = safeDecodeURIComponent(String(metadata.originalname));
        const safeOriginal = safeFileName(String(metadata.safeoriginal));
        const origin = (process.env.BASE_URL || 'https://bref.adamdh7.org').replace(/\/+$/, '');
        const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;

        console.log(`[UPLOAD] CONFIRM_OK token=${token} size=${size} usedBytes=${status.totalSize} originalname=${JSON.stringify(originalName)}`);

        return res.json({
            success: true,
            token,
            size,
            originalname: originalName,
            safeoriginal: safeOriginal,
            url: `${origin}${sharePath}`,
            sharePath,
            totalSize: status.totalSize,
            limitBytes: LIMIT_R2_BYTES
        });
    } catch (error) {
        console.error(`[UPLOAD] CONFIRM_FAILED error=${error.message || error}`);
        return res.status(Number(error.status || 500)).json({ error: error.message || 'upload confirmation failed' });
    }
});

app.get(['/TF-:token/download', '/TF-:token/download/:name'], async (req, res) => {
    try {
        let token = req.params.token;
        if (token) token = token.replace(/\/$/, '');

        const requestedName = req.params.name ? safeDecodeURIComponent(req.params.name) : null;
        const entry = await ensureMappingFromR2(token);
        if (!entry) return sendUnknown(req, res);

        const realName = entry.safeOriginal || entry.originalName || 'file';
        const filename = requestedName || realName;
        const isValidName = !requestedName || requestedName === entry.safeOriginal || requestedName === entry.originalName;

        if (!isValidName) return res.status(404).send('Fichye pa ekziste');

        const downloadUrl = await buildPresignedDownloadUrl(token, filename, entry.mime || contentTypeFromName(filename));
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        return res.redirect(302, downloadUrl);
    } catch(err) {
        return sendUnknown(req, res);
    }
});

app.get(['/TF-:token', '/TF-:token/', '/TF-:token/:name'], async (req, res) => {
    try {
        let token = req.params.token;
        if (token) token = token.replace(/\/$/, '');

        const requestedName = req.params.name || null;
        const entry = await ensureMappingFromR2(token);
        if (!entry) return sendUnknown(req, res);

        const realName = entry.safeOriginal || entry.originalName || 'file';
        const filename = requestedName || realName;
        const isVideo = isVideoFile(filename);
        const isImage = isImageFile(filename);
        const isValidName = requestedName && (requestedName === entry.safeOriginal || requestedName === entry.originalName);
        const origin = (process.env.BASE_URL || 'https://bref.adamdh7.org').replace(/\/+$/, '');
        const directR2Url = buildRemoteUrl(token);
        const downloadUrl = `${origin}/TF-${token}/download/${encodeURIComponent(realName)}`;

        if (!requestedName) {
            if (isVideo) {
                const fullUrl = `${origin}/TF-${token}`;
                return res.status(200).type('html').send(buildCustomPlayerHtml(realName, directR2Url, fullUrl, entry.mime || contentTypeFromName(filename), downloadUrl));
            }
            return sendUnknown(req, res);
        }

        if (!isValidName) {
            if (isEmbeddedMediaRequest(req)) return res.status(404).send('Fichye pa ekziste');
            return res.status(200).type('html').send(buildForceDownloadHtml(realName, downloadUrl));
        }

        if (isEmbeddedMediaRequest(req)) {
            if (isVideo || isImage) {
                const mediaUrl = await buildPresignedMediaUrl(token, realName, entry.mime || contentTypeFromName(realName));
                return res.redirect(302, mediaUrl);
            }
            const mediaDownloadUrl = await buildPresignedDownloadUrl(token, realName, entry.mime || contentTypeFromName(realName));
            return res.redirect(302, mediaDownloadUrl);
        }

        if (isVideo || isImage) {
            return res.status(200).type('html').send(buildLightweightViewerHtml(realName, directR2Url, downloadUrl, isVideo, entry.mime || contentTypeFromName(filename)));
        }
        return res.status(200).type('html').send(buildForceDownloadHtml(realName, downloadUrl));
    } catch(err) {
        return sendUnknown(req, res);
    }
});

app.get('/_admin/mappings', async (req, res) => {
    try {
        const objects = await listR2Objects();
        const tokens = [];

        for (const object of objects.slice(0, 50)) {
            try {
                const meta = await getRemoteObjectMeta(object.key);
                const token = meta && meta.metadata ? String(meta.metadata.token || '').trim() : '';
                if (token) tokens.push(token);
            } catch (error) {}
        }

        const status = await syncStatusFromR2();
        const pendingCount = await PendingUploadModel.countDocuments();
        return res.json({ count: objects.length, tokens, totalSize: status.totalSize, limitBytes: LIMIT_R2_BYTES, pendingCount });
    } catch(e) {
        return res.status(500).json({ error: 'Server Error' });
    }
});

app.get('/sitemap.xml', (req, res) => {
    res.sendFile(path.join(__dirname, 'sitemap.xml'));
});

app.get('/poste.json', (req, res) => {
    try {
        const filePath = path.join(__dirname, 'poste.json');
        if (!fs.existsSync(filePath)) return res.json([]);
        const fileContent = fs.readFileSync(filePath, 'utf8');
        const jsonData = JSON.parse(fileContent);

        if (!Array.isArray(jsonData)) return res.json(jsonData);
        const shuffled = [...jsonData].sort(() => 0.5 - Math.random());
        const randomCount = Math.floor(Math.random() * 2) + 3;
        res.json(shuffled.slice(0, randomCount));
    } catch(err) {
        res.status(500).json({ error: 'Server Error' });
    }
});

app.get('/health', (req, res) => {
    res.json({ ok: true });
});

app.get('*', (req, res, next) => {
    if (req.path.startsWith('/TF-') || req.path.startsWith('/upload') || req.path.startsWith('/_admin') || req.path.startsWith('/health')) {
        return next();
    }
    const indexPath = path.join(__dirname, 'public', 'index.html');
    if (fs.existsSync(indexPath)) {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        return res.sendFile(indexPath);
    }
    return res.status(404).send('Paj sa pa ekziste');
});

app.use((err, req, res, next) => {
    if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'Fichye a twò gwo. Max direct PUT: 4.995Go' });
    if (err) return res.status(500).json({ error: 'Erè nan sève a' });
    next();
});

setInterval(async () => {
    try {
        await cleanupExpiredPendingUploads();
    } catch (error) {
        console.error(`[UPLOAD] PENDING_CLEANUP_ERROR error=${error.message || error}`);
    }
}, PENDING_CLEANUP_INTERVAL_MS);

setInterval(async () => {
    try {
        await syncStatusFromR2();
    } catch (error) {
        console.error(`[R2] STATUS_SYNC_ERROR error=${error.message || error}`);
    }
}, R2_STATUS_SYNC_INTERVAL_MS);

setInterval(async () => {
    try {
        await cleanupOldR2Objects();
    } catch (error) {
        console.error(`[R2] OLD_OBJECT_CLEANUP_ERROR error=${error.message || error}`);
    }
}, 3600000);

app.listen(PORT, () => {
    console.log(`[SERVER] LISTENING port=${PORT}`);
});
