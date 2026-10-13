'use strict';

/**
 * LAN Share - zero dependency LAN file sharing server.
 *
 * Two transfer paths:
 *   1. Peer-to-peer: WebRTC DataChannel between browsers. This server only
 *      relays signalling (offer / answer / ICE) and never sees file bytes.
 *   2. Server vault: plain HTTP upload / download. Files land on disk here.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT_DIR = __dirname;
const PUBLIC_DIR = path.join(ROOT_DIR, 'public');
const UPLOAD_DIR = path.resolve(process.env.UPLOAD_DIR || path.join(ROOT_DIR, 'uploads'));
/* 断点续传的半成品放在这里。名字是 name|size 的哈希，避开中文/特殊字符的编码坑，
 * 同时保证同名但不同大小的文件互不干扰。 */
const PARTS_DIR = path.join(UPLOAD_DIR, '.lanshare-parts');

/* 文件流的块大小。Node 的 fs 流默认 highWaterMark 只有 64KB，千兆局域网上
 * 意味着每秒几万次 read/write 系统调用，实测把下载压在 50MB/s 上下。提到
 * 1MB 后系统调用次数降一个数量级，内存代价每个流 1MB（可接受）。
 * 上传（写盘）和下载（读盘）用同一个值。 */
const STREAM_BLOCK = 1024 * 1024;

fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
  '.woff2': 'font/woff2',
};

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const OP_CONT = 0x0;
const OP_TEXT = 0x1;
const OP_BINARY = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;

// ---------------------------------------------------------------- utilities

function safeJoin(baseDir, target) {
  const resolved = path.resolve(baseDir, target);
  if (resolved !== baseDir && !resolved.startsWith(baseDir + path.sep)) return null;
  return resolved;
}

/** Strip any directory component so a crafted name cannot escape UPLOAD_DIR. */
function safeName(raw) {
  const base = path.basename(String(raw || '').replace(/\\/g, '/'));
  const cleaned = base.split('').filter(function (ch) { var c = ch.charCodeAt(0); return c > 31 && c !== 127; }).join('').trim();
  return cleaned.length ? cleaned : 'unnamed';
}

function uniqueTarget(name) {
  let candidate = path.join(UPLOAD_DIR, name);
  if (!fs.existsSync(candidate)) return candidate;
  const ext = path.extname(name);
  const stem = path.basename(name, ext);
  for (let i = 1; i < 999; i += 1) {
    candidate = path.join(UPLOAD_DIR, `${stem} (${i})${ext}`);
    if (!fs.existsSync(candidate)) return candidate;
  }
  return path.join(UPLOAD_DIR, `${Date.now()}-${name}`);
}

function humanSize(bytes) {
  const n = Number(bytes) || 0;
  if (n < 1024) return `${n} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = n / 1024;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return `${value.toFixed(value >= 100 || idx === 0 ? 0 : 1)} ${units[idx]}`;
}

function lanAddresses() {
  const out = [];
  for (const list of Object.values(os.networkInterfaces())) {
    for (const item of list || []) {
      if (item.family === 'IPv4' && !item.internal) out.push(item.address);
    }
  }
  return out;
}

function sendJson(res, status, payload) {
  const body = Buffer.from(JSON.stringify(payload), 'utf8');
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': body.length,
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

// ------------------------------------------------------- WebSocket plumbing

function parseFrame(buf) {
  if (buf.length < 2) return null;
  const b0 = buf[0];
  const b1 = buf[1];
  const fin = (b0 & 0x80) !== 0;
  const opcode = b0 & 0x0f;
  const masked = (b1 & 0x80) !== 0;
  let length = b1 & 0x7f;
  let offset = 2;

  if (length === 126) {
    if (buf.length < 4) return null;
    length = buf.readUInt16BE(2);
    offset = 4;
  } else if (length === 127) {
    if (buf.length < 10) return null;
    const big = buf.readBigUInt64BE(2);
    if (big > BigInt(64 * 1024 * 1024)) return null;
    length = Number(big);
    offset = 10;
  }

  let mask = null;
  if (masked) {
    if (buf.length < offset + 4) return null;
    mask = buf.subarray(offset, offset + 4);
    offset += 4;
  }

  if (buf.length < offset + length) return null;
  const payload = Buffer.from(buf.subarray(offset, offset + length));
  if (mask) {
    for (let i = 0; i < payload.length; i += 1) payload[i] ^= mask[i % 4];
  }
  return { fin, opcode, payload, consumed: offset + length };
}

function writeFrame(socket, opcode, data) {
  const payload = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
  const length = payload.length;
  let header;
  if (length < 126) {
    header = Buffer.alloc(2);
    header[1] = length;
  } else if (length < 65536) {
    header = Buffer.alloc(4);
    header[1] = 126;
    header.writeUInt16BE(length, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }
  header[0] = 0x80 | opcode;
  socket.write(Buffer.concat([header, payload]));
}

// ------------------------------------------------------------ client registry

const clients = new Map(); // id -> { id, name, ip, socket }
let clientSeq = 0;

/* 按 IP 记住上次用过的设备名。
 * 浏览器刷新或重连时服务端会重新分配随机名，没有这层记忆的话
 * 设备名每刷新一次就变一次，对端根本认不出是哪台机器。
 * 客户端 localStorage 优先，这里只作兜底（比如换浏览器、隐私模式）。 */
const nameByIp = new Map(); // ip -> 上次使用的名字

const ADJECTIVES = ['蓝色', '银色', '安静', '敏捷', '温暖', '明亮', '沉稳', '轻盈', '清澈', '柔和'];
const ANIMALS = ['海豚', '猎鹰', '水獭', '狐狸', '鲸鱼', '山猫', '燕子', '小鹿', '海豹', '苍鹭'];

function randomName() {
  const a = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const b = ANIMALS[Math.floor(Math.random() * ANIMALS.length)];
  return `${a}${b}`;
}

function peerList() {
  return [...clients.values()].map((c) => ({ id: c.id, name: c.name, ip: c.ip }));
}

function sendTo(client, obj) {
  try {
    writeFrame(client.socket, OP_TEXT, JSON.stringify(obj));
    return true;
  } catch (_) {
    return false;
  }
}

function broadcastPeers() {
  const peers = peerList();
  for (const client of clients.values()) sendTo(client, { type: 'peers', peers });
}

function attachClient(socket, ip) {
  clientSeq += 1;
  const client = {
    id: `dev-${clientSeq}-${crypto.randomBytes(3).toString('hex')}`,
    name: nameByIp.get(ip) || randomName(),
    ip,
    socket,
  };
  clients.set(client.id, client);

  let buffer = Buffer.alloc(0);
  let fragOpcode = 0;
  let fragParts = [];

  const handleMessage = (text) => {
    let msg;
    try {
      msg = JSON.parse(text);
    } catch (_) {
      return;
    }
    if (msg.type === 'rename' && typeof msg.name === 'string') {
      const trimmed = msg.name.trim().slice(0, 24);
      if (trimmed) {
        client.name = trimmed;
        nameByIp.set(client.ip, trimmed);
      }
      sendTo(client, { type: 'self', id: client.id, name: client.name });
      broadcastPeers();
      return;
    }
    if (msg.type === 'signal' && msg.to) {
      const target = clients.get(msg.to);
      if (target) {
        sendTo(target, { type: 'signal', from: client.id, data: msg.data });
      } else {
        sendTo(client, { type: 'signal-lost', to: msg.to });
      }
    }
  };

  socket.on('data', (chunk) => {
    buffer = buffer.length ? Buffer.concat([buffer, chunk]) : chunk;
    for (;;) {
      const frame = parseFrame(buffer);
      if (!frame) break;
      buffer = buffer.subarray(frame.consumed);

      if (frame.opcode === OP_CLOSE) {
        socket.end();
        return;
      }
      if (frame.opcode === OP_PING) {
        writeFrame(socket, OP_PONG, frame.payload);
        continue;
      }
      if (frame.opcode === OP_PONG) continue;

      if (frame.opcode === OP_CONT) {
        fragParts.push(frame.payload);
        if (frame.fin) {
          const full = Buffer.concat(fragParts);
          fragParts = [];
          if (fragOpcode === OP_TEXT) handleMessage(full.toString('utf8'));
          fragOpcode = 0;
        }
        continue;
      }

      if (!frame.fin) {
        fragOpcode = frame.opcode;
        fragParts = [frame.payload];
        continue;
      }
      if (frame.opcode === OP_TEXT) handleMessage(frame.payload.toString('utf8'));
    }
  });

  const cleanup = () => {
    if (!clients.has(client.id)) return;
    clients.delete(client.id);
    broadcastPeers();
  };
  socket.on('close', cleanup);
  socket.on('error', cleanup);

  sendTo(client, { type: 'self', id: client.id, name: client.name });
  broadcastPeers();
}

function handleUpgrade(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return;
  }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  socket.setNoDelay(true);
  const ip = (req.socket.remoteAddress || '').replace(/^::ffff:/, '');
  attachClient(socket, ip);
}

// --------------------------------------------------------------- HTTP server

function serveStatic(req, res, pathname) {
  const rel = pathname === '/' ? 'index.html' : decodeURIComponent(pathname).replace(/^\/+/, '');
  const filePath = safeJoin(PUBLIC_DIR, rel);
  if (!filePath || !fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('404 Not Found');
    return;
  }
  const ext = path.extname(filePath).toLowerCase();
  const stat = fs.statSync(filePath);
  res.writeHead(200, {
    'Content-Type': MIME[ext] || 'application/octet-stream',
    'Content-Length': stat.size,
    'Cache-Control': 'no-cache',
  });
  fs.createReadStream(filePath, { highWaterMark: STREAM_BLOCK }).pipe(res);
}

function listFiles(req, res) {
  fs.readdir(UPLOAD_DIR, { withFileTypes: true }, (err, entries) => {
    if (err) {
      sendJson(res, 500, { error: '无法读取存储目录' });
      return;
    }
    const files = [];
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      const full = path.join(UPLOAD_DIR, entry.name);
      try {
        const stat = fs.statSync(full);
        files.push({
          name: entry.name,
          size: stat.size,
          sizeText: humanSize(stat.size),
          mtime: stat.mtimeMs,
        });
      } catch (_) {
        /* skip unreadable entries */
      }
    }
    files.sort((a, b) => b.mtime - a.mtime);
    sendJson(res, 200, { files, total: files.reduce((sum, f) => sum + f.size, 0) });
  });
}

/** 半成品文件名：name|size 的哈希，避免中文与特殊字符在文件系统上的麻烦。 */
function partPathFor(name, size) {
  const key = crypto.createHash('sha1').update(`${name}|${size}`).digest('hex').slice(0, 24);
  return path.join(PARTS_DIR, `${key}.part`);
}

function currentReceived(name, size) {
  try {
    const stat = fs.statSync(partPathFor(name, size));
    return size > 0 ? Math.min(stat.size, size) : stat.size;
  } catch (_) {
    return 0;
  }
}

/** 不带 size/offset 的整包上传：老客户端与测试脚本走这条，行为保持原样。 */
function uploadWhole(req, res, name) {
  let target;
  try {
    target = uniqueTarget(name);
  } catch (err) {
    sendJson(res, 500, { error: `创建文件失败: ${err.message}` });
    return;
  }

  const stream = fs.createWriteStream(target, { highWaterMark: STREAM_BLOCK });
  let bytes = 0;
  req.on('data', (chunk) => {
    bytes += chunk.length;
  });
  stream.on('error', (err) => {
    sendJson(res, 500, { error: `写入失败: ${err.message}` });
  });
  stream.on('finish', () => {
    sendJson(res, 200, {
      ok: true,
      name: path.basename(target),
      size: bytes,
      sizeText: humanSize(bytes),
      done: true,
    });
  });
  req.on('aborted', () => {
    stream.destroy();
    fs.unlink(target, () => {});
  });
  req.pipe(stream);
}

/* 分块上传：客户端带 size 与 offset，服务端把这一块追加进 .part 文件。
 * 中断时 .part 保留（这是续传的依据，绝不能像整包那样删掉），
 * 收满 size 后再重命名成正式文件。 */
function uploadChunk(req, res, name, size, offset) {
  fs.mkdirSync(PARTS_DIR, { recursive: true });
  const partPath = partPathFor(name, size);
  const existing = currentReceived(name, size);

  if (offset > existing) {
    /* 客户端要跳到还没收到的位置——顺序写做不到，让它按实际进度重来 */
    sendJson(res, 409, { error: 'offset 超出已接收范围', received: existing });
    return;
  }

  let bytes = 0;
  /* offset 为 0 时用 'w' 重建；否则用 'r+' 就地覆盖，保证断电/重连后
   * 已落盘的部分不被截断。客户端是顺序写的，不会回头补洞。 */
  const stream = fs.createWriteStream(partPath, { flags: offset > 0 ? 'r+' : 'w', start: offset, highWaterMark: STREAM_BLOCK });
  req.on('data', (chunk) => {
    bytes += chunk.length;
  });
  stream.on('error', (err) => {
    sendJson(res, 500, { error: `写入失败: ${err.message}` });
  });
  stream.on('finish', () => {
    const written = offset + bytes;
    /* 收满就落定。写成 `size > 0 &&` 的话 0 字节文件永远定不了名，
     * 分块模式下 size 是必填参数，直接比较即可。 */
    if (written >= size) {
      try {
        const target = uniqueTarget(name);
        fs.renameSync(partPath, target);
        sendJson(res, 200, {
          ok: true,
          name: path.basename(target),
          size: written,
          sizeText: humanSize(written),
          done: true,
        });
      } catch (err) {
        sendJson(res, 500, { error: `落盘失败: ${err.message}` });
      }
      return;
    }
    sendJson(res, 200, {
      ok: true, name, size: written, sizeText: humanSize(written), done: false, received: written,
    });
  });
  req.on('aborted', () => stream.destroy());
  req.pipe(stream);
}

function uploadFile(req, res, url) {
  const requested = url.searchParams.get('name') || 'unnamed';
  const name = safeName(requested);
  const sizeParam = url.searchParams.get('size');
  const offsetParam = url.searchParams.get('offset');

  if (sizeParam === null && offsetParam === null) return uploadWhole(req, res, name);

  const size = Number(sizeParam);
  const offset = Number(offsetParam) || 0;
  if (!Number.isFinite(size) || size < 0) {
    sendJson(res, 400, { error: 'size 无效' });
    return;
  }
  if (!Number.isFinite(offset) || offset < 0) {
    sendJson(res, 400, { error: 'offset 无效' });
    return;
  }
  uploadChunk(req, res, name, size, offset);
}

/** 续传前先问一句：这个文件已经收了多少字节。 */
function uploadStatus(req, res, url) {
  const name = safeName(url.searchParams.get('name') || '');
  const size = Number(url.searchParams.get('size')) || 0;
  sendJson(res, 200, {
    ok: true, name, size, received: currentReceived(name, size),
  });
}

function downloadFile(req, res, url) {
  const name = safeName(url.searchParams.get('name') || '');
  const filePath = safeJoin(UPLOAD_DIR, name);
  if (!filePath || !fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('404 Not Found');
    return;
  }
  const stat = fs.statSync(filePath);
  const encoded = encodeURIComponent(name);
  const baseHeaders = {
    'Content-Type': 'application/octet-stream',
    'Content-Disposition': `attachment; filename*=UTF-8''${encoded}`,
    'Accept-Ranges': 'bytes',
  };

  /* Range 支持：浏览器与下载器靠它做断点续传。
   * 只处理 bytes=start- 这一种，多段区间（逗号分隔）直接忽略走全量。 */
  const rangeHeader = req.headers.range;
  const match = rangeHeader ? /^bytes=(\d+)-(\d*)$/.exec(rangeHeader.trim()) : null;
  if (match) {
    const start = Number(match[1]);
    const end = match[2] ? Math.min(Number(match[2]), stat.size - 1) : stat.size - 1;
    if (start >= stat.size || start > end) {
      res.writeHead(416, {
        ...baseHeaders,
        'Content-Range': `bytes */${stat.size}`,
        'Content-Length': 0,
      });
      res.end();
      return;
    }
    res.writeHead(206, {
      ...baseHeaders,
      'Content-Range': `bytes ${start}-${end}/${stat.size}`,
      'Content-Length': end - start + 1,
    });
    fs.createReadStream(filePath, { start, end, highWaterMark: STREAM_BLOCK }).pipe(res);
    return;
  }

  res.writeHead(200, { ...baseHeaders, 'Content-Length': stat.size });
  fs.createReadStream(filePath, { highWaterMark: STREAM_BLOCK }).pipe(res);
}

function deleteFile(req, res, url) {
  const name = safeName(url.searchParams.get('name') || '');
  const filePath = safeJoin(UPLOAD_DIR, name);
  if (!filePath || !fs.existsSync(filePath)) {
    sendJson(res, 404, { error: '文件不存在' });
    return;
  }
  fs.unlink(filePath, (err) => {
    if (err) {
      sendJson(res, 500, { error: `删除失败: ${err.message}` });
      return;
    }
    sendJson(res, 200, { ok: true, name });
  });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const { pathname } = url;

  if (pathname === '/api/files' && req.method === 'GET') return listFiles(req, res);
  if (pathname === '/api/upload' && req.method === 'POST') return uploadFile(req, res, url);
  if (pathname === '/api/upload-status' && req.method === 'GET') return uploadStatus(req, res, url);
  if (pathname === '/api/download' && req.method === 'GET') return downloadFile(req, res, url);
  if (pathname === '/api/files' && req.method === 'DELETE') return deleteFile(req, res, url);
  if (pathname === '/api/info' && req.method === 'GET') {
    return sendJson(res, 200, {
      peers: clients.size,
      uploadDir: UPLOAD_DIR,
      uploadCount: fs.readdirSync(UPLOAD_DIR).length,
    });
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    sendJson(res, 405, { error: 'Method Not Allowed' });
    return;
  }
  return serveStatic(req, res, pathname);
});

server.on('upgrade', (req, socket, head) => {
  if (head && head.length) socket.unshift(head);
  handleUpgrade(req, socket);
});

server.listen(PORT, HOST, () => {
  const self = lanAddresses();
  console.log('');
  console.log('  LAN Share 已启动');
  console.log('  ------------------------------------------');
  console.log(`  本机访问:      http://localhost:${PORT}`);
  for (const ip of self) {
    console.log(`  局域网访问:    http://${ip}:${PORT}`);
  }
  console.log(`  文件存储目录:  ${UPLOAD_DIR}`);
  console.log('  ------------------------------------------');
  console.log('  设备间传文件走 WebRTC 直连，服务端不中转文件内容。');
  console.log('  存到服务端的文件会落盘在上面的目录中，可随时下载。');
  console.log('');
});

/* Ctrl-C 退出。
 * 原来只写 server.close(() => process.exit(0))，看起来没问题，实际上
 * server.close() 要等**所有**连接结束才回调——浏览器开着的 WebSocket 和
 * HTTP keep-alive 连接不会自己断，回调永远不触发，Ctrl-C 按下去就像卡住。
 * 所以：先主动断掉信令连接，再给一个宽限期，超时就地退出。
 * 正在上传的文件会被截断，但分块上传有 .part 半成品，重传能从断点续上。 */
process.on('SIGINT', () => {
  console.log('\n正在关闭...');
  const force = setTimeout(() => process.exit(0), 2000);
  force.unref();
  server.close(() => {
    clearTimeout(force);
    process.exit(0);
  });
  for (const c of clients.values()) {
    try { c.socket.close(); } catch (_) { /* 已经断了 */ }
  }
});

/* SIGTERM（systemd / kill）没有宽限期，直接退 */
process.on('SIGTERM', () => process.exit(0));
