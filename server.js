'use strict';

/**
 * LAN Share - zero dependency LAN file sharing server.
 *
 * Two transfer paths:
 *   1. Peer-to-peer: WebRTC DataChannel between browsers. The server relays
 *      signalling (offer / answer / ICE) and, when direct connection cannot be
 *      established, falls back to relaying the file bytes themselves over the
 *      same WebSocket ("兜底通道"). Direct connection is always preferred;
 *      the relay is the safety net.
 *   2. Server vault: plain HTTP upload / download. Files land on disk here.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const dgram = require('dgram');

const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT_DIR = __dirname;
const PUBLIC_DIR = path.join(ROOT_DIR, 'public');
const UPLOAD_DIR = path.resolve(process.env.UPLOAD_DIR || path.join(ROOT_DIR, 'uploads'));
/* 断点续传的半成品放在这里。名字是 name|size 的哈希，避开中文/特殊字符的编码坑，
 * 同时保证同名但不同大小的文件互不干扰。 */
const PARTS_DIR = path.join(UPLOAD_DIR, '.lanshare-parts');

/* 可选的 TURN 中继配置。设了这个环境变量，服务端在返回首页时会把 index.html
 * 里那段占位标记整段替换成它的值，前端再把 TURN 加进 iceServers。这样 AP 隔离
 * / 客户端隔离下设备间直连被丢弃时，仍可经自建 TURN 打通。不设 = 只用服务端
 * 自带的局域网 STUN（下面 stunReply 那块）。 */
const TURN_CONFIG = process.env.LAN_SHARE_TURN || '';
/* 要被替换掉的占位标记原文。它在 index.html 里只能出现一次——替换命中的是
 * "第一次出现"，在注释里再抄一遍原文就会打进注释、真正要用的那行反而没换。 */
const TURN_TOKEN = '/*__TURN_CONFIG__*/ null';

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

/* 单帧上限，同时也是单个连接的接收缓冲上限。兜底数据通道要发 64KB 分块
 * （1 字节 id 长度 + id + 载荷），还要容得分片重组，128KB 留足余量；
 * 同时它是一道硬闸：声明长度一读出来就判超限，半条巨帧不能把内存吃光。 */
const WS_BUF = 131072;

/* 中转进度每 8MB 打一行。只报"连接中断"是没用的，得知道断之前两边各走了多远。 */
const RELAY_LOG_EVERY = 8 * 1024 * 1024;

/* 中转侧的限流日志计数。超限丢帧、找不到目标，都是"静默丢 = 进度条卡死"的
 * 典型来源，必须留痕，但又不能每帧一行把日志冲爆。 */
let relayTooBig = 0;
let relayDropped = 0;

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

/** 写帧头（FIN 置位），返回帧头占几个字节。 */
function wsHeader(out, opcode, length) {
  out[0] = 0x80 | opcode;
  if (length < 126) {
    out[1] = length;
    return 2;
  }
  if (length < 65536) {
    out[1] = 126;
    out.writeUInt16BE(length, 2);
    return 4;
  }
  out[1] = 127;
  out.writeBigUInt64BE(BigInt(length), 2);
  return 10;
}

/**
 * 解析一帧。
 *   返回 null              —— 数据还不够，等下一批；
 *   返回 { tooBig: true }  —— 声明长度超过 maxPayload，调用方必须立刻关连接。
 *
 * 超限要能在"读完帧头、载荷还没到"时就判出来：原来写的是拿到整个载荷才比对，
 * 于是一条假的"长度 2^40"帧会让这条连接永远停在等待里、缓冲一路涨到内存耗尽。
 */
function parseFrame(buf, maxPayload) {
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
    if (big > BigInt(maxPayload)) return { tooBig: true };
    length = Number(big);
    offset = 10;
  }
  if (length > maxPayload) return { tooBig: true };

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
  const header = Buffer.allocUnsafe(10);
  const hlen = wsHeader(header, opcode, payload.length);
  /* 帧头与载荷分两次写、中间 cork 住：省掉一次 64KB 级的 memcpy（兜底通道
   * 每秒要发上百个 64KB 帧），同时保证它们进同一个 writev、只发一次系统调用。 */
  socket.cork();
  socket.write(header.subarray(0, hlen));
  socket.write(payload);
  socket.uncork();
}

/**
 * 二进制帧下行（opcode 0x2）。这是"兜底数据通道"的载体：WebRTC 直连建不起来时
 * （典型是 Chrome 把 host 候选伪装成 mDNS .local 名字，而 Wi-Fi AP 或
 * 有线/无线桥接把组播拦了，候选永远解析不出来），文件字节就走这里中转。
 * 帧格式统一为 [1 字节: 对端 id 长度][id][payload]，id 是**发送方**的 id，
 * 与前端 public/relay-channel.js 的 prefixRelayFrame 严格对齐。
 */
function wsSendBinary(client, payload, peerId) {
  const id = Buffer.from(String(peerId || ''), 'utf8');
  if (id.length === 0 || id.length > 255) return;
  const body = 1 + id.length + payload.length;
  if (body > WS_BUF) {
    /* 超限只能丢（一帧塞不下）。静默丢会让发送端永远等不到确认——
     * 表现是进度卡死在某个百分比，而服务端日志一片空白。 */
    relayTooBig += 1;
    if (relayTooBig <= 5 || relayTooBig % 500 === 0) {
      console.error(`[lan-share] 中转丢弃：单帧 ${body} > WS_BUF=${WS_BUF}（累计 ${relayTooBig} 帧）`);
    }
    return;
  }
  /* 帧头 + [idlen][id] 前缀合成一块小缓冲，载荷单独写——同上，避免整帧复制。 */
  const head = Buffer.allocUnsafe(10 + 1 + id.length);
  const hlen = wsHeader(head, OP_BINARY, body);
  head[hlen] = id.length;
  id.copy(head, hlen + 1);
  const socket = client.socket;
  socket.cork();
  socket.write(head.subarray(0, hlen + 1 + id.length));
  socket.write(payload);
  socket.uncork();
  client.relayOut += payload.length;
}

/**
 * 把一条二进制帧转给目标客户端。找不到就静默丢弃——对端可能刚断线，上层有
 * 超时和重传兜着，没必要为了一个找不到的 id 关掉转发方的连接。但丢了多少
 * 必须留下痕迹：否则表现就是"进度条突然不动了"，而日志全空白。
 */
function forwardBinary(client, payload) {
  if (payload.length < 1) return;
  const idlen = payload[0];
  if (idlen === 0 || idlen > payload.length - 1) return;
  const to = payload.subarray(1, 1 + idlen).toString('utf8');
  client.relayIn += payload.length;
  /* 每 8MB 打一行进度。 */
  if (Math.floor(client.relayIn / RELAY_LOG_EVERY) !== Math.floor((client.relayIn - payload.length) / RELAY_LOG_EVERY)) {
    console.error(
      `[lan-share] 中转进度 ${client.id}→${to}：已转发 ${Math.floor(client.relayIn / (1024 * 1024))} MB`
    );
  }
  const target = clients.get(to);
  if (target && target.socket && !target.socket.destroyed) {
    wsSendBinary(target, payload.subarray(1 + idlen), client.id);
    return;
  }
  relayDropped += 1;
  if (relayDropped <= 5 || relayDropped % 500 === 0) {
    console.error(`[lan-share] 中转丢弃：找不到目标 ${to}（累计 ${relayDropped} 帧，来自 ${client.id}）`);
  }
}

/**
 * 一条完整的 WS 消息该去哪：文本 = 信令 JSON，二进制 = 兜底通道的字节。
 * opcode 是这条消息的**首帧** opcode（分片消息也一样），分片拼接由调用方负责，
 * 所以到这里必须已经是一整条消息——二进制尤其重要，因为下游要靠载荷开头的
 * [idlen][id] 前缀决定转发给谁，半条消息的载荷前缀是坏的。
 */
function handleWsDataFrame(client, opcode, payload) {
  if (opcode === OP_TEXT) client.handleMessage(payload.toString('utf8'));
  else forwardBinary(client, payload);
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
    /* 兜底通道（二进制中转）统计。关闭时打出来，就能看出"是谁先断的、
     * 断的时候两边各走到哪一步"——只报一句"连接中断"是查不出东西的。 */
    relayIn: 0, // 从本连接收到的二进制帧字节（含帧首 id 前缀）
    relayOut: 0, // 转发给本连接的二进制帧字节（纯载荷）
    reason: '',
  };
  clients.set(client.id, client);

  let buffer = Buffer.alloc(0);
  let fragOpcode = 0; // 正在拼接的分片消息首帧 opcode（0 = 没有）
  let frag = null; // 分片重组缓冲，上限 WS_BUF

  /* 关连接一律走这里，并且必须说清"为什么关"。
   * 原来直接 socket.end()，什么也不打。于是出现这种局面：客户端报 1006
   * （对端没说再见就消失），服务端进程还在、日志一片空白，谁关的、在哪一步
   * 关的完全无从判断——只能靠猜。现在每条关闭都带原因、缓冲水位和中转计数。 */
  const closeWhy = (why) => {
    if (client.reason) return;
    client.reason = why;
    console.error(
      `[lan-share] 关闭连接 id=${client.id} ip=${client.ip} 原因=${why} ` +
        `残留=${buffer.length} 分片=${frag ? frag.length : 0} ` +
        `中转 收=${client.relayIn} 发=${client.relayOut}`
    );
    try {
      socket.destroy();
    } catch (_) {
      /* 已经断了 */
    }
  };

  client.handleMessage = (text) => {
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
      const frame = parseFrame(buffer, WS_BUF);
      if (frame && frame.tooBig) {
        closeWhy(`单帧超限（> ${WS_BUF}）`);
        return;
      }
      if (!frame) break;
      buffer = buffer.subarray(frame.consumed);

      if (frame.opcode === OP_CLOSE) {
        /* 对端说了再见：按规范回一个关闭帧再关，否则它看到的是 1006
         * （异常断开），和网络掉线长得一模一样，查起来白费功夫。 */
        try {
          writeFrame(socket, OP_CLOSE, Buffer.alloc(0));
        } catch (_) {
          /* 已经断了 */
        }
        closeWhy('收到对端关闭帧');
        return;
      }
      if (frame.opcode === OP_PING) {
        writeFrame(socket, OP_PONG, frame.payload);
        continue;
      }
      if (frame.opcode === OP_PONG) continue;

      if (frame.opcode === OP_CONT) {
        /* 续帧。只有前面确实开了一段分片消息才有意义；没有起始帧的续帧是
         * 协议错误，丢掉就好，别为一条坏帧断链。 */
        if (fragOpcode !== 0) {
          if (frag.length + frame.payload.length >= WS_BUF) {
            closeWhy('分片缓冲超限');
            return;
          }
          frag = Buffer.concat([frag, frame.payload]);
          if (frame.fin) {
            const op = fragOpcode;
            const full = frag;
            fragOpcode = 0;
            frag = null;
            handleWsDataFrame(client, op, full);
          }
        }
        continue;
      }

      if (frame.opcode === OP_TEXT || frame.opcode === OP_BINARY) {
        if (frame.fin) {
          /* 完整消息。上一段落了一半的分片消息按协议不该出现，
           * 丢掉半截继续——不能因为对端发错一帧就断掉整条连接。 */
          fragOpcode = 0;
          frag = null;
          handleWsDataFrame(client, frame.opcode, frame.payload);
        } else {
          /* 分片消息的第一片。
           *
           * 关键：二进制（0x2）也会分片，必须和文本一样攒起来。原来只认文本
           * 首帧，二进制的续帧全落进缓冲又永远等不到清零条件，于是缓冲一路涨
           * 到上限、把整条连接关掉。网上那句"浏览器一次 send 就是一帧"是错的：
           * Chrome 发大消息时会自己切成多片（首片带原 opcode，其余是续帧）。 */
          if (frame.payload.length >= WS_BUF) {
            closeWhy('分片首片就超限');
            return;
          }
          fragOpcode = frame.opcode;
          frag = frame.payload;
        }
        continue;
      }
      /* 其余 opcode（0xA、保留位等）忽略 */
    }
  });

  const cleanup = () => {
    if (!clients.has(client.id)) return;
    clients.delete(client.id);
    if (!client.reason) {
      /* 没走 closeWhy 就没了：对端直接断开、半路消失。也要留痕，
       * 否则"谁先断的"永远查不出来。 */
      console.error(
        `[lan-share] 连接结束 id=${client.id} ip=${client.ip} 原因=对端直接断开（未发关闭帧） ` +
          `中转 收=${client.relayIn} 发=${client.relayOut}`
      );
    }
    broadcastPeers();
  };
  socket.on('close', cleanup);
  socket.on('error', (err) => {
    if (!client.reason) client.reason = `socket 出错 ${err.code || err.message}`;
    cleanup();
  });

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

// ------------------------------------------------------------------ STUN

/* 局域网 STUN（自反射候选）——服务端兼任，零配置。
 *
 * 为什么需要它：Chrome 默认把 host 候选伪装成 mDNS .local 名字，对端必须靠
 * 组播（224.0.0.251:5353）解析出真实 IP。普通家用路由器默认开 IGMP Snooping /
 * 组播增强，无线侧经常收不到组播，候选就永远解析不出来，ICE 一条都建不起来——
 * 表现就是"有线↔有线通，无线↔无线、有线↔无线都不通"，而且和应用层没关系。
 *
 * 关键点是 Chrome **不对 srflx 候选做 mDNS 伪装**：只要有一个可达的 STUN，
 * 客户端就能拿到"自己真实内网 IP:端口"的候选，直连不依赖组播。
 * 把 STUN 做进服务端（UDP 与 TCP 同端口号互不冲突），局域网里就永远有一个
 * 可达的 STUN：不依赖外网（stun.l.google.com 在国内本来就不可达）、不需要
 * 部署 coturn、纯离线也能用。
 */
const STUN_MAGIC = 0x2112a442;
const STUN_FP_XOR = 0x5354554e;

/* STUN 指纹用的 CRC32（IEEE 802.3，反射多项式 0xEDB88320） */
const CRC32_TABLE = (() => {
  const table = new Int32Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[i] = c;
  }
  return table;
})();

function stunCrc32(buf, len) {
  let crc = -1;
  for (let i = 0; i < len; i += 1) crc = CRC32_TABLE[(crc ^ buf[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ -1) >>> 0;
}

function ipv4ToInt(ip) {
  const parts = String(ip).split('.');
  if (parts.length !== 4) return 0;
  return (
    ((Number(parts[0]) << 24) | (Number(parts[1]) << 16) | (Number(parts[2]) << 8) | Number(parts[3])) >>> 0
  );
}

/**
 * 只解析并应答 Binding Request（class=00, method=0x001）。其余一律忽略——
 * 这是个单文件服务，不是通用 STUN 服务。只应答 IPv4：局域网里
 * stun:<内网IPv4>:<端口> 一定命中，而 IPv6 的 srflx 在局域网里没有意义。
 */
function stunReply(req, rinfo) {
  if (req.length < 20) return null;
  if ((req[0] & 0xc0) !== 0) return null; /* 前两位必须是 0 */
  if (req.readUInt32BE(4) !== STUN_MAGIC) return null; /* RFC 3489 老格式，不处理 */
  const type = req.readUInt16BE(0);
  if ((type & 0x0110) !== 0) return null; /* 只要请求 */
  if ((type & 0x3eef) !== 0x0001) return null; /* 只要 Binding */
  if (rinfo.family !== 'IPv4') return null;

  const out = Buffer.alloc(128);
  out.writeUInt16BE(0x0101, 0); /* Binding Response */
  out.writeUInt16BE(0, 2); /* 长度稍后填 */
  out.writeUInt32BE(STUN_MAGIC, 4);
  req.copy(out, 8, 8, 20); /* 事务 ID 原样带回 */
  let pos = 20;

  const addr = ipv4ToInt(rinfo.address);
  const port = rinfo.port;

  /* MAPPED-ADDRESS（RFC 3489 客户端用） */
  out.writeUInt16BE(0x0001, pos);
  out.writeUInt16BE(8, pos + 2);
  out[pos + 4] = 0;
  out[pos + 5] = 0x01; /* IPv4 */
  out.writeUInt16BE(port, pos + 6);
  out.writeUInt32BE(addr, pos + 8);
  pos += 12;

  /* XOR-MAPPED-ADDRESS（RFC 5389，Chrome 认这个） */
  out.writeUInt16BE(0x0020, pos);
  out.writeUInt16BE(8, pos + 2);
  out[pos + 4] = 0;
  out[pos + 5] = 0x01;
  out.writeUInt16BE((port ^ ((STUN_MAGIC >>> 16) & 0xffff)) & 0xffff, pos + 6);
  out.writeUInt32BE((addr ^ STUN_MAGIC) >>> 0, pos + 8);
  pos += 12;

  /* FINGERPRINT：让应答完全符合 RFC 5389，省掉对端严格实现的兼容问题。
   * RFC 要求长度字段先按"含 FINGERPRINT 属性"填，再对截止该属性之前的内容算 CRC。 */
  out.writeUInt16BE(pos - 20 + 8, 2);
  const crc = (stunCrc32(out, pos) ^ STUN_FP_XOR) >>> 0;
  out.writeUInt16BE(0x8028, pos);
  out.writeUInt16BE(4, pos + 2);
  out.writeUInt32BE(crc, pos + 4);
  pos += 8;
  out.writeUInt16BE(pos - 20, 2);

  return out.subarray(0, pos);
}

/* 同一个端口号上再开一个 UDP socket 做 STUN。TCP 的 8080 和 UDP 的 8080 是两个
 * 独立的 socket，互不冲突，所以前端写 stun:<host>:<port> 就能命中，用户不需要
 * 额外记一个端口、路由器上也不用多开一个洞。
 * 起不来不致命——退化成"没有 STUN"，行为和以前一样，只是拿不到自反射候选。 */
function startStun(port, host, onStatus) {
  const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
  sock.on('message', (msg, rinfo) => {
    const reply = stunReply(msg, rinfo);
    if (reply) sock.send(reply, rinfo.port, rinfo.address, () => {});
  });
  sock.on('listening', () => onStatus(true, `UDP ${port}`));
  sock.on('error', (err) => {
    onStatus(false, err.code || err.message);
    try {
      sock.close();
    } catch (_) {
      /* 没起来 */
    }
  });
  sock.bind(port, host);
  return sock;
}

// --------------------------------------------------------------- HTTP server

/**
 * 首页要过一遍，因为 TURN 配置是注入进去的（见 TURN_CONFIG）。
 * 占位标记只替换"第一次出现"——这就是它必须在 index.html 里只出现一次的原因。
 */
function sendIndexWithTurn(res, filePath) {
  let body;
  try {
    body = fs.readFileSync(filePath, 'utf8');
  } catch (err) {
    res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(`500 ${err.message}`);
    return false;
  }
  const found = body.includes(TURN_TOKEN);
  /* 排查用：曾出现"服务端说已启用、但页面里 window.__LAN_SHARE_TURN__
   * 仍是 null"，只能靠这行日志区分是"根本没调用"还是"调用了但没找到占位符"。 */
  console.error(`[turn-inject] cfg=${TURN_CONFIG} body_len=${body.length} found=${found ? 1 : 0}`);
  if (!found) return false;
  const injected = body.replace(TURN_TOKEN, TURN_CONFIG);
  const buf = Buffer.from(injected, 'utf8');
  res.writeHead(200, {
    'Content-Type': MIME['.html'],
    'Content-Length': buf.length,
    'Cache-Control': 'no-cache',
  });
  res.end(buf);
  return true;
}

function serveStatic(req, res, pathname) {
  const rel = pathname === '/' ? 'index.html' : decodeURIComponent(pathname).replace(/^\/+/, '');
  const filePath = safeJoin(PUBLIC_DIR, rel);
  if (!filePath || !fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('404 Not Found');
    return;
  }
  if (TURN_CONFIG && rel === 'index.html' && sendIndexWithTurn(res, filePath)) return;
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
      /* 和前端的"设备直传"面板一起用：C 版回 "c"，这里回 "node"，
       * 便于脚本 / 对照测试一眼看出打到的是哪个实现。 */
      engine: 'node',
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
  console.log('  设备间传文件优先走 WebRTC 直连；直连建不起来时自动落到 WebSocket');
  console.log('  兜底中转——那种情况下文件字节会经过本服务，用完即走、不落盘。');
  console.log('  存到服务端的文件会落盘在上面的目录中，可随时下载。');

  /* UDP 同端口号上开 STUN。bind 是异步的，成功/失败都回这里再打印，
   * 免得横幅先报"已启用"、随后又冒出绑定失败，自相矛盾。 */
  startStun(PORT, HOST, (ok, detail) => {
    if (ok) {
      console.log(
        `  局域网 STUN:   已启用（${detail}，给客户端发真实内网 IP 的自反射候选，\n` +
          '                 绕开 Chrome 的 mDNS 伪装对组播的依赖）'
      );
    } else {
      console.log(`  局域网 STUN:   未启用（${detail}）——直连将依赖 mDNS 组播解析`);
    }
    console.log('');
  });
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
    /* 必须用 destroy()。net.Socket 上没有 close() 这个方法，原来那句
     * c.socket.close() 直接抛 TypeError 被 catch 吞掉——看着"已经断掉了"，
     * 实际一条连接都没断，每次都只能等 2 秒的宽限期兜底。 */
    try { c.socket.destroy(); } catch (_) { /* 已经断了 */ }
  }
});

/* SIGTERM（systemd / kill）没有宽限期，直接退 */
process.on('SIGTERM', () => process.exit(0));
