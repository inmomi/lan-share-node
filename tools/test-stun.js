'use strict';

/* 服务端局域网 STUN 的应答校验。
 *
 * 为什么值得单独测：STUN 是"直连不依赖 mDNS 组播"这条路的唯一支点，而它恰恰
 * 是**纯字节**的东西——XOR-MAPPED-ADDRESS 的异或基数是 magic cookie、端口的
 * 异或基数是 cookie 高 16 位、FINGERPRINT 的 CRC 覆盖范围还要按 RFC 5389 的
 * 规定先把长度字段算成"含 FINGERPRINT"。这些只要错一个 bit，浏览器就静默地
 * 拿不到 srflx 候选，表现和"服务端压根没起 STUN"一模一样，光看界面查不出来。
 *
 * 这里自己拼一条 Binding Request，把应答逐字段解出来对；FINGERPRINT 再本地
 * 独立复算一遍。只用 Node 内置模块，零依赖。
 *
 * 用法：
 *   node tools/test-stun.js            # 自己拉起服务端（随机端口 + 临时目录）
 *   node tools/test-stun.js --port 8080 --no-spawn   # 服务端已经起好了
 */

const dgram = require('dgram');
const crypto = require('crypto');
const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');

const ROOT = path.join(__dirname, '..');

function arg(name, dflt) {
  const i = process.argv.indexOf(`--${name}`);
  if (i < 0) return dflt;
  const v = process.argv[i + 1];
  return v === undefined ? true : v;
}

const PORT = Number(arg('port', 0)) || (8400 + Math.floor(Math.random() * 300));
const NO_SPAWN = arg('no-spawn', null) === true;

let passed = 0;
let failed = 0;

function check(name, cond, extra) {
  if (cond) {
    passed += 1;
    console.log(`PASS  ${name}${extra ? ` :: ${extra}` : ''}`);
  } else {
    failed += 1;
    console.log(`FAIL  ${name}${extra ? ` :: ${extra}` : ''}`);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* STUN 指纹用的 CRC32（IEEE 802.3，反射多项式 0xEDB88320）。
 * 与服务端那份是**各写一遍**的：如果两边共用一份实现，算错也会一起错。 */
const CRC32_TABLE = (() => {
  const t = new Int32Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[i] = c;
  }
  return t;
})();

function crc32(buf, len) {
  let crc = -1;
  for (let i = 0; i < len; i += 1) crc = CRC32_TABLE[(crc ^ buf[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ -1) >>> 0;
}

function buildBindingRequest(txid) {
  const buf = Buffer.alloc(20);
  buf.writeUInt16BE(0x0001, 0); // Binding Request
  buf.writeUInt16BE(0x0000, 2);
  buf.writeUInt32BE(0x2112a442, 4);
  txid.copy(buf, 8);
  return buf;
}

function parseAttrs(msg) {
  const out = { raw: msg, attrs: [] };
  let pos = 20;
  while (pos + 4 <= msg.length) {
    const type = msg.readUInt16BE(pos);
    const len = msg.readUInt16BE(pos + 2);
    const value = msg.subarray(pos + 4, pos + 4 + len);
    out.attrs.push({ type, value });
    if (type === 0x0001) out.mapped = { reserved: value[0], family: value[1], port: value.readUInt16BE(2), raw: value.subarray(4, 8) };
    if (type === 0x0020) {
      out.xor = {
        family: value[1],
        port: value.readUInt16BE(2) ^ ((0x2112a442 >>> 16) & 0xffff),
        addrInt: (value.readUInt32BE(4) ^ 0x2112a442) >>> 0,
      };
    }
    if (type === 0x8028) out.fingerprint = value.readUInt32BE(0);
    pos += 4 + len + ((4 - (len % 4)) % 4);
  }
  return out;
}

function intToIp(n) {
  return `${(n >>> 24) & 255}.${(n >>> 16) & 255}.${(n >>> 8) & 255}.${n & 255}`;
}

async function waitServerReady(port, timeoutMs) {
  const http = require('http');
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const ok = await new Promise((resolve) => {
      const req = http.get({ host: '127.0.0.1', port, path: '/api/info', timeout: 1500 }, (res) => {
        res.resume();
        resolve(res.statusCode === 200);
      });
      req.on('error', () => resolve(false));
      req.on('timeout', () => { req.destroy(); resolve(false); });
    });
    if (ok) return true;
    if (Date.now() > deadline) return false;
    await sleep(200);
  }
}

function askStun(port, request) {
  return new Promise((resolve) => {
    const sock = dgram.createSocket('udp4');
    const timer = setTimeout(() => {
      try { sock.close(); } catch (_) {}
      resolve(null);
    }, 3000);
    sock.on('message', (msg) => {
      clearTimeout(timer);
      try { sock.close(); } catch (_) {}
      resolve(msg);
    });
    sock.on('error', () => {
      clearTimeout(timer);
      try { sock.close(); } catch (_) {}
      resolve(null);
    });
    sock.send(request, port, '127.0.0.1');
  });
}

(async () => {
  let child = null;
  let tmpDir = null;

  if (!NO_SPAWN) {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lanshare-stun-'));
    child = spawn(process.execPath, ['server.js'], {
      cwd: ROOT,
      env: { ...process.env, PORT: String(PORT), UPLOAD_DIR: tmpDir },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', (d) => process.stdout.write(`[srv] ${d}`));
    child.stderr.on('data', (d) => process.stderr.write(`[srv!] ${d}`));
    const ready = await waitServerReady(PORT, 20000);
    if (!ready) {
      console.log(`FAIL  服务端未能在 20 秒内就绪（端口 ${PORT}）`);
      child.kill();
      process.exit(1);
    }
    /* UDP 与 TCP 是同一个端口号，bind 是异步的；留一点余量再发请求 */
    await sleep(300);
  }

  console.log(`\n--- 局域网 STUN（UDP ${PORT}）---`);

  const txid = crypto.randomBytes(12);
  const reply = await askStun(PORT, buildBindingRequest(txid));

  check('Binding Request 有应答（UDP 同端口可达）', !!reply);
  if (reply) {
    check('应答类型是 Binding Response(0x0101)', reply.readUInt16BE(0) === 0x0101,
      `0x${reply.readUInt16BE(0).toString(16)}`);
    check('magic cookie 正确', reply.readUInt32BE(4) === 0x2112a442);
    check('事务 ID 原样带回', reply.subarray(8, 20).equals(txid));

    const p = parseAttrs(reply);
    /* 长度字段 = 应答总长 - 20 字节头 */
    check('长度字段与报文长度一致', reply.readUInt16BE(2) === reply.length - 20,
      `${reply.readUInt16BE(2)} vs ${reply.length - 20}`);

    check('含 MAPPED-ADDRESS(0x0001)', !!p.mapped);
    if (p.mapped) {
      check('MAPPED-ADDRESS 家族是 IPv4', p.mapped.family === 0x01, `0x${p.mapped.family.toString(16)}`);
      check('MAPPED-ADDRESS 保留位为 0', p.mapped.reserved === 0);
      check('MAPPED-ADDRESS 就是我们发请求的地址',
        intToIp(p.mapped.raw.readUInt32BE(0)) === '127.0.0.1',
        intToIp(p.mapped.raw.readUInt32BE(0)));
    }

    check('含 XOR-MAPPED-ADDRESS(0x0020)', !!p.xor);
    if (p.xor) {
      /* 这是最关键的一条：Chrome 只认 XOR-MAPPED-ADDRESS，且异或基数写错
       * 不会报错，只会静默给出一个不可达的候选地址。 */
      check('XOR-MAPPED-ADDRESS 解出真实地址',
        intToIp(p.xor.addrInt) === '127.0.0.1', intToIp(p.xor.addrInt));
      check('XOR-MAPPED-ADDRESS 端口是合法端口',
        p.xor.port > 0 && p.xor.port < 65536, String(p.xor.port));
      check('两个属性的端口应当一致',
        !p.mapped || p.mapped.port === p.xor.port,
        p.mapped ? `${p.mapped.port} vs ${p.xor.port}` : '(无 MAPPED-ADDRESS)');
    }

    check('含 FINGERPRINT(0x8028)', p.fingerprint !== undefined);
    if (p.fingerprint !== undefined) {
      /* 独立复算：长度字段要按"含 FINGERPRINT"取值，CRC 覆盖截止该属性之前的内容 */
      const cut = reply.length - 8;
      const head = Buffer.from(reply.subarray(0, cut));
      head.writeUInt16BE(reply.readUInt16BE(2), 2);
      const want = (crc32(head, cut) ^ 0x5354554e) >>> 0;
      check('FINGERPRINT 与独立复算一致',
        p.fingerprint === want,
        `报文 0x${p.fingerprint.toString(16)} vs 复算 0x${want.toString(16)}`);
    }

    /* 非 Binding 请求必须被忽略，不能回一条看起来像成功的应答 */
    const bad = Buffer.from(buildBindingRequest(crypto.randomBytes(12)));
    bad.writeUInt16BE(0x0003, 0); // Allocate Request（TURN），不是 Binding
    const noReply = await askStun(PORT, bad);
    check('非 Binding 请求不应答', noReply === null);

    const short = Buffer.from([0x00, 0x01, 0x00, 0x00]);
    check('长度不足的报文不致崩（服务端仍在）', (await askStun(PORT, short)) === null);
  }

  if (child) {
    child.kill();
    await sleep(200);
  }
  if (tmpDir) {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
  }

  console.log(`\n===== 局域网 STUN 单测：通过 ${passed} / 失败 ${failed} =====`);
  process.exit(failed === 0 ? 0 : 1);
})();
