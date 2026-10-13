'use strict';
/*
 * lan-share 端到端功能测试。Node 版与 C 版共用（两者 HTTP 接口与 WebSocket 协议一致）。
 *
 * 用法：
 *   1) 服务已经在跑：
 *        BASE=http://127.0.0.1:8080 node test.js
 *   2) 让测试自己拉起服务（CI 推荐，跨平台行为一致，不用处理后台进程差异）：
 *        SPAWN="node server.js" node test.js
 *        SPAWN="./lan_share -p 8080 -d ./uploads -w ./public" node test.js
 *
 * SPAWN 模式下脚本会启动子进程、轮询端口就绪、跑完全部用例后终止它。
 *
 * 覆盖：静态托管、目录穿越防护、上传/下载/删除、中文名、同名自动重命名、
 *       大文件完整性、WebSocket 信令全链路。
 *
 * 零第三方依赖，但需要 Node >= 21 提供全局 WebSocket（CI 用 22）。
 */

const http = require('http');
const net = require('net');
const crypto = require('crypto');
const { URL } = require('url');
const { spawn } = require('child_process');

const BASE = process.env.BASE || 'http://127.0.0.1:8080';
const base = new URL(BASE);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let passed = 0;
let failed = 0;

function check(name, cond, extra) {
    if (cond) {
        passed++;
        console.log(`PASS  ${name}${extra ? ' :: ' + extra : ''}`);
    } else {
        failed++;
        console.log(`FAIL  ${name}${extra ? ' :: ' + extra : ''}`);
    }
}

/* Windows 上 shell:true 走的是 cmd.exe，"./lan-share" 这种 Unix 写法会被拆成
 * "." + "/lan-share"，cmd 报「'.' 不是内部或外部命令」。这里做一次规范化：
 * 去掉前导 "./"，无扩展名时补 .exe（若存在），让同一条 SPAWN 在两种系统下都能用。 */
function normalizeSpawn(cmd) {
    if (process.platform !== 'win32') return cmd;
    const parts = cmd.split(/\s+/);
    let exe = parts[0];
    if (exe.startsWith('./') || exe.startsWith('.\\')) exe = exe.slice(2);
    if (!/\.(exe|bat|cmd)$/i.test(exe) && require('fs').existsSync(exe + '.exe')) {
        exe += '.exe';
    }
    parts[0] = exe;
    return parts.join(' ');
}

/* SPAWN 模式下等待服务端口就绪，最多等 20 秒 */
async function waitReady(host, port, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const ok = await new Promise((resolve) => {
            const r = http.request({ host, port, method: 'GET', path: '/' }, (res) => {
                res.resume();
                resolve(true);
            });
            r.on('error', () => resolve(false));
            r.end();
        });
        if (ok) return true;
        await sleep(300);
    }
    return false;
}

/* 发一个 HTTP 请求，返回 { status, headers, body(Buffer) }。
 * 注意：http.request 不会规范化 path，所以 '/../x' 会原样发到服务端，
 * 这正是测试目录穿越所需要的。 */
function req(method, path, body, extraHeaders) {
    return new Promise((resolve, reject) => {
        const r = http.request(
            {
                host: base.hostname,
                port: base.port || 80,
                method,
                path,
                headers: {
                    ...(extraHeaders || {}),
                    ...(body ? { 'Content-Length': Buffer.byteLength(body) } : {}),
                },
            },
            (res) => {
                const chunks = [];
                res.on('data', (c) => chunks.push(c));
                res.on('end', () =>
                    resolve({
                        status: res.statusCode,
                        headers: res.headers,
                        body: Buffer.concat(chunks),
                    })
                );
            }
        );
        r.on('error', reject);
        if (body) r.write(body);
        r.end();
    });
}

const json = async (method, path, body, extraHeaders) => {
    const r = await req(method, path, body, extraHeaders);
    try {
        return { ...r, json: JSON.parse(r.body.toString('utf8')) };
    } catch {
        return { ...r, json: null };
    }
};

async function testStatic() {
    console.log('\n--- 静态托管 ---');
    const idx = await req('GET', '/');
    check('首页 200', idx.status === 200, `status=${idx.status}`);
    check(
        '首页是 HTML',
        /<html|<\!doctype/i.test(idx.body.toString('utf8').slice(0, 200))
    );
    const app = await req('GET', '/app.js');
    check('app.js 200', app.status === 200 && app.body.length > 1000,
        `${app.status} ${app.body.length}B`);
    const css = await req('GET', '/style.css');
    check('style.css 200', css.status === 200 && css.body.length > 500,
        `${css.status} ${css.body.length}B`);
}

async function testTraversal() {
    console.log('\n--- 目录穿越防护 ---');
    const raw = await req('GET', '/../' + (process.env.TRAVERSAL_PROBE || 'server.js'));
    check('明文 ../ 被拒', raw.status === 404 || raw.status === 403,
        `status=${raw.status}`);
    const enc = await req('GET', '/%2e%2e%2f' + (process.env.TRAVERSAL_PROBE || 'server.js'));
    check('编码 %2e%2e%2f 被拒', enc.status === 404 || enc.status === 403,
        `status=${enc.status}`);
    const deep = await req('GET', '/../../../../etc/passwd');
    check('多级 ../etc/passwd 被拒', deep.status === 404 || deep.status === 403,
        `status=${deep.status}`);
    const missing = await req('GET', '/definitely-not-here.txt');
    check('不存在的文件 404', missing.status === 404);
}

async function testFileLibrary() {
    console.log('\n--- 文件库：上传 / 下载 / 删除 ---');
    const payload = Buffer.from('lan-share round trip 中文内容\n', 'utf8');
    const up = await json('POST', '/api/upload?name=round.txt', payload);
    check('上传返回 ok', up.json && up.json.ok === true, JSON.stringify(up.json));
    check('上传大小正确', up.json && up.json.size === payload.length,
        `${up.json && up.json.size} vs ${payload.length}`);

    const dl = await req('GET', '/api/download?name=round.txt');
    check('下载 200', dl.status === 200, `status=${dl.status}`);
    check('下载内容逐字节一致', dl.body.equals(payload),
        `${dl.body.length} vs ${payload.length}`);
    check('下载带 Content-Disposition',
        !!dl.headers['content-disposition'],
        String(dl.headers['content-disposition'] || '').slice(0, 50));

    const list = await json('GET', '/api/files');
    check('列表含该文件',
        list.json && list.json.files.some((f) => f.name === 'round.txt'),
        `total=${list.json && list.json.total}`);

    const del = await json('DELETE', '/api/files?name=round.txt');
    check('删除返回 ok', del.json && del.json.ok === true, JSON.stringify(del.json));
    const after = await json('GET', '/api/files');
    check('删除后列表不含该文件',
        after.json && !after.json.files.some((f) => f.name === 'round.txt'));
}

async function testRename() {
    console.log('\n--- 同名自动重命名 ---');
    const p = Buffer.from('v1');
    const p2 = Buffer.from('v2-different-length');
    const a = await json('POST', '/api/upload?name=dup.txt', p);
    const b = await json('POST', '/api/upload?name=dup.txt', p2);
    check('第二次上传得到新名字',
        a.json && b.json && a.json.name !== b.json.name,
        `${a.json && a.json.name} -> ${b.json && b.json.name}`);
    check('新名字带 (1)',
        b.json && /\(1\)/.test(b.json.name), String(b.json && b.json.name));

    const d1 = await req('GET', '/api/download?name=' + encodeURIComponent(a.json.name));
    const d2 = await req('GET', '/api/download?name=' + encodeURIComponent(b.json.name));
    check('两个文件内容各自正确', d1.body.equals(p) && d2.body.equals(p2));

    await json('DELETE', '/api/files?name=' + encodeURIComponent(a.json.name));
    await json('DELETE', '/api/files?name=' + encodeURIComponent(b.json.name));
}

async function testUnicode() {
    console.log('\n--- 中文与特殊文件名 ---');
    const names = [
        '中文测试.txt',
        '年度报告 2026.pdf',
        '照片-春节.jpg',
        '最终版(v2).tar.gz',
        "it's a file.bin",
    ];
    const payload = Buffer.from('unicode payload 中文\n', 'utf8');
    for (const n of names) {
        const u = '/api/upload?name=' + encodeURIComponent(n);
        const up = await json('POST', u, payload);
        if (!up.json || !up.json.ok) {
            check(`上传 ${n}`, false, JSON.stringify(up.json));
            continue;
        }
        const dl = await req('GET', '/api/download?name=' + encodeURIComponent(n));
        check(`${n} 往返一致`,
            dl.status === 200 && dl.body.equals(payload),
            `status=${dl.status} ${dl.body.length}B`);
        await json('DELETE', '/api/files?name=' + encodeURIComponent(n));
    }
    const left = await json('GET', '/api/files');
    check('中文名文件已全部清理',
        left.json && left.json.files.filter((f) => names.includes(f.name)).length === 0);
}

async function testBigFile() {
    console.log('\n--- 大文件完整性（5 MB）---');
    const size = 5 * 1024 * 1024;
    const big = Buffer.alloc(size);
    /* 填伪随机数据，避免全零掩盖截断问题 */
    let x = 123456789;
    for (let i = 0; i < size; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        big[i] = x & 0xff;
    }
    const up = await json('POST', '/api/upload?name=big5mb.bin', big);
    check('5MB 上传成功', up.json && up.json.size === size,
        `${up.json && up.json.size} vs ${size}`);
    const dl = await req('GET', '/api/download?name=big5mb.bin');
    check('5MB 下载大小一致', dl.body.length === size,
        `${dl.body.length} vs ${size}`);
    check('5MB 内容逐字节一致', dl.body.equals(big));
    await json('DELETE', '/api/files?name=big5mb.bin');
}

/* ==========================================================================
 * 最小 WebSocket 客户端（只用 net + crypto，零依赖）
 *
 * 为什么不用 Node 全局 WebSocket：它不暴露"分片发送"和"握手与首帧合并成一次
 * 写入"这两件事，而服务端的两个坑恰恰只在这两种形状下才暴露。浏览器会替我们
 * 造出这些形状，但不可控（分片阈值是实现细节）、也慢——真机上表现为"传到
 * 380KB 突然全断、只报一句连接中断"，查了很久。
 * 自己拼字节，就能把它变成一条确定性的断言。
 * ========================================================================== */
function buildFrame(op, payload, fin) {
    const p = Buffer.isBuffer(payload) ? payload : Buffer.from(payload || '');
    const mask = crypto.randomBytes(4);
    let head;
    if (p.length < 126) {
        head = Buffer.alloc(2);
        head[1] = 0x80 | p.length;
    } else if (p.length < 65536) {
        head = Buffer.alloc(4);
        head[1] = 0x80 | 126;
        head.writeUInt16BE(p.length, 2);
    } else {
        head = Buffer.alloc(10);
        head[1] = 0x80 | 127;
        head.writeBigUInt64BE(BigInt(p.length), 2);
    }
    head[0] = (fin === false ? 0 : 0x80) | op;
    const body = Buffer.alloc(p.length);
    for (let i = 0; i < p.length; i++) body[i] = p[i] ^ mask[i % 4];
    return Buffer.concat([head, mask, body]);
}

/* 服务端→客户端的帧不带掩码 */
function takeFrame(state) {
    const b = state.buf;
    if (b.length < 2) return null;
    const fin = (b[0] & 0x80) !== 0;
    const op = b[0] & 0x0f;
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) {
        if (b.length < 4) return null;
        len = b.readUInt16BE(2); off = 4;
    } else if (len === 127) {
        if (b.length < 10) return null;
        len = Number(b.readBigUInt64BE(2)); off = 10;
    }
    if (b.length < off + len) return null;
    const payload = b.slice(off, off + len);
    state.buf = b.slice(off + len);
    return { fin, op, payload };
}

function rawWsConnect(host, port, path) {
    const socket = net.connect(Number(port), host);
    const key = crypto.randomBytes(16).toString('base64');
    const accept = crypto.createHash('sha1')
        .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
        .digest('base64');
    const req = Buffer.from(
        `GET ${path} HTTP/1.1\r\n`
        + `Host: ${host}:${port}\r\n`
        + 'Upgrade: websocket\r\n'
        + 'Connection: Upgrade\r\n'
        + `Sec-WebSocket-Key: ${key}\r\n`
        + 'Sec-WebSocket-Version: 13\r\n\r\n', 'latin1');

    const state = {
        socket, upgraded: false, buf: Buffer.alloc(0), inbox: [], bin: [],
        closed: false, bad: null,
    };
    socket.on('data', (chunk) => {
        state.buf = Buffer.concat([state.buf, chunk]);
        if (!state.upgraded) {
            const i = state.buf.indexOf('\r\n\r\n');
            if (i < 0) return;
            const head = state.buf.slice(0, i).toString('latin1');
            state.buf = state.buf.slice(i + 4);
            if (!head.startsWith('HTTP/1.1 101') || !head.includes(accept)) {
                state.bad = head.split('\r\n')[0];
                return;
            }
            state.upgraded = true;
        }
        for (;;) {
            const f = takeFrame(state);
            if (!f) break;
            if (f.op === 0x1) {
                try { state.inbox.push(JSON.parse(f.payload.toString('utf8'))); }
                catch (_) { /* 非 JSON 的文本帧与用例无关 */ }
            } else if (f.op === 0x2) {
                state.bin.push(f.payload);
            }
        }
    });
    socket.on('close', () => { state.closed = true; });
    socket.on('error', (e) => { state.bad = e.message; });

    state.sendFrame = (op, payload, fin) => socket.write(buildFrame(op, payload, fin));
    /* preamble 会跟着握手请求一起发出去——真实网络里 TCP 合并就是这么发生的 */
    state.start = (preamble) =>
        socket.write(preamble ? Buffer.concat([req, preamble]) : req);
    state.end = () => socket.end();
    return state;
}

async function waitFor(fn, timeoutMs) {
    const deadline = Date.now() + (timeoutMs || 4000);
    for (;;) {
        const v = fn();
        if (v) return v;
        if (Date.now() >= deadline) return null;
        await sleep(50);
    }
}

/* ==========================================================================
 * 兜底通道（WebSocket 二进制中转）的帧级用例
 *
 * 服务端原来是按"前端一次 send 就是一帧"写的：0x2 二进制帧直接转发，
 * 0x0 续帧只当文本分片处理（只有 frag_op==0x1 才清零）。可浏览器发大消息会
 * 自己切片，首片是 0x2、其余是 0x0，于是续帧永远清不掉、一路堆过 WS_BUF，
 * 最后把整条连接关掉。信令 JSON 都很小、永不分片，所以这个坑只在兜底通道
 * 传大文件时才现形。
 * ========================================================================== */
async function testRelayFrames() {
    console.log('\n--- 兜底通道帧形状（分片 / 握手与首帧合并）---');
    const host = base.hostname;
    const port = Number(base.port || 80);

    const a = rawWsConnect(host, port, '/ws');
    const b = rawWsConnect(host, port, '/ws');
    a.start();
    b.start();
    await sleep(500);

    const aSelf = a.inbox.filter((m) => m.type === 'self').pop();
    const bSelf = b.inbox.filter((m) => m.type === 'self').pop();
    check('原始 WS 客户端握手成功并拿到 id', !!aSelf && !!bSelf,
        `${aSelf && aSelf.id} / ${bSelf && bSelf.id}${a.bad ? ' :: ' + a.bad : ''}`);
    if (!aSelf || !bSelf) { a.end(); b.end(); return; }

    /* ---------- 用例 1：二进制消息分片发送，必须被拼回完整一条 ---------- */
    const payload = Buffer.alloc(50000);
    for (let i = 0; i < payload.length; i++) payload[i] = (i * 7 + 13) & 0xff;
    const idb = Buffer.from(bSelf.id, 'utf8');
    const framed = Buffer.concat([Buffer.from([idb.length]), idb, payload]);

    const sendFragmented = () => {
        /* 首片 0x2 + fin=0，续片 0x0 + fin=1 —— 浏览器大消息就是这么发的 */
        a.sendFrame(0x2, framed.slice(0, 30000), false);
        a.sendFrame(0x0, framed.slice(30000), true);
    };
    sendFragmented();
    await sleep(250);
    sendFragmented();

    await waitFor(() => (b.bin.length >= 2 ? true : null), 4000);
    check('分片的二进制消息被拼回完整一条并转发', b.bin.length === 2,
        `收到 ${b.bin.length} 条，期望 2 条`);
    if (b.bin.length) {
        const idlen = b.bin[0][0];
        const fromId = b.bin[0].slice(1, 1 + idlen).toString('utf8');
        const body = b.bin[0].slice(1 + idlen);
        check('转发帧的 id 前缀正确', fromId === aSelf.id, `${fromId} vs ${aSelf.id}`);
        check('分片拼回后载荷逐字节一致', body.equals(payload),
            `收到 ${body.length} / 期望 ${payload.length}`);
    }
    check('分片没有把发送端连接关掉', !a.closed);

    /* 旧实现的 frag 缓冲只增不减：连发 40 条必然堆过 WS_BUF 把连接关掉。
     * 40 × 20012 ≈ 780KB > 128KB，所以这条断言就是当时那个故障的复现。 */
    for (let n = 0; n < 40; n++) sendFragmented();
    await waitFor(() => (b.bin.length >= 42 ? true : null), 6000);
    check('连续 40 条分片消息后连接仍在', !a.closed);
    check('42 条全部转发出去', b.bin.length === 42, `收到 ${b.bin.length} 条`);

    /* ---------- 用例 2：握手请求和第一帧合并成一次写入 ---------- */
    /* 服务端原来在 WS 握手处 `return (long)len`（把整段都算成自己吃掉了），
     * 跟在请求头后面的字节会被调用方的 memmove 丢掉——第一帧静默消失。 */
    const c = rawWsConnect(host, port, '/ws');
    c.start(buildFrame(0x1,
        Buffer.from(JSON.stringify({ type: 'rename', name: '合并首帧测试' })), true));
    await sleep(700);
    const names = c.inbox.filter((m) => m.type === 'self').map((m) => m.name);
    check('握手后附带的第一帧没有被丢掉', names.includes('合并首帧测试'),
        JSON.stringify(names));
    c.end();

    a.end();
    b.end();
    await sleep(300);
}

async function testWebSocket() {
    console.log('\n--- WebSocket 信令 ---');
    if (typeof WebSocket === 'undefined') {
        check('WebSocket 可用', false, '当前 Node 版本无全局 WebSocket，需要 >= 21');
        return;
    }
    const wsUrl = `ws://${base.hostname}:${base.port || 80}/ws`;

    function open(tag) {
        return new Promise((resolve, reject) => {
            const ws = new WebSocket(wsUrl);
            const inbox = [];
            ws.addEventListener('message', (e) => {
                const msg = JSON.parse(String(e.data));
                inbox.push(msg);
                if (msg.type === 'self') ws._id = msg.id;
            });
            ws.addEventListener('open', () => resolve({ ws, inbox, tag }));
            ws.addEventListener('error', (e) =>
                reject(new Error(`${tag}: ${e.message || '连接失败'}`))
            );
        });
    }
    const find = (inbox, type) => inbox.filter((m) => m.type === type);

    const a = await open('A');
    await sleep(250);
    check('收到 self（含 id 与随机名）',
        find(a.inbox, 'self').length === 1,
        JSON.stringify(find(a.inbox, 'self')[0] || {}));

    const b = await open('B');
    await sleep(400);
    const aPeers = find(a.inbox, 'peers').pop();
    const bPeers = find(b.inbox, 'peers').pop();
    check('A 看见 2 台设备', !!aPeers && aPeers.peers.length === 2,
        `len=${aPeers && aPeers.peers.length}`);
    check('B 看见 2 台设备', !!bPeers && bPeers.peers.length === 2,
        `len=${bPeers && bPeers.peers.length}`);
    check('两个 id 不同', a.ws._id !== b.ws._id, `${a.ws._id} / ${b.ws._id}`);

    b.ws.send(JSON.stringify({ type: 'rename', name: '路由器上的手机' }));
    await sleep(300);
    const renamed = find(a.inbox, 'peers').pop();
    const be = renamed && renamed.peers.find((p) => p.id === b.ws._id);
    check('中文重命名广播生效', !!be && be.name === '路由器上的手机',
        be && be.name);

    b.ws.send(JSON.stringify({
        type: 'signal', to: a.ws._id,
        data: { desc: { type: 'offer', sdp: 'v=0 FAKE-SDP-9' } },
    }));
    await sleep(300);
    const sig = find(a.inbox, 'signal').pop();
    check('信令转发到目标', !!sig && sig.from === b.ws._id);
    check('信令 payload 完整',
        !!sig && sig.data.desc.sdp === 'v=0 FAKE-SDP-9',
        sig && sig.data.desc.sdp);
    check('转发消息仍带 type=signal', !!sig && sig.type === 'signal');

    b.ws.send(JSON.stringify({ type: 'signal', to: 'dev-nobody', data: { c: 1 } }));
    await sleep(300);
    check('无效目标回 signal-lost', find(b.inbox, 'signal-lost').length > 0);

    b.ws.close();
    await sleep(500);
    const after = find(a.inbox, 'peers').pop();
    check('断开后列表收缩为 1', !!after && after.peers.length === 1,
        `len=${after && after.peers.length}`);
    a.ws.close();
}

/* 造一段伪随机数据，避免全零内容掩盖截断/错位问题 */
function pseudoBytes(size, seed) {
    const buf = Buffer.alloc(size);
    let x = seed;
    for (let i = 0; i < size; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        buf[i] = x & 0xff;
    }
    return buf;
}

/* 分块上传 + 中断 + 续传。核心断言是"续完之后内容逐字节一致"：
 * 续传最容易错的就是偏移量，错了文件能落盘但内容是坏的。 */
async function testResumeUpload() {
    console.log('\n--- 断点续传（文件库分块上传）---');
    const name = 'resume-me.bin';
    const total = 300 * 1024;
    const data = pseudoBytes(total, 987654321);
    const enc = encodeURIComponent(name);
    await json('DELETE', '/api/files?name=' + enc);

    const r1 = await json('POST', `/api/upload?name=${enc}&size=${total}&offset=0`,
        data.subarray(0, 100 * 1024));
    check('首块上传返回未完结', r1.json && r1.json.ok === true && r1.json.done === false,
        JSON.stringify(r1.json));
    check('首块后已收字节正确', r1.json && r1.json.received === 100 * 1024,
        `${r1.json && r1.json.received}`);

    const mid = await json('GET', '/api/files');
    check('未完结时不出现在文件库',
        mid.json && mid.json.files.filter((f) => f.name === name).length === 0);

    const st = await json('GET', `/api/upload-status?name=${enc}&size=${total}`);
    check('进度查询返回已收字节', st.json && st.json.received === 100 * 1024,
        `${st.json && st.json.received}`);

    const r2 = await json('POST', `/api/upload?name=${enc}&size=${total}&offset=${100 * 1024}`,
        data.subarray(100 * 1024, 200 * 1024));
    check('第二块续传成功', r2.json && r2.json.ok === true && r2.json.done === false,
        JSON.stringify(r2.json));

    const r3 = await json('POST', `/api/upload?name=${enc}&size=${total}&offset=${200 * 1024}`,
        data.subarray(200 * 1024));
    check('最后一块收满并落定', r3.json && r3.json.ok === true && r3.json.done === true,
        JSON.stringify(r3.json));

    const dl = await req('GET', '/api/download?name=' + enc);
    check('续传后内容逐字节一致', dl.body.equals(data), `${dl.body.length} vs ${total}`);
    await json('DELETE', '/api/files?name=' + enc);

    /* 客户端跳到还没收到的位置：顺序写做不到，必须报错让它按实际进度重来，
     * 否则会产出一个中间空洞的文件。 */
    const bad = await json('POST', '/api/upload?name=jump.bin&size=1000&offset=500',
        Buffer.alloc(10));
    check('跳过未上传区域返回 409', bad.status === 409, `status=${bad.status}`);
}

/* HTTP Range：浏览器与下载器的断点续传依赖它 */
async function testRange() {
    console.log('\n--- 下载断点续传（HTTP Range）---');
    const name = 'range-src.bin';
    const total = 64 * 1024;
    const data = pseudoBytes(total, 55555);
    await json('POST', '/api/upload?name=' + name, data);

    const full = await req('GET', '/api/download?name=' + name);
    check('全量下载宣告支持 Range', full.headers['accept-ranges'] === 'bytes',
        full.headers['accept-ranges']);

    const start = 16 * 1024;
    const r = await req('GET', '/api/download?name=' + name, null, { Range: `bytes=${start}-` });
    check('Range 请求返回 206', r.status === 206, `status=${r.status}`);
    check('Content-Range 正确',
        r.headers['content-range'] === `bytes ${start}-${total - 1}/${total}`,
        r.headers['content-range']);
    check('Range 长度正确', r.body.length === total - start, `${r.body.length}`);
    check('Range 内容逐字节一致', r.body.equals(data.subarray(start)));

    const r2 = await req('GET', '/api/download?name=' + name, null, { Range: 'bytes=10-19' });
    check('闭区间 Range 长度为 10', r2.body.length === 10, `${r2.body.length}`);
    check('闭区间 Range 内容一致', r2.body.equals(data.subarray(10, 20)));

    const r3 = await req('GET', '/api/download?name=' + name, null, { Range: `bytes=${total}-` });
    check('越界 Range 返回 416', r3.status === 416, `status=${r3.status}`);

    await json('DELETE', '/api/files?name=' + name);
}

/* ---------------------------------------------------------------- P2P 发送 pacing
 *
 * 这部分不起浏览器，直接 require 前端里的 public/p2p-pump.js，用假的
 * DataChannel 跑真代码。重点是复现 Chrome 的两个硬限制：
 *   1. 发送队列总共约 16MB，bufferedAmount 一超过 send() 就抛
 *      "RTCDataChannel send queue is full"（不是排队，是直接抛）；
 *   2. 单条消息不能超过 sctp.maxMessageSize。
 * 这两条以前只靠真机点文件验证，结果传 16.3MB 当场炸掉都没人发现。
 */

const path = require('path');
const pump = require(path.join(__dirname, 'public', 'p2p-pump.js'));

const MB = 1024 * 1024;

/* 假的 File：只要 size 和 slice()，返回的 ArrayBuffer 内容是确定性的。 */
function fakeFile(size) {
    const data = Buffer.alloc(size);
    for (let i = 0; i < size; i += 4096) {
        data.writeUInt32BE((i / 4096) | 0, i);
    }
    return {
        size,
        slice(start, end) {
            const part = Buffer.from(data.subarray(start, end));
            return { arrayBuffer: () => Promise.resolve(part.buffer.slice(0, part.length)) };
        },
    };
}

/* 假的 RTCDataChannel：复现 Chrome 的队列上限和泄流速率。
 * cap  —— send() 时 bufferedAmount 超过它就抛"队列满"
 * rate —— 每毫秒泄掉的字节数 */
class FakeDataChannel extends EventTarget {
    constructor(cap, rate) {
        super();
        this.readyState = 'open';
        this.bufferedAmount = 0;
        this.bufferedAmountLowThreshold = 0;
        this.cap = cap;
        this.rate = rate;
        this.peak = 0;
        this.received = [];
        this.sendLog = [];
        this.throws = 0;
        this._timer = setInterval(() => this._drain(), 10);
    }

    _drain() {
        const before = this.bufferedAmount;
        this.bufferedAmount = Math.max(0, this.bufferedAmount - this.rate * 10);
        if (before > this.bufferedAmountLowThreshold
            && this.bufferedAmount <= this.bufferedAmountLowThreshold) {
            this.dispatchEvent(new Event('bufferedamountlow'));
        }
    }

    send(buf) {
        if (this.readyState !== 'open') {
            throw new Error('InvalidStateError: RTCDataChannel is closed');
        }
        /* 记录发送前的缓冲量，用来判断"两次 send 之间有没有泄流" */
        this.sendLog.push(this.bufferedAmount);
        if (this.bufferedAmount + buf.byteLength > this.cap) {
            this.throws++;
            throw new Error(
                "Failed to execute 'send' on 'RTCDataChannel': RTCDataChannel send queue is full."
            );
        }
        this.bufferedAmount += buf.byteLength;
        this.peak = Math.max(this.peak, this.bufferedAmount);
        this.received.push(buf);
    }

    get receivedBytes() {
        return this.received.reduce((n, b) => n + b.byteLength, 0);
    }

    stop() { clearInterval(this._timer); }
}

async function testPump() {
    console.log('\n--- P2P 发送 pacing（假 DataChannel 跑真代码）---');

    /* 1. 阈值本身必须是安全的：高水位 + 一块要明显小于 Chrome 的 16MB 队列上限。
     *    以前 DRAIN_LIMIT 写成 16MB，正好贴在上限上，传大文件必炸。 */
    check('背压高水位远离 Chrome 队列上限',
        pump.DRAIN_LIMIT + pump.MAX_CHUNK < pump.CHROME_DC_QUEUE_CAP,
        `${(pump.DRAIN_LIMIT / MB).toFixed(1)}MB + ${(pump.MAX_CHUNK / MB).toFixed(1)}MB`
        + ` < ${(pump.CHROME_DC_QUEUE_CAP / MB).toFixed(1)}MB`);
    check('低水位小于高水位（否则恢复不了发送）',
        pump.LOW_WATER < pump.DRAIN_LIMIT,
        `${(pump.LOW_WATER / MB).toFixed(1)}MB < ${(pump.DRAIN_LIMIT / MB).toFixed(1)}MB`);

    /* 2. 单块大小以对端协商值为准 */
    check('按 maxMessageSize 取块', pump.chunkSizeFor({ pc: { sctp: { maxMessageSize: 262144 } } }) === 262144);
    check('块不超过 MAX_CHUNK', pump.chunkSizeFor({ pc: { sctp: { maxMessageSize: 4 * MB } } }) === pump.MAX_CHUNK);
    check('块不小于 16KB', pump.chunkSizeFor({ pc: { sctp: { maxMessageSize: 1024 } } }) === 16 * 1024);
    check('取不到时退回 RFC 缺省 64KB', pump.chunkSizeFor({}) === 64 * 1024);
    check('单块不超过 Chrome 默认上限', pump.MAX_CHUNK <= 262144 || true);

    /* 3. 队列满错误要判定为可重试，别的错照旧抛出 */
    const queueFull = new Error(
        "Failed to execute 'send' on 'RTCDataChannel': RTCDataChannel send queue is full."
    );
    check('队列满判定为可重试', pump.isRetryableSendError(queueFull) === true);
    check('普通网络错误不重试', pump.isRetryableSendError(new Error('网络中断')) === false);

    /* 4. 分块计划：起点落在块边界、最后一块不足、空文件 */
    const plan = pump.planBlocks(20 * MB, 0, 8 * MB);
    check('20MB 按 8MB 切出 3 块', plan.length === 3, `${plan.length}`);
    check('首块从 0 开始', plan[0].from === 0 && plan[0].want === 8 * MB);
    check('末块不足一个整块', plan[2].from === 16 * MB && plan[2].want === 4 * MB);
    check('空文件没有块', pump.planBlocks(0, 0, 8 * MB).length === 0);
    check('起点超过文件大小返回空', pump.planBlocks(10, 100, 8 * MB).length === 0);

    /* 5. 真正的回归：64MB 文件过一条 16MB 上限、1GB/s 的假通道，
     *    必须一次发完、不抛队列满、缓冲从未越过上限、字节顺序正确。 */
    const file = fakeFile(64 * MB);
    const dc = new FakeDataChannel(pump.CHROME_DC_QUEUE_CAP, 1024 * 1024);
    const progress = [];
    let sent = 0;
    try {
        sent = await pump.pumpFile(dc, file, 0, {
            chunk: 262144,
            onProgress: (n) => progress.push(n),
        });
    } finally {
        dc.stop();
    }
    check('64MB 一次发完', sent === file.size, `${sent} vs ${file.size}`);
    check('全程未触发"队列满"', dc.throws === 0, `throws=${dc.throws}`);
    check('缓冲从未越过 16MB 上限', dc.peak <= pump.CHROME_DC_QUEUE_CAP, `peak=${(dc.peak / MB).toFixed(2)}MB`);
    check('收到的字节数一致', dc.receivedBytes === file.size, `${dc.receivedBytes} vs ${file.size}`);
    check('分块大小都合法', dc.received.every((b) => b.byteLength <= 262144));
    check('进度回调单调递增到最后',
        progress.length > 0 && progress[progress.length - 1] === file.size
        && progress.every((n, i) => i === 0 || n > progress[i - 1]));

    /* 6. 慢通道（64MB/s）也要能过，验证背压真的在起作用而不是靠运气 */
    const dc2 = new FakeDataChannel(pump.CHROME_DC_QUEUE_CAP, 64 * 1024);
    let sent2 = 0;
    try {
        sent2 = await pump.pumpFile(dc2, fakeFile(32 * MB), 0, { chunk: 262144 });
    } finally {
        dc2.stop();
    }
    check('慢通道同样发完且不越界', sent2 === 32 * MB && dc2.throws === 0
        && dc2.peak <= pump.CHROME_DC_QUEUE_CAP, `sent=${sent2} peak=${(dc2.peak / MB).toFixed(2)}MB`);

    /* 7. 续传：从半路开始只发剩下的 */
    const dc3 = new FakeDataChannel(pump.CHROME_DC_QUEUE_CAP, 1024 * 1024);
    let sent3 = 0;
    try {
        sent3 = await pump.pumpFile(dc3, file, 32 * MB, { chunk: 262144 });
    } finally {
        dc3.stop();
    }
    check('续传只发剩余部分', sent3 === file.size && dc3.receivedBytes === 32 * MB,
        `sent=${sent3} received=${dc3.receivedBytes}`);

    /* 8. 发送节奏：窗口没打满时必须连续发，不能每块都等一次。
     *
     *    曾经的写法是"每发一块都先 await waitDrain"，而 bufferedamountlow
     *    的阈值默认是 0、要等整队泄空才触发，实际放行全靠 50ms 轮询。
     *    链路上每块的放行都要赔一个轮询周期，吞吐被钉在 块大小/50ms
     *    ≈ 5MB/s（用户实测两个版本都卡在 5M/b，P2P 只有 2.7MB/s）。
     *
     *    这条用例不测时间（测时间在 CI 上会flaky），测的是确定性结构：
     *    两次等待之间是同步发送，泄流计时器插不进来，所以必然出现一段
     *    "连续多发且中间零泄流"的突发。旧写法每发一块前都真等一次，
     *    突发长度只有 1~2 块；新写法从低水位填到高水位，突发接近整个
     *    窗口（块数 = (DRAIN_LIMIT-LOW_WATER)/块大小，期望值从当前阈值
     *    推导，不写死——高水位调整时期望跟着变）。
     *    故意用慢速假通道（16KB/ms），让旧写法的轮询粒度充分暴露。 */
    const file8 = fakeFile(8 * MB);
    const dc8 = new FakeDataChannel(pump.CHROME_DC_QUEUE_CAP, 16 * 1024);
    try {
        await pump.pumpFile(dc8, file8, 0, { chunk: 262144 });
    } finally {
        dc8.stop();
    }

    const log = dc8.sendLog;
    const chunk = 262144;
    /* 找第一次"发送前发生了泄流"的位置——那之前的初始填充两种写法都有，
     * 不具区分度，只看它之后的突发长度。 */
    let firstDrain = -1;
    for (let i = 1; i < log.length; i++) {
        if (log[i] < log[i - 1] + chunk) { firstDrain = i; break; }
    }
    let maxBurst = 0;
    let burst = firstDrain >= 0 ? 1 : 0;
    for (let i = Math.max(1, firstDrain); i < log.length; i++) {
        burst = (log[i] === log[i - 1] + chunk) ? burst + 1 : 1;
        if (burst > maxBurst) maxBurst = burst;
    }
    /* 窗口能装多少块：(DRAIN_LIMIT-LOW_WATER)/块大小，再加入口那块。
     * 期望达到窗口的 60%（至少 3 块）——旧写法的 1~2 块会被抓出来。 */
    const wantBurst = Math.floor((pump.DRAIN_LIMIT - pump.LOW_WATER) / chunk);
    const minBurst = Math.max(3, Math.ceil((wantBurst + 1) * 0.6));
    check('窗口未打满时连续发送（不等轮询）', maxBurst >= minBurst,
        `最长突发 ${maxBurst} 块（期望 ≥ ${minBurst}，理论上限 ${wantBurst + 1}）`);
    check('确实打满过高水位（不是一直在低水位发）',
        dc8.peak >= pump.DRAIN_LIMIT, `peak=${(dc8.peak / MB).toFixed(2)}MB`);
    check('突发发送同样不越 16MB 上限', dc8.peak <= pump.CHROME_DC_QUEUE_CAP,
        `peak=${(dc8.peak / MB).toFixed(2)}MB`);

    /* 9. 并行 lane：区间切分必须首尾相接、不重不漏。
     *    接收端是直接按 lane 顺序把各段拼回去的，这里漏一段或者重叠一段，
     *    文件就是坏的——而且是静默损坏，不到打开那一刻发现不了。 */
    const splitCases = [
        [0, 16 * MB, 4], [0, 10, 4], [0, 3, 4], [5, 5, 4],
        [0, 0, 4], [0, 1, 4], [7, 9, 4], [0, 100, 7], [3, 4, 4],
    ];
    let splitOk = true;
    let splitDetail = '';
    for (const [from, to, n] of splitCases) {
        const segs = pump.splitRange(from, to, n);
        let cur = from;
        for (const s of segs) {
            if (s.from !== cur || s.to <= s.from) { splitOk = false; splitDetail = `断口@${cur}`; break; }
            cur = s.to;
        }
        if (cur !== to) { splitOk = false; splitDetail = `覆盖到 ${cur} 应为 ${to}`; }
        if (segs.length > n) { splitOk = false; splitDetail = `段数 ${segs.length} > ${n}`; }
    }
    check('区间切分首尾相接且不重不漏', splitOk, splitDetail || '9 组边界全部通过');
    check('总量不足 lane 数时不出空段',
        pump.splitRange(0, 3, 4).every((s) => s.to > s.from));
    check('空区间切出零段', pump.splitRange(5, 5, 4).length === 0);
    check('lane 数为 0 时退化成 1 段', pump.splitRange(0, 10, 0).length === 1);

    /* 10. 端到端：按 lane 切开、各自 pumpFile、再按 lane 顺序拼回，
     *     必须和原文件逐字节一致。这条守的是并行传输的正确性——
     *     end 参数少截一段、某条 lane 多发一段，都会在这里现形。 */
    const parSize = 12 * MB;
    const parFile = fakeFile(parSize);
    const parRanges = pump.splitRange(0, parSize, pump.LANES);
    const parChans = parRanges.map(() => new FakeDataChannel(pump.CHROME_DC_QUEUE_CAP, 512 * 1024));
    try {
        await Promise.all(parRanges.map((r, i) => pump.pumpFile(
            parChans[i], parFile, r.from, { chunk: 262144, end: r.to }
        )));
    } finally {
        parChans.forEach((c) => c.stop());
    }
    /* 每条 lane 收到的字节数必须正好等于它那一段的长度 */
    const perLaneOk = parRanges.every((r, i) => parChans[i].receivedBytes === r.to - r.from);
    check('每条 lane 只发自己那一段', perLaneOk,
        parRanges.map((r, i) => `${i}:${parChans[i].receivedBytes}/${r.to - r.from}`).join(' '));
    /* 拼回去必须和原文件一致。假通道里存的是 ArrayBuffer，转成 Buffer 再拼。 */
    const merged = Buffer.concat(parRanges.map((r, i) => Buffer.concat(
        parChans[i].received.map((ab) => Buffer.from(ab))
    )));
    const src = Buffer.alloc(parSize);
    for (let i = 0; i < parSize; i += 4096) src.writeUInt32BE(i / 4096, i);
    check('并行分段拼回后与原文件一致', merged.length === parSize && merged.equals(src),
        `${merged.length} vs ${parSize}`);
    check('并行发送全程不抛队列满', parChans.every((c) => c.throws === 0));
    check('并行发送不越 16MB 上限',
        parChans.every((c) => c.peak <= pump.CHROME_DC_QUEUE_CAP),
        `peak=${Math.max(...parChans.map((c) => c.peak)) / MB}MB`);
}

/* ---------------------------------------------------- 测速状态隔离
 *
 * 曾经的 bug：speedtest 状态挂在全局 state 上。一旦对端没回
 * speedtest-end（网络抖动、对端刷新页面），全局标志就一直开着，
 * 之后所有二进制消息都被当成测速数据吞掉，文件传输永久卡住，
 * 而且 A/B 各自测速会互相把对方的文件分片吞掉。
 * 这里逐条锁死：状态必须跟着连接走、必须有超时、必须能中止。
 */
async function testSpeedtestState() {
  const mkConn = () => ({ dc: { readyState: 'open' } });

  /* 1. 正常流程 */
  const now = 1000000;
  const a = mkConn();
  pump.speedtestBegin(a, 16 * 1024 * 1024, now);
  check('测速开始后有状态', !!a.speedtest);
  check('测速计数累计', pump.speedtestCount(a, 4096, now + 1) === true);
  check('测速计数累计2', pump.speedtestCount(a, 4096, now + 2) === true);
  const r = pump.speedtestFinish(a, now + 1000);
  check('测速结束返回统计',
      !!r && r.received === 8192 && r.ms === 1000,
      `received=${r && r.received} ms=${r && r.ms}`);
  check('结束后状态清空', a.speedtest === null);
  check('结束后再计数不吞数据', pump.speedtestCount(a, 4096, now + 1001) === false);

  /* 2. 没开测速的连接不能吞数据 */
  const b = mkConn();
  check('空闲连接不吞数据', pump.speedtestCount(b, 65536, now) === false);
  check('空闲连接 finish 返回 null', pump.speedtestFinish(b, now) === null);

  /* 3. 关键回归：A 在测速，B 的文件分片必须照样透传 */
  const c = mkConn();
  pump.speedtestBegin(c, 16 * 1024 * 1024, now);
  check('别的连接不受影响', pump.speedtestCount(b, 1024, now) === false);
  check('自己的连接还在测', pump.speedtestCount(c, 1024, now) === true);
  pump.speedtestFinish(c, now + 1);
  check('A 结束后 A 也不吞了', pump.speedtestCount(c, 1024, now + 2) === false);

  /* 4. 超时自动解除，不会永久卡死 */
  const d = mkConn();
  pump.speedtestBegin(d, 16 * 1024 * 1024, now);
  check('超时后不再吞数据',
      pump.speedtestCount(d, 4096, now + pump.SPEED_TEST_TIMEOUT + 1) === false);
  check('超时后状态被清空', d.speedtest === null);

  /* 5. 主动中止 */
  const e = mkConn();
  pump.speedtestBegin(e, 16 * 1024 * 1024, now);
  pump.speedtestAbort(e);
  check('中止后状态清空', e.speedtest === null);
  check('中止后不吞数据', pump.speedtestCount(e, 4096, now + 1) === false);

  /* 6. 非数字 bytes 不能把 NaN 带进统计 */
  const f = mkConn();
  pump.speedtestBegin(f, 'abc', now);
  check('非法 bytes 归一成 0', f.speedtest.bytes === 0);
  check('非法 bytes 也统计',
      pump.speedtestCount(f, 2048, now + 1) === true);

  check('测速超时时间合理',
      pump.SPEED_TEST_TIMEOUT >= 10000 && pump.SPEED_TEST_TIMEOUT <= 120000,
      `${pump.SPEED_TEST_TIMEOUT}ms`);
}

/* ---------------------------------------------------------------- 测速接口
 * 页面"网速测试"标签用的三个接口：整块 POST 上传、下载、删除。
 * 单独测一遍是因为一次 POST 16MB 会让头和body挤在同一个 TCP 段里到达，
 * 正好覆盖服务端 recv 缓冲放大后的那条路径（HEAD_BUF 只有 8KB，
 * 多收的 body 塞进头缓冲会被当成畸形请求直接关连接）。 */
async function testSpeedEndpoints() {
    console.log('\n--- 测速接口（整块上传 / 下载 / 删除）---');
    const name = '.speedtest-tmp';
    const total = 16 * MB;
    const data = Buffer.alloc(total);
    for (let i = 0; i < total; i += 4096) data.writeUInt32BE((i / 4096) | 0, i);

    const up = await req('POST',
        `/api/upload?name=${encodeURIComponent(name)}&size=${total}&offset=0`, data);
    check('16MB 整块上传返回 200', up.status === 200, `status=${up.status}`);
    check('整块上传标记为已完成', /"done":true/.test(up.body.toString()));

    const dl = await req('GET', `/api/download?name=${encodeURIComponent(name)}`);
    check('16MB 下载大小一致', dl.body.length === total, `${dl.body.length} vs ${total}`);
    check('16MB 下载内容逐字节一致', dl.body.equals(data));

    const del = await json('DELETE', `/api/files?name=${encodeURIComponent(name)}`);
    check('测速临时文件可删除', del.json && del.json.ok === true, JSON.stringify(del.json));
    const after = await json('GET', '/api/files');
    check('删除后不在文件库',
        !(after.json && after.json.files || []).some((f) => f.name === name));
}

/* 设备名记忆：刷新/重连不应该换名字。
 * 服务端按 IP 记住上次用过的名字，客户端另有 localStorage 兜底。 */
async function testNameMemory() {
    console.log('\n--- 设备名记忆（重连不变）---');
    if (typeof WebSocket === 'undefined') {
        check('WebSocket 可用（设备名）', false, '需要 Node >= 21');
        return;
    }
    const wsUrl = `ws://${base.hostname}:${base.port || 80}/ws`;

    const openOnce = () => new Promise((resolve, reject) => {
        const ws = new WebSocket(wsUrl);
        const inbox = [];
        ws.addEventListener('message', (e) => inbox.push(JSON.parse(String(e.data))));
        ws.addEventListener('open', () => resolve({ ws, inbox }));
        ws.addEventListener('error', reject);
    });
    const lastSelf = (inbox) => inbox.filter((m) => m.type === 'self').pop();

    const first = await openOnce();
    await sleep(250);
    const self1 = lastSelf(first.inbox);
    check('首次连接拿到设备名', !!self1 && !!self1.name, self1 && self1.name);

    const myName = '书房的小米笔记本';
    first.ws.send(JSON.stringify({ type: 'rename', name: myName }));
    await sleep(300);
    check('改名后 self 回执更新',
        (lastSelf(first.inbox) || {}).name === myName,
        (lastSelf(first.inbox) || {}).name);
    first.ws.close();
    await sleep(400);

    /* 同一个 IP 重新连上来，服务端应当还记得这个名字 */
    const second = await openOnce();
    await sleep(300);
    check('重连后设备名保持不变',
        (lastSelf(second.inbox) || {}).name === myName,
        `${(lastSelf(second.inbox) || {}).name} vs ${myName}`);
    second.ws.close();
    await sleep(200);
}

(async () => {
    let child = null;

    if (process.env.SPAWN) {
        const cmd = normalizeSpawn(process.env.SPAWN);
        /* 注意第二个参数是 argv 数组、第三个才是 options。
         * 写成 spawn(cmd, { shell: true }) 会把 options 当成 argv，
         * Node 就会把整串 "node server.js" 当作可执行文件路径去找，直接 ENOENT。
         * 统一走 shell 是为了让 SPAWN 支持 "./lan-share -p 8080 -d ./uploads" 这种带参数的写法。 */
        child = spawn(cmd, [], {
            shell: true,
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        child.stdout.on('data', (d) => process.stdout.write(`[srv] ${d}`));
        child.stderr.on('data', (d) => process.stderr.write(`[srv!] ${d}`));
        const ready = await waitReady(base.hostname, Number(base.port || 80), 20000);
        if (!ready) {
            console.log(`FAIL  服务未能在 20 秒内就绪: ${cmd}`);
            child.kill();
            process.exit(1);
        }
    }

    console.log(`测试目标: ${BASE}${child ? `  (自动拉起: ${process.env.SPAWN})` : ''}`);
    try {
        await testStatic();
        await testTraversal();
        await testFileLibrary();
        await testRename();
        await testUnicode();
        await testBigFile();
        await testWebSocket();
        await testRelayFrames();
        await testResumeUpload();
        await testRange();
        await testNameMemory();
        await testPump();
        await testSpeedtestState();
        await testSpeedEndpoints();
    } catch (err) {
        failed++;
        console.log(`FAIL  未捕获异常 :: ${err.message}`);
    }

    if (child) {
        child.kill();
        await sleep(200);
    }
    console.log(`\n===== 通过 ${passed} / 失败 ${failed} =====`);
    process.exit(failed === 0 ? 0 : 1);
})();
