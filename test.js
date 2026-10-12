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
function req(method, path, body) {
    return new Promise((resolve, reject) => {
        const r = http.request(
            {
                host: base.hostname,
                port: base.port || 80,
                method,
                path,
                headers: body
                    ? { 'Content-Length': Buffer.byteLength(body) }
                    : {},
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

const json = async (method, path, body) => {
    const r = await req(method, path, body);
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

(async () => {
    let child = null;

    if (process.env.SPAWN) {
        const cmd = process.env.SPAWN;
        const shell = process.platform === 'win32';
        child = spawn(cmd, { shell, stdio: ['ignore', 'pipe', 'pipe'] });
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
