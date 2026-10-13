'use strict';
/* 服务端吞吐基准：分别测 C 版和 Node 版的上传/下载，看瓶颈在不在服务端。
 * 用法：node bench.js <engine: c|node> [port]
 */
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const engine = process.argv[2] || 'c';
const PORT = Number(process.argv[3] || 8123);
const MB = 1024 * 1024;
const TOTAL = 64 * MB;
const CHUNK = 4 * MB;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function waitPort(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    return (async function loop() {
        while (Date.now() < deadline) {
            const ok = await new Promise((resolve) => {
                const r = http.request({ host: '127.0.0.1', port: PORT, method: 'GET', path: '/api/info' }, (res) => {
                    res.resume(); resolve(res.statusCode === 200);
                });
                r.on('error', () => resolve(false));
                r.end();
            });
            if (ok) return true;
            await sleep(200);
        }
        return false;
    })();
}

function pseudoBuf(size) {
    const b = Buffer.allocUnsafe(size);
    for (let i = 0; i < size; i += 4096) b.writeUInt32BE((i / 4096) | 0, i);
    return b;
}

function post(name, size, offset, body) {
    return new Promise((resolve, reject) => {
        const req = http.request({
            host: '127.0.0.1', port: PORT, method: 'POST',
            path: `/api/upload?name=${encodeURIComponent(name)}&size=${size}&offset=${offset}`,
            headers: { 'Content-Type': 'application/octet-stream', 'Content-Length': body.length },
        }, (res) => {
            let d = '';
            res.on('data', (c) => { d += c; });
            res.on('end', () => {
                try { resolve({ status: res.statusCode, body: d ? JSON.parse(d) : {} }); }
                catch (_) { resolve({ status: res.statusCode, body: {} }); }
            });
        });
        req.on('error', reject);
        req.end(body);
    });
}

function download(name) {
    return new Promise((resolve, reject) => {
        http.get({
            host: '127.0.0.1', port: PORT,
            path: `/api/download?name=${encodeURIComponent(name)}`,
        }, (res) => {
            const bufs = [];
            let n = 0;
            res.on('data', (c) => { bufs.push(c); n += c.length; });
            res.on('end', () => resolve({ status: res.statusCode, bytes: n }));
        }).on('error', reject);
    });
}

(async () => {
    const upDir = path.join(os.tmpdir(), `bench-${engine}-${PORT}`);
    fs.rmSync(upDir, { recursive: true, force: true });
    fs.mkdirSync(upDir, { recursive: true });

    let child = null;
    if (engine === 'c') {
        const exe = path.join(__dirname, 'lan_share.exe');
        child = spawn(exe, ['-p', String(PORT), '-d', upDir, '-w', path.join(__dirname, 'public')], { stdio: 'ignore' });
    } else {
        child = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
            stdio: 'ignore',
            env: { ...process.env, PORT: String(PORT), UPLOAD_DIR: upDir },
        });
    }

    if (!await waitPort(15000)) {
        console.log('服务未就绪');
        child.kill();
        process.exit(1);
    }

    const name = `bench-${engine}.bin`;
    const data = pseudoBuf(TOTAL);
    const results = {};

    /* ---- 上传：和前端一样 4MB 一块串行 ---- */
    let t0 = Date.now();
    for (let off = 0; off < TOTAL; off += CHUNK) {
        const end = Math.min(off + CHUNK, TOTAL);
        const r = await post(name, TOTAL, off, data.subarray(off, end));
        if (r.status !== 200) { console.log('上传失败', r); break; }
    }
    results.upload = (TOTAL / MB) / ((Date.now() - t0) / 1000);

    /* ---- 上传（整块一次 POST，测协议本身的开销）---- */
    const name2 = `bench-${engine}-one.bin`;
    t0 = Date.now();
    const r2 = await post(name2, TOTAL, 0, data);
    results.uploadOneShot = (TOTAL / MB) / ((Date.now() - t0) / 1000);

    /* ---- 下载 ---- */
    t0 = Date.now();
    const dl = await download(name);
    results.download = (dl.bytes / MB) / ((Date.now() - t0) / 1000);
    results.downloadBytes = dl.bytes;

    console.log(`\n===== ${engine} 版服务端吞吐 =====`);
    console.log(`  上传（4MB 分块串行）  ${results.upload.toFixed(1)} MB/s`);
    console.log(`  上传（整块一次 POST）  ${results.uploadOneShot.toFixed(1)} MB/s`);
    console.log(`  下载                  ${results.download.toFixed(1)} MB/s  (${results.downloadBytes} 字节)`);
    console.log(`  平台 ${os.type()} ${os.arch()}  CPU ${os.cpus()[0].model.trim()}`);

    child.kill();
    fs.rmSync(upDir, { recursive: true, force: true });
    process.exit(0);
})();
