#!/usr/bin/env node
'use strict';
/* 兜底数据通道（public/relay-channel.js）单测。不起浏览器、不需要服务端——
 * 帧格式、分块上限、背压窗口这些边界靠真机点文件是点不出来的。
 *
 * 用法： node tools/test-relay.js
 */

const path = require('path');
const LSRelay = require(path.join(__dirname, '..', 'public', 'relay-channel.js'));
const pump = require(path.join(__dirname, '..', 'public', 'p2p-pump.js'));

let passed = 0;
let failed = 0;

function check(name, cond, extra) {
    if (cond) { passed++; console.log(`PASS  ${name}${extra ? ' :: ' + extra : ''}`); }
    else { failed++; console.log(`FAIL  ${name}${extra ? ' :: ' + extra : ''}`); }
}

/* ---- 1. 帧格式：封装 → 解封必须还原出同样的 from 和 payload ---- */
(function testFrameRoundTrip() {
    const peer = 'ab12cd34';
    const payload = new Uint8Array(1000);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    const frame = LSRelay.prefixRelayFrame(peer, payload.buffer);
    const back = LSRelay.parseRelayFrame(frame);
    check('帧解封拿到对端 id', back && back.from === peer, back ? back.from : 'null');
    check('帧解封拿到完整载荷',
        back && new Uint8Array(back.payload).length === payload.length,
        back ? String(new Uint8Array(back.payload).length) : 'null');
    const eq = back && new Uint8Array(back.payload).every((v, i) => v === payload[i]);
    check('帧解封字节一致', !!eq);

    /* 长 id（UUID 长度）也要能过：1 字节长度字段最多 255 */
    const longId = 'x'.repeat(200);
    const f2 = LSRelay.prefixRelayFrame(longId, payload.buffer);
    const b2 = LSRelay.parseRelayFrame(f2);
    check('长 id 也能正确解封', b2 && b2.from === longId && new Uint8Array(b2.payload).length === 1000);

    /* 畸形帧不能抛异常（网络对端可能发任何东西） */
    let threw = false;
    try {
        LSRelay.parseRelayFrame(new Uint8Array([0]).buffer);
        LSRelay.parseRelayFrame(new Uint8Array([9, 1, 2]).buffer);
    } catch (_) { threw = true; }
    check('畸形帧不抛异常', !threw);
})();

/* ---- 2. 分块上限：不能超过服务端单帧上限 ---- */
(function testChunkCap() {
    const sink = { frames: [], bufferedAmount: 0, send(d) { this.frames.push(d); } };
    const ch = new LSRelay.RelayChannel('peer', sink);
    /* 故意发一块 256KB——服务端 WS_BUF 是 128KB，整帧超限会被直接丢掉，
     * 表现是进度条卡死不动而没有任何报错。这里必须被切成 ≤64KB 的小块。 */
    const big = new Uint8Array(256 * 1024);
    ch.send(big.buffer);
    const sizes = sink.frames.map((f) => f.byteLength);
    const maxFrame = Math.max.apply(null, sizes);
    check('大块被切成小帧', sink.frames.length > 1, `${sink.frames.length} 帧`);
    check('单帧不超过 64KB+id 头', maxFrame <= LSRelay.RELAY_CHUNK + 64, `最大帧 ${maxFrame} 字节`);
    check('切块后字节总数不变',
        sizes.reduce((a, b) => a + b, 0) === 256 * 1024 + sink.frames.length * (1 + 4),
        `合计 ${sizes.reduce((a, b) => a + b, 0)}`);
})();

/* ---- 3. 背压：inflight 必须计入 bufferedAmount，收到 ack 才泄 ---- */
(function testBackpressure() {
    const sink = { bufferedAmount: 0, send() {} };
    const ch = new LSRelay.RelayChannel('peer', sink);
    const blk = new Uint8Array(LSRelay.RELAY_CHUNK);
    for (let i = 0; i < 40; i++) ch.send(blk.buffer.slice(0));
    const peak = ch.bufferedAmount;
    check('未确认字节计入 bufferedAmount', peak === 40 * LSRelay.RELAY_CHUNK, String(peak));
    ch.ack(10 * LSRelay.RELAY_CHUNK);
    check('ack 后 bufferedAmount 下降',
        ch.bufferedAmount === 30 * LSRelay.RELAY_CHUNK, String(ch.bufferedAmount));
    ch.ack(999 * 1024 * 1024);
    check('ack 不会把缓冲压成负数', ch.bufferedAmount === 0, String(ch.bufferedAmount));
})();

/* ---- 4. 用真的 pumpFile 跑一遍：字节必须完整、且不会卡死 ---- */
async function testPumpOverRelay() {
    const SIZE = 6 * 1024 * 1024;
    const data = Buffer.alloc(SIZE);
    for (let i = 0; i < SIZE; i += 4096) data.writeUInt32BE((i / 4096) | 0, i);

    const file = {
        size: SIZE,
        slice(start, end) {
            const part = Buffer.from(data.subarray(start, end));
            return { arrayBuffer: () => Promise.resolve(part.buffer.slice(0, part.length)) };
        },
    };

    const sink = { got: 0, bufferedAmount: 0 };
    const ch = new LSRelay.RelayChannel('peer', sink);
    /* 模拟接收端的确认节奏：攒够 RELAY_ACK_EVERY 就回一次 ack。
     * 不这么模拟的话 inflight 只增不减，pumpFile 会卡在 waitDrain 上——
     * 真实链路里也是靠这个 ack 泄流的，所以这一段本身就是背压的回归测试。 */
    let pending = 0;
    sink.send = function (frame) {
        const parsed = LSRelay.parseRelayFrame(frame);
        if (!parsed) return;
        const u = new Uint8Array(parsed.payload);
        sink.got += u.length;
        pending += u.length;
        if (pending >= LSRelay.RELAY_ACK_EVERY) { ch.ack(pending); pending = 0; }
    };

    let last = 0;
    await pump.pumpFile(ch, file, 0, {
        chunk: pump.chunkSizeFor({ relay: true }),
        onProgress: (sent) => { last = sent; },
    });
    check('pumpFile 走兜底通道发满全程', last === SIZE, `${last}/${SIZE}`);
    check('兜底通道单块取 64KB', pump.chunkSizeFor({ relay: true }) === 64 * 1024,
        String(pump.chunkSizeFor({ relay: true })));
}

/* ---- close 必须派发 close 事件 ----
 * 上层（app.js）就是靠这个事件把"这条通道死了"翻译成「已中断（N MB），可续传」
 * 的。不派发的话，信令 WS 断掉之后接收中的传输会永远停在"接收中"：既等不到
 * lane-end 也没有失败提示，用户连续传入口都拿不到。 */
(function testCloseEmitsEvent() {
    const ch = new LSRelay.RelayChannel('peer1', { send() {}, bufferedAmount: 0 });
    let closed = 0;
    ch.addEventListener('close', () => { closed++; });
    ch.close();
    check('close() 派发 close 事件', closed === 1, String(closed));
    ch.close();
    check('重复 close() 只派发一次', closed === 1, String(closed));
    check('close() 之后 readyState 为 closed', ch.readyState === 'closed', ch.readyState);
    let threw = false;
    try { ch.send('x'); } catch (_) { threw = true; }
    check('关闭后 send 抛错（上层据此收尾而不是干等）', threw);
})();

(async () => {
    try {
        await testPumpOverRelay();
    } catch (err) {
        failed++;
        console.log(`FAIL  pumpFile 走兜底通道 :: ${err.message}`);
    }
    console.log(`\n===== 兜底通道单测：通过 ${passed} / 失败 ${failed} =====`);
    process.exit(failed === 0 ? 0 : 1);
})();
