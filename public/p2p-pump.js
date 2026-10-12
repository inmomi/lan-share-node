/* LAN Share — WebRTC DataChannel 发送 pacing
 *
 * 单独拆一个文件（而不是塞在 app.js 里）是为了能在 Node 里直接 require 做单测：
 * 背压阈值、分块边界、16MB 硬上限这些边界条件靠真机点是点不出来的。
 *
 * 三条硬约束，改动前先读：
 *   1. Chrome 的 DataChannel 发送队列总共只有约 16MB，bufferedAmount 一超过，
 *      send() 不是继续排队而是直接抛
 *        "RTCDataChannel send queue is full"
 *      （PeerJS issue #291 实测，本机 16.3MB 处必现）。所以高水位必须离它足够远。
 *   2. 单条消息不能超过对端协商的 RTCSctpTransport.maxMessageSize
 *      （Chrome 默认 262144，RFC 8841 缺省 64KB），超了同样抛错。
 *   3. 不要每 256KB 就 file.slice().arrayBuffer() 一次：每次都走一遍 Blob IPC，
 *      读延迟会整串地排在发送循环前面，带宽直接被读速卡住。
 */
(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    else root.LSPump = api;
})(typeof self !== 'undefined' ? self : globalThis, function () {
    'use strict';

    /* Chrome DataChannel 发送队列的实际上限。不是规范里写死的值，是实测值，
     * 不同版本可能略有出入，所以阈值要离它足够远，不要贴边。 */
    const CHROME_DC_QUEUE_CAP = 16 * 1024 * 1024;

    /* 单块上限。Chrome 会把它压到 sctp.maxMessageSize（默认 256KB），
     * 允许更大的浏览器可以吃到更大的块。 */
    const MAX_CHUNK = 1024 * 1024;
    /* 背压：缓冲超过 DRAIN_LIMIT 就停发，等泄到 LOW_WATER 以下再继续。
     * LOW_WATER 同时也是 bufferedAmountLowThreshold。 */
    const DRAIN_LIMIT = 4 * 1024 * 1024;
    const LOW_WATER = 1024 * 1024;
    /* 一次从磁盘读多少字节，再在内存里切成 MAX_CHUNK。
     * 必须远大于 MAX_CHUNK，否则省不掉 IPC 次数；也不必太大。 */
    const READ_BLOCK = 8 * 1024 * 1024;

    /* 静态自检：高水位加上一块必须明显小于 Chrome 的队列上限，
     * 否则 send() 会在传输中途抛"队列满"。这条断言挡的是"把阈值往上调"这类改动。 */
    if (DRAIN_LIMIT + MAX_CHUNK >= CHROME_DC_QUEUE_CAP) {
        throw new Error('LSPump: DRAIN_LIMIT 离 Chrome 发送队列上限太近，send() 会抛队列满');
    }
    if (LOW_WATER >= DRAIN_LIMIT) {
        throw new Error('LSPump: LOW_WATER 必须小于 DRAIN_LIMIT，否则恢复不了发送');
    }

    /** 把 [startOffset, fileSize) 切成若干大块，返回 [{ from, want }]。
     *  纯函数，边界情况（起点在块边界、最后一块不足、空文件）都在这里收敛。 */
    function planBlocks(fileSize, startOffset, blockSize) {
        const plan = [];
        let from = Math.max(0, Math.min(Number(startOffset) || 0, fileSize));
        while (from < fileSize) {
            const want = Math.min(blockSize, fileSize - from);
            if (want <= 0) break;
            plan.push({ from, want });
            from += want;
        }
        return plan;
    }

    /** 读出从 from 开始的那一大块。 */
    function readBlockAt(file, from) {
        const want = Math.min(READ_BLOCK, file.size - from);
        return file.slice(from, from + want).arrayBuffer();
    }

    /** 单条消息大小，以对端协商出来的 maxMessageSize 为准。
     *  取不到时退回 RFC 8841 的缺省 64KB，不要想当然按本端能力发大块。 */
    function chunkSizeFor(conn) {
        const cap = conn && conn.pc && conn.pc.sctp ? conn.pc.sctp.maxMessageSize : 0;
        if (!cap || !Number.isFinite(cap)) return 64 * 1024;
        return Math.max(16 * 1024, Math.min(MAX_CHUNK, cap));
    }

    /** send() 抛的是不是"队列满 / 通道已关"这类等一等就能重发的错。 */
    function isRetryableSendError(err) {
        const msg = String((err && err.message) || err || '');
        return /queue is full|InvalidState|closed/i.test(msg);
    }

    /** 等发送队列泄到 limit 以下。target 默认 DRAIN_LIMIT；
     *  send() 抛"队列满"之后要用更保守的 LOW_WATER 再等一次，否则重发还是满的。
     *  三种触发都要：bufferedamountlow 事件（正常路径）、50ms 轮询（有的浏览器
     *  不保证派发这个事件）、以及一个硬超时兜底，别把传输永久卡死。 */
    function waitDrain(dc, target) {
        const limit = target || DRAIN_LIMIT;
        if (dc.bufferedAmount <= limit) return Promise.resolve();
        return new Promise((resolve) => {
            let done = false;
            let poll = null;
            const finish = () => {
                if (done) return;
                done = true;
                dc.removeEventListener('bufferedamountlow', finish);
                if (poll) clearInterval(poll);
                resolve();
            };
            dc.addEventListener('bufferedamountlow', finish);
            poll = setInterval(() => {
                if (dc.bufferedAmount <= limit) finish();
            }, 50);
            setTimeout(finish, 15000);
        });
    }

    /**
     * 把 file 从 startOffset 开始通过 dc 发完，返回实际发出去的字节数。
     *
     * 抽成独立函数（而不是留在 app.js 的 sendOne 里）是为了能用假 DataChannel
     * 在 Node 里跑完整回归：假的 dc 只要复现 Chrome 那 16MB 队列上限和泄流速率，
     * 就能验证"传 64MB 不抛队列满、字节数对、顺序对、续传起点对"。
     * 这些边界靠真机点文件是点不出来的。
     */
    async function pumpFile(dc, file, startOffset, hooks) {
        hooks = hooks || {};
        const chunk = hooks.chunk || 64 * 1024;
        const onProgress = hooks.onProgress || function () {};

        let offset = Math.max(0, Math.min(Number(startOffset) || 0, file.size));
        onProgress(offset);

        let block = await readBlockAt(file, offset);
        let blockPos = 0;
        let prefetch = (offset + block.byteLength < file.size)
            ? readBlockAt(file, offset + block.byteLength)
            : null;

        while (offset < file.size) {
            if (blockPos >= block.byteLength) {
                block = prefetch ? await prefetch : await readBlockAt(file, offset);
                blockPos = 0;
                prefetch = (offset + block.byteLength < file.size)
                    ? readBlockAt(file, offset + block.byteLength)
                    : null;
            }

            const buf = block.slice(blockPos, Math.min(blockPos + chunk, block.byteLength));
            blockPos += buf.byteLength;
            offset += buf.byteLength;

            await waitDrain(dc);
            if (dc.readyState !== 'open') throw new Error('连接中断');

            try {
                dc.send(buf);
            } catch (err) {
                /* Chrome 发送队列约 16MB 是硬上限，超了不是排队而是直接抛。
                 * 不能把整次传输判死：等队列泄到低水位再把这一块重发一次。 */
                if (!isRetryableSendError(err)) throw err;
                await waitDrain(dc, LOW_WATER);
                if (dc.readyState !== 'open') throw new Error('连接中断');
                dc.send(buf);
            }

            onProgress(offset);
        }
        return offset;
    }

    return {
        CHROME_DC_QUEUE_CAP,
        MAX_CHUNK, DRAIN_LIMIT, LOW_WATER, READ_BLOCK,
        planBlocks, readBlockAt, chunkSizeFor, isRetryableSendError, waitDrain, pumpFile,
    };
});
