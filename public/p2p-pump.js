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
    /* 接收端测速状态的硬超时。远大于正常测速耗时（16MB 即使只有 1MB/s 也就
     * 16 秒），但又不能无限——见 speedtestCount 里的说明。 */
    const SPEED_TEST_TIMEOUT = 60000;

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
     *  不保证派发这个事件）、以及一个硬超时兜底，别把传输永久卡死。
     *
     *  注意 bufferedAmountLowThreshold 默认是 0：不显式设置的话，
     *  bufferedamountlow 要等整队泄空才触发，50ms 轮询就成了实际放行手段——
     *  每轮最多放行一块，吞吐被钉死在 块大小/50ms（256KB 时约 5MB/s），
     *  链路再快也提不上去。所以等待前先把阈值设成 limit。 */
    function waitDrain(dc, target) {
        const limit = target || DRAIN_LIMIT;
        if (dc.bufferedAmount <= limit) return Promise.resolve();
        return new Promise((resolve) => {
            let done = false;
            let poll = null;
            let hard = null;
            const finish = () => {
                if (done) return;
                done = true;
                dc.removeEventListener('bufferedamountlow', finish);
                if (poll) clearInterval(poll);
                if (hard) clearTimeout(hard);
                resolve();
            };
            /* 先挂监听再改阈值，避开"改完阈值事件已经派发完"的竞态 */
            dc.addEventListener('bufferedamountlow', finish);
            try {
                if (dc.bufferedAmountLowThreshold !== limit) {
                    dc.bufferedAmountLowThreshold = limit;
                }
            } catch (_) { /* 个别实现该属性只读，靠轮询兜底 */ }
            poll = setInterval(() => {
                if (dc.bufferedAmount <= limit) finish();
            }, 50);
            hard = setTimeout(finish, 15000);
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

            /* 发送节奏：没满就连续发，把 1MB~4MB 的在途窗口始终填着；
             * 只有真的到高水位了才等一次，等到低水位再接着填。
             *
             * 曾经是"每发一块都先 await waitDrain"，而 bufferedamountlow 的阈值
             * 默认是 0、要等整队泄空才触发，实际放行全靠 50ms 轮询——于是每块
             * 都要赔一个最多 50ms 的轮询周期，吞吐被钉在 块大小/50ms ≈ 5MB/s，
             * 链路再快也上不去（用户实测两个版本都卡在 5M/b）。
             * 改成突发填充后，等待只发生在窗口真的打满时，且由事件驱动、
             * 不受轮询粒度影响；两次等待之间是同步发送，泄流计时器插不进来。 */
            if (dc.bufferedAmount >= DRAIN_LIMIT) {
                await waitDrain(dc, LOW_WATER);
                if (dc.readyState !== 'open') throw new Error('连接中断');
            }

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

    /* ------------------------------------------------------------ 测速计数
     * 计数状态挂在连接上（conn.speedtest），不是全局。
     * 挂在全局会出这种事：A 和 B 在测速，同时 C 发文件给 A，A 把 C 的文件分块
     * 也当成测速流量计数——数字虚高，而且 C 的文件永远收不完。
     * 每个函数都做成纯函数形式（conn 只是载体），方便在 Node 里单测。 */

    /** 对端发起测速：开始计数。now 传进来是为了测试能控制时间。 */
    function speedtestBegin(conn, bytes, now) {
        conn.speedtest = {
            bytes: Number(bytes) || 0,
            received: 0,
            t0: now,
            deadline: now + SPEED_TEST_TIMEOUT,
        };
    }

    /** 收到一块测速数据。返回 true 表示这一块被测速吃掉了。 */
    function speedtestCount(conn, byteLength, now) {
        const st = conn.speedtest;
        if (!st) return false;
        if (now > st.deadline) {
            /* 超时：这次测速大概永远不会正常结束（发送方抛异常、掉线、
             * 标签页被关都会导致 speedtest-end 不来）。必须作废，
             * 否则后面真实文件的分块会被继续吞掉，传输看起来就是无限卡死。 */
            conn.speedtest = null;
            return false;
        }
        st.received += byteLength;
        return true;
    }

    /** 对端发来收尾：返回统计并作废状态。没有进行中的测速则返回 null。 */
    function speedtestFinish(conn, now) {
        const st = conn.speedtest;
        conn.speedtest = null;
        return st ? { received: st.received, ms: now - st.t0 } : null;
    }

    /** 发送方没能正常收尾，明确作废。 */
    function speedtestAbort(conn) {
        conn.speedtest = null;
    }

    return {
        CHROME_DC_QUEUE_CAP,
        MAX_CHUNK, DRAIN_LIMIT, LOW_WATER, READ_BLOCK, SPEED_TEST_TIMEOUT,
        planBlocks, readBlockAt, chunkSizeFor, isRetryableSendError, waitDrain, pumpFile,
        speedtestBegin, speedtestCount, speedtestFinish, speedtestAbort,
    };
});
