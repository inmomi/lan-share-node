/* 兜底数据通道（WebSocket 中转）——统一传输抽象的第二层。
 *
 * 上层（file-offer / pumpFile / lane-end / progress）只认一套 DataChannel 式的
 * 接口：
 *   send(string|ArrayBuffer) / readyState / bufferedAmount /
 *   bufferedAmountLowThreshold / addEventListener('message' | 'bufferedamountlow')
 * 底下是 WebRTC DataChannel 还是这条 WS 中转，对上层完全一样——所以
 * 「Wi-Fi 点对点」「有线↔Wi-Fi」「AP 隔离」「mDNS 组播被拦」「UDP 被禁」
 * 这些情况不必各写一套逻辑：ICE 建得起来就走 DataChannel，建不起来自动落到
 * 这一层。
 *
 * 为什么必须有这一层：Chrome 默认把 host 候选伪装成 mDNS .local 名字，要靠
 * 组播解析；家用路由器默认开 IGMP Snooping，无线侧经常收不到组播，候选就
 * 永远解析不出来，ICE 一条都建不起来。而 WebSocket 走单播 TCP，且信令本来
 * 就已经通了（页面就是从服务端加载的），所以它一定能通——代价只是多一跳、
 * 没有多 lane 并行。
 *
 * 抽成独立文件（而不是塞在 app.js 里）是为了能在 Node 里直接 require 跑
 * 单测：帧格式、背压窗口、字节完整性这些边界靠真机点文件是点不出来的。
 */
(function (root, factory) {
    if (typeof module === 'object' && module.exports) module.exports = factory();
    else root.LSRelay = factory();
})(typeof self !== 'undefined' ? self : globalThis, function () {
    'use strict';

    /* 背压靠 pumpFile 的 waitDrain：bufferedAmount 反映的是 inflight（未确认
     * 字节），超过 DRAIN_LIMIT(2MB) 就停发。DataChannel 那边是 Chrome 内部
     * 16MB 队列在兜着，这里没有，所以 inflight 必须如实反映对端进度——
     * 不限的话发送端会把服务端和接收端的内存吃光。 */
    /* 接收端每收到这么多字节回一次确认 */
    var RELAY_ACK_EVERY = 512 * 1024;
    /* 单块上限：服务端单帧上限 WS_BUF(128KB) 还要扣 1 字节 id 长度和 id 本身 */
    var RELAY_CHUNK = 64 * 1024;

    /** 二进制帧封装：[1 字节 id 长度][id][payload]，与服务端 ws_send_binary 对齐。 */
    function prefixRelayFrame(peerId, buf) {
        var id = new TextEncoder().encode(peerId);
        var out = new Uint8Array(1 + id.length + buf.byteLength);
        out[0] = id.length;
        out.set(id, 1);
        out.set(new Uint8Array(buf), 1 + id.length);
        return out.buffer;
    }

    function parseRelayFrame(ab) {
        var u = new Uint8Array(ab);
        if (u.length < 2) return null;
        var idlen = u[0];
        if (idlen === 0 || u.length <= 1 + idlen) return null;
        return {
            from: new TextDecoder().decode(u.subarray(1, 1 + idlen)),
            payload: ab.slice(1 + idlen),
        };
    }

    /**
     * @param peerId  对端 id
     * @param sink    { send(buf|string) } —— 真正把字节发出去的地方（浏览器里是
     *                 WebSocket，测试里是假的收集器）。抽出来是为了可测。
     */
    function RelayChannel(peerId, sink) {
        this.peerId = peerId;
        this.sink = sink;
        this.readyState = 'open';
        this.binaryType = 'arraybuffer';
        this.bufferedAmountLowThreshold = 0;
        this.bufferedAmount = 0;
        this.inflight = 0;   /* 已发出、还没被对端确认的字节 */
        this._ls = Object.create(null);
    }

    RelayChannel.prototype.addEventListener = function (type, fn) {
        if (!this._ls[type]) this._ls[type] = [];
        if (this._ls[type].indexOf(fn) < 0) this._ls[type].push(fn);
    };

    RelayChannel.prototype.removeEventListener = function (type, fn) {
        var s = this._ls[type];
        if (!s) return;
        var i = s.indexOf(fn);
        if (i >= 0) s.splice(i, 1);
    };

    RelayChannel.prototype._emit = function (type, event) {
        var s = this._ls[type];
        if (!s) return;
        for (var i = 0; i < s.slice().length; i++) {
            try { s.slice()[i](event); } catch (err) {
                if (typeof console !== 'undefined') console.error('兜底通道监听出错', err);
            }
        }
    };

    /** bufferedAmount = 未确认字节 + 本地发送队列，pumpFile 的 waitDrain 靠它背压。 */
    RelayChannel.prototype._sync = function () {
        var queued = this.sink && this.sink.bufferedAmount ? this.sink.bufferedAmount : 0;
        this.bufferedAmount = Math.max(0, this.inflight) + queued;
        if (this.bufferedAmount <= this.bufferedAmountLowThreshold) {
            this._emit('bufferedamountlow', {});
        }
    };

    RelayChannel.prototype.send = function (data) {
        if (this.readyState !== 'open') throw new Error('连接中断');
        if (typeof data === 'string') {
            /* 控制消息（file-offer / accept / lane-end / progress）走原来的信令
             * 通道，服务端原样转发 JSON，不用额外开一条链路。 */
            this.sink.send(data);
            return;
        }
        var buf = data;
        if (!(buf instanceof ArrayBuffer)) buf = buf && buf.buffer ? buf.buffer : null;
        if (!buf) return;
        if (buf.byteLength > RELAY_CHUNK) {
            /* 超出单帧上限就切块：整帧超限会被服务端直接丢掉，
             * 表现是进度条卡死不动而没有任何报错。 */
            var view = new Uint8Array(buf);
            for (var off = 0; off < view.length; off += RELAY_CHUNK) {
                /* 必须用 slice 而不是 subarray：subarray 只是同一块 ArrayBuffer
                 * 上的视图，.buffer 拿到的还是整块原始数据，等于没切——
                 * 整帧超限照样会被服务端丢掉。 */
                this.sink.send(prefixRelayFrame(this.peerId,
                    view.slice(off, Math.min(off + RELAY_CHUNK, view.length)).buffer));
                this.inflight += Math.min(RELAY_CHUNK, view.length - off);
            }
        } else {
            this.sink.send(prefixRelayFrame(this.peerId, buf));
            this.inflight += buf.byteLength;
        }
        this._sync();
    };

    RelayChannel.prototype.ack = function (bytes) {
        this.inflight -= bytes;
        if (this.inflight < 0) this.inflight = 0;
        this._sync();
    };

    /** 收到一帧/一条控制消息时调用：派发成 'message' 事件。
     *  必须走事件而不是直接调上层函数——waitAccept / laneSpeedtest 的应答监听器
     *  是挂在 dc.addEventListener('message') 上的，绕过事件它们就永远收不到，
     *  表现是"对端没有回应"（实际上对端回了，只是没人听见）。 */
    RelayChannel.prototype.deliver = function (data) {
        this._emit('message', { data: data });
    };

    RelayChannel.prototype.close = function () {
        if (this.readyState === 'closed') return;   /* 重复关只派发一次 */
        this.readyState = 'closed';
        /* 必须派发 close：上层（app.js）就是靠它把"这条通道死了"翻译成
         * 「已中断（N MB），可续传」的。不派发的话信令 WS 断了以后界面会一直
         * 停在"接收中"，用户既看不到失败也拿不到续传入口。 */
        this._emit('close', {});
    };

    return {
        RelayChannel: RelayChannel,
        prefixRelayFrame: prefixRelayFrame,
        parseRelayFrame: parseRelayFrame,
        RELAY_ACK_EVERY: RELAY_ACK_EVERY,
        RELAY_CHUNK: RELAY_CHUNK,
    };
});
