'use strict';

/* LAN Share client
 * - Signalling over WebSocket (server only relays offers/answers/ICE).
 * - File bytes travel over a WebRTC DataChannel straight between browsers.
 * - The vault tab uploads to / downloads from the server disk.
 * - DataChannel 的发送 pacing 在 p2p-pump.js（LSPump），浏览器里以全局变量暴露。
 */

/* 背压与分块策略在 p2p-pump.js（LSPump），单独一个文件是为了能在 Node 里
 * 直接 require 做单测。注意那里的 DRAIN_LIMIT 必须远小于 Chrome DataChannel
 * 发送队列约 16MB 的硬上限，超了 send() 会直接抛
 * "RTCDataChannel send queue is full"，把整次传输判死。 */
const { LOW_WATER, LANES, splitRange, chunkSizeFor, pumpFile,
  speedtestBegin, speedtestCount, speedtestFinish, speedtestAbort } = LSPump;

/* 文件库上传的分块大小。走 HTTP，没有 256KB 的限制，取大一点减少往返。 */
const UP_CHUNK = 4 * 1024 * 1024;
/* 进度条刷新间隔。原来每收到一块就写一次 DOM，100MB 文件要动 1600 次，
 * 光重排就把主线程拖住了。改成 120ms 批量刷一次。 */
const UI_TICK = 120;

const NAME_KEY = 'lanshare.deviceName';

const state = {
  myId: null,
  myName: '',
  peers: [],
  conns: new Map(),
  queue: Promise.resolve(),
  incoming: null,
  /* 非空表示本连接正在接收对端的测速流量，此时二进制消息只计数不当文件。
   * 计数状态按连接存（conn.speedtest），不是全局——挂在全局会让一个连接的
   * 测速流量把另一个连接的文件分块吞掉。 */
  pendingAccept: new Map(),
  transfers: new Map(),
  targetPeer: null,
  modalResolve: null,
  /* 接收方未完成的半成品，按 fileId 存。通道断了不清空，
   * 对端重发同一个文件时用它里的 received 作为续传起点。 */
  resume: new Map(),
  dirty: new Set(),
};

const $ = (id) => document.getElementById(id);
const makeId = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36);

function esc(text) {
  return String(text).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
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

function peerName(id) {
  const peer = state.peers.find((p) => p.id === id);
  return peer ? peer.name : '已断开的设备';
}

/* 设备名持久化。
 * 服务端每次新连接都会重新随机取名，光靠它刷新一次就换一个名字，
 * 对端根本认不出这台机器是谁。所以本机记住名字，连上就报给服务端。
 * 服务端那边另外按 IP 记一层，作为换浏览器/清缓存时的兜底。 */
function loadSavedName() {
  try {
    return localStorage.getItem(NAME_KEY) || '';
  } catch (_) {
    return '';
  }
}

function saveName(name) {
  try {
    localStorage.setItem(NAME_KEY, name);
  } catch (_) {
    /* 隐私模式下写不了 localStorage，名字这次会话内有效即可 */
  }
}

/** 续传的标识：同名同大小才算同一个文件。带 lastModified 防止文件内容变了却续上。 */
function fileIdOf(name, size, lastModified) {
  return `${name}|${size}|${lastModified || 0}`;
}

function humanSpeed(bytesPerSecond) {
  return `${humanSize(bytesPerSecond)}/s`;
}

/* ------------------------------------------------------------------ toasts */

function toast(message, ms = 3200) {
  const el = document.createElement('div');
  el.className = 'toast';
  el.textContent = message;
  $('toasts').appendChild(el);
  setTimeout(() => el.remove(), ms);
}

/* ------------------------------------------------------------------- modal */

function confirmDialog(title, body) {
  $('modalTitle').textContent = title;
  $('modalBody').textContent = body;
  $('modal').classList.remove('hidden');
  return new Promise((resolve) => {
    state.modalResolve = resolve;
  });
}

function closeModal(result) {
  $('modal').classList.add('hidden');
  const resolve = state.modalResolve;
  state.modalResolve = null;
  if (resolve) resolve(result);
}

$('modalYes').addEventListener('click', () => closeModal(true));
$('modalNo').addEventListener('click', () => closeModal(false));

/* --------------------------------------------------------------- signalling */

let ws = null;

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);
  /* 兜底数据通道直接收 ArrayBuffer，省掉一次 Blob→ArrayBuffer 的异步转换 */
  ws.binaryType = 'arraybuffer';

  ws.addEventListener('open', () => {
    const saved = loadSavedName();
    if (saved) {
      state.myName = saved;
      if (document.activeElement !== $('myName')) $('myName').value = saved;
      send({ type: 'rename', name: saved });
    }
  });

  ws.addEventListener('message', (event) => {
    /* 二进制帧 = 兜底数据通道的文件字节。服务端按帧首的 id 转过来的，
     * 这里按同一个 id 找到对端的兜底 lane，交给和 DataChannel 完全相同的
     * onChannelMessage 处理——上层不区分数据是从哪一层来的。 */
    if (typeof event.data !== 'string' && event.data) {
      const frame = LSRelay.parseRelayFrame(event.data);
      if (frame && frame.payload.byteLength) {
        const lane = ensureRelay(frame.from);
        lane.ackPending += frame.payload.byteLength;
        /* 收到就回确认：发送端的 inflight 窗口靠它泄流，否则 pumpFile 会
         * 一直卡在 waitDrain 上（表现是速度掉到 0 然后超时）。 */
        if (lane.ackPending >= RELAY_ACK_EVERY) {
          sendSignal(frame.from, { ch: 0, relayAck: lane.ackPending });
          lane.ackPending = 0;
        }
        lane.dc.deliver(frame.payload);
      }
      return;
    }
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch (_) {
      return;
    }
    if (msg.type === 'self') {
      state.myId = msg.id;
      const saved = loadSavedName();
      if (saved) {
        /* 本机定过的名字优先，服务端给的随机名不能覆盖它 */
        state.myName = saved;
        if (document.activeElement !== $('myName')) $('myName').value = saved;
        if (msg.name !== saved) send({ type: 'rename', name: saved });
      } else {
        /* 第一次来：接受服务端名字并记住，此后刷新就一直是它 */
        state.myName = msg.name;
        saveName(msg.name);
        if (document.activeElement !== $('myName')) $('myName').value = msg.name;
      }
      renderPeers();
    } else if (msg.type === 'peers') {
      state.peers = msg.peers.filter((p) => p.id !== state.myId);
      renderPeers();
    } else if (msg.type === 'signal') {
      handleSignal(msg.from, msg.data);
    } else if (msg.type === 'signal-lost') {
      toast('目标设备已离线');
    }
  });

  ws.addEventListener('close', (event) => {
    /* 记下关闭原因。信令一断兜底通道就跟着废，正在跑的传输会以"连接中断"
     * 收场——但那条报错完全不说明是谁断的。code 1006（异常断开）/ 1009
     * （消息过大）/ 1001（对端离开）分别指向完全不同的原因，没有它只能猜。 */
    window.__wsClose = {
      code: event ? event.code : -1,
      reason: event ? event.reason : '',
      clean: event ? event.wasClean : null,
      at: Date.now(),
      sent: ws ? ws.bufferedAmount : -1,
    };
    /* 也往控制台打一份：CI 会把页面 console 汇到日志里，这样"谁在哪一刻断的"
     * 和服务端日志能对上时间轴，不用两边猜。 */
    console.log('[lan-share] 信令已断:', JSON.stringify(window.__wsClose),
                '我=', state.myId, '已有连接=', state.conns.size);
    state.peers = [];
    /* 兜底通道是挂在信令 WebSocket 上的：连接一断它就废了。必须显式关掉，
     * 否则正在跑的传输会卡在 waitDrain 上（readyState 还是 open，却永远
     * 发不出去），只能等 15s 硬超时。 */
    for (const conn of state.conns.values()) {
      if (conn.relay) {
        conn.relay.dc.close();
        conn.relay = null;
      }
    }
    renderPeers();
    setTimeout(connect, 2000);
  });
}

function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function sendSignal(to, data) {
  send({ type: 'signal', to, data });
}

/* ==========================================================================
 * 兜底数据通道（WebSocket 中转）——统一传输抽象的第二层
 *
 * 实现在 public/relay-channel.js（独立模块，能在 Node 里直接单测）。上层
 * （file-offer / pumpFile / lane-end / progress）只认一套 DataChannel 式的
 * 接口：send(string|ArrayBuffer) / readyState / bufferedAmount /
 * bufferedAmountLowThreshold / addEventListener('message' | 'bufferedamountlow')。
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
 * ========================================================================== */

const RELAY_ACK_EVERY = LSRelay.RELAY_ACK_EVERY;
/* 直连宽限期：这段时间内一条 lane 都没开，就认定直连不可用、降级到兜底。
 * 不能等到 LANE_OPEN_TIMEOUT(20s)——用户只会看到界面没反应。 */
const DIRECT_GRACE_MS = 9000;
/* 已经有 lane 打开之后，连续这么久没有新 lane 打开就开工（见 ensureChannel）。
 * LANE_SETTLE_MAX_MS 是硬上限，防止一直有 lane 慢慢开而拖太久。 */
const LANE_SETTLE_MS = 1500;
const LANE_SETTLE_MAX_MS = 12000;

/* 兜底通道的出站口：控制消息走信令 JSON，数据帧直接投给 WebSocket。
 * 单独抽出来是为了让 RelayChannel 本身不依赖页面全局（ws / send），
 * 这样它能在 Node 里被单测。 */
function makeRelaySink(peerId) {
  return {
    peerId,
    send(data) {
      if (typeof data === 'string') {
        /* file-offer / accept / lane-end / progress 走原来的信令通道，
         * 服务端原样转发 JSON，不用额外开一条链路。 */
        send({ type: 'signal', to: peerId, data: { ch: 0, relay: data } });
        return;
      }
      if (!ws || ws.readyState !== WebSocket.OPEN) throw new Error('连接中断');
      ws.send(data);
    },
    get bufferedAmount() {
      return ws && ws.readyState === WebSocket.OPEN ? ws.bufferedAmount : 0;
    },
  };
}

/** 从本地 SDP 里数各类 ICE 候选，顺带看 host 候选是不是被伪装成 .local。
 *  降级时把它打出来，一眼就能区分"根本没收集到可用的真实 IP 候选"和
 *  "候选有了但对端不可达"——这两种的修法完全不同，之前只能靠猜。 */
function candidateTypeStats(pc) {
  const sdp = (pc && pc.localDescription && pc.localDescription.sdp) || '';
  const types = {};
  let obfuscated = 0;
  for (const line of sdp.split('\r\n')) {
    if (!line.startsWith('a=candidate:')) continue;
    const f = line.split(' ');
    const typ = f[f.length - 2];
    const addr = f[4];
    if (typ) types[typ] = (types[typ] || 0) + 1;
    if (addr && /\.local$/.test(addr)) obfuscated++;
  }
  const counts = Object.keys(types).map((k) => `${k}:${types[k]}`).join(' ');
  return {
    text: (counts || '无候选') + (obfuscated ? ` · ${obfuscated} 条伪装成 .local` : ''),
    types, obfuscated,
  };
}

/** 拿到（或建好）到某个对端的兜底 lane。它不在 conn.lanes 里——那 32 条是
 *  WebRTC 的，会被 retireLane 换掉；兜底 lane 与 WebSocket 同生命周期。 */
function ensureRelay(peerId) {
  const conn = getConn(peerId);
  if (conn.relay) return conn.relay;
  const dc = new LSRelay.RelayChannel(peerId, makeRelaySink(peerId));
  const lane = {
    session: conn, index: 0, peerId,
    pc: null, dc, ready: Promise.resolve(dc),
    role: 'relay', relay: true, speedtest: null,
    pendingCands: [], ackPending: 0,
  };
  /* 和 attachChannel 的做法保持一致：收到的数据派发成 dc 上的 'message' 事件，
   * 再由它进 onChannelMessage。这样 waitAccept / laneSpeedtest 挂在 dc 上的
   * 应答监听器，在兜底通道上和 DataChannel 上行为完全相同——否则对端回了
   * speedtest-ready 也没人听见，表现就是"对端没有回应测速请求"。 */
  dc.addEventListener('message', (e) => onChannelMessage(lane, e.data));
  /* 信令 WS 一断，这条兜底通道就废了。和 attachChannel 里的 DataChannel 一样
   * 挂 close：否则传输会一直卡在"接收中"，既等不到 lane-end 也没有失败提示。 */
  dc.addEventListener('close', () => {
    if (conn.relay === lane) conn.relay = null;
    markInterrupted(lane);
    renderPeers();
  });
  conn.relay = lane;
  return lane;
}

/* ------------------------------------------------------- peer connections */

/* 每个对端一个 session，里面 LANES 条并行的 lane。
 *
 * 为什么不是一条：一个 RTCPeerConnection 只有一个 SCTP 关联，拥塞窗口、接收
 * 窗口、消息粒度全是按关联算的。实测同一对设备 TCP（SMB）能跑 30MB/s，而单条
 * DataChannel 只有 3.2MB/s——瓶颈在每关联的机制里，不在链路上，所以加带宽
 * 没用，只能加关联。每条 lane 有独立的 pc / DataChannel，也就有独立的
 * ICE + DTLS + SCTP + 拥塞窗口。
 *
 * 反过来，同一条关联上开多个 DataChannel 是没用的：它们共享同一个拥塞窗口，
 * 实测不会有任何提升。 */
function getConn(peerId) {
  let conn = state.conns.get(peerId);
  if (conn) return conn;

  conn = { peerId, lanes: [] };
  for (let i = 0; i < LANES; i++) conn.lanes.push(makeLane(conn, i));
  state.conns.set(peerId, conn);
  return conn;
}

/* 构造 ICE 服务器列表：基础 STUN 永远在；若页面被服务端注入了 TURN 配置
 * （window.__LAN_SHARE_TURN__，由服务端按 LAN_SHARE_TURN 环境变量透传），
 * 把其中每一条都加进来。返回新数组，避免污染全局。
 * TURN 为可选：缺省（null）时纯 STUN + host，数据绝不经第三方；一旦服务端
 * 配了 TURN，AP / 客户端隔离下设备间直连被丢弃时即可走中继，不再"中继无效"。 */
function buildIceServers() {
  const servers = [];
  /* 服务端在同一个端口上兼任局域网 STUN（UDP），给客户端发"自己真实内网 IP"
   * 的自反射候选。Chrome 只对 host 候选做 mDNS 伪装，srflx 是真实 IP——
   * 所以只要这个 STUN 可达，直连就不再依赖组播解析。
   *
   * 原来写死 stun.l.google.com 有两个问题：一是国内基本不可达，ICE 要等它超时
   * 才继续（白等几秒）；二是即使可达，同一个局域网里两台设备拿到的 srflx 都是
   * 路由器公网 IP，要靠 NAT 回环才能互通，对局域网直连毫无帮助。 */
  if (typeof location !== 'undefined' && location.hostname) {
    const host = location.hostname.includes(':')
      ? `[${location.hostname}]`
      : location.hostname;
    const port = location.port || (location.protocol === 'https:' ? 443 : 80);
    servers.push({ urls: `stun:${host}:${port}` });
  } else {
    servers.push({ urls: 'stun:stun.l.google.com:19302' });
  }
  const extra = (typeof window !== 'undefined') && window.__LAN_SHARE_TURN__;
  if (Array.isArray(extra)) {
    for (const s of extra) {
      if (s && s.urls) {
        servers.push({
          urls: s.urls,
          username: s.username || '',
          credential: s.credential || '',
        });
      }
    }
    console.log('[lan-share] 已启用 TURN 中继，共 ' + servers.length + ' 个 ICE 服务器');
  }
  return servers;
}

function makeLane(session, index) {
  /* 必须带 STUN：纯 host 候选会被 Chrome 默认伪装成 mDNS .local 名字，两台
   * Windows PC 在真实局域网里经常解析失败，于是 32 条 lane 里绝大多数 ICE
   * 连通性检查都过不了——表现就是回环(loopback)能开 32 条、真机千兆只开 1 条、
   * 速度卡在单关联 ~5MB/s。加 STUN 后浏览器还会收集 srflx 候选（可达的 IPv4 UDP
   * 端点），绕开 mDNS/IPv6 主机候选的不可达问题。默认只加 STUN、不加 TURN
   * （数据绝不经第三方）；若服务端注入了 TURN（见 buildIceServers），则一并加入。
   * 纯离线局域网下 STUN 请求会超时，但 host 候选照常使用，只是 ICE 稍慢一点。 */
  const pc = new RTCPeerConnection({ iceServers: buildIceServers() });
  const lane = {
    session, index, peerId: session.peerId,
    pc, dc: null, ready: null, role: null, speedtest: null,
    pendingCands: [],   // 远端描述还没 set 之前到的候选，先攒着，setRemote 后补加
  };

  /* 信令带 ch，告诉对端这是哪一条 lane 的 offer / candidate */
  pc.addEventListener('icecandidate', (event) => {
    if (event.candidate) sendSignal(session.peerId, { ch: index, candidate: event.candidate });
  });

  pc.addEventListener('datachannel', (event) => attachChannel(lane, event.channel));

  pc.addEventListener('connectionstatechange', () => {
    if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      retireLane(lane);
    } else {
      renderPeers();
    }
  });

  return lane;
}

/* 这条 lane 废了：换一个干净的 pc，下次传输时重新建链。
 * 不能把整个 session 扔掉——其他 lane 可能还是好的，正在传的文件也靠它们续。 */
function retireLane(lane) {
  markInterrupted(lane);
  try { lane.pc.close(); } catch (_) { /* 已经关了 */ }
  lane.session.lanes[lane.index] = makeLane(lane.session, lane.index);
  renderPeers();
}

function attachChannel(lane, dc) {
  lane.dc = dc;
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = LOW_WATER;
  lane.ready = new Promise((resolve) => {
    dc.addEventListener('open', () => resolve(dc), { once: true });
  });
  /* lane.index 决定这一条上的字节属于文件的哪一段，接收端靠它拼回去 */
  dc.addEventListener('message', (event) => onChannelMessage(lane, event.data));
  dc.addEventListener('close', () => {
    markInterrupted(lane);
    if (lane.dc === dc) {
      lane.dc = null;
      lane.ready = null;
      lane.role = null;
    }
    renderPeers();
  });
  renderPeers();
}

async function initiate(peerId) {
  const conn = getConn(peerId);
  const pending = conn.lanes.filter((l) => !l.dc);
  /* 逐条建立、每条之间留 150ms。真实局域网里 Chrome 默认把 host 候选伪装成
   * mDNS .local 名字，32 条同时建会瞬间灌出 60+ 个 mDNS 组播查询，操作系统
   * 大量丢弃 → 多数候选永远解析不出、对应 lane 的 ICE 连不上（典型症状：
   * 回环能开 32 条、真机只开 1~4 条、速度卡在单关联 ~5MB/s）。一条一条来，
   * mDNS 查询错开，几乎每条都能解析出真实 IP，32 条基本全开；而且每条走的是
   * 干净的直接 host 连接，单关联速度也会从"mDNS 降级路径"回到正常的几十
   * MB/s——这样少量 lane 就能跑满千兆。 */
  const GAP = 150;
  for (const l of pending) {
    await initiateLane(conn, l);
    await new Promise((r) => setTimeout(r, GAP));
  }
  return conn;
}

async function initiateLane(session, lane) {
  if (lane.dc) return;
  lane.role = 'offerer';
  const dc = lane.pc.createDataChannel('files', { ordered: true });
  attachChannel(lane, dc);
  const offer = await lane.pc.createOffer();
  await lane.pc.setLocalDescription(offer);
  sendSignal(session.peerId, { ch: lane.index, desc: lane.pc.localDescription });
}

async function handleSignal(from, data) {
  /* 兜底通道的两类消息走的是同一条信令链路，但不属于任何一条 WebRTC lane，
   * 所以必须在按 ch 取 lane 之前先分流：
   *   relay    = 对端发来的控制消息（file-offer / accept / lane-end / progress）
   *   relayAck = 对端回的收字节确认，用来泄发送端的 inflight 窗口 */
  if (data && typeof data.relay === 'string') {
    ensureRelay(from).dc.deliver(data.relay);
    return;
  }
  if (data && data.relayAck) {
    ensureRelay(from).dc.ack(Number(data.relayAck) || 0);
    return;
  }

  const conn = getConn(from);
  const ch = Math.max(0, Math.min(LANES - 1, Number(data.ch) || 0));
  const lane = conn.lanes[ch];

  if (data.desc && data.desc.type === 'offer') {
    // Simple glare handling: if I already sent an offer and my id sorts
    // first, ignore theirs and let mine win. 每条 lane 各自判断。
    if (lane.role === 'offerer' && state.myId && state.myId < from) return;
    lane.role = 'answerer';
    try {
      await lane.pc.setRemoteDescription(data.desc);
      await flushPendingCands(lane);
      const answer = await lane.pc.createAnswer();
      await lane.pc.setLocalDescription(answer);
      sendSignal(from, { ch, desc: lane.pc.localDescription });
    } catch (err) {
      /* 双方同时 offer 时状态机会打架，输了的一方扔掉这条重来 */
      retireLane(lane);
    }
    return;
  }

  if (data.desc && data.desc.type === 'answer') {
    try {
      await lane.pc.setRemoteDescription(data.desc);
      await flushPendingCands(lane);
    } catch (_) { /* 冲突时对方迟到的 answer，忽略 */ }
    return;
  }

  if (data.candidate) {
    try {
      if (lane.pc.remoteDescription && lane.pc.remoteDescription.type) {
        await lane.pc.addIceCandidate(data.candidate);
      } else {
        /* 远端描述还没 set（offer/answer 还在路上，或 glare 互抢时候选比
         * 描述先到），先缓存起来。真实网络下这很常见——候选一旦被直接
         * addIceCandidate 且远端还没 set，会抛错被吞掉、这条候选就永久丢了，
         * 该关联 ICE 就再也连不上。32 条里只剩 1 条能开的现象多半由此而来。 */
        lane.pendingCands.push(data.candidate);
      }
    } catch (err) {
      /* stale candidate, safe to ignore */
    }
  }
}

/* 远端描述 set 完成后，把缓存的候选补加进去。错峰建链 + 缓存候选一起，
 * 让 32 条 lane 在真实局域网里也能基本全部连上。 */
async function flushPendingCands(lane) {
  if (!lane.pendingCands.length) return;
  const q = lane.pendingCands;
  lane.pendingCands = [];
  for (const c of q) {
    try { await lane.pc.addIceCandidate(c); } catch (_) { /* stale */ }
  }
}

/* 单条 lane 等待打开的上限。32 条 lane 里坏一条不该让整个连接永远等下去
 * （丢包链路上 ICE/DTLS 握手失败的概率随 lane 数线性增长）。超时的 lane
 * 会被退役，下次 ensureChannel 时重建，本次传输只用活着的 lane。 */
const LANE_OPEN_TIMEOUT = 20000;

async function ensureChannel(peerId) {
  let conn = state.conns.get(peerId);
  if (!conn) conn = getConn(peerId);
  /* initiate 里 32 条 lane 是错开 150ms 逐条 offer 的（避免 mDNS 查询风暴），
   * 全部发完要约 4.8s。不能等它全部 settle 再判断能不能直连——那样光"发现
   * 直连不可用"就要二十几秒。这里让它跑着，同时轮询已经 open 的 lane。 */
  let initiating = Promise.resolve(conn);
  if (conn.lanes.some((l) => !l.dc)) initiating = initiate(peerId);

  const graceEnd = Date.now() + DIRECT_GRACE_MS;
  let live = [];
  for (;;) {
    live = conn.lanes.filter((l) => l.dc && l.dc.readyState === 'open');
    if (live.length || Date.now() >= graceEnd) break;
    await new Promise((r) => setTimeout(r, 200));
  }

  if (!live.length) {
    /* 一条都没建起来：典型是 Chrome 把 host 候选伪装成 mDNS .local 名字、
     * 而 AP / 有线无线桥接把组播拦了，候选永远解析不出来。原来这里直接抛
     * "点对点通道未就绪"，用户看到的就是"传不了"——现在落到 WebSocket 兜底
     * 通道，照样能传，只是单路、多一跳。 */
    await initiating.catch(() => {});
    /* 降级不是终点，得说清楚为什么：候选是根本没有、还是被伪装成 .local 之后
     * 解析不出来。分别对应"UDP/端口被封"和"mDNS 组播不通"两种完全不同的网络。 */
    const st = conn.lanes[0] ? candidateTypeStats(conn.lanes[0].pc) : { text: '无 pc' };
    const ice = conn.lanes[0] && conn.lanes[0].pc ? conn.lanes[0].pc.iceConnectionState : '?';
    console.log(`[lan-share] 直连未就绪（ice=${ice}，本地候选 ${st.text}），改用 WebSocket 兜底通道`);
    toast('直连未建立，已改用服务器中转');
    const relay = ensureRelay(peerId);
    return { conn, live: [relay], mode: 'relay' };
  }

  /* 已经开了一部分，再等一会儿让其余 lane 赶上（多开几条换吞吐的收益）。
   *
   * 这里原来是 Promise.all 等每条 lane 的 ready、每条各带 20s 超时：只要
   * 32 条里少开一条（真机上的常态），就要**白等整整 20 秒**才开始传，用户
   * 看到的就是"点了发送没反应"。改成自适应收敛——连续 LANE_SETTLE_MS 没有
   * 新的 lane 打开就走，既不会为开不了的 lane 干等，也能把谈好的都收进来。 */
  let seen = -1;
  let stableSince = Date.now();
  const settleEnd = Date.now() + LANE_SETTLE_MAX_MS;
  for (;;) {
    live = conn.lanes.filter((l) => l.dc && l.dc.readyState === 'open');
    if (live.length !== seen) {
      seen = live.length;
      stableSince = Date.now();
    }
    if (Date.now() - stableSince >= LANE_SETTLE_MS) break;
    if (Date.now() >= settleEnd) break;
    await new Promise((r) => setTimeout(r, 200));
  }
  await initiating.catch(() => {});
  live = conn.lanes.filter((l) => l.dc && l.dc.readyState === 'open');
  /* 没打开的 lane 退役掉：半开的 pc 留着只会漏资源，且下一次会在这里重建。
   * 先拷贝再遍历——retireLane 会往 conn.lanes 里写新对象。 */
  for (const l of conn.lanes.slice()) {
    if (!live.includes(l) && l.dc) retireLane(l);
  }
  if (!live.length) throw new Error('点对点通道未就绪');
  return { conn, live, mode: 'p2p' };
}

/* ------------------------------------------------------------ data channel */

/** 对端回应接收请求。resolve 出 { ok, offset }，offset 是对方已收到的字节数。 */
function waitAccept(tid) {
  return new Promise((resolve) => {
    state.pendingAccept.set(tid, resolve);
    setTimeout(() => {
      if (state.pendingAccept.has(tid)) {
        state.pendingAccept.delete(tid);
        resolve({ ok: false, offset: 0 });
      }
    }, 60000);
  });
}

/** 单块大小。Chrome 的 sctp.maxMessageSize 默认 262144，超过 send() 会直接抛错。 */

async function sendOne(peerId, file) {
  const { conn, live } = await ensureChannel(peerId);
  const tid = makeId();
  const fileId = fileIdOf(file.name, file.size, file.lastModified);

  const rec = addTransfer({
    tid,
    name: file.name,
    size: file.size,
    dir: 'up',
    peerId,
    peerLabel: peerName(peerId),
    file,
    retry: () => enqueueSend(peerId, file),
  });
  rec.fileId = fileId;

  /* 这条链路用几条 lane：首次传输前探一次（probeLanes），之后按探测结果
   * 和实测表现动态调整。只广告真正会用的 lane——对端按交集数段数，
   * 广告 32 条却只用 16 条，对端会永远等不到收尾。 */
  /* 兜底通道只有一条，也不用探 lane 数（探测本身就是一次 4MB 往返） */
  const want = live.length === 1 && live[0].relay ? 1 : await chooseLanes(conn, live);
  const chosen = live.slice(0, want);

  /* 控制消息走第一条活 lane，数据随后按范围铺到所用 lane 上。
   * lanes 带上本端活着的 lane 编号：对端用自己的活lane 集合取交集，
   * 两边段数才能对齐（任何一边少一条 lane，写死 LANES 就会永远等不到收尾）。 */
  chosen[0].dc.send(JSON.stringify({
    t: 'file-offer', tid, fileId, name: file.name, size: file.size,
    lanes: chosen.map((l) => l.index),
  }));

  const accept = await waitAccept(tid);
  if (!accept.ok) {
    updateTransfer(tid, { status: '对方拒绝', failed: true });
    return;
  }

  const start = accept.offset || 0;
  /* 双方活 lane 的交集才是真能用的：发送端活着但接收端已死的 lane，
   * 字节发过去直接进黑洞，接收端永远等不到那条 lane-end。
   * 注意是在 chosen（本次实际使用的子集）里取交集，不是全部活 lane。 */
  const use = accept.lanes
    ? chosen.filter((l) => accept.lanes.includes(l.index))
    : chosen;
  /* 实际并行路数直接显示出来：如果这里长期只有 1~4 路，速度上不去的
   * 根因就是真机上多数 lane 没建起来（典型是 mDNS 候选解析失败），而不是
   * 数据分发的问题。 */
  console.log('[lan-share] 发送端活 lane:', live.length, '本次使用:', use.length, 'chosen:', chosen.length);
  if (!use.length) throw new Error('对端没有可用的传输通道');
  /* 把 [start, size) 均分给活 lane。段与段首尾相接，接收端按 lane 顺序
   * 拼回去就是原文件——所以这里切的顺序不能乱，也不能漏段。
   * 每条 lane 是独立的 SCTP 关联，拥塞窗口互不影响：丢包链路上单关联
   * 的窗口会坍缩，多关联并联是对抗手段（CI 实测 2% 丢包下 4 条 0.9MB/s、
   * 32 条 5.1MB/s）。 */
  /* 走的是哪一层必须让用户看见：直连 = "N 路并行"，兜底 = "服务器中转"。
   * 之前只有"点对点通道未就绪"一句报错，用户既不知道为什么、也没有退路。 */
  const laneLabel = (use.length === 1 && use[0].relay)
    ? '1 路 · 服务器中转（直连未建立）'
    : use.length + ' 路并行';
  const ranges = splitRange(start, file.size, use.length);
  const laneSent = use.map(() => 0);
  const report = () => {
    const total = start + laneSent.reduce((a, b) => a + b, 0);
    updateTransfer(tid, { sent: total, status: '传输中 · ' + laneLabel });
  };

  const sendStart = Date.now();
  await Promise.all(ranges.map((range, i) => {
    if (range.to <= range.from) return Promise.resolve();
    const lane = use[i];
    /* 字节搬运在 p2p-pump.js 的 pumpFile 里：分块读 + 预读 + 背压 + 队列满重试。
     * 那边可以用假 DataChannel 在 Node 里跑回归测试。end 让每条 lane 只发自己那段。 */
    return pumpFile(lane.dc, file, range.from, {
      chunk: chunkSizeFor(lane),
      end: range.to,
      onProgress: (sent) => {
        laneSent[i] = sent - range.from;
        report();
      },
    }).then(() => {
      /* 这一段发完了。接收端按 lane 计数，齐了才组装文件——不能用 lane 0 上的
       * 一条 file-end 收尾，跨通道没有顺序保证，它会追过别的 lane 的数据。 */
      lane.dc.send(JSON.stringify({ t: 'lane-end', tid, ch: i }));
    });
  }));

  /* 这一趟的对端实收速率（progress 回执算出来的）如果明显慢，下次给同一
   * 对端直接加满 lane——链路变差不用等下次探测。只升不降，避免抖动。 */
  const moved = Math.max(0, rec.acked - start);
  const secs = (Date.now() - sendStart) / 1000;
  if (moved > 4 * 1024 * 1024 && secs > 0.5
    && moved / secs < ESCALATE_BELOW && conn.lanePref < live.length) {
    conn.lanePref = live.length;
  }

  /* 全部 lane-end 都已发出，本端不再发数据。但"已送达"要等对端 file-done
   * 回执才算数——此刻对端可能还有几十 MB 在队列里没落盘，这里把进度拉满、
   * acked 也拉满，会让发送端显示冲到接收端前面一大截、速率飙上天（就是
   * 用户看到的"做过山车"）。所以只标 sent，进度仍由对端回执驱动。 */
  updateTransfer(tid, { status: '已发出，等待对端保存', sent: file.size });
}

function enqueueSend(peerId, file) {
  state.queue = state.queue.then(() => sendOne(peerId, file)).catch((err) => {
    const rec = [...state.transfers.values()].reverse().find(
      (r) => r.file === file && !r.done
    );
    if (rec) updateTransfer(rec.tid, { status: `发送失败：${err.message}`, failed: true });
    toast(`发送「${file.name}」失败：${err.message}`);
  });
}

function onChannelMessage(lane, data) {
  if (typeof data === 'string') {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch (_) {
      return;
    }
    /* 一旦进入正式的文件协议，就认为这条 session 上的测速状态作废。
     * 双保险：除了 deadline 超时，也要保证测速残留不会把文件分块吞掉。 */
    if (msg.t === 'file-offer' || msg.t === 'file-accept'
      || msg.t === 'file-reject' || msg.t === 'lane-end') {
      for (const l of lane.session.lanes) speedtestAbort(l);
    }
    if (msg.t === 'file-offer') {
      askReceive(lane, msg);
    } else if (msg.t === 'progress') {
      /* 对端汇报实收字节数。发送端的进度条和速率以它为准——
       * 本端的 sent 只是"交给发送队列"的字节，会被高水位吸收而虚高。
       * 对端在 16/32 条 lane 上各发各的 progress，跨 lane 没有顺序保证：
       * 晚到的旧值会把 acked 拉回去（进度条缩水、速率算出负数再被压成 0），
       * 显示就"做过山车"。所以 acked 只增不减。 */
      const pr = state.transfers.get(msg.tid);
      const received = Number(msg.received) || 0;
      if (pr && (pr.acked == null || received >= pr.acked)) {
        updateTransfer(msg.tid, { acked: received });
      }
    } else if (msg.t === 'file-done') {
      /* 对端已组装落盘，发送端这才算真正送达——而不是本端把字节交给
       * 发送队列就当完事（那种假完成会让发送端进度冲到接收端前面一大截）。 */
      updateTransfer(msg.tid, { acked: Number(msg.received) || 0, status: '已送达', done: true });
    } else if (msg.t === 'file-accept' || msg.t === 'file-reject') {
      const resolve = state.pendingAccept.get(msg.tid);
      if (resolve) {
        state.pendingAccept.delete(msg.tid);
        resolve({ ok: msg.t === 'file-accept', offset: Number(msg.offset) || 0 });
      }
      if (msg.t === 'file-reject') toast('对方拒绝了接收');
    } else if (msg.t === 'lane-end') {
      /* 每条 lane 发完自己那一段就报一次。段数是双方用同一个 splitRange
       * 算出来的，所以齐了就是真齐了，不会早也不会晚。 */
      const inc = state.incoming;
      if (inc && inc.tid === msg.tid) {
        inc.lanesDone = (inc.lanesDone || 0) + 1;
        if (inc.lanesDone >= inc.expectedLanes) finishReceive(lane);
      }
    } else if (msg.t === 'text') {
      toast(`来自 ${peerName(lane.peerId)}：${msg.text}`);
    } else if (msg.t === 'speedtest') {
      /* 对端在测链路速度：接下来这条连接上的二进制只计数，不弹窗、不进文件库。
       * 计数挂在 lane 上并有硬超时，万一 speedtest-end 永远不来也不会
       * 把后续真实文件的分块吞掉（那种症状就是传输无限卡死）。 */
      speedtestBegin(lane, msg.bytes, Date.now());
      lane.dc.send(JSON.stringify({ t: 'speedtest-ready', ch: lane.index }));
    } else if (msg.t === 'speedtest-end') {
      const st = speedtestFinish(lane, Date.now());
      if (st) {
        lane.dc.send(JSON.stringify({
          t: 'speedtest-result',
          ch: lane.index,
          received: st.received,
          ms: st.ms,
        }));
      }
    } else if (msg.t === 'speedtest-abort') {
      /* 发送方没能正常收尾，明确告诉接收端作废计数状态 */
      speedtestAbort(lane);
    }
    return;
  }

  /* 测速进行中：只累加字节数。放在 state.incoming 判断之前，
   * 保证测速数据永远不会被误当成文件内容。 */
  if (speedtestCount(lane, data.byteLength, Date.now())) return;

  const inc = state.incoming;
  if (inc && inc.laneBufs[lane.index]) {
    /* 这一条 lane 对应文件的一段，分块按到达顺序进本段的缓冲。
     * lane 内部是有序的（ordered:true），段与段之间靠 splitRange 的顺序拼。 */
    inc.laneBufs[lane.index].push(data);
    inc.received += data.byteLength;
    updateTransfer(inc.tid, { sent: inc.received });
    /* 周期性回传实收字节数：发送端的进度和速率用它来显示。
     * 不这么做的话，发送端只能显示"交给发送队列的字节"——那会被 lane 的
     * 高水位吸收（32 条 × 2MB = 64MB），进度条先冲到 100% 再卡住，
     * 速率在 2~40MB/s 之间荡，而真实速度一直很稳。 */
    const now = Date.now();
    if (inc.received - (inc.lastAck || 0) >= 1024 * 1024
      || now - (inc.lastAckTs || 0) >= 500) {
      inc.lastAck = inc.received;
      inc.lastAckTs = now;
      try {
        lane.dc.send(JSON.stringify({ t: 'progress', tid: inc.tid, received: inc.received }));
      } catch (_) { /* 通道正在关，下一条数据也到不了了 */ }
    }
  }
}

async function askReceive(lane, msg) {
  const from = peerName(lane.peerId);
  /* 同一个文件之前收到一半就断了：用已收字节数作为续传起点，
   * 让对端从断点接着发，而不是从 0 重来。 */
  const prior = msg.fileId ? state.resume.get(msg.fileId) : null;
  const already = prior ? prior.received : 0;

  let accepted;
  if (already > 0 && already < msg.size) {
    accepted = await confirmDialog(
      '继续接收',
      `${from} 继续发送「${msg.name}」(${humanSize(msg.size)})，`
        + `本机已收到 ${humanSize(already)}，将从断点续传。`
    );
  } else {
    accepted = await confirmDialog(
      '收到传输请求',
      `${from} 想发送「${msg.name}」(${humanSize(msg.size)})`
    );
  }
  if (!accepted) {
    lane.dc.send(JSON.stringify({ t: 'file-reject', tid: msg.tid }));
    return;
  }

  /* 期望的段数 = 双方活 lane 的交集大小（和发送端算的是同一个交集：
   * 它拿本端活集合和回传的集合取交集后切段）。写死 LANES 会在 lane 死亡
   * 或文件很小、段数不足时永远等不到收尾。
   * 老版本对端不发 lanes 字段时退回本端数量（双方都全活时相等）。 */
  /* 兜底通道不在 session.lanes 里（它是 conn.relay），且整条链路只有它一条：
   * 直接按 [0] 走，别拿 32 条 WebRTC lane 的开合状态去和它取交集——
   * 那边可能一条都没开，交集算出来是 0，收端会误判成"没有可用通道"而拒绝。 */
  const myLive = lane.relay
    ? [0]
    : lane.session.lanes
      .filter((l) => l.dc && l.dc.readyState === 'open')
      .map((l) => l.index);
  console.log('[lan-share] 接收端活 lane:', myLive.length, '对端要约:', (msg.lanes || []).length);
  const myLiveSet = new Set(myLive);
  const useCount = Array.isArray(msg.lanes) && msg.lanes.length
    ? msg.lanes.filter((i) => myLiveSet.has(i)).length
    : LANES;
  if (useCount <= 0) {
    lane.dc.send(JSON.stringify({ t: 'file-reject', tid: msg.tid }));
    return;
  }
  const inc = {
    tid: msg.tid,
    fileId: msg.fileId,
    name: msg.name,
    size: msg.size,
    from,
    /* 续传时上一段已经收到的字节。上一段的 lane 缓冲按 lane 顺序摊平就是
     * [0, already)，直接接在新数据前面，不需要重新分组。 */
    priorChunks: (prior && already < msg.size) ? prior.laneBufs.flat() : [],
    laneBufs: lane.session.lanes.map(() => []),
    lanesDone: 0,
    /* 这次传输实际在用的通道。判"中断"时必须拿它过滤——直连建不起来时文件走
     * 兜底通道，而同期那 32 条 WebRTC lane 还在陆续 ICE 失败被判死刑，它们的
     * 死活跟这次传输毫无关系（详见 markInterrupted）。 */
    lanes: new Set(lane.relay ? [lane] : myLive.map((i) => lane.session.lanes[i])),
    expectedLanes: splitRange(already, msg.size, useCount).length,
    received: already,
    /* progress 回执的节流状态：每 1MB 或 500ms 报一次实收字节数 */
    lastAck: already,
    lastAckTs: 0,
  };
  state.incoming = inc;
  if (inc.fileId) state.resume.set(inc.fileId, inc);

  addTransfer({
    tid: msg.tid, name: msg.name, size: msg.size, dir: 'down', peerId: lane.peerId, peerLabel: from,
  });
  updateTransfer(msg.tid, { status: '接收中 · ' + myLive.length + ' 路', sent: inc.received });
  /* 回传本端活着的 lane：发送端取交集后切段，段数和这边才对得上 */
  lane.dc.send(JSON.stringify({
    t: 'file-accept', tid: msg.tid, fileId: msg.fileId, offset: inc.received,
    lanes: myLive,
  }));
}

/** 通道中途断开：保留已收到的分块，等对方重发同一个文件后续上。 */
function markInterrupted(lane) {
  /* 通道断了，这条 lane 上的测速计数状态也必须作废 */
  if (lane) speedtestAbort(lane);
  const inc = state.incoming;
  if (!inc) return;

  /* 死掉的这条 lane 不是本次传输在用的通道，那就跟它无关，不能顺着往下判中断。
   *
   * 这是个必现的竞态，不是偶发：直连建不起来时文件走兜底通道，而同一时间轴上
   * 那 32 条 WebRTC lane 还在各自跑 ICE，失败后由 retireLane 依次调到这里。
   * 老代码不看 lane 归属，于是"第一批判死刑"的那一刻就把正在传的传输写成
   * 「已中断（N MB），可续传」，同时 state.incoming 被置空——后续字节全部没人
   * 接，界面就停在传了几 MB 然后中断，对端却还在发（最终"发送未完成"）。
   *
   * 断点落在哪完全由两条时间轴的相对快慢决定（兜底吞吐 × 首条 lane 判死刑的
   * 时刻），所以真机上看起来是"19.7MB 与 32.6MB 之间随机断"，CI 里则表现为
   * 同一份代码时绿时红。 */
  if (lane && inc.lanes && !inc.lanes.has(lane)) return;

  state.incoming = null;
  if (inc.fileId && inc.received > 0 && inc.received < inc.size) {
    state.resume.set(inc.fileId, inc);
    updateTransfer(inc.tid, { status: `已中断（${humanSize(inc.received)}），可续传`, failed: true });
  } else {
    updateTransfer(inc.tid, { status: '已中断', failed: true });
  }
}

function finishReceive(lane) {
  const inc = state.incoming;
  state.incoming = null;
  if (!inc) return;
  state.resume.delete(inc.fileId);
  /* 拼接顺序：先续传前已经收到的字节，再按 lane 0..N-1 的顺序把各段接上。
   * splitRange 保证各段首尾相接，所以这个顺序就是文件原始顺序。 */
  const blob = new Blob([...inc.priorChunks, ...inc.laneBufs.flat()],
    { type: 'application/octet-stream' });
  inc.priorChunks.length = 0;
  inc.laneBufs.forEach((b) => { b.length = 0; });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = inc.name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 60000);
  updateTransfer(inc.tid, { status: '已保存', sent: inc.size, done: true });
  /* 回执发送端：本机已落盘。它据此把发送端状态从"已发出"改成"已送达"，
   * 而不是发送端自己把队列清空就当完事。通道若正好在关，对端靠 progress
   * 到 size 也能判送达（见 updateTransfer 的收口）。 */
  if (lane && lane.dc && lane.dc.readyState === 'open') {
    try {
      lane.dc.send(JSON.stringify({ t: 'file-done', tid: inc.tid, received: inc.size }));
    } catch (_) { /* 忽略：收尾回执失败不致命 */ }
  }
  toast(`已接收「${inc.name}」`);
}

/* ---------------------------------------------------------------- transfers */

function addTransfer(info) {
  const el = document.createElement('div');
  el.className = 'transfer';
  el.innerHTML = '<div class="t-head"><span class="t-name"></span><span class="t-status"></span></div>'
    + '<div class="bar"><i></i></div><div class="t-sub"></div>'
    + '<button class="mini t-retry hidden">继续</button>';
  el.querySelector('.t-name').textContent = info.name;
  $('transferList').prepend(el);

  const rec = {
    tid: info.tid,
    el,
    size: info.size,
    sent: 0,
    /* 对端实收字节（发送端才有，progress 回执刷新）。null 表示还没收到
     * 第一条回执——那之前只能先显示本端交付量。 */
    acked: null,
    status: '准备中',
    dir: info.dir,
    peerId: info.peerId || null,
    peerLabel: info.peerLabel || '',
    file: info.file || null,
    retry: info.retry || null,
    speed: 0,
  };
  state.transfers.set(info.tid, rec);
  el.querySelector('.t-retry').addEventListener('click', () => {
    if (rec.retry) rec.retry();
  });
  updateTransfer(info.tid, {});
  return rec;
}

/* 进度更新改成节流。原来每收/发一块就写一次 DOM，100MB 的文件要动一千多次，
 * 光重排就把主线程占住了，反而拖慢传输。统一 120ms 批量刷一次。
 * 速率按"显示值"（发送端是 acked，接收端是 sent）的变化算，见 renderTransfer。 */
function updateTransfer(tid, patch) {
  const rec = state.transfers.get(tid);
  if (!rec) return;
  const hadAck = rec.acked != null;
  Object.assign(rec, patch);

  /* 发送端：对端实收达到文件大小才算真正送达。progress 到 size 或
   * file-done 都会走到这，统一在这里收口，不要各写一份。 */
  if (rec.acked != null && rec.size && rec.acked >= rec.size && !rec.done && !rec.failed) {
    rec.done = true;
    if (rec.status !== '已送达') rec.status = '已送达';
  }

  const shown = rec.acked != null ? rec.acked : rec.sent;
  if (typeof patch.sent === 'number' || typeof patch.acked === 'number') {
    /* 第一次收到对端回执：速率基线从"入队字节"切到"实收字节"。之前 sent
     * 是虚高的入队速率，直接沿用会把切过去那一瞬算成负数速率。 */
    if (rec.acked != null && !hadAck) rec.lastTs = 0;
    const now = Date.now();
    if (!rec.lastTs) {
      rec.lastTs = now;
      rec.lastSent = shown;
    } else {
      const dt = (now - rec.lastTs) / 1000;
      if (dt >= 0.4) {
        const inst = Math.max(0, (shown - rec.lastSent) / dt);
        /* 指数平滑压掉单次 progress 跳变：实收速率本身很稳，但 progress 是
         * 1MB 一条、又在多条 lane 上乱序到，瞬时值会跳。 */
        rec.speed = rec.speed > 0 ? rec.speed * 0.6 + inst * 0.4 : inst;
        rec.lastTs = now;
        rec.lastSent = shown;
      }
    }
  }
  state.dirty.add(tid);
}

function renderTransfer(tid) {
  const rec = state.transfers.get(tid);
  if (!rec) return;
  /* 发送端有对端回执时按实收显示：进度条不再被发送队列吸收而虚高，
   * 速率也不再荡。没有回执（刚发起、对端是老版本）才退回本端交付量。 */
  const shown = rec.acked != null ? rec.acked : rec.sent;
  const pct = rec.size ? Math.min(100, Math.round((shown / rec.size) * 100)) : (rec.done ? 100 : 0);
  const who = rec.peerLabel || (rec.peerId ? peerName(rec.peerId) : '');
  const speed = rec.speed > 0 ? ` · ${humanSpeed(rec.speed)}` : '';

  rec.el.querySelector('.bar i').style.width = `${pct}%`;
  rec.el.querySelector('.t-status').textContent = rec.status || `${pct}%`;
  rec.el.querySelector('.t-sub').textContent = rec.dir === 'down'
    ? `来自 ${who} · ${humanSize(shown)} / ${humanSize(rec.size)}${speed}`
    : `发往 ${who} · ${humanSize(shown)} / ${humanSize(rec.size)}${speed}`;
  rec.el.classList.toggle('done', Boolean(rec.done));
  rec.el.classList.toggle('fail', Boolean(rec.failed));
  rec.el.querySelector('.t-retry').classList.toggle('hidden', !(rec.failed && rec.retry));
}

setInterval(() => {
  if (!state.dirty.size) return;
  for (const tid of state.dirty) renderTransfer(tid);
  state.dirty.clear();
}, UI_TICK);

/* ------------------------------------------------------------------ peers */

function renderPeers() {
  const grid = $('peerGrid');
  const others = state.peers;
  $('peerCount').textContent = `${others.length} 台设备`;

  if (!others.length) {
    grid.innerHTML = '<div class="empty">还没有其他设备。在另一台设备的浏览器打开同一个地址即可出现。</div>';
    return;
  }

  grid.innerHTML = '';
  for (const peer of others) {
    const conn = state.conns.get(peer.id);
    let dotClass = 'dot';
    let note = '未连接';
    if (conn) {
      if (conn.relay) {
        /* 兜底通道不在 conn.lanes 里。走到这一层说明直连没建起来——明说出来，
         * 别让用户看到一个"连接中"然后不知道到底连上没有。 */
        dotClass = 'dot busy';
        note = '服务器中转（直连未建立）';
      } else {
        const open = conn.lanes.filter((l) => l.dc && l.dc.readyState === 'open').length;
        if (open === conn.lanes.length) {
          dotClass = 'dot live';
          note = `已直连 ×${open}`;
        } else if (open > 0) {
          dotClass = 'dot busy';
          note = `连接中 ${open}/${conn.lanes.length}`;
        } else {
          dotClass = 'dot busy';
          note = '连接中';
        }
      }
    }

    const card = document.createElement('div');
    card.className = 'peer-card';
    card.dataset.peer = peer.id;
    card.innerHTML = `<div class="avatar">${esc(peer.name.slice(0, 1))}</div>`
      + `<div class="pname">${esc(peer.name)}</div>`
      + `<div class="pmeta"><span class="${dotClass}"></span>${note}</div>`
      + '<button class="mini" data-act="text">发文字</button>';

    card.addEventListener('click', () => {
      state.targetPeer = peer.id;
      $('peerPicker').click();
    });

    card.querySelector('.mini').addEventListener('click', (event) => {
      event.stopPropagation();
      const text = window.prompt(`发送文字给 ${peer.name}`);
      if (!text) return;
      ensureChannel(peer.id)
        .then((conn2) => {
          conn2.lanes[0].dc.send(JSON.stringify({ t: 'text', text }));
          toast('已发送');
        })
        .catch((err) => toast(`发送失败：${err.message}`));
    });

    card.addEventListener('dragover', (event) => {
      event.preventDefault();
      card.classList.add('dragover');
    });
    card.addEventListener('dragleave', () => card.classList.remove('dragover'));
    card.addEventListener('drop', (event) => {
      event.preventDefault();
      card.classList.remove('dragover');
      for (const file of event.dataTransfer.files) enqueueSend(peer.id, file);
    });

    grid.appendChild(card);
  }
}

$('peerPicker').addEventListener('change', (event) => {
  const target = state.targetPeer;
  const files = [...event.target.files];
  event.target.value = '';
  if (!target) return;
  for (const file of files) enqueueSend(target, file);
});

$('myName').addEventListener('change', (event) => {
  const value = event.target.value.trim().slice(0, 24);
  if (!value) {
    event.target.value = state.myName;
    return;
  }
  state.myName = value;
  saveName(value);
  send({ type: 'rename', name: value });
});

/* ------------------------------------------------------------------ vault */

async function refreshVault() {
  let data;
  try {
    const res = await fetch('/api/files', { cache: 'no-store' });
    data = await res.json();
  } catch (_) {
    $('vaultMeta').textContent = '无法连接服务端';
    return;
  }
  const files = data.files || [];
  $('vaultMeta').textContent = files.length
    ? `共 ${files.length} 个文件 · ${humanSize(data.total || 0)}`
    : '文件库是空的';

  const list = $('vaultList');
  list.innerHTML = '';
  for (const file of files) {
    const li = document.createElement('li');
    li.className = 'vault-item';
    const when = new Date(file.mtime).toLocaleString('zh-CN', { hour12: false });
    li.innerHTML = `<span class="vname">${esc(file.name)}</span>`
      + `<span class="vmeta">${esc(file.sizeText)} · ${esc(when)}</span>`
      + '<span class="vact"><button data-act="dl">下载</button><button data-act="rm" class="danger">删除</button></span>';

    li.querySelector('[data-act="dl"]').addEventListener('click', () => {
      const a = document.createElement('a');
      a.href = `/api/download?name=${encodeURIComponent(file.name)}`;
      a.download = file.name;
      document.body.appendChild(a);
      a.click();
      a.remove();
    });

    li.querySelector('[data-act="rm"]').addEventListener('click', async () => {
      if (!window.confirm(`确定删除「${file.name}」？`)) return;
      const res = await fetch(`/api/files?name=${encodeURIComponent(file.name)}`, { method: 'DELETE' });
      const body = await res.json();
      toast(body.ok ? '已删除' : `删除失败：${body.error || '未知错误'}`);
      refreshVault();
    });

    list.appendChild(li);
  }
}

/** 上传单个分块。服务端按 offset 顺序落进 .part 文件，中断后 .part 保留。 */
function putChunk(name, size, offset, blob) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', `/api/upload?name=${encodeURIComponent(name)}&size=${size}&offset=${offset}`);
    xhr.setRequestHeader('Content-Type', 'application/octet-stream');
    xhr.addEventListener('load', () => {
      if (xhr.status === 200) {
        try {
          resolve(JSON.parse(xhr.responseText));
        } catch (_) {
          resolve({ ok: true });
        }
        return;
      }
      let reason = xhr.statusText;
      try {
        reason = JSON.parse(xhr.responseText).error || reason;
      } catch (_) { /* 保持 statusText */ }
      reject(new Error(reason || `HTTP ${xhr.status}`));
    });
    xhr.addEventListener('error', () => reject(new Error('网络中断')));
    xhr.addEventListener('abort', () => reject(new Error('上传被取消')));
    xhr.send(blob);
  });
}

async function uploadOne(file) {
  const tid = makeId();
  addTransfer({
    tid,
    name: file.name,
    size: file.size,
    dir: 'up',
    peerId: null,
    peerLabel: '服务端',
    file,
    retry: () => uploadOne(file),
  });

  let offset = 0;
  try {
    /* 开传前先问一句：这个文件服务端已经收了多少字节。
     * 上次传一半断掉的话，就从那个位置接着传，不用从头再来。 */
    const st = await fetch(
      `/api/upload-status?name=${encodeURIComponent(file.name)}&size=${file.size}`,
      { cache: 'no-store' }
    ).then((r) => r.json());
    offset = Math.max(0, Math.min(Number(st.received) || 0, file.size));
  } catch (_) {
    offset = 0;
  }

  updateTransfer(tid, {
    sent: offset,
    status: offset > 0 ? `续传中（已收 ${humanSize(offset)}）` : '上传中',
  });

  try {
    if (file.size === 0) {
      await putChunk(file.name, 0, 0, file.slice(0, 0));
      updateTransfer(tid, { status: '已上传', sent: 0, done: true });
      refreshVault();
      return;
    }
    while (offset < file.size) {
      const end = Math.min(offset + UP_CHUNK, file.size);
      const res = await putChunk(file.name, file.size, offset, file.slice(offset, end));
      offset = end;
      /* 收满时服务端把 .part 重命名成正式文件，可能因为重名加了个 (1) */
      if (res && res.done && res.name && res.name !== file.name) {
        updateTransfer(tid, { status: `已保存为「${res.name}」`, sent: file.size, done: true });
        toast(`已上传「${file.name}」→「${res.name}」`);
        refreshVault();
        return;
      }
      updateTransfer(tid, { sent: offset, status: '上传中' });
    }
    updateTransfer(tid, { status: '已上传', sent: file.size, done: true });
    toast(`已上传「${file.name}」`);
    refreshVault();
  } catch (err) {
    updateTransfer(tid, {
      status: `上传失败：${err.message}（已收 ${humanSize(offset)}，可继续）`,
      failed: true,
    });
    toast(`上传「${file.name}」失败：${err.message}`);
  }
}

function uploadFiles(files) {
  for (const file of files) uploadOne(file);
}

$('pickVault').addEventListener('click', () => $('vaultPicker').click());
$('vaultPicker').addEventListener('change', (event) => {
  uploadFiles([...event.target.files]);
  event.target.value = '';
});

const dropzone = $('dropzone');
['dragenter', 'dragover'].forEach((name) => {
  dropzone.addEventListener(name, (event) => {
    event.preventDefault();
    dropzone.classList.add('dragover');
  });
});
['dragleave', 'dragend'].forEach((name) => {
  dropzone.addEventListener(name, () => dropzone.classList.remove('dragover'));
});
dropzone.addEventListener('drop', (event) => {
  event.preventDefault();
  dropzone.classList.remove('dragover');
  if (event.dataTransfer.files.length) uploadFiles([...event.dataTransfer.files]);
});

/* ------------------------------------------------------------------ 网速测试 */

/* 测速用的数据量。16MB 足够让瞬时速率稳定下来，又不至于把路由器的
 * 磁盘和内存折腾太久。 */
const SPEED_BYTES = 16 * 1024 * 1024;
const SPEED_NAME = '.speedtest-tmp';

function speedSet(id, text) {
  const el = $(id);
  if (el) el.textContent = text;
}

function speedLog(text) {
  const el = $('speedLog');
  if (el) el.textContent = text;
}

/* 服务端下载：整块拉回来，测 HTTP 吞吐。 */
function speedDownload() {
  return new Promise((resolve, reject) => {
    const t0 = performance.now();
    const xhr = new XMLHttpRequest();
    xhr.open('GET', `/api/download?name=${encodeURIComponent(SPEED_NAME)}`);
    xhr.responseType = 'arraybuffer';
    let bytes = 0;
    xhr.addEventListener('progress', (e) => { bytes = e.loaded; });
    xhr.addEventListener('load', () => {
      const secs = (performance.now() - t0) / 1000;
      const got = bytes || (xhr.response ? xhr.response.byteLength : 0);
      resolve({ mbps: got / 1048576 / secs, bytes: got });
    });
    xhr.addEventListener('error', () => reject(new Error('下载测速失败')));
    xhr.addEventListener('timeout', () => reject(new Error('下载测速超时')));
    xhr.timeout = 120000;
    xhr.send();
  });
}

/* 服务端上传：整块推上去，测 HTTP 吞吐。 */
function speedUpload() {
  return new Promise((resolve, reject) => {
    const blob = new Blob([new Uint8Array(SPEED_BYTES)]);
    const t0 = performance.now();
    const xhr = new XMLHttpRequest();
    xhr.open('POST', `/api/upload?name=${encodeURIComponent(SPEED_NAME)}`
      + `&size=${SPEED_BYTES}&offset=0`);
    xhr.addEventListener('load', () => {
      const secs = (performance.now() - t0) / 1000;
      resolve({ mbps: SPEED_BYTES / 1048576 / secs });
    });
    xhr.addEventListener('error', () => reject(new Error('上传测速失败')));
    xhr.addEventListener('timeout', () => reject(new Error('上传测速超时')));
    xhr.timeout = 120000;
    xhr.send(blob);
  });
}

/* 从 getStats 里挖出选中的候选对：类型和 RTT。
 * 这两个数字直接回答"慢是不是因为没走直连"——出现 srflx/relay，或者 RTT 几十
 * 毫秒，就先别折腾发送端了，路本身就不对。host↔host + 个位数 RTT 才是内网直连。 */
async function linkDiag(pc) {
  try {
    const stats = await pc.getStats();
    const byId = new Map();
    let transport = null;
    stats.forEach((s) => {
      byId.set(s.id, s);
      if (s.type === 'transport') transport = s;
    });
    const pair = transport ? byId.get(transport.selectedCandidatePairId) : null;
    if (!pair) return { text: '候选对未知', rtt: null, kind: null };
    const local = byId.get(pair.localCandidateId) || {};
    const remote = byId.get(pair.remoteCandidateId) || {};
    const rtt = (pair.currentRoundTripTime != null)
      ? Math.round(pair.currentRoundTripTime * 1000) : null;
    const kind = `${local.candidateType || '?'}↔${remote.candidateType || '?'}`;
    return {
      text: `${kind} · RTT ${rtt != null ? rtt + 'ms' : '未知'}`
        + ` · ${(local.protocol || '').toUpperCase()}`,
      rtt, kind,
    };
  } catch (_) {
    return { text: '统计不可用', rtt: null, kind: null };
  }
}

/* 点对点：把 SPEED_BYTES 均分给几条 lane 同时推，用与真实文件传输完全相同的
 * pumpFile 通道（同样的背压、分块、并行度），所以测出来的就是实际传文件的速度。
 * 对端只计数、不弹窗、不当文件收。
 *
 * 为什么要并行着测：单条 DataChannel 只有 3.2MB/s，而同一对设备的 TCP 能跑
 * 30MB/s——瓶颈在单个 SCTP 关联里（拥塞窗口/接收窗口/消息粒度），不在链路上。
 * 只测一条的话，测出来的只是关联的上限，会误判成"网络不行"。 */
/* 在指定的一组 lane 上跑一次逐 lane 测速：握手 → 泵数据 → 等对端回执。
 * speedP2p（页面上的测速按钮）和 chooseLanes（传输前的 lane 数决策）用的是
 * 同一套机制，只有 lane 子集和字节数不同，所以抽成公共函数。
 * 速率由对端计时（speedtest-result 回执），不受发送队列吸收影响。 */
async function laneSpeedtest(lanes, totalBytes, blob) {
  const t0 = performance.now();
  /* 和真实传输用同一个切分函数，段数、边界完全一致 */
  const ranges = splitRange(0, totalBytes, lanes.length);
  const use = lanes.slice(0, ranges.length);

  /* 每条 lane 独立打招呼：发 speedtest、等对端 ready。
   * 丢包链路上这条控制消息自己就可能丢（用户 Wi-Fi 实测 2% 丢包时必中），
   * 所以每 2 秒重发一次、最多等 30 秒——不能一丢就判整个测速失败。 */
  await Promise.all(use.map((lane, i) => new Promise((resolve, reject) => {
    let tries = 0;
    const send = () => {
      tries++;
      try {
        lane.dc.send(JSON.stringify({
          t: 'speedtest', bytes: ranges[i].to - ranges[i].from, ch: lane.index,
        }));
      } catch (err) {
        clearInterval(rep); clearTimeout(to);
        reject(err); return;
      }
    };
    const rep = setInterval(send, 2000);
    const to = setTimeout(() => {
      clearInterval(rep);
      lane.dc.removeEventListener('message', onMsg);
      reject(new Error(`lane ${i} 对端没有回应测速请求（重发 ${tries} 次）`));
    }, 30000);
    const onMsg = (e) => {
      if (typeof e.data !== 'string') return;
      let m;
      try { m = JSON.parse(e.data); } catch (_) { return; }
      if (m.t === 'speedtest-ready' && Number(m.ch) === lane.index) {
        clearInterval(rep); clearTimeout(to);
        lane.dc.removeEventListener('message', onMsg);
        resolve();
      }
    };
    lane.dc.addEventListener('message', onMsg);
    send();
  })));

  /* 等每条 lane 的回执。超时给 null，不影响其他 lane 出数 */
  const echoes = use.map((lane) => new Promise((resolve) => {
    const to = setTimeout(() => resolve(null), 120000);
    const onMsg = (e) => {
      if (typeof e.data !== 'string') return;
      let m;
      try { m = JSON.parse(e.data); } catch (_) { return; }
      if (m.t === 'speedtest-result' && Number(m.ch) === lane.index) {
        clearTimeout(to);
        lane.dc.removeEventListener('message', onMsg);
        resolve(m);
      }
    };
    lane.dc.addEventListener('message', onMsg);
  }));

  try {
    await Promise.all(ranges.map((r, i) => {
      const lane = use[i];
      return pumpFile(lane.dc, blob.slice(r.from, r.to), 0, { chunk: chunkSizeFor(lane) })
        .then(() => {
          lane.dc.send(JSON.stringify({ t: 'speedtest-end', ch: lane.index }));
        });
    }));
  } catch (err) {
    /* 没能正常收尾一定要告诉对端，否则它会一直停在计数状态，
     * 把后续真实文件的分块全吞掉。 */
    for (const lane of use) {
      try { lane.dc.send(JSON.stringify({ t: 'speedtest-abort', ch: lane.index })); } catch (_) {}
    }
    throw err;
  }

  const results = await Promise.all(echoes);
  const received = results.reduce((n, r) => n + (r ? r.received : 0), 0);
  /* 并行时取最慢那条 lane 的耗时当分母，偏保守但不会虚高 */
  const slowest = results.reduce((mx, r) => Math.max(mx, r ? r.ms : 0), 0);
  return {
    received, slowest,
    secs: (performance.now() - t0) / 1000,
    perLane: results.map((r) => (r && r.ms > 0 ? (r.received / 1048576) / (r.ms / 1000) : 0)),
  };
}

/* ---------------- lane 数决策：16 条探一次，慢才加开到 32 ----------------
 *
 * CI 实测（--sweep，速率由对端计时）表明最优 lane 数随链路丢包变化：
 *   无损 loopback：1~4 条就够（26~39MB/s），16/32 条回落到 16~18；
 *   5ms + 0.5% 丢包：16 条 20.0 > 32 条 17.1——轻度丢包 16 条最优；
 *   20ms + 2% 丢包：16 条 3.0 < 32 条 4.2——重丢包才吃得上 32 条。
 * 而且多开 lane 不是免费的：每条关联都有独立的 DTLS/SCTP 开销，Chrome 的
 * DataChannel 通路是 CPU-bound 的（Aalto 大学实测：高端 PC 上双端各吃满一个
 * 核，且接收端比发送端更耗 CPU），32 条在轻度丢包的 Wi-Fi 上等于拿 CPU 和
 * 空口控制帧换一点点带宽。所以先用 16 条探 4MB：对端聚合 ≥10MB/s 就用
 * 16 条（省 CPU、发送队列吸收窗口小、数字稳），低于阈值才加满到 32 条。
 *
 * 阈值 10MB/s 在三档 CI 档案上都分得开：0.5% 丢包的 16 条聚合是 20（不升），
 * 2% 丢包是 3.0（升），无损是 16~29（不升）。 */
const PROBE_LANES = 16;
const PROBE_BYTES = 4 * 1024 * 1024;
const ESCALATE_BELOW = 10 * 1024 * 1024;

/** 本次传输用几条 lane。探测结果按连接缓存（conn.lanePref）——
 *  同一对端第二次发文件不再重新探，链路变化由 sendOne 末尾的实收速率兜底
 *  （明显慢就加满，只升不降，避免抖动）。 */
async function chooseLanes(conn, live) {
  if (conn.lanePref) return Math.min(conn.lanePref, live.length);
  const lanes = live.slice(0, PROBE_LANES);
  let agg = 0;
  try {
    const blob = new Blob([new Uint8Array(PROBE_BYTES)]);
    const r = await laneSpeedtest(lanes, PROBE_BYTES, blob);
    agg = r.slowest > 0 ? (r.received / 1048576) / (r.slowest / 1000) : 0;
  } catch (_) {
    /* 探测本身失败（链路很差才会）——按最坏情况办，直接加满 */
    conn.lanePref = live.length;
    return live.length;
  }
  conn.lanePref = agg < 10 ? live.length : PROBE_LANES;
  return conn.lanePref;
}

async function speedP2p(peerId) {
  const { live } = await ensureChannel(peerId);

  /* 兜底通道没有 pc，取不到 ICE 候选对——直接说明当前走的是哪一层，
   * 这正是用户想知道的："为什么没走直连"。 */
  const diag = live[0].pc
    ? await linkDiag(live[0].pc)
    : { text: '服务器中转（WebSocket）：直连未建立，通常是 Chrome 的 mDNS '
        + '候选被网络拦掉或 UDP 不通' };
  speedLog(`链路：${diag.text}`);

  const blob = new Blob([new Uint8Array(SPEED_BYTES)]);
  const r = await laneSpeedtest(live, SPEED_BYTES, blob);
  /* 两个数字的含义不同：
   *   mbps       —— 本机把字节交给 DataChannel 的速率，含背压等待，
   *                 也就是"我这边发得多快"。它会被发送队列吸收（每条 lane
   *                 最多囤 2MB，32 条就是 64MB），链路一抖动就虚高，
   *                 只能看趋势。
   *   peerMbps   —— 对端真正收完的速率，由对端计时，这才是端到端速度。 */
  return {
    mbps: SPEED_BYTES / 1048576 / r.secs,
    peerMbps: r.slowest > 0 ? (r.received / 1048576) / (r.slowest / 1000) : 0,
    lanes: live.length,
    diag,
    echoed: r.received > 0 ? { received: r.received, ms: r.slowest } : null,
  };
}

async function runSpeedTest() {
  const btn = $('runSpeed');
  if (btn) btn.disabled = true;
  speedSet('spDown', '测试中…');
  speedSet('spUp', '测试中…');
  speedSet('spP2p', '测试中…');

  const peer = state.peers.find((p) => p.id === state.targetPeer) || state.peers[0];
  speedSet('spPeerName', peer ? `（${peer.name}）` : '（没有在线设备）');

  try {
    /* 先上传再下载：下载要用刚传上去的那个文件 */
    const up = await speedUpload();
    speedSet('spUp', `${up.mbps.toFixed(1)} MB/s`);
    const down = await speedDownload();
    speedSet('spDown', `${down.mbps.toFixed(1)} MB/s`);
    /* 清理临时文件，失败就算了，不挡着出结果 */
    fetch(`/api/files?name=${encodeURIComponent(SPEED_NAME)}`, { method: 'DELETE' })
      .then(refreshVault).catch(() => {});

    if (peer) {
      const p2p = await speedP2p(peer.id);
      /* 头条用对端确认的速率：那才是端到端真实速度。本机"交付出手"的速率
       * 会被发送队列吸收（每条 lane 最多囤 2MB，32 条就是 64MB 的吸收
       * 窗口），链路一抖动就虚高——用户实测发送端在 2~40MB/s 之间荡、
       * 对端却一直稳定 13MB/s，荡的就是这个数。两个都显示，但主次分明。 */
      const extra = p2p.echoed
        ? ` · 本端交付 ${p2p.mbps.toFixed(1)}`
        : ' · 对端未回执';
      speedSet('spP2p', `${p2p.peerMbps.toFixed(1)} MB/s${extra} · ${p2p.lanes} 条并行`);
      /* 单条 lane 的速率也报一下：并行没提升就说明瓶颈不在每关联的机制上，
       * 而在链路本身，继续加 lane 是徒劳。 */
      if (p2p.lanes > 1) {
        speedLog(`点对点并行：${p2p.lanes} 条 lane 对端合计 ${p2p.peerMbps.toFixed(1)} MB/s`
          + `（本端交付 ${p2p.mbps.toFixed(1)}）`);
      }
    } else {
      speedSet('spP2p', '跳过（没有在线设备）');
    }
    speedLog('完成');
  } catch (err) {
    speedLog(`测速中断：${err.message}`);
  } finally {
    if (btn) btn.disabled = false;
  }
}

if ($('runSpeed')) $('runSpeed').addEventListener('click', () => { runSpeedTest(); });

/* -------------------------------------------------------------------- tabs */

document.querySelectorAll('.tab').forEach((tab) => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t === tab));
    const name = tab.dataset.tab;
    document.querySelectorAll('.pane').forEach((pane) => {
      pane.classList.toggle('active', pane.id === `pane-${name}`);
    });
    if (name === 'vault') refreshVault();
  });
});

renderPeers();
refreshVault();
connect();
setInterval(() => {
  if ($('pane-vault').classList.contains('active')) refreshVault();
}, 15000);
