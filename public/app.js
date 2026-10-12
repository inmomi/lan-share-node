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

  ws.addEventListener('open', () => {
    const saved = loadSavedName();
    if (saved) {
      state.myName = saved;
      if (document.activeElement !== $('myName')) $('myName').value = saved;
      send({ type: 'rename', name: saved });
    }
  });

  ws.addEventListener('message', (event) => {
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

  ws.addEventListener('close', () => {
    state.peers = [];
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

function makeLane(session, index) {
  const pc = new RTCPeerConnection({ iceServers: [] });
  const lane = {
    session, index, peerId: session.peerId,
    pc, dc: null, ready: null, role: null, speedtest: null,
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
  await Promise.all(conn.lanes.map((lane) => initiateLane(conn, lane)));
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
    } catch (_) { /* 冲突时对方迟到的 answer，忽略 */ }
    return;
  }

  if (data.candidate) {
    try {
      await lane.pc.addIceCandidate(data.candidate);
    } catch (err) {
      /* stale candidate, safe to ignore */
    }
  }
}

/* 单条 lane 等待打开的上限。32 条 lane 里坏一条不该让整个连接永远等下去
 * （丢包链路上 ICE/DTLS 握手失败的概率随 lane 数线性增长）。超时的 lane
 * 会被退役，下次 ensureChannel 时重建，本次传输只用活着的 lane。 */
const LANE_OPEN_TIMEOUT = 20000;

async function ensureChannel(peerId) {
  let conn = state.conns.get(peerId);
  if (!conn || conn.lanes.some((l) => !l.dc)) conn = await initiate(peerId);
  await Promise.all(conn.lanes.map((l) => (l.ready
    ? Promise.race([
      l.ready,
      new Promise((r) => setTimeout(r, LANE_OPEN_TIMEOUT)),
    ])
    : Promise.resolve())));
  const live = conn.lanes.filter((l) => l.dc && l.dc.readyState === 'open');
  /* 没打开的 lane 退役掉：半开的 pc 留着只会漏资源，且下一次会在这里重建。
   * 先拷贝再遍历——retireLane 会往 conn.lanes 里写新对象。 */
  for (const l of conn.lanes.slice()) {
    if (!live.includes(l) && l.dc) retireLane(l);
  }
  if (!live.length) throw new Error('点对点通道未就绪');
  return { conn, live };
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
  const { live } = await ensureChannel(peerId);
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

  /* 控制消息走第一条活 lane，数据随后按范围铺到所有活 lane 上。
   * lanes 带上本端活着的 lane 编号：对端用自己的活lane 集合取交集，
   * 两边段数才能对齐（任何一边少一条 lane，写死 LANES 就会永远等不到收尾）。 */
  live[0].dc.send(JSON.stringify({
    t: 'file-offer', tid, fileId, name: file.name, size: file.size,
    lanes: live.map((l) => l.index),
  }));

  const accept = await waitAccept(tid);
  if (!accept.ok) {
    updateTransfer(tid, { status: '对方拒绝', failed: true });
    return;
  }

  const start = accept.offset || 0;
  /* 双方活 lane 的交集才是真能用的：发送端活着但接收端已死的 lane，
   * 字节发过去直接进黑洞，接收端永远等不到那条 lane-end。 */
  const use = accept.lanes
    ? live.filter((l) => accept.lanes.includes(l.index))
    : live;
  if (!use.length) throw new Error('对端没有可用的传输通道');
  /* 把 [start, size) 均分给活 lane。段与段首尾相接，接收端按 lane 顺序
   * 拼回去就是原文件——所以这里切的顺序不能乱，也不能漏段。
   * 每条 lane 是独立的 SCTP 关联，拥塞窗口互不影响：丢包链路上单关联
   * 的窗口会坍缩，多关联并联是对抗手段（CI 实测 2% 丢包下 4 条 0.9MB/s、
   * 32 条 5.1MB/s）。 */
  const ranges = splitRange(start, file.size, use.length);
  const laneSent = use.map(() => 0);
  const report = () => {
    const total = start + laneSent.reduce((a, b) => a + b, 0);
    updateTransfer(tid, { sent: total, status: '传输中' });
  };

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

  updateTransfer(tid, { status: '已送达', sent: file.size, done: true });
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
        if (inc.lanesDone >= inc.expectedLanes) finishReceive();
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
  const myLive = lane.session.lanes
    .filter((l) => l.dc && l.dc.readyState === 'open')
    .map((l) => l.index);
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
    expectedLanes: splitRange(already, msg.size, useCount).length,
    received: already,
  };
  state.incoming = inc;
  if (inc.fileId) state.resume.set(inc.fileId, inc);

  addTransfer({
    tid: msg.tid, name: msg.name, size: msg.size, dir: 'down', peerId: lane.peerId, peerLabel: from,
  });
  updateTransfer(msg.tid, { status: '接收中', sent: inc.received });
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
  state.incoming = null;
  if (inc.fileId && inc.received > 0 && inc.received < inc.size) {
    state.resume.set(inc.fileId, inc);
    updateTransfer(inc.tid, { status: `已中断（${humanSize(inc.received)}），可续传`, failed: true });
  } else {
    updateTransfer(inc.tid, { status: '已中断', failed: true });
  }
}

function finishReceive() {
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
 * 光重排就把主线程占住了，反而拖慢传输。统一 120ms 批量刷一次。 */
function updateTransfer(tid, patch) {
  const rec = state.transfers.get(tid);
  if (!rec) return;
  Object.assign(rec, patch);

  if (typeof patch.sent === 'number') {
    const now = Date.now();
    if (!rec.lastTs) {
      rec.lastTs = now;
      rec.lastSent = rec.sent;
    } else {
      const dt = (now - rec.lastTs) / 1000;
      if (dt >= 0.4) {
        rec.speed = Math.max(0, (rec.sent - rec.lastSent) / dt);
        rec.lastTs = now;
        rec.lastSent = rec.sent;
      }
    }
  }
  state.dirty.add(tid);
}

function renderTransfer(tid) {
  const rec = state.transfers.get(tid);
  if (!rec) return;
  const pct = rec.size ? Math.min(100, Math.round((rec.sent / rec.size) * 100)) : (rec.done ? 100 : 0);
  const who = rec.peerLabel || (rec.peerId ? peerName(rec.peerId) : '');
  const speed = rec.speed > 0 ? ` · ${humanSpeed(rec.speed)}` : '';

  rec.el.querySelector('.bar i').style.width = `${pct}%`;
  rec.el.querySelector('.t-status').textContent = rec.status || `${pct}%`;
  rec.el.querySelector('.t-sub').textContent = rec.dir === 'down'
    ? `来自 ${who} · ${humanSize(rec.sent)} / ${humanSize(rec.size)}${speed}`
    : `发往 ${who} · ${humanSize(rec.sent)} / ${humanSize(rec.size)}${speed}`;
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
async function speedP2p(peerId) {
  const { live } = await ensureChannel(peerId);
  const t0 = performance.now();

  const diag = await linkDiag(live[0].pc);
  speedLog(`链路：${diag.text}`);

  const blob = new Blob([new Uint8Array(SPEED_BYTES)]);
  /* 和真实传输用同一个切分函数，段数、边界完全一致 */
  const ranges = splitRange(0, SPEED_BYTES, live.length);
  const lanes = live.slice(0, ranges.length);

  /* 每条 lane 独立打招呼：发 speedtest、等对端 ready。
   * 丢包链路上这条控制消息自己就可能丢（用户 Wi-Fi 实测 2% 丢包时必中），
   * 所以每 2 秒重发一次、最多等 30 秒——不能一丢就判整个测速失败。 */
  await Promise.all(lanes.map((lane, i) => new Promise((resolve, reject) => {
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
  const echoes = lanes.map((lane) => new Promise((resolve) => {
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
      const lane = lanes[i];
      return pumpFile(lane.dc, blob.slice(r.from, r.to), 0, { chunk: chunkSizeFor(lane) })
        .then(() => {
          lane.dc.send(JSON.stringify({ t: 'speedtest-end', ch: lane.index }));
        });
    }));
  } catch (err) {
    /* 没能正常收尾一定要告诉对端，否则它会一直停在计数状态，
     * 把后续真实文件的分块全吞掉。 */
    for (const lane of lanes) {
      try { lane.dc.send(JSON.stringify({ t: 'speedtest-abort', ch: lane.index })); } catch (_) {}
    }
    throw err;
  }

  const results = await Promise.all(echoes);
  const secs = (performance.now() - t0) / 1000;
  /* 两个数字的含义不同：
   *   mbps       —— 本机把字节交给 DataChannel 的速率，含背压等待，
   *                 也就是"我这边发得多快"。
   *   peerMbps   —— 对端真正收完的速率，由对端计时，这才是端到端速度。
   * 背压存在时本机数字会被等待拉低，所以以对端数字为准。
   * 并行时取最慢那条 lane 的耗时当分母，偏保守，但不会虚高。 */
  const received = results.reduce((n, r) => n + (r ? r.received : 0), 0);
  const slowest = results.reduce((mx, r) => Math.max(mx, r ? r.ms : 0), 0);
  const peerMbps = slowest > 0 ? (received / 1048576) / (slowest / 1000) : 0;
  return {
    mbps: SPEED_BYTES / 1048576 / secs,
    peerMbps,
    lanes: lanes.length,
    diag,
    echoed: received > 0 ? { received, ms: slowest } : null,
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
      /* 对端确认的速率才是端到端真实速度；本机速率只是"交付出手"的速度，
       * 背压等待会把它压低。两个都显示，方便判断是谁在等。 */
      const extra = p2p.echoed
        ? ` · 对端收到 ${(p2p.echoed.received / 1048576).toFixed(1)}MB`
          + ` @ ${p2p.peerMbps.toFixed(1)} MB/s`
        : ' · 对端未回执';
      speedSet('spP2p', `${p2p.mbps.toFixed(1)} MB/s${extra} · ${p2p.lanes} 条并行`);
      /* 单条 lane 的速率也报一下：并行没提升就说明瓶颈不在每关联的机制上，
       * 而在链路本身，继续加 lane 是徒劳。 */
      if (p2p.lanes > 1) {
        speedLog(`点对点并行：${p2p.lanes} 条 lane 合计 ${p2p.mbps.toFixed(1)} MB/s`
          + `（单条约 ${(p2p.mbps / p2p.lanes).toFixed(1)} MB/s）`);
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
