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
const { LOW_WATER, chunkSizeFor, pumpFile,
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

function getConn(peerId) {
  let conn = state.conns.get(peerId);
  if (conn) return conn;

  const pc = new RTCPeerConnection({ iceServers: [] });
  conn = { pc, dc: null, ready: null, role: null, peerId };

  pc.addEventListener('icecandidate', (event) => {
    if (event.candidate) sendSignal(peerId, { candidate: event.candidate });
  });

  pc.addEventListener('datachannel', (event) => {
    attachChannel(conn, event.channel);
  });

  pc.addEventListener('connectionstatechange', () => {
    if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      markInterrupted(conn);
      state.conns.delete(peerId);
      renderPeers();
    } else {
      renderPeers();
    }
  });

  state.conns.set(peerId, conn);
  return conn;
}

function attachChannel(conn, dc) {
  conn.dc = dc;
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = LOW_WATER;
  conn.ready = new Promise((resolve) => {
    dc.addEventListener('open', () => resolve(dc), { once: true });
  });
  dc.addEventListener('message', (event) => onChannelMessage(conn, event.data));
  dc.addEventListener('close', () => {
    markInterrupted(conn);
    state.conns.delete(conn.peerId);
    renderPeers();
  });
  renderPeers();
}

async function initiate(peerId) {
  const conn = getConn(peerId);
  if (conn.dc) return conn;
  conn.role = 'offerer';
  const dc = conn.pc.createDataChannel('files', { ordered: true });
  attachChannel(conn, dc);
  const offer = await conn.pc.createOffer();
  await conn.pc.setLocalDescription(offer);
  sendSignal(peerId, { desc: conn.pc.localDescription });
  return conn;
}

async function handleSignal(from, data) {
  const conn = getConn(from);

  if (data.desc && data.desc.type === 'offer') {
    // Simple glare handling: if I already sent an offer and my id sorts
    // first, ignore theirs and let mine win.
    if (conn.role === 'offerer' && state.myId && state.myId < from) return;
    conn.role = 'answerer';
    await conn.pc.setRemoteDescription(data.desc);
    const answer = await conn.pc.createAnswer();
    await conn.pc.setLocalDescription(answer);
    sendSignal(from, { desc: conn.pc.localDescription });
    return;
  }

  if (data.desc && data.desc.type === 'answer') {
    await conn.pc.setRemoteDescription(data.desc);
    return;
  }

  if (data.candidate) {
    try {
      await conn.pc.addIceCandidate(data.candidate);
    } catch (err) {
      /* stale candidate, safe to ignore */
    }
  }
}

async function ensureChannel(peerId) {
  let conn = state.conns.get(peerId);
  if (!conn || !conn.dc) conn = await initiate(peerId);
  await conn.ready;
  if (!conn.dc || conn.dc.readyState !== 'open') throw new Error('点对点通道未就绪');
  return conn;
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
  const conn = await ensureChannel(peerId);
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

  conn.dc.send(JSON.stringify({
    t: 'file-offer', tid, fileId, name: file.name, size: file.size,
  }));

  const accept = await waitAccept(tid);
  if (!accept.ok) {
    updateTransfer(tid, { status: '对方拒绝', failed: true });
    return;
  }

  /* 字节搬运在 p2p-pump.js 的 pumpFile 里：分块读 + 预读 + 背压 + 队列满重试。
   * 那边可以用假 DataChannel 在 Node 里跑回归测试。 */
  await pumpFile(conn.dc, file, accept.offset || 0, {
    chunk: chunkSizeFor(conn),
    onProgress: (sent) => updateTransfer(tid, { sent, status: '传输中' }),
  });

  conn.dc.send(JSON.stringify({ t: 'file-end', tid, fileId }));
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

function onChannelMessage(conn, data) {
  if (typeof data === 'string') {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch (_) {
      return;
    }
    /* 一旦进入正式的文件协议，就认为测速状态作废。
     * 双保险：除了 deadline 超时，也要保证测速残留不会把文件分块吞掉。 */
    if (msg.t === 'file-offer' || msg.t === 'file-accept'
      || msg.t === 'file-reject' || msg.t === 'file-end') {
      state.speedtest = null;
    }
    if (msg.t === 'file-offer') {
      askReceive(conn, msg);
    } else if (msg.t === 'file-accept' || msg.t === 'file-reject') {
      const resolve = state.pendingAccept.get(msg.tid);
      if (resolve) {
        state.pendingAccept.delete(msg.tid);
        resolve({ ok: msg.t === 'file-accept', offset: Number(msg.offset) || 0 });
      }
      if (msg.t === 'file-reject') toast('对方拒绝了接收');
    } else if (msg.t === 'file-end') {
      finishReceive();
    } else if (msg.t === 'text') {
      toast(`来自 ${peerName(conn.peerId)}：${msg.text}`);
    } else if (msg.t === 'speedtest') {
      /* 对端在测链路速度：接下来这条连接上的二进制只计数，不弹窗、不进文件库。
       * 计数挂在 conn 上并有硬超时，万一 speedtest-end 永远不来也不会
       * 把后续真实文件的分块吞掉（那种症状就是传输无限卡死）。 */
      speedtestBegin(conn, msg.bytes, Date.now());
      conn.dc.send(JSON.stringify({ t: 'speedtest-ready' }));
    } else if (msg.t === 'speedtest-end') {
      const st = speedtestFinish(conn, Date.now());
      if (st) {
        conn.dc.send(JSON.stringify({
          t: 'speedtest-result',
          received: st.received,
          ms: st.ms,
        }));
      }
    } else if (msg.t === 'speedtest-abort') {
      /* 发送方没能正常收尾，明确告诉接收端作废计数状态 */
      speedtestAbort(conn);
    }
    return;
  }

  /* 测速进行中：只累加字节数。放在 state.incoming 判断之前，
   * 保证测速数据永远不会被误当成文件内容。 */
  if (speedtestCount(conn, data.byteLength, Date.now())) return;

  if (state.incoming) {
    state.incoming.chunks.push(data);
    state.incoming.received += data.byteLength;
    updateTransfer(state.incoming.tid, { sent: state.incoming.received });
  }
}

async function askReceive(conn, msg) {
  const from = peerName(conn.peerId);
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
    conn.dc.send(JSON.stringify({ t: 'file-reject', tid: msg.tid }));
    return;
  }

  const inc = (prior && already < msg.size)
    ? prior
    : { tid: msg.tid, fileId: msg.fileId, name: msg.name, size: msg.size, from, chunks: [], received: 0 };
  inc.tid = msg.tid;
  state.incoming = inc;
  if (inc.fileId) state.resume.set(inc.fileId, inc);

  addTransfer({
    tid: msg.tid, name: msg.name, size: msg.size, dir: 'down', peerId: conn.peerId, peerLabel: from,
  });
  updateTransfer(msg.tid, { status: '接收中', sent: inc.received });
  conn.dc.send(JSON.stringify({
    t: 'file-accept', tid: msg.tid, fileId: msg.fileId, offset: inc.received,
  }));
}

/** 通道中途断开：保留已收到的分块，等对方重发同一个文件后续上。 */
function markInterrupted(conn) {
  /* 通道断了，这条连接上的测速计数状态也必须作废 */
  if (conn) speedtestAbort(conn);
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
  const blob = new Blob(inc.chunks, { type: 'application/octet-stream' });
  inc.chunks.length = 0;
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
    if (conn && conn.dc && conn.dc.readyState === 'open') {
      dotClass = 'dot live';
      note = '已直连';
    } else if (conn) {
      dotClass = 'dot busy';
      note = '连接中';
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
          conn2.dc.send(JSON.stringify({ t: 'text', text }));
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

/* 点对点：往选中的设备推 SPEED_BYTES 字节，用与真实文件传输完全相同的
 * pumpFile 通道（同样的背压和分块），所以测出来的就是实际传文件的速度。
 * 对端只计数、不弹窗、不当文件收。 */
async function speedP2p(peerId) {
  const conn = await ensureChannel(peerId);
  const dc = conn.dc;
  dc.send(JSON.stringify({ t: 'speedtest', bytes: SPEED_BYTES }));

  const ready = await new Promise((resolve) => {
    const to = setTimeout(() => resolve(false), 10000);
    const onMsg = (e) => {
      if (typeof e.data !== 'string') return;
      let m;
      try { m = JSON.parse(e.data); } catch (_) { return; }
      if (m.t === 'speedtest-ready') {
        clearTimeout(to);
        dc.removeEventListener('message', onMsg);
        resolve(true);
      }
    };
    dc.addEventListener('message', onMsg);
  });
  if (!ready) throw new Error('对端没有回应测速请求');

  const echoed = new Promise((resolve) => {
    const to = setTimeout(() => resolve(null), 120000);
    const onMsg = (e) => {
      if (typeof e.data !== 'string') return;
      let m;
      try { m = JSON.parse(e.data); } catch (_) { return; }
      if (m.t === 'speedtest-result') {
        clearTimeout(to);
        dc.removeEventListener('message', onMsg);
        resolve(m);
      }
    };
    dc.addEventListener('message', onMsg);
  });

  const blob = new Blob([new Uint8Array(SPEED_BYTES)]);
  const t0 = performance.now();
  try {
    await pumpFile(dc, blob, 0, { chunk: chunkSizeFor(conn) });
    dc.send(JSON.stringify({ t: 'speedtest-end' }));
  } catch (err) {
    /* 没能正常收尾一定要告诉对端，否则它会一直停在计数状态，
     * 把后续真实文件的分块全吞掉。 */
    try { dc.send(JSON.stringify({ t: 'speedtest-abort' })); } catch (_) { /* 通道已断 */ }
    throw err;
  }
  const r = await echoed;
  const secs = (performance.now() - t0) / 1000;
  /* 注意两个数字的含义不同：
   *   mbps       —— 本机把字节交给 DataChannel 的速率，含背压等待，
   *                 也就是"我这边发得多快"。
   *   peerMbps   —— 对端真正收完的速率，由对端计时，这才是端到端速度。
   * 背压存在时本机数字会被等待拉低，所以以对端数字为准。 */
  const peerMbps = (r && r.ms > 0) ? (r.received / 1048576) / (r.ms / 1000) : 0;
  return { mbps: SPEED_BYTES / 1048576 / secs, peerMbps, echoed: r };
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
      speedSet('spP2p', `${p2p.mbps.toFixed(1)} MB/s${extra}`);
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
