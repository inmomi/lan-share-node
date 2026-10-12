'use strict';

/* LAN Share client
 * - Signalling over WebSocket (server only relays offers/answers/ICE).
 * - File bytes travel over a WebRTC DataChannel straight between browsers.
 * - The vault tab uploads to / downloads from the server disk.
 */

const CHUNK = 64 * 1024;
const DRAIN_LIMIT = 1024 * 1024;
const LOW_WATER = 256 * 1024;

const state = {
  myId: null,
  myName: '',
  peers: [],
  conns: new Map(),
  queue: Promise.resolve(),
  incoming: null,
  pendingAccept: new Map(),
  transfers: new Map(),
  targetPeer: null,
  modalResolve: null,
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
    if (state.myName) send({ type: 'rename', name: state.myName });
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
      state.myName = msg.name;
      if (document.activeElement !== $('myName')) $('myName').value = msg.name;
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

function waitDrain(dc) {
  if (dc.bufferedAmount <= DRAIN_LIMIT) return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      dc.removeEventListener('bufferedamountlow', done);
      resolve();
    };
    dc.addEventListener('bufferedamountlow', done);
    setTimeout(done, 5000);
  });
}

function waitAccept(tid) {
  return new Promise((resolve) => {
    state.pendingAccept.set(tid, resolve);
    setTimeout(() => {
      if (state.pendingAccept.has(tid)) {
        state.pendingAccept.delete(tid);
        resolve(false);
      }
    }, 60000);
  });
}

async function sendOne(peerId, file) {
  const conn = await ensureChannel(peerId);
  const tid = makeId();
  addTransfer({
    tid, name: file.name, size: file.size, dir: 'up', peerId,
  });

  conn.dc.send(JSON.stringify({ t: 'file-offer', tid, name: file.name, size: file.size }));

  const ok = await waitAccept(tid);
  if (!ok) {
    updateTransfer(tid, { status: '对方拒绝', failed: true });
    return;
  }

  updateTransfer(tid, { status: '传输中' });
  let offset = 0;
  while (offset < file.size) {
    const slice = file.slice(offset, offset + CHUNK);
    const buf = await slice.arrayBuffer();
    await waitDrain(conn.dc);
    if (conn.dc.readyState !== 'open') throw new Error('连接中断');
    conn.dc.send(buf);
    offset += buf.byteLength;
    updateTransfer(tid, { sent: offset });
  }
  conn.dc.send(JSON.stringify({ t: 'file-end', tid }));
  updateTransfer(tid, { status: '已送达', sent: file.size, done: true });
}

function enqueueSend(peerId, file) {
  state.queue = state.queue.then(() => sendOne(peerId, file)).catch((err) => {
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
    if (msg.t === 'file-offer') {
      askReceive(conn, msg);
    } else if (msg.t === 'file-accept' || msg.t === 'file-reject') {
      const resolve = state.pendingAccept.get(msg.tid);
      if (resolve) {
        state.pendingAccept.delete(msg.tid);
        resolve(msg.t === 'file-accept');
      }
      if (msg.t === 'file-reject') toast('对方拒绝了接收');
    } else if (msg.t === 'file-end') {
      finishReceive();
    } else if (msg.t === 'text') {
      toast(`来自 ${peerName(conn.peerId)}：${msg.text}`);
    }
    return;
  }

  if (state.incoming) {
    state.incoming.chunks.push(data);
    state.incoming.received += data.byteLength;
    updateTransfer(state.incoming.tid, { sent: state.incoming.received });
  }
}

async function askReceive(conn, msg) {
  const from = peerName(conn.peerId);
  const accepted = await confirmDialog(
    '收到传输请求',
    `${from} 想发送「${msg.name}」(${humanSize(msg.size)})`
  );
  if (!accepted) {
    conn.dc.send(JSON.stringify({ t: 'file-reject', tid: msg.tid }));
    return;
  }
  state.incoming = {
    tid: msg.tid, name: msg.name, size: msg.size, from, chunks: [], received: 0,
  };
  addTransfer({ tid: msg.tid, name: msg.name, size: msg.size, dir: 'down', peerId: conn.peerId });
  updateTransfer(msg.tid, { status: '接收中' });
  conn.dc.send(JSON.stringify({ t: 'file-accept', tid: msg.tid }));
}

function finishReceive() {
  const inc = state.incoming;
  state.incoming = null;
  if (!inc) return;
  const blob = new Blob(inc.chunks, { type: 'application/octet-stream' });
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
    + '<div class="bar"><i></i></div><div class="t-sub"></div>';
  el.querySelector('.t-name').textContent = info.name;
  $('transferList').prepend(el);
  const rec = { el, size: info.size, sent: 0, status: '准备中', dir: info.dir, peerId: info.peerId };
  state.transfers.set(info.tid, rec);
  updateTransfer(info.tid, {});
  return rec;
}

function updateTransfer(tid, patch) {
  const rec = state.transfers.get(tid);
  if (!rec) return;
  Object.assign(rec, patch);
  const pct = rec.size ? Math.min(100, Math.round((rec.sent / rec.size) * 100)) : (rec.done ? 100 : 0);
  rec.el.querySelector('.bar i').style.width = `${pct}%`;
  rec.el.querySelector('.t-status').textContent = rec.status || `${pct}%`;
  rec.el.querySelector('.t-sub').textContent = rec.dir === 'down'
    ? `来自 ${peerName(rec.peerId)} · ${humanSize(rec.sent)} / ${humanSize(rec.size)}`
    : `发往 ${peerName(rec.peerId)} · ${humanSize(rec.sent)} / ${humanSize(rec.size)}`;
  rec.el.classList.toggle('done', Boolean(rec.done));
  rec.el.classList.toggle('fail', Boolean(rec.failed));
}

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

function uploadFiles(files) {
  for (const file of files) {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', `/api/upload?name=${encodeURIComponent(file.name)}`);
    xhr.upload.addEventListener('progress', (event) => {
      if (event.lengthComputable && event.loaded === event.total) toast(`已上传「${file.name}」`);
    });
    xhr.addEventListener('load', () => {
      if (xhr.status === 200) {
        refreshVault();
      } else {
        let reason = xhr.statusText;
        try {
          reason = JSON.parse(xhr.responseText).error || reason;
        } catch (_) { /* keep status text */ }
        toast(`上传失败：${reason}`);
      }
    });
    xhr.addEventListener('error', () => toast(`上传失败：${file.name}`));
    xhr.send(file);
  }
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
