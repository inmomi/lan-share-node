'use strict';

/* 真实浏览器端到端点对点测试。
 *
 * 前面所有测试都是拿假 DataChannel 在 Node 里跑 pacing 逻辑，能验证边界，
 * 但验证不了"真实 Chrome 的 SCTP 到底能跑多快"——而这正是用户遇到的问题：
 * 同一对设备 SMB 30MB/s，点对点只有 3.2MB/s，4 条并行也只到 5.8MB/s。
 * 瓶颈可能在真实浏览器的 SCTP 栈里，也可能在链路上，猜是猜不出来的。
 *
 * 这个脚本起两个真实浏览器（各自独立进程、独立 profile），都打开应用页面，
 * 让它们互相发现，然后：
 *   1. 跑页面内置的 speedP2p()——和真实传文件共用同一条 pumpFile 通路，
 *      同时报链路诊断（候选对类型 / RTT）和单条 lane 的速率；
 *   2. 真传一个文件（默认 64MB），自动点确认框，等下载落盘，
 *      逐字节校验内容和长度——并行分段拼回去只要错一段，这里就会现形。
 *
 * 用法：
 *   node tools/p2p-e2e.js                        # 自己拉服务端（node server.js）
 *   node tools/p2p-e2e.js --url http://127.0.0.1:8099   # 服务端已经起好了
 *   node tools/p2p-e2e.js --file-mb 256 --timeout 300
 *
 * 依赖：只用 Node 内置模块 + tools/cdp.js，不装任何 npm 包。
 * 浏览器：CI 用 ubuntu-latest 自带的 Chrome；本机可以 CHROME_PATH 指过去。
 */

const { spawn } = require('child_process');
const path = require('path');
const os = require('os');
const fs = require('fs');
const crypto = require('crypto');
const { launchBrowser } = require('./cdp.js');

/* ---------------------------------------------------------------- 参数 */

function arg(name, dflt) {
  const i = process.argv.indexOf(`--${name}`);
  if (i < 0) return dflt;
  const v = process.argv[i + 1];
  return v === undefined || v.startsWith('--') ? true : v;
}

const FILE_MB = Number(arg('file-mb', 64));
const TIMEOUT_MS = Number(arg('timeout', 240)) * 1000;
/* C 版在这里读 --server 指定可执行文件；Node 版的服务端就是 server.js，
 * 由本脚本用当前 node 直接拉起，没有可指定的产物，所以没有这个参数。 */
const SERVER_URL = arg('url', null) || arg('server-url', null);
const LABEL = arg('label', 'e2e');
/* 真实双机 / netns 场景：每个浏览器走各自的信令 URL、各自的 netns 包裹前缀、
 * 各自的 DevTools 暴露地址。缺省回退到统一的 SERVER_URL / 同机回环。 */
const SERVER_URL_A = arg('server-url-a', null) || SERVER_URL;
const SERVER_URL_B = arg('server-url-b', null) || SERVER_URL;
const DEVTOOLS_A = arg('devtools-host-a', '127.0.0.1');
const DEVTOOLS_B = arg('devtools-host-b', '127.0.0.1');
/* DevTools 端口：浏览器 A / B 各一个。netns 场景下 ci-realnet.sh 起的转发代理
 * 必须按同样的端口转发（nsA→9331、nsB→9332），两边约定一致才连得上——之前
 * 两个 ns 都按 9331 起代理，B 的 9332 没人转发，于是"DevTools 未就绪"。 */
const DEVTOOLS_PORT_A = Number(arg('devtools-port-a', 9331));
const DEVTOOLS_PORT_B = Number(arg('devtools-port-b', 9332));

/* --no-mdns-bypass：按真实浏览器的默认行为跑（开 mDNS 候选伪装）。
 * 默认不加这条时测试会关掉 mDNS 伪装，那等于把真机上最常见的故障机制
 * 屏蔽掉——同一路由器同 SSID 的两台设备能看到对方却建不起直连，就是这个
 * 机制在起作用。要复现它就必须开着 mDNS 跑。 */
const MDNS_BYPASS = arg('no-mdns-bypass', null) !== true;

/* --require-mode p2p|relay：断言最终走的是哪一层。
 * 不能只断言"传完了"：兜底通道一加上，直连失败也能传完，T1 就会退化成
 * "永远通过"。直连用例必须要求真的走 p2p，兜底用例必须要求真的降级了，
 * 两边才有意义。 */
const REQUIRE_MODE = String(arg('require-mode', 'any') || 'any');
/* 吞吐下限（MB/s）：低于此值判失败，用于守住"中继/直连确实能用"的回归。0 表示不检查。 */
const THROUGHPUT_FLOOR = Number(arg('throughput-floor', 0));

/* --probe-lane-retire：传输进行中，故意把一条"注定失败"的 WebRTC lane 走
 * retireLane 判死刑，断言这次传输必须活下来。
 *
 * 守的是一个必现的竞态：直连建不起来时文件走兜底通道，而那 32 条 WebRTC lane
 * 还在同一时间轴上陆续 ICE 失败、被 retireLane 依次收掉。老代码的
 * markInterrupted 不区分死的是哪条 lane，于是"第一批判死刑"的那一刻就把正在
 * 传的传输写成「已中断（N MB），可续传」并丢弃后续所有字节——真机上表现为
 * "传了 19.7MB 到 32.6MB 之间就断"，CI 里则表现为同一份代码时绿时红。
 *
 * 这里不依赖任何时序：直接调用那条真实路径，改前必红、改后必绿。
 * 默认在 relay 档开启（正是会走兜底的那一档）。 */
const PROBE_LANE_RETIRE =
  String(arg('probe-lane-retire', REQUIRE_MODE === 'relay' ? '1' : '0')) !== '0';

/* 全局硬超时：任何意外的永久挂起（比如兜底超时之前、或收尾阶段卡住）都不能
 * 让 job 一直吃到 GitHub 的 30 分钟墙。内部上传循环有 TIMEOUT_MS 上限，
 * 这里在它之外再给足余量后强制退出。 */
const HARD_MS = TIMEOUT_MS + 180000;
setTimeout(() => {
  log(`全局硬超时 ${HARD_MS}ms，强制退出（疑似永久挂起）`);
  process.exit(1);
}, HARD_MS).unref();

/* 把 "ip netns exec nsA" 这样的空格分隔串拆成命令前缀数组。 */
function splitWrap(s) {
  if (!s || typeof s !== 'string') return [];
  return s.trim().split(/\s+/).filter(Boolean);
}
const WRAP_A = splitWrap(arg('chrome-wrap-a', null));
const WRAP_B = splitWrap(arg('chrome-wrap-b', null));
/* lane 数量扫描：--sweep 1,2,4,8,16。回答"并行到底能不能线性扩展"——
 * 4 条在 Runner 上合计约 26MB/s、单条约 6.5MB/s，如果是每关联的上限，
 * 加 lane 就该继续涨；如果到顶了，说明瓶颈是共享的，加 lane 是徒劳。
 * 每一轮都走真实 pumpFile + 对端回执，速率由对端计时，不受发送队列干扰。 */
const SWEEP = arg('sweep', null);
const SWEEP_MB = Number(arg('sweep-mb', 16));
const ROOT = path.join(__dirname, '..');
const PORT = 8471;

/* 文件内容用确定性图案生成，Node 侧能用同样的规则复算哈希，
 * 这样"拼回去对不对"就是逐字节可比对的，而不是只看长度。 */
const PATTERN = (i) => ((i * 7 + (i >>> 3)) & 0xFF);

function expectedBuffer(size) {
  const b = Buffer.allocUnsafe(size);
  for (let i = 0; i < size; i++) b[i] = PATTERN(i);
  return b;
}

function log(...a) {
  /* 直接写 fd 1：管道下游是 CI 日志或被 timeout 杀掉时，console.log 的缓冲
   * 会整段丢失，排查起来全是空的。 */
  fs.writeSync(1, a.join(' ') + '\n');
}
function fail(msg) { log(`FAIL  ${msg}`); process.exitCode = 1; }

/* ------------------------------------------------------------ 服务端 */

function startServer() {
  /* Node 版：直接用跑测试的这个 node 拉起 server.js，cwd 必须是仓库根目录
   * （server.js 从相对路径找 public/ 和 uploads/）。端口和上传目录用环境
   * 变量给（Node 版没有 -p/-d/-w 命令行参数），上传目录指到临时目录，跑完即焚。 */
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lanshare-e2e-'));
  const child = spawn(process.execPath, ['server.js'], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), UPLOAD_DIR: dir },
    stdio: 'ignore',
  });
  child.on('error', (err) => { throw new Error(`服务端启动失败: ${err.message}`); });
  return child;
}

async function waitServer(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const r = await fetch(url);
      if (r.ok) return;
    } catch (_) { /* 还没起来 */ }
    if (Date.now() > deadline) throw new Error('服务端 20 秒内未就绪');
    await new Promise((r) => setTimeout(r, 200));
  }
}

/* ------------------------------------------------------------ 浏览器侧帮助 */

const READY = `(function(){
  if (typeof state === 'undefined') return 'no-state';
  if (!state.myId) return 'no-id';
  return state.peers.length ? 'ready' : 'no-peer';
})()`;

async function waitReady(page, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const s = await page.evaluate(READY);
    if (s === 'ready') return;
    if (s === 'no-state' || s === 'no-id') {
      if (Date.now() > deadline) throw new Error(`页面未就绪: ${s}`);
    } else if (Date.now() > deadline) {
      throw new Error('等不到对端出现（两台浏览器没有互相发现）');
    }
    await new Promise((r) => setTimeout(r, 250));
  }
}

const peerIds = (page) => page.evaluate('state.peers.map(p => p.id)');
const peerNames = (page) => page.evaluate('state.peers.map(p => p.name)');

/* 等 WebRTC 连接真正 established。app 在发现对端后会自动建 lane（每条一个
 * RTCPeerConnection），这里轮询每条 lane 的 pc.connectionState，全部 connected
 * 才继续；failed 或超时则把 ICE 候选对诊断打出来，让 CI 日志直接告诉我们
 * "直连/中继到底通没通"，而不是默默卡死等到 job 被掐。 */
async function waitConnected(page, peerId, timeoutMs, minConnected) {
  /* minConnected：至少连上几条才算通（默认 1）。LANES 是 32，全连上要受
   * initiate 的 150ms 逐条建链、以及 TURN 逐条分配中继端口影响，拿"全部连上"
   * 当闸门会误杀；能通不能通看至少 1 条，快不快交给后面的吞吐下限去卡。 */
  const need = minConnected || 1;
  const deadline = Date.now() + timeoutMs;
  let last = { found: false, lanes: [] };
  for (;;) {
    const st = await page.evaluate(`(function(){
      const conn = state.conns.get(${JSON.stringify(peerId)});
      if (!conn) return { found: false };
      return { found: true, relay: !!conn.relay, lanes: conn.lanes.map(function(l){
        return { cs: l.pc ? l.pc.connectionState : 'no-pc', ice: l.pc ? l.pc.iceConnectionState : 'no-pc' };
      }) };
    })()`);
    last = st;
    /* 走兜底通道也算"通"：这一路的正确性由后面的真实文件传输 + 校验去卡，
     * 而不是看有没有 WebRTC lane。直连失败本来就是被测场景之一。 */
    if (st.found && st.relay) {
      return { ok: true, connected: 0, total: st.lanes.length, lanes: st.lanes, mode: 'relay' };
    }
    if (st.found && st.lanes.length) {
      /* 不把瞬时 failed 判死：app 的 retireLane 会把 failed 的 lane 换成全新
       * 的 pc（回到 new/connecting 重新建链），这种可自愈的抖动不该让闸门失败。 */
      const n = st.lanes.filter((l) => l.cs === 'connected').length;
      if (n >= need) return { ok: true, connected: n, total: st.lanes.length, lanes: st.lanes, mode: 'p2p' };
    }
    if (Date.now() > deadline) {
      const lanes = st.found ? st.lanes : [];
      return {
        ok: false, timeout: true, mode: 'none',
        connected: lanes.filter((l) => l.cs === 'connected').length,
        total: lanes.length, lanes,
      };
    }
    await new Promise((r) => setTimeout(r, 300));
  }
}

/* 从 getStats 挖出每条 lane 选中的候选对（类型 + 地址）。悬而未连时这份
 * 信息能直接定位是"host 候选没互通"还是"中继根本没用上"。 */
async function dumpConnDiag(page, peerId) {
  try {
    return await page.evaluate(`(async () => {
      const conn = state.conns.get(${JSON.stringify(peerId)});
      if (!conn) return { noConn: true };
      const out = [];
      for (const l of conn.lanes) {
        const pc = l.pc;
        /* ss=signalingState：区分"offer 已发出在等 answer"（have-local-offer）
         * 和"压根没建链"（stable/new）——连不上时这是最关键的一条信息。 */
        const info = {
          cs: pc ? pc.connectionState : null,
          ice: pc ? pc.iceConnectionState : null,
          ss: pc ? pc.signalingState : null,
        };
        if (pc && pc.getStats) {
          try {
            const stats = await pc.getStats();
            let pair = null, local = null, remote = null;
            stats.forEach((s) => {
              if (s.type === 'candidate-pair' && s.state === 'succeeded' && !pair) pair = s;
              if (s.type === 'local-candidate' && !local && s.candidateType) local = s;
              if (s.type === 'remote-candidate' && !remote && s.candidateType) remote = s;
            });
            if (pair) info.pair = { type: pair.candidateType || pair.type };
            if (local) info.local = { type: local.candidateType, addr: (local.address || '') + ':' + (local.port || '') };
            if (remote) info.remote = { type: remote.candidateType, addr: (remote.address || '') + ':' + (remote.port || '') };
          } catch (e) { info.statsErr = String(e); }
        }
        out.push(info);
      }
      return out;
    })()`);
  } catch (e) { return { err: e.message }; }
}

/* ------------------------------------------------------------ 主流程 */

(async () => {
  let server = null;
  let pageA = null;
  let pageB = null;
  const dlDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lanshare-dl-'));

  try {
    const urlA = SERVER_URL_A || `http://127.0.0.1:${PORT}`;
    const urlB = SERVER_URL_B || `http://127.0.0.1:${PORT}`;
    /* 只有完全没给任何 URL 时才自己拉服务端（默认同机回环）。
     * 给了 --server-url* 就认为服务端已经由外部（CI 步骤）起好。 */
    if (!SERVER_URL_A && !SERVER_URL_B && !SERVER_URL) {
      server = startServer();
      await waitServer(urlA, 20000);
    } else {
      await waitServer(urlA, 20000);
    }
    log(`服务端: A→${urlA}  B→${urlB}`);

    log('拉起两个浏览器…');
    pageA = await launchBrowser({
      port: DEVTOOLS_PORT_A, wrap: WRAP_A,
      devtoolsHost: DEVTOOLS_A, devtoolsPort: DEVTOOLS_PORT_A,
      mdnsBypass: MDNS_BYPASS,
    });
    pageB = await launchBrowser({
      port: DEVTOOLS_PORT_B, wrap: WRAP_B,
      devtoolsHost: DEVTOOLS_B, devtoolsPort: DEVTOOLS_PORT_B,
      mdnsBypass: MDNS_BYPASS,
    });
    await pageA.navigate(urlA);
    await pageB.navigate(urlB);
    await waitReady(pageA, 30000);
    await waitReady(pageB, 30000);

    const idsA = await peerIds(pageA);
    const idsB = await peerIds(pageB);
    const namesB = await peerNames(pageB);
    log(`A 看到对端: ${idsA.join(',')}（${namesB.join('、')}）`);
    if (!idsA.length || !idsB.length) throw new Error('两端没有互相发现，无法继续');
    const targetFromA = idsA[0];

    /* ---------------------------------------------------- 0. TCP 基线 */
    /* 同一对进程、同一台机器上服务端 HTTP 上传/下载能跑多快。
     * P2P 数字只有和这个基线比才有意义：基线 300MB/s 而 P2P 5MB/s，
     * 瓶颈在 WebRTC/SCTP 侧；基线也只有 10MB/s，那就是机器/网络的问题。 */
    log('\n--- TCP 基线（服务端上传/下载，同机对照）---');
    try {
      const tcp = await pageA.evaluate(`(async () => {
        const up = await speedUpload();
        const down = await speedDownload();
        return { up: up.mbps, down: down.mbps };
      })()`);
      log(`  上传: ${tcp.up.toFixed(1)} MB/s · 下载: ${tcp.down.toFixed(1)} MB/s`);
    } catch (err) {
      log(`  TCP 基线测速失败（不影响 P2P 结论）: ${err.message}`);
    }

    /* ---------------------------------------------------- 连接闸门
     * 先显式等连接建立再跑测速：否则一旦 netns 直连或中继不通，speedP2p 会
     * 永远 await，整个 job 只能被 GitHub 掐掉。这里 60s 内没 connected 就直接
     * 失败并打印 ICE 诊断，日志里一眼能看出是"host 候选不通"还是"中继没用上"。 */
    log('\n--- 等待 WebRTC 连接建立（闸门，60s）---');
    /* 必须走 app 自己的 ensureChannel()，不能只调 initiate()：
     *  - getConn 是懒创建的，不调它 state.conns 里根本没有这条连接，
     *    waitConnected 只会空等超时（第一次跑就得到 {"noConn":true}）；
     *  - 更重要的是兜底通道是 ensureChannel 在"直连建不起来"时才建的。只调
     *    initiate 的话 conn.relay 永远是 undefined，兜底用例会误判成连接超时，
     *    而真实原因其实是"压根没人去决定走哪一层"。 */
    const mode0 = await pageA.evaluate(
      `(async () => { const r = await ensureChannel(${JSON.stringify(targetFromA)}); return r.mode; })()`,
      90000);
    log(`ensureChannel 选定传输层: ${mode0}`);
    const connState = await waitConnected(pageA, targetFromA, 60000, 1);
    if (!connState.ok) {
      /* 两侧都要看：A 停在 have-local-offer 说明 offer 发出去了但没等到 answer，
       * 而 B 侧 noConn 就说明 offer 根本没送到——两种原因修法完全不同。 */
      const myId = await pageA.evaluate('state.myId');
      /* 最关键的一条：TURN 到底有没有真的进到页面里。没进去的话隔离场景下
       * 就只剩被 DROP 的 host 候选，必然连不上，而日志上和"中继配错了"长得一样。 */
      const iceA = await pageA.evaluate(
        'JSON.stringify({ injected: window.__LAN_SHARE_TURN__ || null,'
        + ' built: (typeof buildIceServers === "function") ? buildIceServers() : "no-fn" })');
      const iceB = await pageB.evaluate(
        'JSON.stringify({ injected: window.__LAN_SHARE_TURN__ || null,'
        + ' built: (typeof buildIceServers === "function") ? buildIceServers() : "no-fn" })');
      log(`A 页 ICE 配置: ${iceA}`);
      log(`B 页 ICE 配置: ${iceB}`);
      log(`连接未建立: ${JSON.stringify(connState)}`);
      log(`A 侧（发给 ${targetFromA}）: ${JSON.stringify(await dumpConnDiag(pageA, targetFromA))}`);
      log(`B 侧（发给 ${myId}）: ${JSON.stringify(await dumpConnDiag(pageB, myId))}`);
      throw new Error('WebRTC 连接未在 60s 内建立（netns 直连或中继路径不通）');
    }
    if (connState.mode === 'relay') {
      log('直连未建立，走兜底通道（WebSocket 中转）——本用例就是要验证这一路也能传');
    } else {
      log(`连接已建立: ${connState.connected}/${connState.total} 条 lane connected`);
    }
    if (REQUIRE_MODE !== 'any' && connState.mode !== REQUIRE_MODE) {
      throw new Error(`本用例要求走 ${REQUIRE_MODE} 层，实际走的是 ${connState.mode} 层`);
    }
    log(`传输层: ${connState.mode}（要求 ${REQUIRE_MODE}）✓`);

    /* ---------------------------------------------------- 1. 内置测速 */
    log('\n--- 内置测速（speedP2p，与真实传输同一条 pumpFile 通路）---');
    /* 连跑两次：第一次连接是刚建好的（冷启动，SCTP 拥塞窗口从零开始爬），
     * 第二次连接已经热了。两次差很多就说明慢在冷启动而不是链路。 */
    const runs = [];
    for (let i = 0; i < 2; i++) {
      const t0 = Date.now();
      let sp;
      try {
        sp = await pageA.evaluate(`(async () => {
          const r = await speedP2p(${JSON.stringify(targetFromA)});
          return JSON.parse(JSON.stringify(r));
        })()`);
      } catch (err) {
        /* 测速失败时把"传输层是怎么断的"挖出来一起打。只报一句"连接中断"
         * 完全定位不了：信令 WebSocket 断了、兜底通道被换掉、还是对端先挂，
         * 三种原因在日志里长得一模一样。 */
        log(`  测速失败: ${err.message}`);
        for (const [tag, pg] of [['A', pageA], ['B', pageB]]) {
          try {
            /* 之前这里按 targetFromA 去查 conns——那是"对端"的 id，在 B 的
             * 表里根本不存在，于是 B 侧永远打出 relay:null/lanes:-1，看着像
             * "B 什么都没建"，其实是查错了键。现在整表打出来，一个都不漏。 */
            const d = await pg.evaluate(`(function(){
              const out = {
                myId: state.myId,
                wsState: ws ? ws.readyState : -1,
                wsBuffered: ws ? ws.bufferedAmount : -1,
                wsClosed: window.__wsClose || null,
                conns: [],
              };
              state.conns.forEach(function(conn, key){
                out.conns.push({
                  key: key,
                  relay: conn.relay ? {
                    ready: conn.relay.dc.readyState,
                    inflight: conn.relay.dc.inflight,
                    buffered: conn.relay.dc.bufferedAmount,
                    ackPending: conn.relay.ackPending,
                  } : null,
                  lanes: (conn.lanes || []).length,
                  open: (conn.lanes || []).filter(function(l){
                    return l.dc && l.dc.readyState === 'open';
                  }).length,
                });
              });
              return JSON.stringify(out);
            })()`);
            log(`  ${tag} 侧传输层状态: ${d}`);
          } catch (e2) { log(`  ${tag} 侧状态读取失败: ${e2.message}`); }
        }
        throw err;
      }
      const spSecs = ((Date.now() - t0) / 1000).toFixed(1);
      runs.push(sp);
      log(`  [第 ${i + 1} 次${i === 0 ? '（冷连接）' : '（热连接）'}]`);
      log(`    链路诊断 : ${sp.diag ? sp.diag.text : '（无）'}`);
      log(`    本端交付 : ${sp.mbps.toFixed(2)} MB/s（含背压等待，${spSecs}s 墙钟）`);
      log(`    对端收到 : ${(sp.echoed ? sp.echoed.received : 0) / 1048576} MB`
        + ` @ ${sp.peerMbps.toFixed(2)} MB/s`);
      log(`    并行 lane: ${sp.lanes} 条（单条约 ${(sp.mbps / sp.lanes).toFixed(2)} MB/s）`);
    }
    const sp = runs[runs.length - 1];

    /* ---------------------------------------------------- 2. 真实文件传输 */
    log(`\n--- 真实文件传输（${FILE_MB}MB，自动确认接收，逐字节校验）---`);
    await pageB.enableDownloads(dlDir);
    /* 接收端的确认框是自定义 DOM 弹窗（不是 window.confirm），CDP 的对话框
     * 自动应答管不到它。这里挂个 MutationObserver，弹窗一出现就点"确定"。 */
    const hooked = await pageB.evaluate(`(function(){
      const modal = document.getElementById('modal');
      const yes = document.getElementById('modalYes');
      if (!modal || !yes) return 'no-modal';
      const obs = new MutationObserver(() => {
        if (!modal.classList.contains('hidden')) yes.click();
      });
      obs.observe(modal, { attributes: true, attributeFilter: ['class'] });
      window.__autoAccept = obs;
      return 'hooked';
    })()`);
    if (hooked !== 'hooked') fail(`接收端弹窗挂钩失败: ${hooked}`);
    const fileName = `e2e-${LABEL}-${FILE_MB}mb.bin`;

    const sendStart = Date.now();
    await pageA.evaluate(`(async () => {
      const size = ${FILE_MB} * 1024 * 1024;
      const buf = new Uint8Array(size);
      for (let i = 0; i < size; i++) buf[i] = ((i * 7 + (i >>> 3)) & 0xFF);
      const file = new File([buf], ${JSON.stringify(fileName)});
      enqueueSend(${JSON.stringify(targetFromA)}, file);
      return true;
    })()`);

    /* 等发送端记录变成"已送达" */
    let upRec = null;
    let laneRetireProbe = null;
    const upDeadline = Date.now() + TIMEOUT_MS;
    let lastDump = 0;
    for (;;) {
      upRec = await pageA.evaluate(`(function(){
        for (const r of state.transfers.values()) {
          /* 记录里没有 name 字段（名字只写进 DOM），发送端带 file 对象，按它匹配 */
          if (r.dir === 'up' && r.file && r.file.name === ${JSON.stringify(fileName)}) return { status: r.status, sent: r.sent, size: r.size, done: !!r.done, failed: !!r.failed };
        }
        return null;
      })()`);
      if (upRec && (upRec.done || upRec.failed)) break;

      /* 传输进行中注入一次"注定失败的 WebRTC lane 被判死刑"（见文件头
       * PROBE_LANE_RETIRE 的说明）。只注入一次，且必须挑一条不属于本次传输的
       * lane——挑错了这次注入就没有意义，宁可判失败也不能假绿。 */
      if (PROBE_LANE_RETIRE && !laneRetireProbe) {
        const pr = await pageB.evaluate(`(function(){
          const conn = [...state.conns.values()].find((c) => c.relay);
          if (!conn) return null;             /* 还没降级到兜底，稍后再试 */
          if (!state.incoming) return null;   /* 传输还没进入接收状态 */
          const inc = state.incoming;
          const victim = conn.lanes.find((l) => !(inc.lanes && inc.lanes.has(l))) || conn.lanes[0];
          const statusOf = () => {
            for (const r of state.transfers.values()) if (r.dir === 'down') return r.status;
            return null;
          };
          const statusBefore = statusOf();
          const receivedBefore = inc.received;
          retireLane(victim);                 /* 走真机上的真实路径 */
          return {
            victimIsRelay: victim.relay === true,
            receivedBefore,
            survived: !!state.incoming,
            statusBefore,
            statusAfter: statusOf(),
          };
        })()`);
        if (pr) {
          laneRetireProbe = pr;
          log(`  注入探针: 判死一条 WebRTC lane（isRelay=${pr.victimIsRelay}，`
            + `已收 ${(pr.receivedBefore / 1048576).toFixed(1)} MB）→ `
            + `传输${pr.survived ? '仍在 ✓' : '被误判中断 ✗'} · `
            + `${JSON.stringify(pr.statusBefore)} → ${JSON.stringify(pr.statusAfter)}`);
          if (pr.victimIsRelay) {
            fail('探针挑到了兜底通道本身（isRelay=true），这次注入无效');
          } else if (!pr.survived) {
            fail('一条与本次传输无关的 WebRTC lane 判死，却把正在进行的兜底传输判成了中断');
          }
        }
      }

      /* 卡住的时候最需要知道两端各自停在哪：发送记录、对端连接状态、
       * 待确认的 accept、接收端 incoming。每 3 秒打一次。 */
      if (Date.now() - lastDump > 3000) {
        lastDump = Date.now();
        const dump = await pageA.evaluate(`(function(){
          const conns = [];
          for (const c of state.conns.values()) {
            conns.push(c.lanes.map((l) => l.dc ? l.dc.readyState : 'no-dc').join('/'));
          }
          return {
            queue: state.queue === Promise.resolve() ? 'idle' : 'busy',
            pending: state.pendingAccept.size,
            conns,
            incoming: state.incoming ? { name: state.incoming.name, received: state.incoming.received, lanesDone: state.incoming.lanesDone, expected: state.incoming.expectedLanes } : null,
          };
        })()`);
        log(`  …发送端 ${JSON.stringify(upRec)} | A: ${JSON.stringify(dump)}`);
        const dumpB = await pageB.evaluate(`(function(){
          const conns = [];
          for (const c of state.conns.values()) {
            conns.push(c.lanes.map((l) => l.dc ? l.dc.readyState : 'no-dc').join('/'));
          }
          return {
            pending: state.pendingAccept.size,
            conns,
            modalHidden: document.getElementById('modal') ? document.getElementById('modal').classList.contains('hidden') : 'no-modal',
            incoming: state.incoming ? { name: state.incoming.name, received: state.incoming.received, lanesDone: state.incoming.lanesDone, expected: state.incoming.expectedLanes } : null,
            down: [...state.transfers.values()].filter((r) => r.dir === 'down').map((r) => ((r.file && r.file.name) || r.tid) + ':' + r.status),
          };
        })()`);
        log(`  …接收端 ${JSON.stringify(dumpB)}`);
      }
      if (Date.now() > upDeadline) break;
      await new Promise((r) => setTimeout(r, 300));
    }
    const sendSecs = (Date.now() - sendStart) / 1000;

    /* 兜底档里探针一定能注入（通道已降级 + 接收状态会持续整个传输）。
     * 没注入成功就是这条断言失效了，必须炸出来——否则它会静默变成"永远绿"。 */
    if (PROBE_LANE_RETIRE && !laneRetireProbe) {
      fail('lane 判死探针没能注入（兜底档里应当总能拿到 relay 通道与进行中的接收）');
    }

    if (!upRec || !upRec.done) {
      fail(`发送未完成: ${JSON.stringify(upRec)}`);
    } else {
      log(`  发送端: ${(upRec.sent / 1048576).toFixed(1)} MB / ${(upRec.size / 1048576).toFixed(1)} MB`
        + `，${sendSecs.toFixed(1)}s → ${(upRec.size / 1048576 / sendSecs).toFixed(2)} MB/s`);
    }

    /* 等接收端把文件存下来 */
    let transferOk = false;
    let e2eRate = 0;
    const saved = await pageB.waitForDownload(fileName, TIMEOUT_MS);
    /* 真正的端到端速率要算到"文件落盘"为止。发送端的 done 现在代表对端
     * 已组装落盘（收到 file-done 回执），与 waitForDownload 命中的时间点
     * 几乎重合，所以下面的"交付速率"和"端到端"会收敛到同一个数。 */
    const e2eSecs = (Date.now() - sendStart) / 1000;
    if (!saved) {
      fail('接收端没有出现下载文件');
    } else {
      const got = fs.readFileSync(saved);
      const want = expectedBuffer(FILE_MB * 1024 * 1024);
      const hashGot = crypto.createHash('sha256').update(got).digest('hex');
      const hashWant = crypto.createHash('sha256').update(want).digest('hex');
      if (got.length !== want.length) {
        fail(`文件长度不对: ${got.length} != ${want.length}`);
      } else if (hashGot !== hashWant) {
        /* 找出第一个不一致的偏移，直接告诉你是哪一段拼错了 */
        let firstBad = -1;
        for (let i = 0; i < want.length; i++) {
          if (got[i] !== want[i]) { firstBad = i; break; }
        }
        fail(`文件内容不一致（第一个差异在 ${firstBad} 字节，即第 ${Math.floor(firstBad / (FILE_MB * 1024 * 1024 / 4))} 段附近）`);
      } else {
        log(`  接收端: ${(got.length / 1048576).toFixed(1)} MB 落盘，SHA-256 一致 ✓`);
        log(`  交付速率 : ${(FILE_MB / sendSecs).toFixed(2)} MB/s（发起到对端回执 done）`);
        log(`  端到端   : ${(FILE_MB / e2eSecs).toFixed(2)} MB/s（发起到对端落盘，这才是真实速率）`);
        transferOk = true;
        e2eRate = FILE_MB / e2eSecs;
      }
    }

    /* 吞吐下限：守住"直连 / 中继确实能跑通且达到可用速率"的回归。 */
    if (transferOk && THROUGHPUT_FLOOR > 0) {
      if (e2eRate < THROUGHPUT_FLOOR) {
        fail(`端到端吞吐 ${e2eRate.toFixed(2)} MB/s 低于下限 ${THROUGHPUT_FLOOR} MB/s`);
      } else {
        log(`  吞吐下限 : ${e2eRate.toFixed(2)} MB/s ≥ ${THROUGHPUT_FLOOR} MB/s ✓`);
      }
    }

    /* ---------------------------------------------------- 3. 接收端状态 */
    const downRec = await pageB.evaluate(`(function(){
      /* 接收端记录不带 file，测试里只有一个文件，按 size 匹配 */
      for (const r of state.transfers.values()) {
        if (r.dir === 'down' && r.size === ${FILE_MB * 1024 * 1024}) return { status: r.status, sent: r.sent, size: r.size };
      }
      return null;
    })()`);
    log(`  接收端记录: ${JSON.stringify(downRec)}`);
    /* 探针的收尾检查：判死一条无关 lane 之后，这次传输必须仍然走到"已保存"，
     * 不能以"已中断"收场。 */
    if (laneRetireProbe && downRec && /已中断/.test(String(downRec.status))) {
      fail(`注入 lane 判死探针后接收端仍以中断收场: ${JSON.stringify(downRec)}`);
    }

    /* ---------------------------------------------------- 4. lane 数量扫描 */
    if (SWEEP) {
      log(`\n--- lane 数量扫描（热连接，每轮 ${SWEEP_MB}MB，速率由对端计时）---`);
      const counts = String(SWEEP).split(',').map((s) => Number(s.trim()))
        .filter((n) => Number.isFinite(n) && n > 0);
      const rounds = await pageA.evaluate(`(async () => {
        const conn = state.conns.get(${JSON.stringify(targetFromA)});
        const out = [];
        const blob = new Blob([new Uint8Array(${SWEEP_MB} * 1024 * 1024)]);
        for (const n of ${JSON.stringify(counts)}) {
          if (n > conn.lanes.length) { out.push({ n, error: 'lane 不够（只有 ' + conn.lanes.length + ' 条）' }); continue; }
          const lanes = conn.lanes.slice(0, n);
          const ranges = splitRange(0, ${SWEEP_MB} * 1024 * 1024, n);
          /* 和 speedP2p 同一套握手：逐 lane 发 speedtest、等 ready、再泵数据。
           * 丢包链路上控制消息本身会丢（2% 丢包下几十次握手必中几次），
         * 所以每 2 秒重发一次，别一丢就判死。 */
          await Promise.all(lanes.map((lane, i) => new Promise((resolve, reject) => {
            let tries = 0;
            const send = () => {
              tries++;
              try {
                lane.dc.send(JSON.stringify({ t: 'speedtest', bytes: ranges[i].to - ranges[i].from, ch: lane.index }));
              } catch (err) { clearInterval(rep); clearTimeout(to); reject(err); return; }
            };
            const rep = setInterval(send, 2000);
            const to = setTimeout(() => {
              clearInterval(rep);
              lane.dc.removeEventListener('message', onMsg);
              reject(new Error('lane ' + i + ' 对端没有回应测速请求（重发 ' + tries + ' 次）'));
            }, 30000);
            const onMsg = (e) => {
              if (typeof e.data !== 'string') return;
              let m; try { m = JSON.parse(e.data); } catch (_) { return; }
              if (m.t === 'speedtest-ready' && Number(m.ch) === lane.index) {
                clearInterval(rep); clearTimeout(to);
                lane.dc.removeEventListener('message', onMsg); resolve();
              }
            };
            lane.dc.addEventListener('message', onMsg);
            send();
          })));
          const echoes = lanes.map((lane) => new Promise((resolve) => {
            const to = setTimeout(() => resolve(null), 60000);
            const onMsg = (e) => {
              if (typeof e.data !== 'string') return;
              let m; try { m = JSON.parse(e.data); } catch (_) { return; }
              if (m.t === 'speedtest-result' && Number(m.ch) === lane.index) {
                clearTimeout(to); lane.dc.removeEventListener('message', onMsg); resolve(m);
              }
            };
            lane.dc.addEventListener('message', onMsg);
          }));
          await Promise.all(ranges.map((r, i) =>
            pumpFile(lanes[i].dc, blob.slice(r.from, r.to), 0, { chunk: chunkSizeFor(lanes[i]) })
              .then(() => { lanes[i].dc.send(JSON.stringify({ t: 'speedtest-end', ch: lanes[i].index })); })));
          const results = await Promise.all(echoes);
          const received = results.reduce((a, r) => a + (r ? r.received : 0), 0);
          /* 并行时取最慢那条 lane 的耗时当分母，偏保守但不会虚高 */
          const slowest = results.reduce((mx, r) => Math.max(mx, r ? r.ms : 0), 0);
          out.push({
            n, received, slowest,
            mbps: slowest > 0 ? received / 1048576 / (slowest / 1000) : 0,
            perLane: results.map((r) => (r && r.ms > 0 ? (r.received / 1048576) / (r.ms / 1000) : 0)),
          });
        }
        return out;
        /* lane 扫描要按 TIMEOUT_MS 给足预算，不能用 evaluate 的默认 120s：
         * 6 档 lane 数（1,2,4,8,16,32）逐档跑，丢包链路上每档最多要等 30s
         * 握手重传，累计轻松超过 120s——20ms+2% 丢包那档就是这么被误杀的。 */
      })()`, TIMEOUT_MS);
      for (const r of rounds) {
        if (r.error) { log(`  ${String(r.n).padStart(2)} 条: ${r.error}`); continue; }
        log(`  ${String(r.n).padStart(2)} 条 lane: 合计 ${r.mbps.toFixed(1)} MB/s`
          + `（单条 ${r.perLane.map((x) => x.toFixed(1)).join(' / ')} MB/s）`);
      }
    }

    log(`\n===== ${LABEL}: 测速 ${sp.mbps.toFixed(1)} MB/s`
      + `（对端 ${sp.peerMbps.toFixed(1)}）· 端到端传输 ${(FILE_MB / e2eSecs).toFixed(1)} MB/s =====`);
  } catch (err) {
    fail(`运行异常: ${err.stack || err.message}`);
  } finally {
    if (pageA) await pageA.close();
    if (pageB) await pageB.close();
    if (server) server.kill();
    try { fs.rmSync(dlDir, { recursive: true, force: true }); } catch (_) {}
  }
})().then(() => {
  /* 明确退出：失败后残留的 WebSocket / 浏览器子进程会把事件循环一直撑住，
   * 只设 process.exitCode 而不退出的话，job 会空转到全局硬超时才结束
   * （第一次跑就是 08:09 失败、08:16 才被硬超时收掉）。 */
  process.exit(process.exitCode || 0);
});
