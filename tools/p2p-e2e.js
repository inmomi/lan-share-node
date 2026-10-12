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
const SERVER_URL = arg('url', null);
const LABEL = arg('label', 'e2e');
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
   * 变量给，上传目录指到临时目录，跑完即焚。 */
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

/* ------------------------------------------------------------ 主流程 */

(async () => {
  let server = null;
  let pageA = null;
  let pageB = null;
  const dlDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lanshare-dl-'));

  try {
    const url = SERVER_URL || `http://127.0.0.1:${PORT}`;
    if (!SERVER_URL) {
      server = startServer();
      await waitServer(url, 20000);
    }
    log(`服务端: ${url}`);

    log('拉起两个浏览器…');
    pageA = await launchBrowser({ port: 9331 });
    pageB = await launchBrowser({ port: 9332 });
    await pageA.navigate(url);
    await pageB.navigate(url);
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

    /* ---------------------------------------------------- 1. 内置测速 */
    log('\n--- 内置测速（speedP2p，与真实传输同一条 pumpFile 通路）---');
    /* 连跑两次：第一次连接是刚建好的（冷启动，SCTP 拥塞窗口从零开始爬），
     * 第二次连接已经热了。两次差很多就说明慢在冷启动而不是链路。 */
    const runs = [];
    for (let i = 0; i < 2; i++) {
      const t0 = Date.now();
      const sp = await pageA.evaluate(`(async () => {
        const r = await speedP2p(${JSON.stringify(targetFromA)});
        return JSON.parse(JSON.stringify(r));
      })()`);
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

    if (!upRec || !upRec.done) {
      fail(`发送未完成: ${JSON.stringify(upRec)}`);
    } else {
      log(`  发送端: ${(upRec.sent / 1048576).toFixed(1)} MB / ${(upRec.size / 1048576).toFixed(1)} MB`
        + `，${sendSecs.toFixed(1)}s → ${(upRec.size / 1048576 / sendSecs).toFixed(2)} MB/s`);
    }

    /* 等接收端把文件存下来 */
    const saved = await pageB.waitForDownload(fileName, TIMEOUT_MS);
    /* 真正的端到端速率要算到"文件落盘"为止：发送端的 done 只代表字节
     * 交进了 DataChannel 队列（队列上限 16MB，能吸收好几秒的数据），
     * 那之后的排空时间才是链路真实速率的一部分。 */
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
        log(`  交付速率 : ${(FILE_MB / sendSecs).toFixed(2)} MB/s（发送端字节入队到 done）`);
        log(`  端到端   : ${(FILE_MB / e2eSecs).toFixed(2)} MB/s（入队到对端落盘，这才是真实速率）`);
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
      })()`);
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
})();
