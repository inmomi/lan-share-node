'use strict';

/* 极简 Chrome DevTools Protocol 客户端。
 *
 * 为什么不用 puppeteer / playwright：这个仓库（尤其是 C 版）的卖点就是零依赖，
 * 为了跑测试引一个几百 MB 的 npm 包不划算；而我们需要用到的 CDP 方法非常有限——
 * 导航、在页面里求值、自动点确认框、打开下载。Node 18+ 自带 WebSocket，
 * 直接跟 DevTools 的 WebSocket 对话就够了。
 *
 * 只支持我们用到的那几个域：Page / Runtime / Browser 级别的下载开关。
 */

const { spawn } = require('child_process');
const http = require('http');
const os = require('os');
const path = require('path');
const fs = require('fs');

/** 浏览器可执行文件的常见位置。CI 上是 ubuntu-latest 自带的 Chrome。 */
function findBrowser(explicit) {
  if (explicit) {
    if (!fs.existsSync(explicit)) throw new Error(`浏览器不存在: ${explicit}`);
    return explicit;
  }
  if (process.env.CHROME_PATH && fs.existsSync(process.env.CHROME_PATH)) {
    return process.env.CHROME_PATH;
  }
  const candidates = [
    '/usr/bin/google-chrome-stable',
    '/usr/bin/google-chrome',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
    '/usr/bin/microsoft-edge-stable',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  throw new Error('找不到浏览器。设 CHROME_PATH 指向 chrome/edge 可执行文件。');
}

function getJson(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (d) => { body += d; });
      res.on('end', () => {
        try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
      });
    });
    req.setTimeout(timeoutMs || 2000, () => req.destroy(new Error('timeout')));
    req.on('error', reject);
  });
}

/** 等 DevTools 端口起来。浏览器刚拉起时端口还没监听，要重试。 */
/* 把 Chrome 回报的 DevTools WebSocket 地址改写成对外可达的 host:port。
 * 例如 ws://127.0.0.1:9331/devtools/page/abc 在 netns 场景下改成
 * ws://10.9.1.1:9331/devtools/page/abc（10.9.1.1 是 nsA 内代理监听的 IP）。 */
function rewriteWsUrl(url, host, port) {
  const m = /^(wss?):\/\/([^:/]+)(:(\d+))?(\/.*)$/.exec(url);
  if (!m) return url;
  return `${m[1]}://${host}:${port}${m[5]}`;
}

async function waitForDevtools(host, port, timeoutMs) {
  const deadline = Date.now() + (timeoutMs || 20000);
  for (;;) {
    try {
      return await getJson(`http://${host}:${port}/json/version`, 1500);
    } catch (_) {
      if (Date.now() > deadline) throw new Error(`DevTools 端口 ${host}:${port} 20 秒内未就绪`);
      await new Promise((r) => setTimeout(r, 200));
    }
  }
}

class CdpPage {
  constructor(ws, child) {
    this.ws = ws;
    this.child = child;
    this._id = 0;
    this._pending = new Map();
    this._handlers = new Map();
    this._closed = false;

    ws.addEventListener('message', (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch (_) { return; }
      if (msg.id != null && this._pending.has(msg.id)) {
        const { resolve, reject } = this._pending.get(msg.id);
        this._pending.delete(msg.id);
        if (msg.error) reject(new Error(`${msg.error.message} (code ${msg.error.code})`));
        else resolve(msg.result);
        return;
      }
      if (msg.method) {
        const hs = this._handlers.get(msg.method);
        if (hs) for (const h of hs) h(msg.params || {});
      }
    });
    ws.addEventListener('close', () => { this._closed = true; });
  }

  send(method, params) {
    if (this._closed) return Promise.reject(new Error('CDP 连接已关闭'));
    const id = ++this._id;
    return new Promise((resolve, reject) => {
      this._pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params: params || {} }));
    });
  }

  on(method, handler) {
    if (!this._handlers.has(method)) this._handlers.set(method, []);
    this._handlers.get(method).push(handler);
  }

  /** 在页面里执行表达式，awaitPromise 让 async 函数也能直接拿到结果。
   *  timeoutMs（默认 120s）：页面内若是 async 且永远不 settle（比如 WebRTC
   *  连接始终建不起来，speedP2p 卡死），这里必须兜底超时，否则整个测试会
   *  一直挂到 GitHub 的 job 超时（30 分钟）才被掐掉，既浪费又看不到原因。 */
  async evaluate(expression, timeoutMs) {
    const t = timeoutMs || 120000;
    const r = await Promise.race([
      this.send('Runtime.evaluate', {
        expression, returnByValue: true, awaitPromise: true,
      }),
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error(`evaluate 超时 ${t}ms（页面内异步可能永远没返回）`)), t)),
    ]);
    if (r.exceptionDetails) {
      const d = r.exceptionDetails.exception && r.exceptionDetails.exception.description;
      throw new Error(`页面内执行出错: ${d || r.exceptionDetails.text}`);
    }
    return r.result ? r.result.value : undefined;
  }

  async navigate(url) {
    const loaded = new Promise((resolve) => {
      this.on('Page.loadEventFired', () => resolve());
    });
    await this.send('Page.navigate', { url });
    await loaded;
  }

  /** 打开下载并指定落盘目录，返回保存的目录。 */
  async enableDownloads(dir) {
    fs.mkdirSync(dir, { recursive: true });
    this._downloadDir = dir;
    await this.send('Page.setDownloadBehavior', {
      behavior: 'allow', downloadPath: dir, eventsEnabled: true,
    });
    return dir;
  }

  async waitForDownload(filename, timeoutMs) {
    const deadline = Date.now() + (timeoutMs || 120000);
    const target = path.join(this._downloadDir, filename);
    for (;;) {
      if (fs.existsSync(target) && fs.statSync(target).size > 0) return target;
      if (Date.now() > deadline) return null;
      await new Promise((r) => setTimeout(r, 250));
    }
  }

  /** confirm() / alert() 一律点确定，测试里不需要人盯着。 */
  async autoAcceptDialogs() {
    await this.send('Page.enable');
    this.on('Page.javascriptDialogOpening', () => {
      this.send('Page.handleJavaScriptDialog', { accept: true }).catch(() => {});
    });
  }

  async close() {
    try { this.ws.close(); } catch (_) {}
    if (!this.child) return;
    /* Windows 下 Edge/Chrome 是多进程树，只 kill 主进程会留下Renderer 孤儿 */
    if (process.platform === 'win32') {
      spawn('taskkill', ['/F', '/T', '/PID', String(this.child.pid)], { stdio: 'ignore' });
    } else if (this.child.wrapped) {
      /* 被 ip netns exec 包裹时，child 是 ip 进程，Chrome 是它的子进程。
       * 用进程组（detached 时已设为组长）一次杀干净，避免 Chrome 残留。 */
      try { process.kill(-this.child.pid, 'SIGKILL'); } catch (_) {}
    } else {
      this.child.kill('SIGKILL');
    }
  }
}

/**
 * 拉起一个浏览器实例，开好调试端口，连上第一个页面目标。
 * 两个实例要用不同的 user-data-dir 和调试端口，否则第二个起不来。
 */
async function launchBrowser(options) {
  const opts = options || {};
  const executable = findBrowser(opts.executable);
  const port = opts.port || 9331;
  const userDataDir = opts.userDataDir || path.join(os.tmpdir(), `cdp-profile-${port}`);
  /* devtoolsHost / devtoolsPort：DevTools 端口暴露给本测试进程的地址。
   * 默认 127.0.0.1:port（同机回环）。当浏览器被包在 network namespace 里
   * （测试里用 ip netns exec nsX 模拟"另一台设备"），Chrome 的调试端口在
   * nsX 内的 127.0.0.1 上，宿主机连不到，于是用一个跑在 nsX 里、监听 nsX IP
   * 的 TCP 代理把它转出来，这里就填那个 nsX IP。 */
  const devtoolsHost = opts.devtoolsHost || '127.0.0.1';
  const devtoolsPort = opts.devtoolsPort || port;

  const args = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${userDataDir}`,
    '--headless=new',
    '--no-sandbox',
    '--disable-gpu',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-extensions',
    '--disable-background-networking',
    '--disable-dev-shm-usage',
    'about:blank',
  ];
  /* 关掉 mDNS 候选混淆，让 ICE 直接用真实内网 IP。
   *
   * 但这条 flag 本身就是最大的测试盲区：用户手上的 Chrome **开着** mDNS 伪装，
   * host 候选都是 xxx.local，必须靠组播解析；家用路由器默认开 IGMP Snooping，
   * 无线侧收不到组播就一条候选都解析不出来——有线↔有线通、无线↔无线不通，
   * 而带着这条 flag 的测试永远复现不出来。所以除对照用例外一律不加：
   * mdnsBypass=false 表示"按真实浏览器的默认行为跑"。 */
  if (opts.mdnsBypass !== false) {
    args.splice(args.length - 1, 0, '--disable-features=WebRtcHideLocalIpsWithMdns');
  }
  if (opts.extraArgs) args.unshift(...opts.extraArgs);

  /* wrap：命令前缀数组，用于把浏览器进程塞进 netns / 容器等隔离环境，
   * 例如 ['ip','netns','exec','nsA']。宿主机上的测试进程仍按 devtoolsHost
   * 连过去。空数组表示不包裹（默认同机回环）。detached 让 wrap 时的子进程
   * 树成为独立进程组，便于 close() 一次杀干净。 */
  const wrap = (opts.wrap && Array.isArray(opts.wrap) && opts.wrap.length) ? opts.wrap : [];
  const cmd = [...wrap, executable, ...args];
  const child = spawn(cmd[0], cmd.slice(1), { stdio: 'ignore', detached: wrap.length > 0 });
  child.wrapped = wrap.length > 0;
  child.on('error', (err) => { throw new Error(`浏览器启动失败: ${err.message}`); });

  await waitForDevtools(devtoolsHost, devtoolsPort, 30000);
  const targets = await getJson(`http://${devtoolsHost}:${devtoolsPort}/json/list`, 5000);
  let target = targets.find((t) => t.type === 'page');
  if (!target) {
    /* 有些构建默认不开页面，用 /json/new 开一个 */
    target = await getJson(`http://${devtoolsHost}:${devtoolsPort}/json/new?about:blank`, 5000);
  }
  if (!target || !target.webSocketDebuggerUrl) {
    throw new Error('没拿到可调试的页面目标');
  }

  /* Chrome 上报的 webSocketDebuggerUrl 用的是它自己以为的地址（常是
   * 127.0.0.1）。被包在 netns 里时，我们是从宿主机经代理连，要把 host/port
   * 改写成对外暴露的 devtoolsHost:devtoolsPort，否则 WebSocket 连不上。 */
  const wsUrl = rewriteWsUrl(target.webSocketDebuggerUrl, devtoolsHost, devtoolsPort);
  const ws = new WebSocket(wsUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, { once: true });
    ws.addEventListener('error', () => reject(new Error('DevTools WebSocket 连接失败')), { once: true });
  });

  const page = new CdpPage(ws, child);
  page._downloadDir = null;
  await page.autoAcceptDialogs();
  return page;
}

module.exports = { launchBrowser, findBrowser };
