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
async function waitForDevtools(port, timeoutMs) {
  const deadline = Date.now() + (timeoutMs || 20000);
  for (;;) {
    try {
      return await getJson(`http://127.0.0.1:${port}/json/version`, 1500);
    } catch (_) {
      if (Date.now() > deadline) throw new Error(`DevTools 端口 ${port} 20 秒内未就绪`);
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

  /** 在页面里执行表达式，awaitPromise 让 async 函数也能直接拿到结果。 */
  async evaluate(expression) {
    const r = await this.send('Runtime.evaluate', {
      expression, returnByValue: true, awaitPromise: true,
    });
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
    /* 关掉 mDNS 候选混淆，让 ICE 直接用真实内网 IP——测试里要的就是
     * 确定能连上的 host candidate，不想再叠一层 mDNS 解析。 */
    '--disable-features=WebRtcHideLocalIpsWithMdns',
    'about:blank',
  ];
  if (opts.extraArgs) args.unshift(...opts.extraArgs);

  const child = spawn(executable, args, { stdio: 'ignore' });
  child.on('error', (err) => { throw new Error(`浏览器启动失败: ${err.message}`); });

  await waitForDevtools(port, 30000);
  const targets = await getJson(`http://127.0.0.1:${port}/json/list`, 5000);
  let target = targets.find((t) => t.type === 'page');
  if (!target) {
    /* 有些构建默认不开页面，用 /json/new 开一个 */
    target = await getJson(`http://127.0.0.1:${port}/json/new?about:blank`, 5000);
  }
  if (!target || !target.webSocketDebuggerUrl) {
    throw new Error('没拿到可调试的页面目标');
  }

  const ws = new WebSocket(target.webSocketDebuggerUrl);
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
