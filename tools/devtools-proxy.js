'use strict';

/* 极简 TCP 双向转发代理，用于把"包在 network namespace 里的 Chrome"的
 * DevTools 调试端口暴露给宿主机上的测试进程。
 *
 * 背景：测试里用 `ip netns exec nsA google-chrome ...` 把浏览器塞进独立
 * 网络命名空间，模拟"另一台设备"。但 Chrome 的 DevTools 端口只监听 nsA 内的
 * 127.0.0.1，宿主机连不到。于是这个代理也在 nsA 内启动、监听 nsA 自己的 IP
 * （例如 10.9.1.1），把宿主机的连接转给 nsA 回环上的 Chrome。
 *
 * 用法：node tools/devtools-proxy.js --listen 10.9.1.1:9331 --target 127.0.0.1:9331
 *   --listen  对外监听的 host:port（应在 nsX 的 IP 上）
 *   --target  要转发到的 Chrome DevTools 地址（nsX 内 127.0.0.1:port）
 */

const net = require('net');

function getOpt(name) {
  const i = process.argv.indexOf(name);
  return i < 0 ? null : process.argv[i + 1];
}

function parseHostPort(s, dfltHost, dfltPort) {
  if (!s) return { host: dfltHost, port: dfltPort };
  const m = /^(.*):(\d+)$/.exec(s);
  if (!m) return { host: s, port: dfltPort };
  return { host: m[1] || dfltHost, port: Number(m[2]) };
}

const listen = parseHostPort(getOpt('--listen'), '127.0.0.1', 9331);
const target = parseHostPort(getOpt('--target'), '127.0.0.1', 9331);

const server = net.createServer((client) => {
  const upstream = net.connect(target.port, target.host, () => {});
  client.pipe(upstream);
  upstream.pipe(client);
  client.on('error', () => { try { upstream.destroy(); } catch (_) {} });
  upstream.on('error', () => { try { client.destroy(); } catch (_) {} });
  upstream.on('close', () => { try { client.destroy(); } catch (_) {} });
  client.on('close', () => { try { upstream.destroy(); } catch (_) {} });
});

server.on('error', (err) => {
  process.stderr.write(`devtools-proxy 监听失败 ${listen.host}:${listen.port}: ${err.message}\n`);
  process.exit(1);
});

server.listen(listen.port, listen.host, () => {
  process.stderr.write(`devtools-proxy 监听 ${listen.host}:${listen.port} -> ${target.host}:${target.port}\n`);
});
