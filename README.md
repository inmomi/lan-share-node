# LAN Share — Node 版

局域网文件分享。一台机器跑服务，其他设备用浏览器打开即可，不用装任何东西。

> 需要跑在路由器或嵌入式设备上？用 [C 版](../lan-share-c)：只依赖 libc，静态链接约 130 KB，常驻内存约 1.5 MB。
> 本仓库是 Node 实现，适合开发调试、桌面和服务器上长期运行。

提供两条独立的传输路径：

| 路径 | 数据怎么走 | 适合什么 |
| --- | --- | --- |
| **设备直传** | 浏览器之间 WebRTC DataChannel 点对点 | 当场发文件给某台设备，服务端不接触文件内容 |
| **服务端文件库** | 上传落盘到服务端磁盘，再下载 | 存下来慢慢取，或发给不在线的人 |

两条路径都支持断点续传：直传中断后重发会接着上次的字节偏移继续；文件库上传按偏移续写 `.part` 半成品；下载走 HTTP Range，浏览器原生续传。设备名在浏览器本地存一份、服务端再按 IP 记一份，刷新页面、重连都不会变回随机名。

**关于速度**：P2P 直传走 WebRTC DataChannel，有两条浏览器硬限制——单条消息不能超过 `sctp.maxMessageSize`（Chrome 默认 256KB），发送队列总共只有约 16MB，`bufferedAmount` 一超过 `send()` 就直接抛 `RTCDataChannel send queue is full`（不是排队，是抛异常）。 pacing 逻辑集中在 `public/p2p-pump.js`，高水位取 4MB、离上限留足余量，并且一次读 8MB 再切块发送，避免每 256KB 就走一次 Blob IPC 把带宽卡在读速上。

## 运行

```bash
node server.js
```

Windows 双击 `start.bat` 即可。

启动后会打印局域网地址，其他设备（手机、平板、另一台电脑）浏览器打开那个地址就能用。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `8080` | 监听端口 |
| `HOST` | `0.0.0.0` | 监听地址 |
| `UPLOAD_DIR` | `./uploads` | 服务端文件库目录 |

## 使用

**设备直传**：两台设备都打开页面，各自显示为一个随机名字（可改）。点对方卡片选文件，或把文件直接拖到卡片上。对方点"接收"后开始直传，有进度条。卡片上的圆点变绿表示已建立直连。

**服务端文件库**：切到第二个标签页，拖放或选择文件上传，文件立刻出现在列表里，任何设备都能下载或删除。

**测速**：第三个标签页把带宽拆成三段单独测——服务端下载、服务端上传、点对点直传，点对点那项还会显示对端实际收到的速率。哪一段慢就说明瓶颈在哪：服务端慢查磁盘和 HTTP 栈，点对点慢查 Wi-Fi 信号和 WebRTC。测速状态按连接隔离、带 60 秒超时、失败主动发 `speedtest-abort`，不会因为对端没回执就把后续文件分片误吞导致传输卡死。

## 实现说明

- **零第三方依赖**。WebSocket 服务端是按 RFC 6455 自己实现的握手与帧编解码，不装 `ws`；上传用裸请求体流式落盘，不装 `multer`/`busboy`。`npm install` 什么都不用装。
- **不配 STUN/TURN**。纯内网场景下 WebRTC 用本机内网 IP 的 host candidate 就能直连，因此完全离线可用。
- **分块 + 背压 + 预读**。切片取 `RTCSctpTransport.maxMessageSize` 与 1 MB 的较小值（Chrome 默认 256 KB，比旧的 64 KB 快一个量级）；背压高水位 4 MB、低水位 1 MB。**高水位不要往 16 MB 调**——Chrome 的 DataChannel 发送队列总共只有约 16 MB，`bufferedAmount` 一超过，`send()` 不是排队而是直接抛 `RTCDataChannel send queue is full`，整次传输当场判死（本机实测 16.3 MB 处必现）。发送前一次读 8 MB 再切块，避免每块都串行等一次磁盘读取。
- **断点续传**。直传按 `name|size|lastModified` 生成 `fileId`，中断时记下已收偏移，重发时接收端回 `offset` 从断点继续；文件库上传按 4 MB 分块带 `offset` 续写 `.part`，先查 `/api/upload-status` 定位起点；下载由 HTTP Range 提供。
- **设备名持久化**。浏览器端存 `localStorage`，服务端按 IP 记住上次的名字，刷新和重连都保持。
- **pacing 单独一个文件**。`public/p2p-pump.js` 用 UMD 包了一层，Node 里可以直接 `require`；测试用假 DataChannel 复现 Chrome 的 16 MB 队列上限来跑真代码——背压阈值这类边界靠真机点文件是点不出来的。
- **测速状态按连接隔离**。曾经挂在全局 `state.speedtest` 上，对端没回 `speedtest-end`（网络抖动、页面刷新）时标志就一直开着，之后所有二进制消息都被当成测速数据吞掉，文件传输永久卡住；而且两台设备各自测速会互相吞掉对方的文件分片。现在状态存在连接对象上、带 60 秒硬超时、失败时明确发 `speedtest-abort`。
- **Ctrl-C 秒退**。`server.close()` 要等**所有**连接结束才回调，浏览器开着的 WebSocket 和 keep-alive 连接不会自己断，回调永远不触发，按 Ctrl-C 就像卡住。现在先主动断掉信令连接，再给 2 秒宽限期，超时就地退出；SIGTERM 直接退。正在上传的文件会被截断，但分块上传有 `.part` 半成品，重传能从断点续上。
- **串行传输**。同一时间只传一个文件，接收端不需要区分数据块归属。
- **文件名安全**。上传前剥离路径、过滤控制字符，并校验最终路径仍在存储目录内，防止目录穿越。

## 测试

```bash
npm test
```

`test.js` 会自己拉起服务、跑完 85 项断言再关闭它，覆盖：

- 静态托管、目录穿越防护（明文与编码两种）
- 上传 / 下载 / 删除、中文与特殊文件名往返、同名自动重命名
- 5 MB 与 16 MB 文件逐字节完整性
- WebSocket 信令全链路（设备发现、改名广播、信令转发、无效目标回执、断线收缩）
- **上传断点续传**：分块写入、未完结不进文件库、`/api/upload-status` 查进度、续传后内容逐字节一致、跳过未上传区域返回 409
- **下载 Range**：`Accept-Ranges: bytes`、206 + `Content-Range`、开区间与闭区间内容校验、越界返回 416
- **设备名记忆**：改名回执、断开重连后名字不变
- **P2P 发送 pacing**（不起浏览器）：用假 DataChannel 跑 `public/p2p-pump.js` 的真代码，
  复现 Chrome 那 16MB 发送队列上限，验证 64MB 文件一次发完、不抛"队列满"、
  缓冲从不越界、慢通道与续传起点都正确
- **测速接口**：整块 POST / 下载 / 删除

页面里有「网速测试」标签，分**服务端下载、服务端上传、点对点**三段测。用法是拿它定位瓶颈：

- 三段都慢 → 局域网链路的问题（2.4G Wi-Fi、百兆口、老路由器 NAT 最常见），不是软件。
  可以用 `iperf3` 在两台设备间直测印证。
- 只有点对点慢 → 浏览器 DataChannel 的问题，把三个数字发回来继续查。

想不经过浏览器直测服务端，可以跑基准脚本：

```bash
node bench.js c 8123      # 测 C 版
node bench.js node 8124   # 测 Node 版
```

实测 C 版约 190 / 450 / 270 MB/s，Node 版约 92 / 130 / 136 MB/s——服务端从来不是瓶颈。

需要 Node ≥ 21（用到全局 `WebSocket`）。也可以手动指定目标：

```bash
BASE=http://127.0.0.1:9000 node test.js
```

CI 在 Ubuntu / macOS / Windows 三个平台上跑完整测试，见 [ci.yml](.github/workflows/ci.yml)。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/files` | 文件列表，`{ files: [{name,size,sizeText,mtime}], total }` |
| POST | `/api/upload?name=xxx` | 请求体为原始文件流，落盘；同名自动追加 `(1)` |
| POST | `/api/upload?name=xxx&size=N&offset=M` | 分块上传。先写 `uploads/.lanshare-parts/<sha1(name\|size)>.part`，收满 `size` 字节才改名进文件库；返回 `done:false,received:N` 表示还没完。跳过未上传区域返回 409 |
| GET | `/api/upload-status?name=xxx&size=N` | 查询该半成品已收字节数 `{ received }`，续传前用它定位起点 |
| GET | `/api/download?name=xxx` | 下载，带 UTF-8 文件名的 `Content-Disposition`。支持 `Range: bytes=...`，回 206 + `Content-Range`，越界回 416，始终带 `Accept-Ranges: bytes` |
| DELETE | `/api/files?name=xxx` | 删除 |
| GET | `/api/info` | 在线设备数、存储目录、文件数 |

WebSocket 服务端在 `/ws`，消息格式：

```jsonc
{"type":"self",  "id":"dev-1-abcd", "name":"蓝色海豚"}   // 连上时告诉你是谁
{"type":"peers", "peers":[{"id":"...","name":"...","ip":"..."}]}  // 在线设备列表变化
{"type":"signal","from":"...","to":"...","data":{...}}   // 转发 WebRTC offer/answer/ICE
{"type":"signal-lost"}                                    // 目标设备已离线
```

客户端发送的：`{"type":"rename","name":"新名字"}`、`{"type":"signal","to":"...","data":{...}}`。

## 注意

- 服务端文件库默认无鉴权，同一局域网内任何人都能上传下载删除。只在可信网络里用。
- 路由器开启 **AP 隔离** 会让设备之间的点对点连接失败（文件库功能不受影响）。
- 用手机系统分享菜单、PWA 安装或消息通知需要 HTTPS；局域网 HTTP 下传文件正常。
- WebRTC 接收的文件先攒在浏览器内存里再落盘，几百 MB 以上没问题，超过 2 GB 建议走文件库路径。

## 许可

MIT
