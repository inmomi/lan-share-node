# LAN Share（Node 版）

局域网里互传文件的**零依赖**小服务。只有一个 `server.js`，不用 `npm install`、不用编译、不用打包。

一台设备跑服务，其他设备用浏览器打开就能互传——**不用装客户端、不用登录、不用联外网**。

```
┌─────────┐   浏览器    ┌─────────┐
│ 设备 A  │◀──────────▶│ 设备 B  │      直连：WebRTC DataChannel，多 lane 并行
└────┬────┘             └────┬────┘      兜底：都连不上时走服务端 WebSocket 中转
     │  HTTP + WebSocket     │
     └───────────┬───────────┘
            ┌────┴─────┐
            │ server.js │   ← 一个文件，前端就在旁边的 public/
            └───────────┘
```

服务端本身**只依赖 Node 内置模块**（`http` / `fs` / `net` / `dgram` / `crypto` / `os`），依赖树是空的。

需要 Node **18 或更高**（跑自带测试需要 21+，因为用到了全局 `WebSocket`）。

> 同样的东西还有一份 [C 版](https://github.com/inmomi/lan-share-c)：静态链接的单个可执行文件，约 180 KB、常驻内存 1.5 MB，适合直接丢到路由器上。两版的**前端与协议完全一致**，可以混合部署。

---

## 快速开始

### 1. 拿到程序

从 [Releases](https://github.com/inmomi/lan-share-node/releases/latest) 下源码包，或者直接 clone：

```bash
git clone https://github.com/inmomi/lan-share-node.git
cd lan-share-node
```

没有依赖要装——`package.json` 里没有 `dependencies`。

### 2. 跑起来

```bash
# Windows：双击 start.bat，或者
node server.js

# Linux / macOS
PORT=8080 UPLOAD_DIR=/data/share node server.js
```

跑起来后终端会打印局域网地址，例如 `http://192.168.1.100:8080`：

```
  LAN Share 已启动
  ------------------------------------------
  本机访问:      http://localhost:8080
  局域网访问:    http://192.168.1.100:8080
  文件存储目录:  /data/share
  ------------------------------------------
  设备间传文件优先走 WebRTC 直连；直连建不起来时自动落到 WebSocket
  兜底中转——那种情况下文件字节会经过本服务，用完即走、不落盘。
  局域网 STUN:   已启用（UDP 8080，给客户端发真实内网 IP 的自反射候选，
                 绕开 Chrome 的 mDNS 伪装对组播的依赖）
```

### 3. 开始传

**其它设备用浏览器打开那个地址**，就能看到在线设备列表。

- 点设备卡片 → 选文件 → 对端页面弹出接收确认，点「接收」开始传；
- 传完的文件落在对端的浏览器下载目录；
- 想存到服务端磁盘（给不在线的人取），用「文件库」标签页上传。

传大文件时进度条上的速率是**对端实收速率**，不是本端入队速率；显示「已送达」才是真的落盘完成。

---

## 三种用法

界面分三个标签页，对应三种用途。

### 设备直传

浏览器之间点对点，**服务端不接触文件内容**（只转发信令）。适合当场把文件发给某台设备。

文件按字节范围切成多段，**最多 32 条并行通道**各自推一段，接收端按通道顺序拼回原文件。面板上会显示实际开了几路并行。

传输过程中某条通道断了会自动退役换一条；已收到的字节会保留，重发同一个文件时**从断点续传**。

直连建不起来时（同 SSID 的两台机器却连不上、AP 隔离、UDP 被禁），会自动降到**服务端中转**——设备卡片上会写「服务器中转（直连未建立）」，能传但慢一些。

### 服务端文件库

上传落到服务端磁盘，再从任意设备下载。适合"存下来慢慢取"或发给不在线的人。

上传分块流式写入 `.lanshare-parts/` 下的 `.part` 半成品，收满才改名进文件库；下载走 HTTP `Range`，浏览器原生续传。**上传中断后重试会接着上次的偏移继续**，不用从头发。

### 网速测试

把"慢"拆开定位的三段测速：

| 测什么 | 慢说明什么 |
| --- | --- |
| 服务端下载 / 上传 | 磁盘或服务端 HTTP 栈的问题 |
| 点对点直传 | Wi-Fi 链路或 WebRTC SCTP 的问题 |

三段都慢 → 局域网链路问题（2.4G Wi-Fi、百兆口很常见）。只有点对点慢 → 是 DataChannel 那一层的事。点对点那项还会把**对端实际收到的速率**一并显示，避免被本端发送队列的缓冲吸收骗到。

---

## 配置

没有命令行参数，全部走环境变量：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `8080` | 监听端口。**同一端口号的 UDP 会一并被 STUN 占用**（TCP/UDP 是两个独立 socket，互不冲突） |
| `HOST` | `0.0.0.0` | 监听地址。只想本机可见就设 `127.0.0.1` |
| `UPLOAD_DIR` | `./uploads` | 文件库落盘目录 |
| `LAN_SHARE_TURN` | 空 | 可选。一段合法 JSON 数组，服务端会在返回首页时**注入**进页面，前端把它加进 `iceServers`。留空则只用自带的局域网 STUN，数据不经第三方 |

`LAN_SHARE_TURN` 的例子（配合自建 coturn）：

```bash
LAN_SHARE_TURN='[{"urls":"turn:192.168.1.100:3478","username":"u","credential":"p"}]' node server.js
```

### Windows 双击启动

双击 `start.bat` 即可，它等价于在这个目录下执行 `node server.js`（`PORT` / `UPLOAD_DIR` 可以直接用系统环境变量覆盖）。

### 部署成后台服务（Linux）

`/etc/systemd/system/lan-share.service`：

```ini
[Unit]
Description=LAN Share
After=network.target

[Service]
ExecStart=/usr/bin/node /opt/lan-share/server.js
Environment=PORT=8080
Environment=UPLOAD_DIR=/data/share
Restart=on-failure
# 按外接存储的实际挂载权限改
User=lan-share

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now lan-share
```

Node 版**没有**"只拷一个文件"的能力——`server.js` 要和 `public/` 放在一起。想在路由器这种存储紧张、又没有 Node 运行时的环境上跑，请用 [C 版](https://github.com/inmomi/lan-share-c)。

---

## 怎么设计的

### 一个抽象层，盖住所有链路情况

现实里的局域网千奇百怪：同一个路由器、有线对无线、开了 AP 隔离、组播被 IGMP Snooping 拦掉、UDP 被禁……如果每种情况写一套传输逻辑，代码会变成一堆互相打架的特例。

所以数据通道只暴露**一套 DataChannel 形状的接口**：

```
send(string | ArrayBuffer) / readyState / bufferedAmount
bufferedAmountLowThreshold / addEventListener('message' | 'close')
```

上层（分块、pacing、断点续传、进度回执）**完全不关心字节是从哪一层走的**。具体实现在 `public/relay-channel.js`，抽成独立文件是为了能在 Node 里直接 `require` 做单测——帧格式、背压窗口这些边界靠真机点文件是点不出来的。

唯一的入口是 `ensureChannel(peerId)`，它只回答一个问题：

```js
{ mode: 'p2p' }    // 直连建起来了
{ mode: 'relay' }  // 建不起来，走服务端中转
```

### 第一层：WebRTC 多 lane 直连

一个 `RTCPeerConnection` 只有一个 SCTP 关联，拥塞窗口、接收窗口、消息粒度全按关联算。实测同一对设备 TCP（SMB）能跑 30MB/s，而单条 DataChannel 只有 3.2MB/s——**瓶颈在每关联的机制里，不在链路上**，所以加带宽没用，只能加关联。

因此每个对端开 32 条 lane，每条独立的 pc / ICE / DTLS / SCTP / 拥塞窗口，文件按字节范围切段并行推。

注意：同一条关联上开多个 DataChannel **没有用**，它们共享同一个拥塞窗口。

lane 数不硬编码。最优值随丢包变化，这是双 Chrome + `tc netem` 扫出来的（速率由对端计时；两版前端逐字节相同，数据通用）：

| 链路 | 4 条 | 8 条 | 16 条 | 32 条 |
|---|---|---|---|---|
| 无损 loopback（TCP 300+ MB/s） | 39 MB/s | 37 | 32 | 32 |
| 5ms + 0.5% 丢包（TCP 26~38） | 6.4 | 11.7 | 11.0 | **13.5** |
| 20ms + 2% 丢包（TCP 3.6~6.7） | 0.9 | 1.6 | 3.1 | **5.1** |

所以每次连接先用 16 条探 4MB：聚合 ≥10MB/s 就用 16 条（省 CPU、吸收窗口小、数字稳），慢了才加满 32 条。结果按连接缓存，之后每趟传输实收速率明显变慢会自动升档（只升不降）。

还有两个细节：

- **32 条 lane 逐条错峰建立（每条间隔 150ms）**。32 条同时握手会瞬间灌出 60+ 个 mDNS 组播查询，被操作系统大量丢弃，多数候选永远解析不出、对应 lane 的 ICE 连不上（真机只开 1~4 条、速度卡 5MB/s 的典型成因）。逐条来让查询错开，几乎每条都能解析出真实 IP。
- **任何一条 lane 20 秒没打开就退役**。`file-offer` / `file-accept` 互换双方活 lane 集合、按交集切段，保证两边段数永远对齐。

### 第二层：WebSocket 兜底中转

直连建不起来时（见下一节），字节改走**已经通了的那条信令 WebSocket**，由服务端原样转发：

```
[1 字节: 对端 id 长度][id][payload]
```

同一个抽象层的第二份实现，所以上层一行都不用改。两条约束：

- **单帧不能超过服务端的 `WS_BUF`（128 KB）**，客户端按 64 KB 切块。整帧超限会被服务端直接丢掉（并在日志里打一行"中转丢弃"），症状是进度卡死而没有任何提示——所以切块必须用 `slice` 而不是 `subarray`（后者拿到的还是整块原始 buffer，等于没切）。
- **背压靠对端确认**。`bufferedAmount` 报的是"已发出但还没被确认"的字节，超过 2MB 就停发；接收端每 512KB 回一次确认。DataChannel 那边有 Chrome 内部 16MB 队列兜着，这一层没有，不限流会把服务端和接收端的内存吃光。

代价是多一跳、没有多 lane 并行，速率低于直连——但**它永远能通**。

> 这里踩过一个必现的竞态：兜底通道在传文件的同时，那 32 条 WebRTC lane 还在各自跑 ICE 并陆续失败退役。早期版本判"传输中断"时不看死掉的是哪条 lane，于是**第一批判死刑的瞬间就把正在传的传输判死**，表现为"无线对无线传十几 MB 到三十几 MB 之间随机中断"。现在 `state.incoming` 会记下本次传输实际在用的通道，无关 lane 的死活不再影响它。

### 服务端兼任局域网 STUN

Chrome 默认把 **host 候选**伪装成 mDNS `.local` 名字，要靠组播解析；而家用路由器普遍开 IGMP Snooping，无线侧经常收不到组播 → 候选永远解析不出来 → 一条 lane 都建不起来。

服务端因此**在同一个端口号上再开一个 UDP socket**（`dgram`），实现最小 STUN：应答 Binding Request，回一个携带**客户端真实内网 IP** 的 `srflx` 候选（`MAPPED-ADDRESS` / `XOR-MAPPED-ADDRESS` / `FINGERPRINT`，magic cookie `0x2112A442`，CRC32 手写）。

Chrome **只对 host 候选做 mDNS 伪装，`srflx` 是真实 IP**，所以直连不再依赖组播。

为什么不直接用公共 STUN：国内基本不可达，ICE 要白等它超时；而且同一个局域网里两台设备拿到的 `srflx` 都是**路由器的公网 IP**，要靠 NAT 回环才能互通，对局域网直连毫无帮助。

**只做 STUN，不做 TURN，数据不经第三方。** 需要的话用 `LAN_SHARE_TURN` 注入 TURN 配置，行为不变。

### 判定"直连不可用"与自动降级

连续 9 秒（`DIRECT_GRACE_MS`）一条 lane 都没打开，就认定直连不可用、切兜底通道——不能等到 lane 的 20 秒超时，否则用户只会看到界面没反应。已经有 lane 打开后，连续 1.5 秒没有新 lane 打开就开工（硬上限 12 秒，防止一直有 lane 慢慢开而拖太久）。

### 设备身份与信令

服务端在 `/ws` 上维护在线设备表，消息只有 4 种：`self` / `peers` / `signal` / `signal-lost`。**服务端不解析 SDP 和 ICE，只按 id 转发**。

设备名按 IP 记住（服务端一层）并存在浏览器 localStorage（客户端一层），刷新、重连、换浏览器都不会变回随机名。

### 进度为什么按对端实收显示

只统计"交给发送队列的字节"的话，进度条会被各 lane 的高水位吸收（32 条 × 2MB = 64MB 的吸收窗口）先冲到 100% 再卡住，速率在 2~40MB/s 之间荡——**荡的是表，不是链路**。接收端每 1MB 回传一次实收字节数，发送端按它画进度算速率，两边看到同一条曲线。progress 在 32 条 lane 上各发各的、跨 lane 无顺序保证，所以发送端对回执值**只增不减**，晚到的旧值不会把进度拉回去。

"已送达"以对端落盘回执（`file-done`）为准，不是字节入队就拉满——发送完先显示"已发出，等待对端保存"，对端组装落盘、回执回来后才是"已送达"。

### pacing：窗口没打满就连续发

Chrome 的 DataChannel 发送队列约 16MB，`bufferedAmount` 一超过 `send()` 就**直接抛异常**（不是排队）。

pacing 集中在 `public/p2p-pump.js`：高水位 2MB、低水位 1MB，**窗口没打满就连续发，打满了才等一次**。曾经是每发一块都等一次，而 `bufferedamountlow` 阈值默认 0、要等整队泄空才触发，实际放行全靠 50ms 轮询——每块赔一个轮询周期，吞吐被钉死在 5MB/s 左右。另外一次读 8MB 再切块发送，避免每 256KB 就走一次 Blob IPC 把带宽卡在读速上。

### 服务端实现要点

- **零依赖**：WebSocket 握手（SHA-1 + Base64 走 `crypto`）、帧解析与组帧、STUN 应答、CRC32 全部手写，`package.json` 里没有 `dependencies`。
- **手写 WS 帧解析**，单帧上限 `WS_BUF = 128 KB`。声明长度**读出帧头就判超限**并立刻关连接——等载荷到齐再比的话，一条伪造的"长度 2^40"帧会让这条连接永远停在等待里、缓冲一路涨到内存耗尽。
- **WebSocket 分片按 RFC 6455 通用处理**。浏览器发大消息会自己切片（首片带原 opcode、其余是 `0x0` 续帧），**二进制同样会分片**。早期版本假设"一次 send 就是一帧"，二进制续帧全落进缓冲又永远等不到清零条件，一路堆到上限把连接关掉——只有兜底通道会发 64KB 级消息，所以这个坑藏了很久。
- **写帧用 `cork` 把帧头和载荷合成一次 `writev`**。兜底通道每秒要发上百个 64KB 帧，先 `Buffer.concat` 再写出等于每帧白复制一次 64KB。
- **关连接必须说清"为什么关"**。每条关闭都打一行：原因、残留缓冲、分片长度、中转收发字节数。只回一个 1006 加一片空白的日志，谁关的、在哪一步关的完全无从判断。
- **文件流 `highWaterMark` 提到 1 MB**。Node 的 `fs` 流默认只有 64KB，千兆局域网上意味着每秒几万次 read/write 系统调用，实测把下载压在 50MB/s 上下；提上去后系统调用次数降一个数量级。
- **输出排队是追加而不是覆盖**。`socket.write` 只是写进内核缓冲，连续两次排队（改名回执 `self` + `peers` 广播）不能把前一条整帧丢了。
- **Ctrl-C 先 `destroy()` 所有信令连接再等 `server.close()`**。`server.close()` 要等所有连接结束才回调，浏览器开着的 WebSocket 与 keep-alive 不会自己断，回调永远不触发。顺带一提：这里原来写的是 `socket.close()`——`net.Socket` 上**没有**这个方法，异常被 `catch` 吞掉，看着"已经断掉了"其实一条都没断。
- **文件名安全**。上传前剥离路径、过滤控制字符，并校验最终路径仍在存储目录内，防目录穿越。

---

## HTTP / WebSocket 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/files` | 文件列表 `{ files: [{name,size,sizeText,mtime}], total }` |
| POST | `/api/upload?name=xxx` | 请求体为原始文件流，落盘；同名自动追加 `(1)` |
| POST | `/api/upload?name=xxx&size=N&offset=M` | 分块上传。先写 `.lanshare-parts/<sha1(name\|size)>.part`，收满 `size` 字节才改名进文件库；返回 `done:false,received:N`。跳过未上传区域返回 409 |
| GET | `/api/upload-status?name=xxx&size=N` | 查询半成品已收字节数 `{ received }`，续传前用它定位起点 |
| GET | `/api/download?name=xxx` | 下载，带 UTF-8 文件名的 `Content-Disposition`。支持 `Range`，回 206 + `Content-Range`，越界 416，始终带 `Accept-Ranges: bytes` |
| DELETE | `/api/files?name=xxx` | 删除 |
| GET | `/api/info` | 在线设备数、存储目录、`engine:"node"` |

WebSocket 在 `/ws`：

```jsonc
{"type":"self",  "id":"dev-1-abcd", "name":"蓝色海豚"}            // 连上时告诉你是谁
{"type":"peers", "peers":[{"id":"...","name":"...","ip":"..."}]}  // 在线设备变化
{"type":"signal","from":"...","to":"...","data":{...}}            // 转发 offer/answer/ICE
{"type":"signal-lost"}                                            // 目标设备已离线
```

客户端发：`{"type":"rename","name":"新名字"}`、`{"type":"signal","to":"...","data":{...}}`。

兜底通道的二进制帧（opcode `0x2`）也走这条 WebSocket，格式见上文。

---

## 测试

```bash
npm test                    # 端到端（自己拉起服务、跑完关掉）
npm run test:relay          # 兜底通道单测（不需要服务端和浏览器）
npm run test:stun           # 局域网 STUN 应答校验（自己拉起服务端）
```

`test.js` 自己拉起服务、跑完 **122 项断言**再关掉，覆盖：静态托管与目录穿越防护、上传/下载/删除、中文与特殊文件名往返、5MB 与 16MB 文件逐字节完整性、WebSocket 信令全链路、**兜底通道的帧形状**（二进制分片重组、握手请求与首帧合并成一次写入）、上传断点续传、下载 `Range`、设备名记忆，以及用**假 DataChannel 跑 `p2p-pump.js` 的真代码**验证 64MB 一次性发完不抛"队列满"。

`tools/test-relay.js` 是兜底通道的 **17 项**帧格式 / 切块上限 / 背压窗口 / `close` 派发单测。
`tools/test-stun.js` 是局域网 STUN 的 **17 项**应答校验——`XOR-MAPPED-ADDRESS` 的异或基数和 `FINGERPRINT` 的 CRC 覆盖范围只要错一个 bit，浏览器就静默拿不到自反射候选，表现和"服务端压根没起 STUN"一模一样。

### 真实浏览器端到端

假 DataChannel 验证不了"真实 Chrome 的 SCTP 到底能跑多快"。`tools/p2p-e2e.js` 拉起两个独立 Chrome（零依赖，直接用 Node 内置 `WebSocket` 走 CDP，不装 puppeteer）：

1. TCP 基线（同机服务端上传/下载）——点对点数字只有和它比才有意义；
2. 冷、热连接各跑一次内置测速，报候选对类型与 RTT；
3. 真实文件传输（默认 64MB），自动点确认框、等下载落盘、**逐字节 SHA-256 校验**——并行分段拼回去只要错一段就会现形；
4. 用 `--require-mode p2p|relay` 断言**最终走的是哪一层**。只断言"传完了"是不行的：兜底通道一加上，直连失败也能传完，直连用例就退化成"永远通过"。

```bash
node tools/p2p-e2e.js --file-mb 64 --timeout 240
node tools/p2p-e2e.js --file-mb 32 --require-mode relay --probe-lane-retire 1
```

注意区分两个速率：**交付速率**算到对端回执 `done`，**端到端速率**算到对端落盘，后者才是真实值。

### CI 上跑什么

每次 push 到主分支，[ci.yml](.github/workflows/ci.yml) 跑四件事：

1. **快速确定性套件**（ubuntu + windows）：端到端 122 断言、兜底通道单测、STUN 单测；
2. **netns 真实网络拓扑**三档，全部用真 Chrome：

   | 档 | 造的网络 | 必须走哪一层 |
   | --- | --- | --- |
   | T1 | 两个 netns 经宿主机转发（模拟同 AP 下两台设备），**开着 mDNS 伪装** | 直连 |
   | T2 | AP 隔离（两子网互访 DROP）+ 自建 coturn | 直连，但 ICE 必须经中继 |
   | T3 | 两子网间 **UDP 全丢** + 开着 mDNS 伪装 | 兜底通道（`--require-mode relay`） |

   T3 会额外注入一次"把一条注定失败的 WebRTC lane 判死"（`--probe-lane-retire`），断言正在进行的兜底传输不受影响——这条不依赖时序，改坏了必红。

3. **Windows 启动脚本**冒烟；
4. 打 `v*` tag 时，**上面全部跑过才准发布** Release（源码 zip + SHA-256）。

[e2e-p2p.yml](.github/workflows/e2e-p2p.yml) 另外跑真实双 Chrome（16MB / 64MB）与 `tc netem` 丢包下的 lane 数扫描。

### 服务端吞吐基准

怀疑服务端是瓶颈时先跑这个（不起浏览器，直连 HTTP）：

```bash
node bench.js node 8123
```

会打出 4MB 分块上传、整块上传、下载三个吞吐。把这个数和点对点速率并排看就能判断瓶颈在哪一段。（`node bench.js c 8123` 会在旁边找 C 版的 `lan_share` 一起对比。）

---

## 常见问题

**能看到对方，但传不了 / 一直显示"连接中"**
先看设备卡片上的标注：写「服务器中转（直连未建立）」说明已自动降级到兜底通道，能传但慢。想恢复直连，通常是 mDNS 组播被拦（开了 IGMP Snooping 的路由器）或 AP 隔离——后者在路由器后台关掉即可。

**传输中途停住**
看接收端那条记录的状态原文。写「已中断（N MB），可续传」说明通道断了，重新发同一个文件会自动从断点继续；一直显示「接收中」不动则是对端没在消费。

**速度只有几 MB/s**
先在「网速测试」里分清是哪一段。服务端三段都快而点对点慢，才是 WebRTC 那一层的问题。**2.4G Wi-Fi 和百兆口是常见硬上限。**

**端口被占用 / 起不来**
`EADDRINUSE` 说明 `PORT` 已被占用；另外**同一端口号的 UDP** 也要用来做 STUN，如果那个端口上已经有别的 UDP 服务，STUN 会绑定失败（横幅里会打「局域网 STUN: 未启用」），此时直连退化成依赖 mDNS 组播解析，其余功能不受影响。

**手机传不了**
手机系统分享菜单、PWA 安装、消息通知需要 HTTPS；局域网 HTTP 下用浏览器页面传文件是正常的。

---

## 注意

- 服务端文件库**默认无鉴权**，同一局域网内任何人都能上传、下载、删除。只在可信网络里用。
- 设备直传默认不经服务端；但**直连建不起来时会自动降级到服务端中转**，那条路上文件字节会经过服务端进程（不落盘、用完即走）。
- 路由器开 **AP 隔离**会让设备之间无法直连（会自动降级到服务端中转，文件库功能不受影响）。
- WebRTC 接收的文件先攒在浏览器内存里再落盘，几百 MB 没问题；**超过 2GB 建议走文件库路径**。

## 许可

MIT
