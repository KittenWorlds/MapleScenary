# MapleScenary

跨平台 IPC 优化中间件，为 [LavaCat](https://github.com/KittenWorlds/LavaCat) 的每页一进程架构设计（对应 Issue #16）。

目标是让控制消息和渲染帧的传递尽量不经过内核拷贝：控制面只走一条本地 socket，数据面直接写入共享内存，用 1 字节 doorbell 唤醒对端。

- 语言：Rust（`edition 2024`，MSRV 1.90），预留 Zig 优化路径
- 产物：`rlib` + `cdylib` + `staticlib`，附带 C 头文件
- 许可证：LGPL-3.0-or-later

## 特性

- **零拷贝发送**：`Sender::reserve(len)` 直接返回共享区域内的 `&mut [u8]`，填充后 `publish(kind)`；接收端 `Message::bytes()` 直接借用同一块内存，`Drop` 时自动归还。
- **无跨进程锁的热路径**：区域按方向对半切分，每侧用进程本地 buddy 分配器；对端通过归还环（return ring）把块还回来，发送方在下次分配前回收。
- **SPSC 环**：每个方向一条消息环（32 字节描述符）和一条归还环（8 字节），`AtomicU32` head/tail 使用 Acquire/Release。
- **Doorbell 唤醒**：生产者仅在 `notified` 由 false 变 true 时写 1 字节；消费者清位后复查，避免丢唤醒。没有额外线程、eventfd 或信号。
- **事件循环接入**：`event_handle()` 交出控制 socket 的 `RawFd`/`RawHandle`，`Watcher` 提供不消耗消息的等待原语。
- **真实四平台实现**：不是条件编译占位。
- **Stream 回退**：共享内存不可用或显式设置 `Config::force_stream` 时，退化为长度前缀帧，语义一致。
- **可选 typed 通道**：`typed` feature 提供基于 `serde` + `bincode` 的 `TypedChannel<T>`。
- **C ABI**：`include/maple_scenary.h` 可直接给非 Rust 宿主使用。

## 平台矩阵

| 平台 | 控制面 | 唤醒 | 共享内存 |
|------|--------|------|----------|
| Linux | `AF_UNIX` `SOCK_SEQPACKET`（失败回退 `SOCK_STREAM`） | 控制 socket 上的 1 字节 | `memfd_create`（`ENOSYS` 时 `shm_open`） |
| Android | abstract `AF_UNIX` `SOCK_SEQPACKET` | 同上 | `memfd_create` |
| macOS | `AF_UNIX` `SOCK_STREAM` | 同上 | `shm_open`，创建后立即 `unlink` |
| Windows | 命名管道（byte mode，overlapped I/O） | 同名管道上的 1 字节 | 命名 file mapping（`Local\` 命名空间） |

macOS 的 `AF_UNIX` 不支持 `SOCK_SEQPACKET`，Windows 命名管道不能跨进程复制句柄，因此 Windows 的共享内存名通过 `Welcome` 握手传递，客户端用 `OpenFileMappingW` 打开。

## 快速开始

```rust
use std::time::Duration;
use maple_scenary::{pair, Config, Listener};

// 进程内/线程间
let (client, server) = pair(Config::compact())?;
client.send(1, b"hello")?;
let message = server.recv(Some(Duration::from_secs(1)))?;
assert_eq!(message.bytes(), b"hello");

// 跨进程
let listener = Listener::bind("/tmp/lavacat.sock", Config::default())?;
let server_channel = listener.accept(Some(Duration::from_secs(5)))?;
// 另一个进程：
// let client_channel = maple_scenary::connect("/tmp/lavacat.sock", Config::default())?;
# Ok::<(), maple_scenary::Error>(())
```

零拷贝：

```rust
use maple_scenary::{pair, Config};

let (client, server) = pair(Config::compact()).unwrap();
let mut outgoing = client.reserve(4).unwrap();
outgoing.buffer_mut().copy_from_slice(b"ping");
outgoing.publish(1).unwrap();
let message = server.recv(Some(std::time::Duration::from_secs(1))).unwrap();
assert_eq!(message.bytes(), b"ping");
```

窗口内的页面往往直接写布局缓冲，这正是 `reserve` 存在的理由：一次映射、一次发布，没有中间 `Vec` 拷贝。

## 共享内存视图

渲染帧和纹理上传需要拿到区域本身，而不只是 `send`/`recv`：

```rust
let region = client.shared_region().expect("shared-memory channel");
let handle = region.duplicate()?; // Unix: OwnedFd；Windows: 命名 mapping handle
let view = region.map_read_only(16, 4096)?; // 任意 offset，内部按页 / 64 KiB 对齐
let bytes = view.as_slice();
# let _ = (handle, bytes);
```

- `shared_region()` 在 stream 回退模式下返回 `None`。
- `duplicate()` 交出独立的 OS 句柄：Unix 用 `into_fd()` 取得 `OwnedFd`，Windows 用 `into_handle()` 或 `name()`（`Local\...`）在子进程里 `OpenFileMappingW`。
- `map_read_only(offset, len)` 接受非对齐 offset，返回的 `RegionView` 只暴露请求的字节；越界或零长度返回 `InvalidConfig`。
- 视图不参与通道的发布协议：只应读取对端已经 `publish` 完成的数据。

Typed 通道：

```toml
maple-scenary = { version = "0.1", features = ["typed"] }
```

```rust
use maple_scenary::{Config, TypedChannel, pair};

#[derive(serde::Serialize, serde::Deserialize)]
struct Layout { width: u32, height: u32 }

let (a, b) = pair(Config::default())?;
let a = TypedChannel::<Layout>::new(a);
let b = TypedChannel::<Layout>::new(b);
a.send(&Layout { width: 1280, height: 720 })?;
assert_eq!(b.recv(None)?.width, 1280);
# Ok::<(), maple_scenary::Error>(())
```

## 可靠性与语义

- `try_send`：无可用块或环满时立即返回 `WouldBlock`，不阻塞。
- `send_many(kind, payloads)`：一次批量投递尽量多的消息，只写一次 doorbell；返回实际发送条数，短计数表示需要背压后重试剩余部分。
- `send_timeout(kind, payload, timeout)`：共享内存模式下用 20µs → 1ms 指数退避轮询；stream 模式下等待 socket 可写。超时返回 `Timeout`。
- `recv(timeout)`：先短自旋探测环（64 次），再阻塞在 doorbell 上；`Some(Duration::ZERO)` 等价于非阻塞；`None` 一直等到消息或对端关闭。
- `Message` 借用共享内存；只要消息还活着，对应块不会被复用。`Drop` 后由发送方回收。
- 关闭一侧的 `Channel` 会 shutdown 控制 socket，对端 `recv` 返回 `Disconnected`。
- 共享内存布局校验（magic、版本、偏移、对齐、长度）在接收端执行，畸形描述符返回 `Protocol`/`Internal`。

## 事件循环接入

`recv` 会把调用线程停在门铃上。已经拥有消息循环的宿主（浏览器合成器、UI 框架、async runtime）不需要为 IPC 再开线程，可以直接等待：

```rust
use std::time::Duration;
use maple_scenary::{pair, Config, Ready};

let (client, server) = pair(Config::compact())?;
let watcher = server.watcher();
match watcher.wait(Some(Duration::from_millis(16)))? {
    Ready::Readable => {
        let message = server.try_recv()?.expect("readable implies a message");
        assert_eq!(message.kind(), 1);
    }
    Ready::Closed => { /* 先排空 try_recv，再退出事件循环 */ }
    Ready::Timeout => { /* 画下一帧 */ }
}
# Ok::<(), maple_scenary::Error>(())
```

- `Channel::event_handle()`、`Sender::event_handle()`、`Receiver::event_handle()` 返回控制 socket / 管道的 `RawFd`（Unix）或 `RawHandle`（Windows），可直接注册到 `epoll`、`kqueue` 或 overlapped I/O。句柄归 channel 所有：不要关闭，channel 释放前停止监听。
- `Watcher::wait` 是非破坏性的：返回 `Ready::Readable` 后，消息仍然由 `try_recv` 取走；对端关闭前已发布的消息会先报 `Readable`，排空后才报 `Closed`。
- `Watcher::try_wait()` 是零超时版本；超时返回 `Ready::Timeout` 而不是错误。
- 共享内存模式等的是门铃字节；stream 回退改为等待 socket 可读，避免把帧首字节当作门铃吞掉。Windows 命名管道没有 `poll`，`Watcher` 在 stream 模式下退化为 1 ms `PeekNamedPipe` 轮询，共享内存模式仍走门铃。

## 队列水位

`stats()` 除了累计计数器，还直接给出环上的实时积压，省去宿主自己算：

```rust
let stats = client.stats();
println!(
    "queued {} messages / {} bytes, return backlog {}",
    stats.queued_messages, stats.queued_bytes, stats.return_backlog,
);
```

- `queued_messages` / `queued_bytes`：该半边发送环里尚未被对端取走的描述符数量与 payload 字节数。发送半看的是自己的发送环，接收半看的是自己的接收环。
- `return_backlog`：归还环里等待本半回收的块数；持续偏高说明对端消费后迟迟不再分配，或回收速度跟不上。
- `Channel::stats()` 描述发送半；要查询接收半的水位，先 `split()`，再用 `Receiver::stats()`。
- stream 回退没有共享环，这三个字段恒为 0。
- 字节数是诊断用的有界扫描（上限为环容量），不改变任何 head/tail，也不与生产者加锁。

## 布局

```
+---------------------------- 4096 B header ----------------------------+
| magic | version | region_len | half offsets | ring offsets | atomics  |
+------------------------------------------------------------------------+
| a2b ring (ring_capacity * 32 B) | b2a ring | ra2b ring | rb2a ring     |
+------------------------------------------------------------------------+
| half A (client -> server 的分配区)        | half B (server -> client)    |
+------------------------------------------------------------------------+
```

每条消息描述符包含 `offset/len/kind/flags/seq`。接收方把块偏移写入归还环；拥有该半区的进程在下次分配前 drain 并释放。分配器状态只存在于拥有者进程内，因此对端崩溃不会留下跨进程锁。

## 构建与测试

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo bench            # 延迟 / 吞吐
cargo build --release  # 生成 cdylib + staticlib
```

集成测试里有一组崩溃恢复用例：测试二进制会以子进程方式重新启动自己，在 `reserve` / `publish` / 持有消息未归还三个阶段直接 `exit`，验证对端能观测到关闭、关闭前已发布的消息仍可读出，且不会死锁或 panic。环回绕、满环和畸形握手/头部的用例是普通单元测试，不需要额外工具链。

四平台检查（需要对应 target）：

```bash
cargo check --target x86_64-pc-windows-msvc
cargo check --target aarch64-apple-darwin
cargo check --target aarch64-linux-android
```

## CI

GitHub Actions 包含：`lint`（fmt、两套 feature 的 clippy、`RUSTDOCFLAGS=-D warnings` 的文档构建）、`test`（三平台，all-features 与默认 feature 各一轮）、`msrv 1.90`、`audit`（`cargo audit`）、`android`（`aarch64` / `x86_64` 两个 target 的 check）、`package`（`cargo package` 打包校验）、`coverage`（llvm-cov，上传 `lcov.info`）、三平台 `benchmark` 与 `bench-chart`，以及 tag（`v*`）触发的 `release`（release 构建并上传 cdylib / staticlib / rlib）。`ci-success` 汇总全部 job，分支保护只需勾选这一个必选检查。

## 基准测试

`benches/roundtrip.rs` 测量 64 B 往返延迟与 64 KiB 吞吐，覆盖五组口径：

| 名称 | 含义 |
|------|------|
| `maple-scenary` | 默认 shm 数据面，`send`/`recv`，两端同进程 |
| `maple-scenary (reserve)` | 零拷贝 `reserve`/`publish`，两端同进程 |
| `maple-scenary (stream)` | 强制 stream 回退（整帧走 socket） |
| `maple-scenary (2 proc)` | 真子进程，经 `Listener`/`connect` + 共享内存句柄传递 |
| `ipc-channel` | 同品类竞品，`Vec<u8>` 收发，数字包含 serde/postcard 序列化 |

每个数字是 `--repeats`（默认 3）次的中位数并带标准差；接收端会真正拷贝/校验 payload，吞吐不是「环空转」的虚高值。

```bash
cargo bench --bench roundtrip
cargo bench --bench roundtrip -- --json bench-results/linux.json --repeats 3
cargo bench --bench roundtrip -- --save-baseline benches/baseline/ubuntu-latest.json
cargo bench --bench roundtrip -- --baseline benches/baseline/ubuntu-latest.json --threshold 30
```

`--baseline` 按 `(library, workload)` 对比中位数：延迟涨幅超过 `--threshold`、或吞吐跌幅超过 `--throughput-threshold`（默认 70%）就以非零状态退出。共享 runner 的绝对性能波动大（Windows 上同一 workload 跨 run 见过 3.4× 的差异），CI 用较宽的阈值（延迟 100%、吞吐 80%）只兜底灾难性回退，真正阻断的是与硬件无关的 2× 竞品契约：同一 runner 上 shm 路径吞吐必须 ≥ 2× `ipc-channel`、延迟必须 ≤ 2× `ipc-channel`。门禁只覆盖进程内路径；`2 proc` 系列只展示、不设绝对阈值（跨进程调度噪声太大）。基线文件不存在时只提示并跳过，方便首次接入。任何 benchmark 失败或 panic 都会记录到 JSON 的 `failures` 并使进程失败，不会静默丢样本；CI 即使门禁失败也会上传原始 JSON 便于排查。

CI 在 Ubuntu / macOS / Windows 三个 runner 上分别执行基准（5 次取中位数）、与 `benches/baseline/<runner>.json` 对比并上传 JSON。`bench-chart` job 只在三平台数据齐全、无 failure 时生成 `latency.png`、`throughput.png` 和 `summary.md`（`benchmark-report` artifact），并在 PR 中创建或更新对比评论；缺任一平台会直接失败而不是发半张表。不同 runner 硬件不同，横向比较请只在同一行内看各库优劣，不要跨平台比绝对值。

## C ABI

```c
#include "maple_scenary.h"

maple_config config;
maple_config_default(&config);

maple_channel *a, *b;
if (maple_pair(&config, &a, &b) != MAPLE_OK) { /* maple_last_error_message() */ }

maple_send(a, 1, (const uint8_t *)"hi", 2);

maple_message *message;
maple_recv(b, 1000, &message);
/* maple_message_data / maple_message_len / maple_message_kind */
maple_message_free(message);
maple_channel_free(a); maple_channel_free(b);
```

## 目录

```
src/
  channel.rs    通道状态机、零拷贝、背压、握手
  listener.rs   Listener
  layout.rs     共享内存 Header 与布局计算
  buddy.rs      进程本地 buddy 分配器
  ring.rs       SPSC 消息环 / 归还环
  region.rs     映射区域视图
  shared.rs     用户可见的共享内存句柄与只读视图
  protocol.rs   Hello / Welcome 编解码
  typed.rs      serde typed 门面（feature）
  ffi.rs        C ABI
  sys/unix.rs   AF_UNIX + memfd/shm_open
  sys/windows.rs 命名管道 + 命名 file mapping
include/maple_scenary.h
benches/roundtrip.rs
benches/baseline/  三平台性能基线（CI 回归门槛）
scripts/bench_chart.py
tests/integration.rs
TODO.md
```

## 状态

v0.1 聚焦 Unix 家族（Linux / Android / macOS）与 Windows 的功能实现。四平台（含 Windows 运行时）已在 GitHub Actions 上通过单元/集成测试；三平台基准数据由 CI 产出并在 PR 中自动对比。Zig 优化路径（批量 memcpy、无分支分配器）留待后续版本。

## 许可证

LGPL-3.0-or-later，见 [LICENSE](LICENSE) 与 [COPYING](COPYING)。

## 轻量接口消息与请求关联

`maple_scenary::interface` 在现有 Channel 上提供可选使用的 32 字节信封，无额外依赖。
它包含接口 ID、方法号、最低接口版本、消息类型和非零请求 ID，支持单向消息、请求及乱序响应。
接收端仍然借用原消息内存；`interface::reserve` 可直接填充共享内存中的业务 payload。

```rust
use maple_scenary::{Config, pair};
use maple_scenary::interface::{self, Header, InterfaceMessage, RequestIds};
use std::time::Duration;

let (client, server) = pair(Config::compact())?;
let (client_tx, client_rx) = client.split();
let (server_tx, server_rx) = server.split();
let ids = RequestIds::new(); // 每个发送方向长期保留一份，不在请求间重置
let request = Header::request(10, 4, 1, ids.next_id()?);
interface::send(&client_tx, request, b"ping")?;

let incoming = InterfaceMessage::from_message(server_rx.recv(Some(Duration::from_secs(1)))?)?;
incoming.header().validate_for(10, 1)?; // 解码 payload 前检查接口和最低版本
interface::send(&server_tx, incoming.header().response()?, b"pong")?;

let response = InterfaceMessage::from_message(client_rx.recv(Some(Duration::from_secs(1)))?)?;
assert!(response.header().is_response_to(request));
assert_eq!(response.payload(), b"pong");
# Ok::<(), maple_scenary::Error>(())
```

这是 framing/correlation 层：业务负责 pending 请求表、超时、取消、重复响应处理和方法分发。
它不会等待指定响应或丢弃交错的事件，也不提供同步 RPC、自动版本协商或字段级序列化。
`min_version` 表示解码该方法及 payload 所需的最低接口版本；字段布局和可选字段兼容规则由业务定义。
接口信封版本、底层握手协议版本及 C ABI 版本相互独立，本次未改变现有 raw/typed API。

线格式全部使用 little-endian，外层 kind 为 `u32::MAX - 1`（typed 使用 `u32::MAX`）。
payload 长度由外层 Channel 帧限定；`max_message_len` 包括这 32 字节头。
未知信封版本、消息类型、非零保留位及非法请求 ID 组合均被拒绝。

| 字节偏移 | 类型 | 含义 |
| --- | --- | --- |
| 0 | 4 bytes | magic：ASCII `MIFC` |
| 4 | u16 | 信封版本：1 |
| 6 | u16 | 类型：0 单向、1 请求、2 响应 |
| 8 | u32 | interface ID |
| 12 | u32 | method ordinal |
| 16 | u32 | min_version |
| 20 | u32 | 保留，必须为 0 |
| 24 | u64 | request ID，单向必须为 0，请求/响应必须非零 |

与 Mojo 对标的当前边界：共享内存消息、事件循环接入和请求关联已有基础实现；
端点跨进程转移、Broker/Invitation 路由、IDL 代码生成及完整能力权限模型尚未提供。
Android 当前由交叉编译检查覆盖，运行时长稳测试仍待完成。
