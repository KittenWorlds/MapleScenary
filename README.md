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
- `send_timeout(kind, payload, timeout)`：共享内存模式下用 20µs → 1ms 指数退避轮询；stream 模式下等待 socket 可写。超时返回 `Timeout`。
- `recv(timeout)`：`Some(Duration::ZERO)` 等价于非阻塞；`None` 一直等到消息或对端关闭。
- `Message` 借用共享内存；只要消息还活着，对应块不会被复用。`Drop` 后由发送方回收。
- 关闭一侧的 `Channel` 会 shutdown 控制 socket，对端 `recv` 返回 `Disconnected`。
- 共享内存布局校验（magic、版本、偏移、对齐、长度）在接收端执行，畸形描述符返回 `Protocol`/`Internal`。

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

四平台检查（需要对应 target）：

```bash
cargo check --target x86_64-pc-windows-msvc
cargo check --target aarch64-apple-darwin
cargo check --target aarch64-linux-android
```

## 基准测试

`benches/roundtrip.rs` 测量 64 B 往返延迟与 64 KiB 吞吐，同时跑 MapleScenary（seqpacket/shm 与 stream 回退两种数据面）和同品类竞品 [`ipc-channel`](https://crates.io/crates/ipc-channel)（LavaCat 目前使用的库，按 `Vec<u8>` 收发，含序列化成本）：

```bash
cargo bench --bench roundtrip
cargo bench --bench roundtrip -- --json bench-results/linux.json
```

CI 在 Ubuntu / macOS / Windows 三个 runner 上分别执行基准并上传 JSON；`bench-chart` job 用 `scripts/bench_chart.py` 汇总生成 `latency.png`、`throughput.png` 和 `summary.md`（`benchmark-report` artifact），并在 PR 中创建或更新对比评论。不同平台的 runner 硬件不同，横向比较请只在同一行内看各库优劣，不要跨平台比绝对值。

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
  protocol.rs   Hello / Welcome 编解码
  typed.rs      serde typed 门面（feature）
  ffi.rs        C ABI
  sys/unix.rs   AF_UNIX + memfd/shm_open
  sys/windows.rs 命名管道 + 命名 file mapping
include/maple_scenary.h
benches/roundtrip.rs
scripts/bench_chart.py
tests/integration.rs
```

## 状态

v0.1 聚焦 Unix 家族（Linux / Android / macOS）与 Windows 的功能实现。Windows 后端已通过交叉 `cargo check`/`clippy`，运行时验证依赖 GitHub Actions 的 Windows job。Zig 优化路径（批量 memcpy、无分支分配器）留待后续版本。

## 许可证

LGPL-3.0-or-later，见 [LICENSE](LICENSE) 与 [COPYING](COPYING)。
