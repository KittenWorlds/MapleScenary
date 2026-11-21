# MapleScenary 路线图

> 排序原则：P0 先把「数字可信」做完；P1 的事件循环接入和端点传递是分水岭——做完这两条，它才从「一条很快的管道」变成「能撑起多进程骨架的东西」。
> 状态以本文件为准，完成一项就改勾。

## P0 收口现状（本周）

- [x] 合并 PR #2，把 bench-chart 的 `if: always()` 改成三平台 JSON 齐全才出表，避免数据缺失仍发评论
- [x] bench 加 `--save-baseline` / `--baseline` 与回归阈值，接入 CI 阻断（现在只是展示型，拦不住性能回退）
- [x] bench 补重复次数与误差棒；`catch_unwind` 吞掉的 panic 要显式报错，不能静默跳过样本
- [x] 让接收端真正消费 payload（拷贝或求和），修正「零拷贝环空转」虚高的吞吐数字
- [x] 补一组 `Listener`/`connect` 真双进程 bench，和 `pair` 并列展示——现在测的是单进程上限
- [x] 对比口径对齐：ipc-channel 改发 raw bytes 或注明含序列化，别混着比
- [x] README 状态段同步实际进度（Windows 运行时已在 CI 跑通、bench 已有三平台数据），删掉「依赖 CI 验证」的旧表述
- [ ] 补 GitHub About 描述 + topics（集成 token 无 repo 管理权限，需手动在 Settings 填写：`Cross-platform IPC middleware: shared-memory message rings, zero-copy payloads and doorbell wakeups for LavaCat`，topics：`rust ipc shared-memory zero-copy cross-platform linux macos windows android browser`）；发第一个 Release / tag（v0.1.0 已发布）

## P1 能力缺口（决定能不能用）

- [x] 事件循环接入：提供 `as_raw_fd` / Windows `HANDLE` 或 `pollable()`，让宿主把 doorbell 挂进自己的消息循环，替代 20µs→1ms 忙等（`Channel`/`Sender`/`Receiver::event_handle()`）
- [x] 给出非阻塞 + Watcher 风格 API（可读/可写/对端关闭/配额超限），哪怕只做最小信号集（`Watcher::wait/try_wait` 返回 `Ready::{Readable, Closed, Timeout}`，非破坏性探测）
- [ ] 端点传递：支持把一条 `Channel` 的端点作为消息内容交给第三方进程，先不做路由，只做「转发一次」
- [x] 共享内存句柄对用户可见：duplicate / 只读视图 / 按 offset 局部映射——渲染帧和纹理共享绕不开（`shared_region()` / `duplicate()` / `map_read_only()`）
- [ ] 一对多/多对一：评估「一个 `Listener` 服务多个 page 进程」的连接表，而不是每页一条独立 socket + shm
- [ ] `ChannelStats` 扩成可查询的队列水位（消息数 / 字节数 / 归还环积压）

## P2 健壮性与不可信边界（沙箱化前提）

- [ ] 崩溃长稳测试：对端在 `reserve` / `publish` / `Drop` 各阶段被 kill，验证归还环残留与块泄漏
- [ ] 补 seq 回绕、ring 满、畸形描述符 fuzz；现在只有 12 个 happy-path 集成测试
- [ ] `Disconnected` 细分原因（进程终止 / 校验失败 / 主动关闭 / 超时），并保留残留消息排空语义
- [ ] 配额与限速：单连接内存上限、消息速率上限，防止不可信 page 撑爆 64 MiB 区域
- [ ] 权限模型最小版：endpoint 名字的访问控制，Windows `Local` 命名映射的 ACL 复核
- [ ] 四平台长稳（尤其 macOS stream 回退的 socket 缓冲、Windows 命名管道 overlapped）压测 ≥1h

## P3 接口层与工程

- [ ] `u32 kind` 换成轻量 schema（哪怕一张 enum 表 + 版本号），支持字段级 MinVersion 式兼容
- [ ] 请求-响应关联（correlation id），别让业务层自己拿 `kind` 凑
- [ ] C ABI 稳定化声明：明确哪些 ABI 保证不变、`ABI_VERSION` 不兼容时如何拒绝握手
- [ ] Zig 优化路径给出 go/no-go 结论，别一直挂着「预留」
- [ ] 补人工 code review（目前 PR #1/#2 都是零评审直接合）
- [ ] 关键 `unsafe` 块补 safety 注释与 Miri / sanitizer 检查

## 明确不做（别把 MapleScenary 写成 Mojo）

- [ ] ❌ mojom 式 IDL 与多语言代码生成
- [ ] ❌ Node / Broker / Invitation 全套组网与代理消除
- [ ] ❌ 同步调用、associated interface、成熟 capability manifest
- [ ] ❌ 为对齐 Mojo 而引入大型运行时依赖
