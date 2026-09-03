# a2a 总线改造升级实施计划（2026-08-01）

**执行者**: cc-oracle（实现+测试）｜**需求与验收**: octopus（八爪鱼）｜**发起**: 振朝

## 协作协议

1. cc-oracle 按顺序执行 T1→T15，**完成一项 → a2a 发消息向 octopus 汇报**（主题：【完成】Txx <名称>，正文含：改动摘要 + 自测结果 + 影响面）
2. octopus 验证通过 → 回复确认（标 replied）→ cc-oracle 开始下一项
3. 遇到阻塞/设计分歧 → 立即 a2a 汇报，不要自行扩大改动范围
4. 每项改动前 `git pull` 最新代码；每项完成后 `git commit + push`（代码仓库 `~/projects/a2a-bus`，GitHub `sunzcdev/a2a-bus`）

## 全局约束

- 代码仓库：`~/projects/a2a-bus`（a2a_api.py / a2a_mcp.py / bin/a2a）
- 改 API 后必须 `sudo systemctl restart a2a-api` 生效
- **安全铁律不破**：MCP (:3011/:3012) 保持绑定 127.0.0.1，不开跨节点 MCP；token 不写 gbrain
- 客户端地址统一用 `http://100.68.80.91:3010`（Tailscale 局域网），**不用 127.0.0.1**
- 每项验证必须有可复现的验证步骤 + 实测结果

---

## 阶段 0：稳定现有系统（bug 修复，互不依赖）

### T1. 修复 inbox 拉取上限 bug（🔴 必修）
- **问题**: `inbox()` 用 `sub.fetch(max(limit, 1000))` 单次从**最旧**拉 1000 条；某收件箱 >1000 条后，「无 since = 最新 limit 条」永远拿不到最新消息
- **改动**: `a2a_api.py` 的 `inbox()`：循环 fetch 直到返回数 < 请求数（拉完），再取 `items[-limit:]`
- **验证**: 脚本灌 1200 条消息到某 agent → `GET /api/inbox/me?limit=10` 返回 seq 最大（最新）的 10 条；`?since=<max-1>` 只返回更新的
- **影响面**: 读路径，低风险

### T2. 修复 systemd 重启 90s 超时（🟡）
- **问题**: SSE 长连接挂住 uvicorn 优雅关闭，`TimeoutStopSec` 超时后 SIGKILL（14:14:43 实锤）
- **改动**: 方案 a) `a2a_api.py` 捕获 SIGTERM，主动关闭所有 SSE 连接与 consumer；方案 b) systemd unit 加 `TimeoutStopSec=10`。两案可都做
- **验证**: 挂着 SSE watch 时 `sudo systemctl restart a2a-api` 完成时间 < 15s，无 `Failed with result 'timeout'`
- **影响面**: 部署体验，所有 agent 受益

### T3. notice 收件箱语义明确化（🟡）
- **问题**: `GET /api/inbox/notice` → 403 含糊（雨雀踩过）；notice 是广播地址，副本在各收件箱
- **改动**: `inbox()` 对 agent=="notice" 返回 400 + 明确提示「notice 是广播地址，副本已分发到各收件箱，请用 /api/inbox/me」
- **验证**: `curl /api/inbox/notice` → 400 + 清晰中文错误
- **影响面**: 只影响错误路径

### T4. SSE 连接/订阅泄漏清理（🟢）
- **问题**: `_subs[agent]` 永不清理；`_pump` 异常不回收；durable `sse-<agent>` 残留
- **改动**: `events()` 连接关闭（finally）时，若无其他活跃 SSE 连接则清理 `_subs`、取消 `_pump` task、删除 durable consumer（`sub.delete()`）
- **验证**: 开 watch → Ctrl-C 断掉 → `_subs` 为空；重复开关 10 次无订阅累积（`nats consumer ls A2A` 无残留）
- **影响面**: 长期稳定性的基础，T8 presence 依赖它

## 阶段 1：配置统一（地址标准化）

### T5. 客户端地址统一为 http://100.68.80.91:3010（🟡）
- **问题**: CLI 默认 127.0.0.1，本机/远程配置不一致，仓库难统一管理
- **改动**: a) `bin/a2a` 的 `DEFAULT_URL` 改为 `http://100.68.80.91:3010`（保留 `--url`/`A2A_URL` 覆盖）；b) 同步更新 skill/指南/README 中的地址示例；c) 确认 `tokens.env` 的 `A2A_NATS_URL` 保持 nats://127.0.0.1:4222（服务端内部不变）
- **验证**: 本机 `a2a --agent octopus health` 走 100.68.80.91 成功；Tencent 主机同 URL 成功（它们本来就是这个）
- **注意**: MCP 端点绑定**不动**（安全铁律）；服务端监听 0.0.0.0 已确认
- **影响面**: 所有客户端，改完需通知全员 git pull

## 阶段 2：标准消息能力补齐（互不依赖，T8 依赖 T4）

### T6. 幂等去重（msg-id）（🟢）
- **背景**: stream 已开 DuplicateWindow 2m，但 API 从不传 `Nats-Msg-Id` → agent 重试会产生重复消息
- **改动**: a) `a2a_api.py` send 支持可选 `msg_id` 字段，publish 时加 `Nats-Msg-Id` header；b) `bin/a2a` send/reply 自动生成 uuid 作为 msg_id
- **验证**: 同 msg_id 发 2 次 → stream 只存 1 条（`nats stream get` 抽查）；不带 msg_id 行为不变
- **影响面**: 发路径，向后兼容

### T7. 多收件人 to=[a,b,c]（🟢）
- **背景**: 只有 1:1 和全广播（notice），中间档缺失
- **改动**: a) `a2a_api.py` send 的 `to` 支持数组（复用 notice 扇出逻辑），返回各收件人 seq；b) `bin/a2a` send 的 `to` 参数支持逗号分隔
- **验证**: `to: ["hermes","see"]` → 两个收件箱各收到 1 条，状态独立
- **影响面**: 向后兼容（字符串仍可用）

### T8. presence 在线状态（🟢）
- **依赖**: T4（连接表要干净）
- **改动**: a) `a2a_api.py` 维护 SSE 连接表（T4 已有），新增 `GET /api/presence` 返回 `{agent: {online: bool, last_seen}}`；b) `bin/a2a` 加 `presence` 子命令
- **验证**: 本机 `a2a watch` 挂着 → `/api/presence` 显示 octopus online；kill 后 30s 内 offline
- **影响面**: 只读新端点，无破坏

### T9. thread 聚合（🟡）
- **背景**: thread 字段半成品——能存不能查，CLI reply 不继承
- **改动**: a) `a2a_api.py` inbox 支持 `?thread=<id>` 过滤；b) `bin/a2a` reply 自动继承原消息 thread；c) `bin/a2a` inbox 可选按 thread 分组显示（`--group`）
- **验证**: 回复链自动挂同 thread；`?thread=` 只返回该会话消息
- **影响面**: 读路径增强 + CLI

## 阶段 3：开会功能（T10→T11→T12→T13 顺序依赖）

### T10. room 频道基础设施（🟡）
- **背景**: 开会需要多 agent 共享频道，subject 分层是 NATS 原生能力
- **改动**: a) `nats stream edit A2A --subjects a2a.*.inbox,a2a.room.>` 扩展 stream；b) `a2a_api.py` send 支持 `to="room:<topic>"` → publish 到 `a2a.room.<topic>`；c) inbox 支持 `agent="room:<topic>"` 拉房间消息；d) 房间消息带 `room` 字段标记
- **验证**: `nats stream info A2A` subjects 含 `a2a.room.>`；发 room 消息能被房间拉取
- **影响面**: stream 扩展是**只加不减**（安全）；新 subject 不影响现有 inbox

### T11. a2a meet 命令（🟡）
- **依赖**: T10
- **改动**: `bin/a2a` 加 `meet <topic>`：进入房间交互（订阅房间消息实时显示 + 输入发送）；`--tail N` 加入时拉最近 N 条历史；`--leave` 退出；Ctrl-C 退出
- **验证**: 两个终端用不同 agent 身份 meet 同 topic → 互发互收；新加入 `--tail 10` 看到历史
- **影响面**: CLI 新增，纯增量

### T12. a2a ask 命令（request-reply + timeout）（🟡）
- **依赖**: T10（可复用房间）
- **改动**: `bin/a2a` 加 `ask <to> <问题> [--timeout 默认10m]`：发消息带 `reply_to` 标记 + 挂起等待回复（SSE watch 过滤 reply_to），收到即打印返回，超时提示
- **验证**: `ask cc-oracle 问题` → cc-oracle 回复 → ask 命令收到并打印；无回复时按 timeout 退出
- **影响面**: CLI 新增

### T13. 会议产出（纪要 + 投票）（🟢）
- **依赖**: T11
- **改动**: a) `bin/a2a` 加 `meet --minutes <topic>`：把房间消息导出为 markdown 纪要（存 gbrain 页面或本地文件）；b) 加 `vote <topic> <proposal>`：用 KV 计票（key=`vote:<topic>:<proposal>`）
- **验证**: 导出纪要完整（含发言人/时间）；投票计数正确
- **影响面**: CLI 新增

## 阶段 4：可选增强（评估后定，不做也行）

### T14. Service 能力注册
- agent 把能力注册成 NATS service（如 cc-oracle 注册 code-review），`nats service list/ping/request` 发现调用。octopus 已实测 demo-service 全链路通
### T15. 状态审计时间戳
- KV 状态从纯字符串改为 `{status, ts, by}`，记录谁何时改的状态

---

## 验收清单（octopus 用）

- [ ] T1-T4 完成后：`a2a-healthcheck.sh` 全绿 + 重启 <15s + 无泄漏
- [ ] T5 后：本机/远程同 URL 全通
- [ ] T6-T9 后：去重/多收件人/presence/thread 逐一实测
- [ ] T10-T13 后：双 agent 开会全流程（meet/ask/纪要/投票）实测
- [ ] 全部完成后：更新 skill（a2a-bus-ops / agent-inbox-ops）+ 全员通知 git pull
