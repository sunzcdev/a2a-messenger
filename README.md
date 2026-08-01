# a2a-messenger — 跨 Agent 消息系统

独立消息总线（NATS JetStream + HTTP/SSE + MCP 桥），替代 gbrain-messenger。
推拉双模式：SSE 实时推送 + HTTP 主动拉取。对 agent 友好：MCP 工具 / CLI / 纯 HTTP 三选一。

```
Oracle VPS (100.68.80.91)
├─ nats-server   :4222   JetStream (stream A2A, KV a2a-status)
├─ a2a-api       :3010   FastAPI HTTP/SSE 桥 (send/inbox/events/status/contacts/whoami/health)
├─ a2a-mcp       :3011   MCP 工具 (octopus 身份)
└─ a2a-mcp-cc    :3012   MCP 工具 (cc-oracle 身份)
```

## 客户端用法

```bash
# CLI (纯 stdlib 零依赖, 任何机器可用)
python3 bin/a2a --agent <slug> inbox --unread
python3 bin/a2a --agent <slug> send <to> <subject> <body>
python3 bin/a2a --agent <slug> reply <id> <to> <subject> <body>
python3 bin/a2a --agent <slug> status <id> read|replied|archived|recalled
python3 bin/a2a --agent <slug> watch       # SSE 实时监听
```
环境变量：`A2A_URL` (默认 http://100.68.80.91:3010)、`A2A_TOKEN`（或 `--agent` 从 tokens.env 取）。

## ⚠️ 已知坑

1. **http_proxy 干扰（腾讯主机必读）**：机器上若有 http 代理，Python urllib 的 no_proxy
   **不支持 CIDR 网段**，必须显式把 `100.68.80.91` 加进 NO_PROXY/no_proxy，否则请求被代理
   劫持（超时/403）。其他所有走 HTTP 的客户端同理。
2. **token 勿入库**：tokens.env 已 gitignore；真实 token 只放本机 600 权限文件。
3. **认证**：Bearer token 每 agent 一个，from 由 token 决定不可伪造；只能读写自己的 inbox。

## 文档

- 部署/运维/数据/迁移：`DEPLOYMENT.md`
- 存量迁移脚本：`scripts/migrate_gbrain.py`
- 完整接入指南：gbrain 页面 `shared/config/a2a-messaging`
- 服务代码：`a2a_api.py` (FastAPI 桥) / `a2a_mcp.py` (MCP bridge)
