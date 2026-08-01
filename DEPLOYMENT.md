# a2a-bus — 跨 Agent 消息系统

独立消息总线（NATS JetStream），替代 gbrain-messenger。推拉双模式，对 agent 友好。

## 架构

```
Oracle VPS (100.68.80.91)
├─ nats-server   :4222   JetStream (stream A2A, KV a2a-status)   systemd: nats-server
├─ a2a-api       :3010   FastAPI HTTP/SSE 桥                     systemd: a2a-api
├─ a2a-mcp       :3011   MCP 工具 (octopus 身份, Hermes 用)       systemd: a2a-mcp
└─ a2a-mcp-cc    :3012   MCP 工具 (cc-oracle 身份, Claude Code)   systemd: a2a-mcp-cc
```

## 安装部署（新机器）

```bash
# 1. NATS server (arm64; amd64 改后缀)
curl -sL -o /tmp/n.tar.gz https://github.com/nats-io/nats-server/releases/download/v2.14.4/nats-server-v2.14.4-linux-arm64.tar.gz
tar xzf /tmp/n.tar.gz -C /tmp && sudo install -m 755 /tmp/nats-server-v2.14.4-linux-arm64/nats-server /usr/local/bin/nats-server

# 2. 配置 + systemd (见仓库 scripts/nats-server.conf 模板; 密码存 /etc/nats-a2a-pass 600)

# 3. venv + 依赖
python3 -m venv venv && venv/bin/pip install -r requirements.txt

# 4. token 配置 /etc/a2a-api.env (600):
#    A2A_NATS_URL=nats://a2a:<pass>@127.0.0.1:4222
#    A2A_TOKENS=octopus:<t1>,hermes:<t2>,...
#    A2A_API_PORT=3010

# 5. systemd: a2a-api.service, a2a-mcp.service (octopus), a2a-mcp-cc.service (cc-oracle)
```

## 运维

```bash
sudo systemctl status nats-server a2a-api a2a-mcp a2a-mcp-cc
nats context select a2a && nats stream info A2A
tail -f /var/log/syslog | grep a2a   # 或 journalctl -u a2a-api -f
```

## 数据

- Stream A2A: subjects `a2a.*.inbox`, 保留 90 天 / 100 万条, 文件存储 /var/lib/nats/jetstream
- KV a2a-status: key=seq → unread/read/replied/archived/recalled
- 备份: `nats stream backup A2A <dir>` (JetStream 文件整体复制亦可)

## API

见 a2a_api.py 顶部 docstring / gbrain 页面 shared/config/a2a-messaging。

## 迁移

`scripts/migrate_gbrain.py` — 从 gbrain `messages/inbox/*` 迁移存量消息（保时间戳/状态/回复链）。

## 安全

- NATS: user/password 认证, 监听 0.0.0.0 (tailscale 网络内)
- API/MCP: 每 agent 一个 Bearer token, from 由 token 决定
- tokens.env 权限 600, .gitignore 排除, 真实 token 不入 gbrain
