#!/usr/bin/env python3
"""a2a-mcp — 把 a2a 总线暴露为 MCP 工具 (Hermes / Claude Code 注册用)

工具:
  a2a_send(to, subject, body, reply_to)        发消息
  a2a_inbox(unread_only, since, limit)         拉收件箱 (本 agent)
  a2a_status(message_id, status)               改状态
  a2a_contacts()                               agent 列表
  a2a_health()                                 健康检查

身份: 环境变量 A2A_TOKEN (本 agent 的 token), A2A_API_URL 默认 http://127.0.0.1:3010
"""
import json
import os
import urllib.error
import urllib.request

from fastmcp import FastMCP

API_URL = os.environ.get("A2A_API_URL", "http://127.0.0.1:3010")
TOKEN = os.environ.get("A2A_TOKEN", "")
if not TOKEN and os.environ.get("A2A_AGENT"):
    _tf = os.path.expanduser("~/projects/a2a-bus/tokens.env")
    try:
        for _line in open(_tf):
            if _line.startswith("A2A_TOKENS="):
                for _p in _line.split("=", 1)[1].split(","):
                    _a, _t = _p.strip().split(":", 1)
                    if _a == os.environ["A2A_AGENT"]:
                        TOKEN = _t
    except OSError:
        pass

mcp = FastMCP("a2a")


def _req(method: str, path: str, body: dict | None = None, timeout: int = 30):
    r = urllib.request.Request(API_URL + path, method=method)
    r.add_header("Authorization", f"Bearer {TOKEN}")
    if body is not None:
        r.add_header("Content-Type", "application/json")
        r.data = json.dumps(body, ensure_ascii=False).encode()
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read().decode())
        except Exception:
            err = {"detail": str(e)}
        return e.code, err
    except Exception as e:
        return 0, {"detail": str(e)}


def _fmt_inbox(d: dict) -> str:
    if "messages" not in d:
        return json.dumps(d, ensure_ascii=False)
    lines = [f"收件箱 {d['agent']}: {d['count']} 条"]
    flag = {"unread": "🆕", "read": "📖", "replied": "↩️",
            "archived": "🗄️", "recalled": "🚫"}
    for m in d["messages"]:
        lines.append(f"#{m['id']} {flag.get(m['status'], m['status'])} "
                     f"{m['from']}→{m['to']} [{m['created']}] {m['subject']}")
        if m.get("body"):
            lines.append("  " + m["body"].replace("\n", "\n  "))
        if m.get("reply_to"):
            lines.append(f"  (回复 #{m['reply_to']})")
    return "\n".join(lines)


@mcp.tool()
def a2a_send(to: str, subject: str, body: str = "", reply_to: int | None = None) -> str:
    """发送一条消息给另一个 agent (异步收件箱)。to=对端 slug, subject=主题, body=正文。

    对端 slug: hermes(雨雀), reading-bot(读书郎), see(兮), claude-code, octopus(八爪鱼), cc-oracle
    """
    payload = {"to": to, "subject": subject, "body": body}
    if reply_to is not None:
        payload["reply_to"] = reply_to
    code, d = _req("POST", "/api/send", payload)
    if code != 200:
        return f"ERROR {code}: {d}"
    return f"已发送 #{d['id']} → {d['to']} [{d['status']}]"


@mcp.tool()
def a2a_inbox(unread_only: bool = False, since: int | None = None, limit: int = 50) -> str:
    """拉取本 agent 的收件箱 (异步消息)。unread_only=true 只看未读; since 只看大于该 id 的消息。
    注意: 本 agent 的 slug 由 token 决定 (octopus/cc-oracle 等), 无需传参。"""
    path = "/api/inbox/me"
    params = [f"limit={limit}"]
    if unread_only:
        params.append("unread_only=true")
    if since is not None:
        params.append(f"since={since}")
    code, d = _req("GET", path + "?" + "&".join(params))
    if code != 200:
        return f"ERROR {code}: {d}"
    return _fmt_inbox(d)


@mcp.tool()
def a2a_status(message_id: int, status: str) -> str:
    """修改消息状态: unread | read | replied | archived | recalled。回复后请把原消息标 replied。"""
    code, d = _req("POST", f"/api/status/{message_id}", {"status": status})
    if code != 200:
        return f"ERROR {code}: {d}"
    return f"#{d['id']} → {d['status']}"


@mcp.tool()
def a2a_contacts() -> str:
    """列出所有 agent (注册表)。"""
    code, d = _req("GET", "/api/contacts")
    if code != 200:
        return f"ERROR {code}: {d}"
    return "Agent 注册表: " + ", ".join(d["agents"])


@mcp.tool()
def a2a_health() -> str:
    """总线健康检查。"""
    code, d = _req("GET", "/api/health")
    if code != 200:
        return f"ERROR {code}: {d}"
    return f"✅ 总线健康: stream={d['stream']} 消息={d['messages']} last_seq={d['last_seq']}"


if __name__ == "__main__":
    mcp.run(transport="http", host=os.environ.get("A2A_MCP_HOST", "127.0.0.1"),
            port=int(os.environ.get("A2A_MCP_PORT", "3011")))
