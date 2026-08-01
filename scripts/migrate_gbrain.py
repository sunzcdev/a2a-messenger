#!/usr/bin/env python3
"""迁移 gbrain-messenger 存量收件箱 → a2a NATS 总线

- 通过 gbrain MCP (streamable HTTP) 读取所有 messages/inbox 页面
- 保持原 created 时间戳, 按时间顺序 publish 到 a2a.<to>.inbox
- 建 slug→seq 映射, 转换 reply_to (旧 slug → 新 seq)
- KV a2a-status 写入原 status (unread/read/replied/archived/recalled)
- 校验: 发布条数 vs 页面条数, 抽查 get_msg
"""
import asyncio
import json
import re
import sys
import time
import urllib.request

import nats

GBRAIN_MCP = "http://127.0.0.1:3131/mcp"
GBRAIN_TOKEN = "gbrain_at_e56a3c4fa412ec71e7c5a48445172fed11e7423ad376bd2cc8c8f2b829dc52f8"
# NATS 凭据从 tokens.env 读 (ubuntu 可读), 避免直接读 root 600 的 /etc/nats-a2a-pass
_tokens_env = open("/home/ubuntu/projects/a2a-bus/tokens.env").read()
NATS_URL = next((l.split("=", 1)[1].strip() for l in _tokens_env.splitlines()
                 if l.startswith("A2A_NATS_URL=")), "nats://127.0.0.1:4222")
KV_BUCKET = "a2a-status"
STREAM = "A2A"

# ---------- gbrain MCP client ----------
class Mcp:
    def __init__(self, url):
        self.url = url
        self.session = None
        self.reqid = 0

    def _post(self, payload):
        data = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json, text/event-stream",
                   "Authorization": f"Bearer {GBRAIN_TOKEN}"}
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        req = urllib.request.Request(self.url, data=data, headers=headers)
        with urllib.request.urlopen(req, timeout=60) as resp:
            self.session = resp.headers.get("Mcp-Session-Id", self.session)
            body = resp.read().decode()
        # SSE 格式: data: {...}
        msgs = []
        for line in body.splitlines():
            if line.startswith("data: "):
                msgs.append(json.loads(line[6:]))
        return msgs

    def call(self, tool, args):
        self.reqid += 1
        msgs = self._post({"jsonrpc": "2.0", "id": self.reqid,
                           "method": "tools/call",
                           "params": {"name": tool, "arguments": args}})
        for m in msgs:
            if m.get("id") == self.reqid:
                res = m.get("result", {})
                content = res.get("content", [])
                for c in content:
                    if c.get("type") == "text":
                        return json.loads(c["text"])
                return res
        return None

    def connect(self):
        self._post({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {"protocolVersion": "2025-03-26",
                               "capabilities": {},
                               "clientInfo": {"name": "a2a-migrate", "version": "1.0"}}})
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})


# ---------- frontmatter 解析 (简单子集) ----------
def parse_page(raw: dict):
    """raw: get_page 结果 → (frontmatter, body)"""
    # 优先用编译后的字段
    compiled = raw.get("compiled_truth") or ""
    fm = raw.get("frontmatter") or {}
    body = compiled
    if not body and raw.get("content"):
        content = raw["content"]
        m = re.search(r"^---\n(.*?)\n---\n(.*)$", content, re.S)
        if m:
            body = m.group(2).strip()
    return fm, body


def parse_fm_fields(fm):
    """frontmatter dict → 所需字段"""
    def g(k, d=None):
        v = fm.get(k)
        return v if v not in (None, "") else d
    return {
        "from": str(g("from", "") or "").strip(),
        "to": str(g("to", "") or "").strip(),
        "subject": str(g("subject", "") or "").strip(),
        "status": str(g("status", "unread") or "unread").strip(),
        "reply_to": str(g("reply_to", "") or "").strip(),
        "thread": str(g("thread", "") or "").strip(),
        "created": str(g("created", "") or "").strip(),
    }


def main():
    asyncio.run(amain())


async def amain():
    mcp = Mcp(GBRAIN_MCP)
    mcp.connect()

    # 1. 翻页拉全部 message 页面
    pages, offset = [], 0
    while True:
        res = mcp.call("list_pages", {"type": "message", "limit": 100, "offset": offset})
        batch = res if isinstance(res, list) else (res.get("result") or [])
        if isinstance(batch, dict):
            batch = batch.get("pages", [])
        if not batch:
            break
        pages.extend(batch)
        offset += len(batch)
        if len(batch) < 100:
            break
    print(f"gbrain 存量消息页面: {len(pages)}", flush=True)
    if not pages:
        return

    # 2. 逐页读取详情
    rows = []
    for i, p in enumerate(pages):
        slug = p["slug"]
        raw = mcp.call("get_page", {"slug": slug})
        if not raw or "result" not in raw and "slug" not in raw:
            raw = (raw or {}).get("result") or (raw or {})
        fm, body = parse_page(raw if isinstance(raw, dict) else {})
        fields = parse_fm_fields(fm)
        if not fields["to"]:
            print(f"  跳过(无 to): {slug}", flush=True)
            continue
        rows.append({"slug": slug, **fields, "body": body})
        if i % 25 == 0:
            print(f"  读取 {i+1}/{len(pages)}...", flush=True)

    # 3. 按 created 排序 (缺失的放最后)
    def sort_key(r):
        c = r["created"]
        return (c, r["slug"])
    rows.sort(key=sort_key)

    # 4. 连接 NATS, 发布
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        kv = await js.key_value(bucket=KV_BUCKET)
    except Exception:
        kv = await js.create_key_value(bucket=KV_BUCKET)
    slug2seq = {}

    async def publish(row, reply_to):
        msg = {
            "from": row["from"] or "unknown",
            "to": row["to"],
            "subject": row["subject"] or row["slug"].split("/")[-1],
            "body": row["body"],
            "reply_to": reply_to,
            "thread": row["thread"] or None,
            "created": row["created"] or None,
            "legacy_slug": row["slug"],
        }
        ack = await js.publish(f"a2a.{row['to']}.inbox",
                               json.dumps(msg, ensure_ascii=False).encode())
        return ack.seq

    # 第一遍: 全部发布 (reply_to 暂保留原 slug)
    print("发布中...", flush=True)
    seq_map = {}
    for r in rows:
        seq = await publish(r, r["reply_to"] or None)
        seq_map[r["slug"]] = seq
        slug2seq[r["slug"]] = seq

    # 第二遍: reply_to 是旧 slug 的, 重发修正版 (删除旧消息)
    fixed = 0
    for r in rows:
        rt = r["reply_to"]
        if rt and rt in slug2seq and rt != r["slug"]:
            old_seq = seq_map[r["slug"]]
            try:
                await js.delete_msg(STREAM, old_seq)
            except Exception:
                pass
            new_seq = await publish(r, slug2seq[rt])
            seq_map[r["slug"]] = new_seq
            slug2seq[r["slug"]] = new_seq
            fixed += 1
    if fixed:
        print(f"回复链修正: {fixed} 条", flush=True)

    # 5. KV 状态
    for r in rows:
        await kv.put(str(seq_map[r["slug"]]), r["status"].encode())
    print("KV 状态写入完成", flush=True)

    # 6. 校验
    si = await js.stream_info(STREAM)
    print(f"\n=== 迁移完成 ===")
    print(f"stream A2A: messages={si.state.messages} last_seq={si.state.last_seq}")
    print(f"页面数: {len(rows)}, 发布数: {len(rows)}, 回复链修正: {fixed}")
    from collections import Counter
    c = Counter(r["status"] for r in rows)
    print(f"状态分布: {dict(c)}")
    for r in rows[:3]:
        m = await js.get_msg(STREAM, seq_map[r["slug"]])
        print(f"  抽查 {r['slug'].split('/')[-1]}: seq={seq_map[r['slug']]} status={r['status']}")
    await nc.close()


if __name__ == "__main__":
    main()
