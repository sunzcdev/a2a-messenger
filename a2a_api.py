#!/usr/bin/env python3
"""a2a-bus API — 跨 Agent 消息桥 (NATS JetStream + FastAPI)

推: GET /api/events (SSE 长连接, durable consumer, 未 ack 重投)
拉: GET /api/inbox/<agent> (ephemeral 查询, 永远看到全部消息)
发: POST /api/send (publish → a2a.<to>.inbox, KV 记 status)
状态: POST /api/status/<seq>

认证: Bearer token, 每个 token 绑定一个 agent slug (A2A_TOKENS env)。
      from 由 token 决定, 不可伪造; 只能读写自己的收件箱。
"""
import asyncio
import contextlib
import json
import os
import re
import time
from contextlib import asynccontextmanager

from datetime import datetime, timezone

import nats
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from nats.js.api import DeliverPolicy, RetentionPolicy, StreamConfig
from pydantic import BaseModel

NATS_URL = os.environ.get("A2A_NATS_URL", "nats://127.0.0.1:4222")
STREAM = "A2A"
KV_BUCKET = "a2a-status"
PORT = int(os.environ.get("A2A_API_PORT", "3010"))

# A2A_TOKENS="octopus:tok1,hermes:tok2,..." → token → agent slug
_TOKEN2AGENT = {}
for pair in os.environ.get("A2A_TOKENS", "").split(","):
    if ":" in pair:
        agent, tok = pair.strip().split(":", 1)
        _TOKEN2AGENT[tok.strip()] = agent.strip()

SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
VALID_STATUS = ("unread", "read", "replied", "archived", "recalled")
NOTICE = "notice"  # 全网通知地址: to=notice → 广播给所有注册 agent

nc = None
js = None
kv = None
_subs: dict[str, object] = {}           # agent -> push subscription
_pump_tasks: dict[str, asyncio.Task] = {}  # agent -> SSE pump task (可取消)
_queues: dict[str, asyncio.Queue] = {}  # agent -> SSE 分发队列 (当前连接)
_conns: dict[str, int] = {}             # agent -> 活跃 SSE 连接数
_shutdown = asyncio.Event()            # SIGTERM/SIGINT 置位 → SSE gen 快速退出


def check_slug(s: str):
    if not SLUG_RE.match(s):
        raise HTTPException(400, f"invalid agent slug: {s!r}")


def require_agent(auth: str | None) -> str:
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "missing bearer token")
    agent = _TOKEN2AGENT.get(auth[7:].strip())
    if not agent:
        raise HTTPException(403, "unknown token")
    return agent


@asynccontextmanager
async def lifespan(_app):
    global nc, js, kv
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        kv = await js.key_value(bucket=KV_BUCKET)
    except Exception:
        kv = await js.create_key_value(bucket=KV_BUCKET)
    try:
        await js.stream_info(STREAM)
    except Exception:
        await js.add_stream(StreamConfig(
            name=STREAM,
            subjects=["a2a.*.inbox"],
            retention=RetentionPolicy.LIMITS,
            max_age=90 * 24 * 3600,
            max_msgs=1_000_000,
        ))
    yield
    # 优雅关闭: 回收所有 SSE sub/pump/队列, 删 durable consumer
    for agent in list(_subs.keys()):
        await _cleanup_agent(agent)
    _queues.clear()
    _conns.clear()
    if nc:
        await nc.drain()


app = FastAPI(title="a2a-bus", lifespan=lifespan)


class SendReq(BaseModel):
    to: str
    subject: str
    body: str = ""
    reply_to: int | None = None
    thread: str | None = None
    msg_id: str | None = None   # 幂等键: 同 msg_id 在 DuplicateWindow(2m) 内只存一条


@app.post("/api/send")
async def send(req: SendReq, authorization: str | None = Header(default=None)):
    me = require_agent(authorization)
    check_slug(req.to)
    if not req.subject.strip():
        raise HTTPException(400, "subject required")
    msg = {
        "from": me,
        "to": req.to,
        "subject": req.subject.strip(),
        "body": req.body,
        "reply_to": req.reply_to,
        "thread": req.thread,
        "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    # 幂等: 有 msg_id 时加 Nats-Msg-Id header, stream DuplicateWindow(2m) 内同 id 只存一条
    hdrs = {"Nats-Msg-Id": req.msg_id} if req.msg_id else None
    # 全网通知: to=notice → 扇出复制到每个注册 agent 的收件箱 (各自独立 seq + 状态)
    if req.to == NOTICE:
        targets = sorted(set(_TOKEN2AGENT.values()))
        seqs = []
        for t in targets:
            ack = await js.publish(f"a2a.{t}.inbox", json.dumps(msg).encode(), headers=hdrs)
            await kv.put(str(ack.seq), b"unread")
            seqs.append(ack.seq)
        return {"id": seqs[0], "status": "unread", "to": req.to,
                "broadcast_to": targets, "seqs": seqs}
    ack = await js.publish(f"a2a.{req.to}.inbox", json.dumps(msg).encode(), headers=hdrs)
    seq = ack.seq
    await kv.put(str(seq), b"unread")
    return {"id": seq, "status": "unread", "to": req.to}


def _msg_item(seq: int, data: dict, status: str) -> dict:
    return {
        "id": seq,
        "from": data.get("from"),
        "to": data.get("to"),
        "subject": data.get("subject"),
        "body": data.get("body"),
        "reply_to": data.get("reply_to"),
        "thread": data.get("thread"),
        "created": data.get("created"),
        "status": status,
    }


async def _kv_status(seq: int) -> str:
    try:
        e = await kv.get(str(seq))
        return e.value.decode()
    except Exception:
        return "unread"


def _sort_key(it: dict):
    """收件箱按 created 时间排序; created 缺失/格式坏 (如旧迁移消息) 兜底按 seq。"""
    try:
        ts = datetime.fromisoformat((it.get("created") or "").replace("Z", "+00:00"))
        return (0, ts, it["id"])
    except Exception:
        return (1, datetime.min.replace(tzinfo=timezone.utc), it["id"])


@app.get("/api/inbox/{agent}")
async def inbox(agent: str, authorization: str | None = Header(default=None),
                since: int | None = None, limit: int = 200, unread_only: bool = False):
    me = require_agent(authorization)
    if agent == NOTICE:
        raise HTTPException(400,
            "notice 是广播地址, 副本已分发到各收件箱, 请用 /api/inbox/me 查自己的收件箱")
    if agent == "me":
        agent = me
    if agent != me:
        raise HTTPException(403, "can only read own inbox")
    check_slug(agent)
    # 循环 fetch 直到拉完: fetch 从最旧开始, 收件箱 > 批量时单次会被截断,
    # 不拉完就取不到最新 limit 条。JetStream pull consumer 默认 max_ack_pending=1000
    # 限制单次投递量, 故: a) 批量固定 1000 (≤ 配额, server 能足额投递, "返回数<批量"
    # 即为拉完); b) 每批收齐后立即 ack 释放配额, 才能拉下一批。consumer 为本次查询
    # 临时创建、用完即删, ack 无副作用 (unread 状态在 KV, 与 ack 无关)。
    fetch_n = 1000
    sub = await js.pull_subscribe(f"a2a.{agent}.inbox")
    try:
        msgs = []
        while True:
            try:
                batch = await sub.fetch(fetch_n, timeout=3)
            except Exception:
                batch = []
            if not batch:
                break
            msgs.extend(batch)
            await asyncio.gather(*(m.ack() for m in batch))
            if len(batch) < fetch_n:
                break
    finally:
        await sub.unsubscribe()
    items = []
    for m in msgs:
        try:
            data = json.loads(m.data)
        except Exception:
            continue
        seq = m.metadata.sequence.stream
        st = await _kv_status(seq)
        if unread_only and st != "unread":
            continue
        items.append(_msg_item(seq, data, st))
    items.sort(key=_sort_key)  # 按 created 时间升序 (旧→新); 坏时间兜底 seq
    if since is not None:
        items = [it for it in items if it["id"] > since]
    else:
        items = items[-limit:]  # 无 since → 最新 limit 条
    return {"agent": agent, "count": len(items), "messages": items}


async def _cleanup_agent(me: str):
    """回收本 agent 的 SSE 状态: 取消 pump、退订 sub、删 durable consumer。幂等。"""
    _queues.pop(me, None)
    sub = _subs.pop(me, None)
    task = _pump_tasks.pop(me, None)
    if task is not None and not task.done():
        task.cancel()
    if sub is not None:
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        try:
            # 删 durable consumer, 防 sse-<agent> 残留 (nats-py 无 sub.delete, 用 jsm API)
            await js._jsm.delete_consumer(STREAM, sub._consumer)
        except Exception:
            pass


async def _pump(agent: str, sub):
    try:
        async for m in sub.messages:
            try:
                data = json.loads(m.data)
                await m.ack()
                seq = m.metadata.sequence.stream
                item = _msg_item(seq, data, "unread")
                q = _queues.get(agent)
                if q:
                    try:
                        q.put_nowait(item)
                    except asyncio.QueueFull:
                        pass
            except Exception:
                pass
    except Exception:
        pass
    finally:
        # pump 意外结束 (sub 失效/NATS 断连/被 cancel) → 回收, 防 _subs 残留
        if _pump_tasks.get(agent) is asyncio.current_task():
            _pump_tasks.pop(agent, None)
        if _subs.get(agent) is sub:
            _subs.pop(agent, None)
            try:
                await js._jsm.delete_consumer(STREAM, sub._consumer)
            except Exception:
                pass


@app.get("/api/events")
async def events(authorization: str | None = Header(default=None)):
    me = require_agent(authorization)
    check_slug(me)
    if me not in _subs:
        sub = await js.subscribe(
            f"a2a.{me}.inbox",
            durable=f"sse-{me}",
            deliver_policy=DeliverPolicy.NEW,
            manual_ack=True,
        )
        _subs[me] = sub
        _pump_tasks[me] = asyncio.create_task(_pump(me, sub))

    _conns[me] = _conns.get(me, 0) + 1
    q = asyncio.Queue(maxsize=200)
    _queues[me] = q

    async def gen():
        try:
            while not _shutdown.is_set():
                # 竞速: 新消息 / 关闭信号(快速退出) / 15s keepalive
                qtask = asyncio.create_task(q.get())
                stask = asyncio.create_task(_shutdown.wait())
                try:
                    done, pending = await asyncio.wait(
                        {qtask, stask}, return_when=asyncio.FIRST_COMPLETED, timeout=15)
                finally:
                    for t in pending:
                        t.cancel()
                if stask in done:
                    break  # 收到 SIGTERM/SIGINT → 结束 SSE, 让 uvicorn 优雅关闭
                if not done:
                    yield ": keepalive\n\n"
                    continue
                item = qtask.result()
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
        finally:
            if _queues.get(me) is q:
                _queues.pop(me, None)
            _conns[me] -= 1
            if _conns.get(me, 0) <= 0:
                # 最后一个 SSE 连接断开 → 回收 sub/pump/durable
                _conns.pop(me, None)
                await _cleanup_agent(me)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


class StatusReq(BaseModel):
    status: str


@app.post("/api/status/{seq}")
async def set_status(seq: int, req: StatusReq,
                     authorization: str | None = Header(default=None)):
    me = require_agent(authorization)
    if req.status not in VALID_STATUS:
        raise HTTPException(400, f"invalid status, use one of {VALID_STATUS}")
    try:
        m = await js.get_msg(STREAM, seq)
    except Exception:
        raise HTTPException(404, "message not found")
    # 归属校验: 按消息 subject 判断落在谁的收件箱, 而非消息体 to 字段。
    # notice 广播副本 to=notice 但 subject=a2a.<me>.inbox — 收件人应能标自己的副本。
    if m.subject != f"a2a.{me}.inbox":
        raise HTTPException(403, "not your message")
    await kv.put(str(seq), req.status.encode())
    return {"id": seq, "status": req.status}


@app.get("/api/whoami")
async def whoami(authorization: str | None = Header(default=None)):
    return {"agent": require_agent(authorization)}


@app.get("/api/contacts")
async def contacts():
    return {
        "agents": sorted(set(_TOKEN2AGENT.values())),
        "notice": "全网通知地址: to=notice 会广播给所有 agent (from 保留发送者, 各收件箱独立)",
    }


@app.get("/api/health")
async def health():
    si = await js.stream_info(STREAM)
    return {"ok": True, "stream": STREAM,
            "messages": si.state.messages, "last_seq": si.state.last_seq}


if __name__ == "__main__":
    import signal
    import uvicorn

    config = uvicorn.Config(app, host="0.0.0.0", port=PORT,
                            timeout_graceful_shutdown=5)  # 安全网: 5s 内强制收尾
    server = uvicorn.Server(config)

    async def _main():
        def _sig(sig, frame=None):
            _shutdown.set()            # → 所有 SSE gen 快速退出
            server.should_exit = True  # → uvicorn 优雅关闭

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda: _sig(signal.SIGTERM))
        loop.add_signal_handler(signal.SIGINT, lambda: _sig(signal.SIGINT))
        await server.serve()

    # 接管信号: 禁用 uvicorn 自带 handler (否则其 signal.signal 会覆盖
    # add_signal_handler, SIGTERM 无法触发应用关闭), 由我们统一处理。
    server.capture_signals = contextlib.nullcontext
    asyncio.run(_main())
