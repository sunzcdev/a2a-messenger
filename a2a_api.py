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
import uuid
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
ROOM_RE = re.compile(r"^room:[a-z0-9][a-z0-9-]{0,31}$")  # 房间: to=room:<topic>

nc = None
js = None
kv = None
_subs: dict[str, object] = {}           # agent -> push subscription
_pump_tasks: dict[str, asyncio.Task] = {}  # agent -> SSE pump task (可取消)
_queues: dict[str, asyncio.Queue] = {}  # agent -> SSE 分发队列 (当前连接)
_conns: dict[str, int] = {}             # agent -> 活跃 SSE 连接数
_last_seen: dict[str, float] = {}       # agent -> 最近一次 SSE 活跃 (epoch, presence 用)
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
            # 注: 房间 subject 用 room.> 而非 a2a.room.> — a2a.*.inbox 与 a2a.room.>
            # 在 a2a.room.inbox 重叠, NATS server (10052) 禁止同 stream 重叠 subjects
            subjects=["a2a.*.inbox", "room.>"],  # T10: 房间频道
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
    to: str | list[str]
    subject: str
    body: str = ""
    reply_to: int | None = None
    thread: str | None = None
    msg_id: str | None = None   # 幂等键: 同 msg_id 在 DuplicateWindow(2m) 内只存一条


class VoteReq(BaseModel):
    topic: str
    proposal: str
    option: str = "赞成"


class ServiceCallReq(BaseModel):
    data: str = ""


@app.post("/api/send")
async def send(req: SendReq, authorization: str | None = Header(default=None)):
    me = require_agent(authorization)
    # 归一化收件人: 单个字符串或数组; notice 出现在目标中 → 并集含全部 agent
    if isinstance(req.to, str):
        raw_to = req.to
        targets = [req.to]
    else:
        raw_to = list(req.to)
        targets = list(req.to)
    if not targets:
        raise HTTPException(400, "to 不能为空")
    if NOTICE in targets:
        # notice 是广播地址: 展开为所有注册 agent, notice 自身不投递
        targets = sorted((set(targets) - {NOTICE}) | set(_TOKEN2AGENT.values()))
    for t in targets:
        if ROOM_RE.match(t):
            continue  # room 目标不是 slug
        check_slug(t)
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
    # 扇出到各收件人 (各自独立 seq + 状态); 复用 notice 逻辑, 支持多收件人
    # ⚠️ Nats-Msg-Id 去重是 stream 级而非 subject 级: 同 msg_id 扇出多个 subject,
    # 后续副本会被 DuplicateWindow 吞掉 (T7 返修实锤)。每个收件人派生独立
    # Nats-Msg-Id: 有 msg_id → f"{msg_id}:{t}" (同收件人重试仍去重, T6 语义保留);
    # 无 msg_id → 每副本独立 uuid。
    seqs = []
    for t in targets:
        per_msg_id = f"{req.msg_id}:{t}" if req.msg_id else str(uuid.uuid4())
        if ROOM_RE.match(t):
            # 房间消息 (T10): publish 到 room.<topic>, 带 room 标记;
            # 共享频道无个人已读概念, 不建 KV 状态
            topic = t.split(":", 1)[1]
            room_msg = {**msg, "room": topic}
            ack = await js.publish(f"room.{topic}", json.dumps(room_msg).encode(),
                                   headers={"Nats-Msg-Id": per_msg_id})
            seqs.append(ack.seq)
        else:
            ack = await js.publish(f"a2a.{t}.inbox", json.dumps(msg).encode(),
                                   headers={"Nats-Msg-Id": per_msg_id})
            await kv.put(str(ack.seq), b"unread")
            seqs.append(ack.seq)
    if raw_to == NOTICE:
        return {"id": seqs[0], "status": "unread", "to": NOTICE,
                "broadcast_to": targets, "seqs": seqs}
    if isinstance(raw_to, str):
        return {"id": seqs[0], "status": "unread", "to": raw_to}
    return {"id": seqs[0], "status": "unread", "to": raw_to, "seqs": seqs}


def _msg_item(seq: int, data: dict, status: str) -> dict:
    return {
        "id": seq,
        "from": data.get("from"),
        "to": data.get("to"),
        "subject": data.get("subject"),
        "body": data.get("body"),
        "reply_to": data.get("reply_to"),
        "thread": data.get("thread"),
        "room": data.get("room"),
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


async def _fetch_inbox(subject: str, since, limit, unread_only, thread) -> list[dict]:
    """从 subject 拉取消息 (循环 fetch 直到拉完), 返回排序+过滤后的 items。"""
    # 循环 fetch 直到拉完: fetch 从最旧开始, 收件箱 > 批量时单次会被截断,
    # 不拉完就取不到最新 limit 条。JetStream pull consumer 默认 max_ack_pending=1000
    # 限制单次投递量, 故: a) 批量固定 1000 (≤ 配额, server 能足额投递, "返回数<批量"
    # 即为拉完); b) 每批收齐后立即 ack 释放配额, 才能拉下一批。consumer 为本次查询
    # 临时创建、用完即删, ack 无副作用 (unread 状态在 KV, 与 ack 无关)。
    fetch_n = 1000
    sub = await js.pull_subscribe(subject)
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
    if thread is not None:
        items = [it for it in items if it.get("thread") == thread]  # T9: thread 过滤
    if since is not None:
        items = [it for it in items if it["id"] > since]
    else:
        items = items[-limit:]  # 无 since → 最新 limit 条
    return items


@app.get("/api/inbox/{agent}")
async def inbox(agent: str, authorization: str | None = Header(default=None),
                since: int | None = None, limit: int = 200, unread_only: bool = False,
                thread: str | None = None):
    me = require_agent(authorization)
    if agent == NOTICE:
        raise HTTPException(400,
            "notice 是广播地址, 副本已分发到各收件箱, 请用 /api/inbox/me 查自己的收件箱")
    if agent.startswith("room:"):
        # 房间是共享频道 (T10, 开会用): 任何 agent 可读, 从 room.<topic> 拉
        topic = agent.split(":", 1)[1]
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", topic):
            raise HTTPException(400, f"invalid room topic: {topic!r}")
        items = await _fetch_inbox(f"room.{topic}", since, limit, unread_only, thread)
        return {"agent": agent, "count": len(items), "messages": items}
    if agent == "me":
        agent = me
    if agent != me:
        raise HTTPException(403, "can only read own inbox")
    check_slug(agent)
    items = await _fetch_inbox(f"a2a.{agent}.inbox", since, limit, unread_only, thread)
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
                    _last_seen[agent] = time.time()  # presence: 活跃更新
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
    _last_seen[me] = time.time()  # presence: 连接即在线
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


@app.post("/api/vote")
async def vote(req: VoteReq, authorization: str | None = Header(default=None)):
    """投票 (T13): 每 agent 对 (topic, proposal) 投一票, KV 计数, 重复投票拒绝。
    KV key 不允许冒号, 用点分隔: vote.<topic>.<proposal> / .agent.<agent>; unicode 提案绕过客户端校验。"""
    me = require_agent(authorization)
    topic = req.topic.strip()
    proposal = re.sub(r"[*>]", "_", req.proposal.strip())[:64]
    option = req.option.strip() or "赞成"
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", topic):
        raise HTTPException(400, f"invalid vote topic: {topic!r}")
    if not proposal:
        raise HTTPException(400, "proposal required")
    prefix = f"vote.{topic}.{proposal}"
    voter_key = f"{prefix}.agent.{me}"
    count_key = prefix
    try:
        await kv.get(voter_key, validate_keys=False)
        raise HTTPException(400, "已投过票, 不能重复投票")
    except HTTPException:
        raise
    except Exception:
        pass  # 未投过
    # CAS 递增计数 (并发安全)
    new_count = 1
    for _ in range(8):
        try:
            e = await kv.get(count_key, validate_keys=False)
            cur = int(e.value.decode())
            rev = e.revision
        except Exception:
            cur, rev = 0, 0
        new_count = cur + 1
        try:
            if rev:
                await kv.update(count_key, str(new_count).encode(), rev, validate_keys=False)
            else:
                await kv.create(count_key, str(new_count).encode(), validate_keys=False)
            break
        except Exception:
            continue
    await kv.put(voter_key, option.encode(), validate_keys=False)
    return {"topic": topic, "proposal": req.proposal.strip(), "voter": me,
            "option": option, "count": new_count}


@app.get("/api/vote")
async def vote_status(topic: str, proposal: str,
                      authorization: str | None = Header(default=None)):
    """查看 (topic, proposal) 的投票计数与投票人 (T13)。"""
    require_agent(authorization)
    proposal_safe = re.sub(r"[*>]", "_", proposal.strip())[:64]
    prefix = f"vote.{topic.strip()}.{proposal_safe}"
    count = 0
    try:
        e = await kv.get(prefix, validate_keys=False)
        count = int(e.value.decode())
    except Exception:
        pass
    voters = []
    try:
        for k in await kv.keys(filters=[f"{prefix}.agent."]):
            voters.append(k.split(".agent.")[-1])
    except Exception:
        pass
    return {"topic": topic.strip(), "proposal": proposal.strip(),
            "count": count, "voters": sorted(voters)}


@app.get("/api/whoami")
async def whoami(authorization: str | None = Header(default=None)):
    return {"agent": require_agent(authorization)}


@app.get("/api/contacts")
async def contacts():
    return {
        "agents": sorted(set(_TOKEN2AGENT.values())),
        "notice": "全网通知地址: to=notice 会广播给所有 agent (from 保留发送者, 各收件箱独立)",
    }


@app.get("/api/presence")
async def presence(authorization: str | None = Header(default=None)):
    """各 agent 在线状态: online=是否有活跃 SSE 连接, last_seen=最近活跃时刻。只读。"""
    require_agent(authorization)
    out = {}
    for agent in sorted(set(_TOKEN2AGENT.values())):
        ls = _last_seen.get(agent)
        out[agent] = {
            "online": _conns.get(agent, 0) > 0,
            "last_seen": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ls)) if ls else None,
        }
    return out


async def _discover_services() -> list[dict]:
    """NATS micro 服务发现 (T14): 发布 {} 到 $SRV.INFO + reply inbox, 收集服务 INFO。
    支持 <agent-slug>.<能力> 带点号服务名 (nats CLI 会拒绝, 故走自有发现)。"""
    inbox = f"_INBOX.srvdisco.{uuid.uuid4().hex}"
    q = asyncio.Queue()
    start = time.monotonic()

    async def _cb(msg):
        try:
            info = json.loads(msg.data)
            q.put_nowait((info, (time.monotonic() - start) * 1000))
        except Exception:
            pass

    sub = await nc.subscribe(inbox, cb=_cb)
    await nc.publish("$SRV.INFO", b"{}", reply=inbox)
    entries = []
    while time.monotonic() - start < 0.8:
        try:
            entries.append(await asyncio.wait_for(q.get(), timeout=0.2))
        except asyncio.TimeoutError:
            continue
    try:
        await sub.unsubscribe()
    except Exception:
        pass
    out = []
    seen = set()
    for info, rtt in entries:
        name = info.get("name")
        sid = info.get("id")
        if (name, sid) in seen:
            continue
        seen.add((name, sid))
        out.append({
            "name": name, "id": sid, "version": info.get("version"),
            "description": info.get("description"),
            "endpoints": [e.get("name") for e in info.get("endpoints", [])],
            "rtt_ms": round(rtt, 1),
        })
    return sorted(out, key=lambda x: x["name"] or "")


@app.get("/api/services")
async def services(authorization: str | None = Header(default=None)):
    """列出所有已注册的 NATS micro 服务 (T14)。"""
    require_agent(authorization)
    return {"services": await _discover_services()}


@app.post("/api/services/{name}/{endpoint}")
async def service_call(name: str, endpoint: str, req: ServiceCallReq,
                       authorization: str | None = Header(default=None)):
    """调用服务端点 (T14): request-reply 到 $SRV.REQ.<name>.<endpoint>。"""
    require_agent(authorization)
    subject = f"$SRV.REQ.{name}.{endpoint}"
    try:
        resp = await nc.request(subject, req.data.encode(), timeout=5)
    except Exception as e:
        raise HTTPException(504, f"service call failed: {e}")
    try:
        result = json.loads(resp.data)
    except Exception:
        result = resp.data.decode()
    return {"service": name, "endpoint": endpoint, "result": result}


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
