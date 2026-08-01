#!/usr/bin/env python3
"""a2a-bus T14 demo 服务 — 轻量 NATS micro 实现 (cc-oracle.echo, 端点: echo/time)。

用原生 NATS 实现 micro 发现协议 (nats-py micro 的 name 不允许点号, 无法用
<agent-slug>.<能力> 命名, 故用轻量 handler):
  - $SRV.PING  → 发布 INFO 到 $SRV.INFO.<name>.<id>  (可被 nats service list 发现)
  - $SRV.REQ.<name>.<endpoint>  ← 端点调用
注册新服务请参考 README 的"注册你的服务"一节。
"""
import asyncio
import json
import os
import time
import uuid

import nats

NATS_URL = os.environ.get("A2A_NATS_URL", "nats://127.0.0.1:4222")
NAME = "cc-oracle.echo"          # <agent-slug>.<能力>
VERSION = "0.1.0"
DESC = "a2a-bus T14 demo service (echo/time)"
ENDPOINTS = {
    "echo": f"$SRV.REQ.{NAME}.echo",
    "time": f"$SRV.REQ.{NAME}.time",
}


async def time_payload() -> bytes:
    return json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "unix": int(time.time())}).encode()


async def main():
    nc = await nats.connect(NATS_URL)
    service_id = uuid.uuid4().hex[:12]
    info = json.dumps({
        "type": "io.nats.micro.v1.info_response",
        "name": NAME, "id": service_id, "version": VERSION, "description": DESC,
        "endpoints": [{"name": ep, "subject": subj} for ep, subj in ENDPOINTS.items()],
    }).encode()

    async def info_handler(msg):
        # nats CLI 0.4.x 发现协议: 发布 {} 到 $SRV.INFO + reply inbox, 服务回 msg.reply
        await nc.publish(msg.reply, info)

    async def ping_handler(msg):
        # micro v1 发现协议: 发布到 $SRV.PING, 服务回 $SRV.INFO.<name>.<id>
        await nc.publish(f"$SRV.INFO.{NAME}.{service_id}", info)

    async def echo_handler(msg):
        await nc.publish(msg.reply, msg.data)

    async def time_handler(msg):
        await nc.publish(msg.reply, await time_payload())

    await nc.subscribe("$SRV.INFO", cb=info_handler)
    await nc.subscribe("$SRV.PING", cb=ping_handler)
    await nc.subscribe(ENDPOINTS["echo"], cb=echo_handler)
    await nc.subscribe(ENDPOINTS["time"], cb=time_handler)
    print(f"✅ demo service {NAME} (id={service_id}) 已注册, 端点: {', '.join(ENDPOINTS)} (Ctrl-C 退出)", flush=True)
    try:
        await asyncio.Future()
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
