#!/usr/bin/env python3
"""八爪鱼共享服务 — octopus.tools (端点: web-fetch / sys-health / weather)

轻量 NATS micro 实现 (同 demo_service.py 协议):
  - $SRV.PING / $SRV.INFO → 发现
  - $SRV.REQ.octopus.tools.<endpoint> ← 调用

给其他 agent 共享的能力:
  - web-fetch: 抓取网页 (curl, 返回前 4000 字符) — 帮被网络限制的 agent 取网页
  - sys-health: Oracle 系统健康 (nats/a2a-api 服务状态 + 磁盘/内存)
  - weather: 城市天气 (wttr.in)
"""
import asyncio
import json
import os
import subprocess
import time
import uuid

import nats

NATS_URL = os.environ.get("A2A_NATS_URL", "nats://127.0.0.1:4222")
NAME = "octopus.tools"
VERSION = "0.1.0"
DESC = "八爪鱼共享服务: web-fetch 网页抓取 / sys-health 系统健康 / weather 天气"
ENDPOINTS = {
    "web-fetch": f"$SRV.REQ.{NAME}.web-fetch",
    "sys-health": f"$SRV.REQ.{NAME}.sys-health",
    "weather": f"$SRV.REQ.{NAME}.weather",
}


def _run(cmd, timeout=15):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception as e:
        return f"ERROR: {e}"


async def web_fetch_handler(msg):
    """输入: URL (或 JSON {"url": ...}) → 返回网页前 4000 字符"""
    raw = msg.data.decode(errors="replace").strip()
    try:
        data = json.loads(raw)
        url = data.get("url", "")
    except Exception:
        url = raw
    if not url.startswith(("http://", "https://")):
        await nc.publish(msg.reply, json.dumps(
            {"ok": False, "error": "URL 必须以 http(s):// 开头"}, ensure_ascii=False).encode())
        return
    try:
        r = subprocess.run(
            ["curl", "-sL", "--max-time", "20", "-A",
             "Mozilla/5.0 (compatible; a2a-octopus/1.0)", url],
            capture_output=True, text=True, timeout=25)
        content = r.stdout
        if not content:
            await nc.publish(msg.reply, json.dumps(
                {"ok": False, "error": f"抓取为空 (stderr: {r.stderr[:200]})",
                 "url": url}, ensure_ascii=False).encode())
            return
        # 去 HTML 标签, 取前 4000 字符
        import re
        text = re.sub(r"<script[^>]*>.*?</script>|<style[^>]*>.*?</style>", " ", content, flags=re.S)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        await nc.publish(msg.reply, json.dumps(
            {"ok": True, "url": url, "chars": len(text),
             "content": text[:4000]}, ensure_ascii=False).encode())
    except Exception as e:
        await nc.publish(msg.reply, json.dumps(
            {"ok": False, "error": str(e), "url": url}, ensure_ascii=False).encode())


async def sys_health_handler(msg):
    """返回 Oracle 系统健康: 服务状态 + 磁盘 + 内存"""
    svc = {}
    for s in ["nats-server", "a2a-api", "a2a-mcp", "a2a-mcp-cc"]:
        r = subprocess.run(["systemctl", "is-active", s], capture_output=True, text=True)
        svc[s] = r.stdout.strip()
    disk = _run(["df", "-h", "/"], timeout=5).splitlines()
    mem = _run(["free", "-h"], timeout=5).splitlines()
    await nc.publish(msg.reply, json.dumps({
        "ok": True, "services": svc,
        "disk_root": disk[1] if len(disk) > 1 else disk,
        "memory": mem[1] if len(mem) > 1 else mem,
        "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }, ensure_ascii=False).encode())


async def weather_handler(msg):
    """输入: 城市名 (中文/拼音) → wttr.in 天气摘要"""
    city = msg.data.decode(errors="replace").strip() or "beijing"
    r = subprocess.run(["curl", "-s", "--max-time", "15",
                        f"https://wttr.in/{city}?format=%l:+%c+%t+%h+%w"],
                       capture_output=True, text=True, timeout=20)
    await nc.publish(msg.reply, json.dumps({
        "ok": bool(r.stdout.strip()), "city": city, "weather": r.stdout.strip(),
        "error": r.stderr[:200] if not r.stdout.strip() else None,
    }, ensure_ascii=False).encode())


async def main():
    global nc
    nc = await nats.connect(NATS_URL)
    service_id = uuid.uuid4().hex[:12]
    info = json.dumps({
        "type": "io.nats.micro.v1.info_response",
        "name": NAME, "id": service_id, "version": VERSION, "description": DESC,
        "endpoints": [{"name": ep, "subject": subj} for ep, subj in ENDPOINTS.items()],
    }).encode()

    async def info_handler(msg):
        await nc.publish(msg.reply, info)

    async def ping_handler(msg):
        await nc.publish(f"$SRV.INFO.{NAME}.{service_id}", info)

    await nc.subscribe("$SRV.INFO", cb=info_handler)
    await nc.subscribe("$SRV.PING", cb=ping_handler)
    await nc.subscribe(ENDPOINTS["web-fetch"], cb=web_fetch_handler)
    await nc.subscribe(ENDPOINTS["sys-health"], cb=sys_health_handler)
    await nc.subscribe(ENDPOINTS["weather"], cb=weather_handler)
    print(f"✅ service {NAME} (id={service_id}) 已注册, 端点: {', '.join(ENDPOINTS)} (Ctrl-C 退出)", flush=True)
    try:
        await asyncio.Future()
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
