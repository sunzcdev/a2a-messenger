#!/usr/bin/env python3
"""Gemini 独立 agent 服务 — octopus.gemini (端点: chat / ask / models)

轻量 NATS micro 实现 (同 octopus_service.py 协议):
  - $SRV.PING / $SRV.INFO → 发现
  - $SRV.REQ.octopus.gemini.<endpoint> ← 调用

给其他 agent 共享的 Gemini LLM 推理能力 (gemini-web2api :8091):
  - chat:   通用对话补全 (model/messages/tools) — 写作/摘要/推理/代码/JSON 提取
  - ask:    单轮快速问答 (prompt + model?) — 一句话答案
  - models: 列出可用模型 + 别名速查

模型别名: fast=3.6-flash / thinking=3.5-flash-thinking / pro=3.1-pro / lite / auto
"""
import asyncio
import json
import os
import uuid

import httpx
import nats

NATS_URL = os.environ.get("A2A_NATS_URL", "nats://127.0.0.1:4222")
NAME = "gemini.llm"
VERSION = "0.1.0"
DESC = "Gemini LLM 推理服务: chat 对话补全 / ask 快速问答 / models 模型列表 (gemini-web2api :8091)"
ENDPOINTS = {
    "chat": f"$SRV.REQ.{NAME}.chat",
    "ask": f"$SRV.REQ.{NAME}.ask",
    "models": f"$SRV.REQ.{NAME}.models",
}
GEMINI_URL = os.environ.get("GEMINI_WEB2API_URL", "http://127.0.0.1:8091")
GEMINI_KEY = os.environ.get("GEMINI_WEB2API_KEY", "sk-gemini")
MODEL_ALIASES = {
    "thinking": "gemini-3.5-flash-thinking",
    "pro": "gemini-3.1-pro",
    "fast": "gemini-3.6-flash",
    "lite": "gemini-flash-lite",
    "auto": "gemini-auto",
}


def _resolve_model(m):
    m = (m or "").strip().lower()
    if not m:
        return "gemini-3.6-flash"
    return MODEL_ALIASES.get(m, m)


async def _gemini_chat(model, messages, tools=None, temperature=None, max_tokens=None):
    payload = {"model": _resolve_model(model), "messages": messages}
    if tools:
        payload["tools"] = tools
    if temperature is not None:
        payload["temperature"] = temperature
    if max_tokens:
        payload["max_tokens"] = max_tokens
    async with httpx.AsyncClient(timeout=180) as c:
        r = await c.post(
            f"{GEMINI_URL}/v1/chat/completions",
            json=payload,
            headers={"Authorization": f"Bearer {GEMINI_KEY}"},
        )
        r.raise_for_status()
        return r.json()


async def chat_handler(msg):
    try:
        data = json.loads(msg.data.decode(errors="replace"))
    except Exception:
        await nc.publish(msg.reply, json.dumps(
            {"ok": False, "error": "请求必须是 JSON: {model?, messages|prompt, tools?, temperature?}"},
            ensure_ascii=False).encode())
        return
    model = data.get("model", "gemini-3.6-flash")
    messages = data.get("messages")
    if not messages:
        p = data.get("prompt", "")
        messages = [{"role": "user", "content": p}] if p else None
    if not messages:
        await nc.publish(msg.reply, json.dumps(
            {"ok": False, "error": "缺少 messages 或 prompt"}, ensure_ascii=False).encode())
        return
    try:
        d = await _gemini_chat(model, messages, data.get("tools"),
                               data.get("temperature"), data.get("max_tokens"))
        choice = d["choices"][0]["message"]
        await nc.publish(msg.reply, json.dumps({
            "ok": True,
            "model": d.get("model"),
            "content": choice.get("content"),
            "tool_calls": choice.get("tool_calls"),
            "usage": d.get("usage"),
        }, ensure_ascii=False).encode())
    except Exception as e:
        await nc.publish(msg.reply, json.dumps(
            {"ok": False, "error": str(e), "model": model}, ensure_ascii=False).encode())


async def ask_handler(msg):
    """快速单轮问答: 输入 JSON {prompt, model?} 或纯文本 prompt"""
    raw = msg.data.decode(errors="replace").strip()
    try:
        data = json.loads(raw)
        prompt, model = data.get("prompt", ""), data.get("model", "gemini-3.6-flash")
    except Exception:
        prompt, model = raw, "gemini-3.6-flash"
    if not prompt:
        await nc.publish(msg.reply, json.dumps(
            {"ok": False, "error": "缺少 prompt"}, ensure_ascii=False).encode())
        return
    try:
        d = await _gemini_chat(model, [{"role": "user", "content": prompt}])
        await nc.publish(msg.reply, json.dumps({
            "ok": True, "model": d.get("model"),
            "answer": d["choices"][0]["message"].get("content"),
            "usage": d.get("usage"),
        }, ensure_ascii=False).encode())
    except Exception as e:
        await nc.publish(msg.reply, json.dumps(
            {"ok": False, "error": str(e), "model": model}, ensure_ascii=False).encode())


async def models_handler(msg):
    await nc.publish(msg.reply, json.dumps({
        "ok": True,
        "service": NAME,
        "models": [
            {"name": "gemini-3.6-flash", "desc": "默认全能模型, ~12k 输出", "alias": "fast"},
            {"name": "gemini-3.5-flash-thinking", "desc": "深度推理, ~20k 输出 (复杂任务首选)", "alias": "thinking"},
            {"name": "gemini-3.5-flash-thinking-lite", "desc": "自适应思考深度, ~15k", "alias": None},
            {"name": "gemini-3.1-pro", "desc": "Pro 模型 (需付费 cookie 真路由)", "alias": "pro"},
            {"name": "gemini-auto", "desc": "自动选模型", "alias": "auto"},
            {"name": "gemini-flash-lite", "desc": "最快最轻, ~10k", "alias": "lite"},
        ],
        "用法": 'call octopus.gemini chat {"model":"thinking","messages":[{"role":"user","content":"..."}]}',
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
    for ep, subj in ENDPOINTS.items():
        handler = {"chat": chat_handler, "ask": ask_handler, "models": models_handler}[ep]
        await nc.subscribe(subj, cb=handler)
    print(f"✅ service {NAME} (id={service_id}) 已注册, 端点: {', '.join(ENDPOINTS)} (Ctrl-C 退出)", flush=True)
    try:
        await asyncio.Future()
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
