#!/usr/bin/env python3
"""a2a-bus T14 服务: cc-oracle.code-review — 代码评审辅助 (只读 git 操作)。

端点:
  status  {repo?}   → git 状态 (branch/changed_files/最近提交)
  diff    {repo?}   → git diff --stat (工作区改动统计)
  syntax  {file}    → .py 文件语法检查 (py_compile)

安全: 只读 git/python 操作; repo 限制在 ~/projects 下; syntax 仅 .py。
运行: venv/bin/python services/code_review_service.py (systemd 常驻)。
"""
import asyncio
import json
import os
import subprocess
import uuid

import nats

NATS_URL = os.environ.get("A2A_NATS_URL", "nats://127.0.0.1:4222")
NAME = "cc-oracle.code-review"
VERSION = "0.1.0"
DESC = "cc-oracle 代码评审辅助: git 状态/差异/语法检查 (只读)"
ENDPOINTS = {
    "status": f"$SRV.REQ.{NAME}.status",
    "diff": f"$SRV.REQ.{NAME}.diff",
    "syntax": f"$SRV.REQ.{NAME}.syntax",
}
ALLOWED_ROOTS = [os.path.realpath(os.path.expanduser("~/projects"))]
DEFAULT_REPO = os.path.realpath(os.path.expanduser("~/projects/a2a-bus"))


def _git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args],
                          capture_output=True, text=True, timeout=15)


def _check_repo(repo: str):
    repo = repo or DEFAULT_REPO
    if not os.path.isdir(repo):
        return None, "repo 路径无效或不存在"
    real = os.path.realpath(repo)
    if not any(real == r or real.startswith(r + os.sep) for r in ALLOWED_ROOTS):
        return None, "repo 不在允许目录下 (~/projects)"
    if _git(real, "rev-parse", "--is-inside-work-tree").returncode != 0:
        return None, "不是 git 仓库"
    return real, None


def _parse_data(msg) -> dict:
    try:
        d = json.loads(msg.data or b"{}")
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _ok(payload: dict) -> bytes:
    return json.dumps({"ok": True, "result": payload}, ensure_ascii=False).encode()


def _err(text: str) -> bytes:
    return json.dumps({"ok": False, "error": text}, ensure_ascii=False).encode()


async def status_handler(msg):
    repo, err = _check_repo(_parse_data(msg).get("repo"))
    if err:
        await msg.respond(_err(err))
        return
    branch = _git(repo, "branch", "--show-current").stdout.strip()
    status = _git(repo, "status", "--short").stdout
    changed = len([l for l in status.splitlines() if l.strip()])
    log = _git(repo, "log", "-5", "--oneline").stdout.strip()
    await msg.respond(_ok({
        "repo": repo, "branch": branch, "changed_files": changed,
        "recent_commits": log.splitlines() if log else [],
    }))


async def diff_handler(msg):
    repo, err = _check_repo(_parse_data(msg).get("repo"))
    if err:
        await msg.respond(_err(err))
        return
    stat = _git(repo, "diff", "--stat").stdout.strip()
    await msg.respond(_ok({"repo": repo, "diff_stat": stat or "(无改动)"}))


async def syntax_handler(msg):
    f = _parse_data(msg).get("file") or ""
    if not f or not os.path.isfile(f):
        await msg.respond(_err("file 路径无效或不存在"))
        return
    if not f.endswith(".py"):
        await msg.respond(_err("仅支持 .py 文件"))
        return
    r = subprocess.run(["python3", "-m", "py_compile", f],
                       capture_output=True, text=True, timeout=15)
    await msg.respond(_ok({
        "file": f, "ok": r.returncode == 0,
        "error": r.stderr.strip()[:500] if r.stderr.strip() else None,
    }))


HANDLERS = {"status": status_handler, "diff": diff_handler, "syntax": syntax_handler}


async def main():
    nc = await nats.connect(NATS_URL)
    service_id = uuid.uuid4().hex[:12]
    info = json.dumps({
        "type": "io.nats.micro.v1.info_response",
        "name": NAME, "id": service_id, "version": VERSION, "description": DESC,
        "endpoints": [{"name": ep, "subject": subj} for ep, subj in ENDPOINTS.items()],
    }).encode()

    async def info_handler(msg):
        # nats CLI 0.4.x 发现协议: 回 msg.reply
        await nc.publish(msg.reply, info)

    async def ping_handler(msg):
        # micro v1 发现协议
        await nc.publish(f"$SRV.INFO.{NAME}.{service_id}", info)
        if msg.reply:
            await nc.publish(msg.reply, info)

    await nc.subscribe("$SRV.INFO", cb=info_handler)
    await nc.subscribe("$SRV.PING", cb=ping_handler)
    for ep, subj in ENDPOINTS.items():
        await nc.subscribe(subj, cb=HANDLERS[ep])
    print(f"✅ service {NAME} (id={service_id}) 已注册, 端点: {', '.join(ENDPOINTS)} (Ctrl-C 退出)", flush=True)
    try:
        await asyncio.Future()
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
