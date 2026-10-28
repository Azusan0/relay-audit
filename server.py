"""relay-audit 本地 Web 服务（零第三方依赖）。

启动: python server.py [端口]
打开: http://127.0.0.1:8737
"""
from __future__ import annotations

import json
import os
import queue
import sys
import threading
import traceback
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(ROOT, "static")

JOBS: dict[str, dict] = {}


def run_job(job_id: str, config: dict):
    job = JOBS[job_id]
    q: queue.Queue = job["queue"]

    def progress(ev):
        q.put(ev)

    try:
        job["status"] = "running"
        report = engine.audit(config, progress=progress, should_stop=lambda: job["stop"])
        job["report"] = report
        job["status"] = "done"
        q.put({"type": "done", "spent_calls": report.get("spent_calls"),
               "spent_usd_est": report.get("spent_usd_est")})
    except Exception as e:  # noqa: BLE001
        job["status"] = "error"
        q.put({"type": "error", "msg": f"{e!r}", "trace": traceback.format_exc()[-2000:]})


class Handler(BaseHTTPRequestHandler):
    server_version = "relay-audit/0.1"

    def log_message(self, fmt, *args):
        pass

    # ---------- helpers ----------
    def _send(self, code, body: bytes, ctype="application/json; charset=utf-8", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"))

    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        return json.loads(self.rfile.read(n).decode("utf-8"))

    # ---------- routes ----------
    def do_GET(self):
        u = urlparse(self.path)
        path, qs = u.path, parse_qs(u.query)
        if path in ("/", "/index.html"):
            return self._file(os.path.join(STATIC, "index.html"), "text/html; charset=utf-8")
        if path.startswith("/static/"):
            name = os.path.basename(path)
            ctype = "text/plain; charset=utf-8"
            if name.endswith(".js"):
                ctype = "application/javascript; charset=utf-8"
            elif name.endswith(".css"):
                ctype = "text/css; charset=utf-8"
            return self._file(os.path.join(STATIC, name), ctype)
        if path == "/api/events":
            return self._events(qs.get("job", [""])[0])
        if path == "/api/report":
            job = JOBS.get(qs.get("job", [""])[0])
            if not job or not job.get("report"):
                return self._json({"ok": False, "error": "报告未就绪"}, 404)
            return self._json({"ok": True, "report": job["report"]})
        if path == "/api/status":
            job = JOBS.get(qs.get("job", [""])[0])
            if not job:
                return self._json({"ok": False, "error": "job 不存在"}, 404)
            return self._json({"ok": True, "status": job["status"]})
        if path == "/api/export":
            job = JOBS.get(qs.get("job", [""])[0])
            if not job or not job.get("report"):
                return self._json({"ok": False, "error": "报告未就绪"}, 404)
            html = render_report_html(job["report"])
            return self._send(200, html.encode("utf-8"), "text/html; charset=utf-8",
                              {"Content-Disposition": 'attachment; filename="relay-audit.html"'})
        return self._json({"ok": False, "error": "not found"}, 404)

    def do_POST(self):
        u = urlparse(self.path)
        if u.path == "/api/models":
            try:
                body = self._read_json()
                flavor = body.get("flavor", "openai")
                if flavor == "auto":
                    ok, data = engine.list_models(body["base"], body["key"], "openai")
                    if not ok:
                        ok, data = engine.list_models(body["base"], body["key"], "anthropic")
                else:
                    ok, data = engine.list_models(body["base"], body["key"], flavor)
                return self._json({"ok": ok, "models": data if ok else [], "error": None if ok else data})
            except Exception as e:  # noqa: BLE001
                return self._json({"ok": False, "models": [], "error": repr(e)}, 400)

        if u.path == "/api/start":
            try:
                config = self._read_json()
                if not config.get("base") or not config.get("key") or not config.get("models"):
                    return self._json({"ok": False, "error": "base / key / models 必填"}, 400)
                job_id = uuid.uuid4().hex[:12]
                JOBS[job_id] = {"queue": queue.Queue(), "status": "pending", "stop": False,
                                "report": None}
                threading.Thread(target=run_job, args=(job_id, config), daemon=True).start()
                return self._json({"ok": True, "job": job_id})
            except Exception as e:  # noqa: BLE001
                return self._json({"ok": False, "error": repr(e)}, 400)

        if u.path == "/api/stop":
            body = self._read_json()
            job = JOBS.get(body.get("job", ""))
            if job:
                job["stop"] = True
                return self._json({"ok": True})
            return self._json({"ok": False, "error": "job 不存在"}, 404)

        return self._json({"ok": False, "error": "not found"}, 404)

    # ---------- impl ----------
    def _file(self, path, ctype):
        try:
            with open(path, "rb") as f:
                data = f.read()
            self._send(200, data, ctype)
        except FileNotFoundError:
            self._json({"ok": False, "error": "file not found"}, 404)

    def _events(self, job_id: str):
        job = JOBS.get(job_id)
        if not job:
            return self._json({"ok": False, "error": "job 不存在"}, 404)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        q: queue.Queue = job["queue"]
        try:
            while True:
                try:
                    ev = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                self.wfile.write(("data: " + json.dumps(ev, ensure_ascii=False) + "\n\n").encode("utf-8"))
                self.wfile.flush()
                if ev.get("type") in ("done", "error"):
                    break
        except (BrokenPipeError, ConnectionResetError):
            pass


def render_report_html(report: dict) -> str:
    def esc(s):
        return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

    rows = []
    for model, r in report["models"].items():
        color = "#1a7f37" if r["risk"] < 21 else ("#9a6700" if r["risk"] < 51 else "#cf222e")
        ev = "<br>".join(
            f"<span style='color:{'#cf222e' if lv == 'high' else ('#9a6700' if lv == 'mid' else ('#1a7f37' if lv == 'good' else '#57606a'))}'>• {esc(t)}</span>"
            for lv, t in r["evidence"])
        rows.append(f"""<tr>
          <td style="font-family:monospace">{esc(model)}</td>
          <td style="color:{color};font-weight:700">{r['risk']}</td>
          <td style="color:{color}">{esc(r['verdict'])}</td>
          <td>{ev}</td></tr>""")
    suite = "".join(
        f"<li>{esc(x['q'])} = <code>{esc(x['expected'])}</code></li>" for x in report.get("suite", []))
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>relay-audit 报告</title>
<style>body{{font-family:system-ui,-apple-system,'Segoe UI',sans-serif;margin:24px;color:#1f2328}}
table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #d0d7de;padding:8px;vertical-align:top;font-size:13px}}
th{{background:#f6f8fa;text-align:left}}h1{{font-size:20px}}code{{background:#f6f8fa;padding:1px 5px;border-radius:4px}}
.small{{color:#57606a;font-size:12px}}</style></head><body>
<h1>中转站模型审计报告</h1>
<p class="small">目标：<code>{esc(report['base'])}</code>（协议 {esc(report['flavor'])}）<br>
开始 {esc(report.get('started'))} · 结束 {esc(report.get('finished'))} ·
调用 {esc(report.get('spent_calls'))} 次 · 估算花费 ${esc(report.get('spent_usd_est'))}</p>
<table><tr><th style="width:26%">模型</th><th style="width:8%">风险分</th><th style="width:12%">结论</th><th>证据</th></tr>
{''.join(rows)}</table>
<h3>基准题标准答案</h3><ul>{suite}</ul>
<p class="small">风险分说明：0–20 暂未发现问题 · 21–50 需注意 · 51–75 高风险 · 76+ 极可能造假。<br>
本报告只基于黑盒探测，请结合价格与真实产出综合判断。</p>
</body></html>"""


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8737
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"relay-audit 已启动: http://127.0.0.1:{port}")
    print("按 Ctrl+C 停止")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
