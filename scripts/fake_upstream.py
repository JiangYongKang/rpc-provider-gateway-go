#!/usr/bin/env python3
"""本地可控的假 JSON-RPC provider，仅用于本地验证。

用法:
  python3 scripts/fake_upstream.py 9101 primary ok
  python3 scripts/fake_upstream.py 9102 backup  ok

行为模式(第三个参数):
  ok        所有方法返回成功
  http503   所有请求返回 HTTP 503（模拟暂时不可用，触发故障转移）
  timeout   挂起不响应（模拟超时）
  rpcerr    返回 JSON-RPC error 对象（确定性错误，不触发故障转移）

对 eth_sendRawTransaction 会在响应里回显收到次数，便于确认非幂等方法只投递一次。
"""
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

counts = {"total": 0, "eth_sendRawTransaction": 0}
counts_lock = threading.Lock()


def make_handler(port: int, name: str, mode: str):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                req = json.loads(raw)
            except Exception:
                self.send_response(400)
                self.end_headers()
                return
            method = req.get("method", "")
            rid = req.get("id")
            with counts_lock:
                counts["total"] += 1
                if method == "eth_sendRawTransaction":
                    counts["eth_sendRawTransaction"] += 1
                send_n = counts["eth_sendRawTransaction"]
                total = counts["total"]
            print(f"[{name}:{port}] req_id={rid} method={method} "
                  f"total_calls={total} send_calls={send_n} mode={mode}", flush=True)

            if mode == "http503":
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"busy"}')
                return
            if mode == "timeout":
                import time
                time.sleep(30)
                return
            if mode == "rpcerr":
                self._json({"jsonrpc": "2.0", "id": rid,
                            "error": {"code": -32010, "message": "execution reverted"}})
                return

            result = {"provider": name, "calls": total}
            if method == "eth_sendRawTransaction":
                result["send_calls"] = send_n
                result["tx_hash"] = "0xdeadbeef"
            self._json({"jsonrpc": "2.0", "id": rid, "result": result})

        def _json(self, obj):
            body = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)
    port = int(sys.argv[1])
    name = sys.argv[2]
    mode = sys.argv[3]
    HTTPServer(("127.0.0.1", port), make_handler(port, name, mode)).serve_forever()


if __name__ == "__main__":
    main()
