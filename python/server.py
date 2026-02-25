#!/usr/bin/env python3
"""
OTLP HTTP 接收服务器 - 支持gzip解压版
处理Content-Encoding: gzip的请求，解压后显示JSON数据
"""
import http.server
import json
import sys
import os
import gzip
import csv
import threading
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# LSTM 训练数据输出目录与文件
CSV_DIR = os.getenv("CARVE_CSV_DIR", "data")
CSV_PATH = os.getenv("CARVE_CSV_PATH", os.path.join(CSV_DIR, "metrics.csv"))
# 可选：只保留指标名包含下列子串的 metric，空则不过滤
METRIC_FILTER = os.getenv("CARVE_METRICS", "").strip().split(",") if os.getenv("CARVE_METRICS") else []
_csv_lock = threading.Lock()
# 收集控制：仅当 _collecting 且 _current_csv_path 非空时才写 CSV
_collect_lock = threading.Lock()
_collecting = False
_current_csv_path = None
# 可 HTTP 修改的指标名过滤（子串匹配）；空列表=不过滤。启动时用 CARVE_METRICS 初始化
_filter_lock = threading.Lock()
_metric_allow_list = [s.strip() for s in METRIC_FILTER if s and s.strip()] if METRIC_FILTER else []

def _safe_filename(name):
    """只允许 basename，禁止 .. 和路径分隔符"""
    if not name or ".." in name or "/" in name or "\\" in name:
        return None
    name = os.path.basename(name.strip())
    if not name or len(name) > 200:
        return None
    return name if all(c.isalnum() or c in "_.-" for c in name) else None

def run_server(port=8080):
    os.makedirs(CSV_DIR, exist_ok=True)
    server_address = ('', port)
    httpd = http.server.HTTPServer(server_address, OTLPRequestHandler)
    
    print(f"[{datetime.now().isoformat()}] OTLP HTTP 服务器启动，端口: {port}")
    print(f"[{datetime.now().isoformat()}] 健康检查: http://localhost:{port}/healthz")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print(f"[{datetime.now().isoformat()}] 服务器已停止")

class OTLPRequestHandler(http.server.BaseHTTPRequestHandler):
    
    def do_POST(self):
        """处理POST请求，支持gzip解压"""
        parsed = urlparse(self.path)
        path, query = parsed.path.rstrip("/") or "/", parsed.query

        # 收集控制接口（不读 body）
        if path == "/collect/start":
            self._handle_collect_start(query)
            return
        if path == "/collect/stop":
            self._handle_collect_stop()
            return
        if path == "/collect/filters":
            self._handle_collect_filters_post(query)
            return

        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length else b''
        content_type = self.headers.get('Content-Type', '')
        content_encoding = self.headers.get('Content-Encoding', '')
        
        
        print(f"[{datetime.now().isoformat()}] 收到新请求，客户端地址: {self.client_address[0]}:{self.client_address[1]}，路径: {self.path}，方法: {self.command}")
        # 尝试解压gzip数据
        processed_data = post_data
        if content_encoding.lower() == 'gzip':
            try:
                processed_data = gzip.decompress(post_data)
            except Exception as e:
                print(f"gzip解压失败: {e}")
                print("将显示原始压缩数据")
        
        print("\n请求体:")
        
        # 处理JSON数据
        if 'application/json' in content_type:
            try:
                json_data = json.loads(processed_data.decode('utf-8'))
                if  path  == '/v1/metrics' and isinstance(json_data, dict):
                    rows = self._otlp_to_lstm_rows(json_data)
                    if rows:
                       with _collect_lock:
                            path_to_write = _current_csv_path if _collecting else None
                        if path_to_write:
                            n = self._append_rows_to_csv(rows, path_to_write)
                            print(f"[{datetime.now().isoformat()}] /v1/metrics 写入 {n} 条 -> {path_to_write}")    
                        for r in rows:
                            # r = (ts_ms, metric, value, service)
                            print(f"    {r[1]} = {r[2]}  (service={r[3] or '-'})")
            except Exception as e:
                print(f"JSON解析失败: {e}")
                print("显示原始文本:")
        
        
        # 发送响应
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        response = {
            "status": "ok", 
            "message": "数据已接收",
            "timestamp": datetime.now().isoformat(),
            "original_size": len(post_data),
            "processed_size": len(processed_data),
            "compressed": content_encoding.lower() == 'gzip'
        }
        self.wfile.write(json.dumps(response).encode())
    
    def _otlp_to_lstm_rows(self, json_data):
        """从 OTLP JSON 抽出 (ts_ms, metric, value, service) 列表，用于 LSTM 训练 CSV"""
        rows = []
        for resource in json_data.get("resourceMetrics", []):
            attrs = {}
            for a in resource.get("resource", {}).get("attributes", []):
                k = a.get("key")
                v = a.get("value") or {}
                if "stringValue" in v:
                    attrs[k] = v["stringValue"]
            service = attrs.get("service.name", "")

            for scope in resource.get("scopeMetrics", []):
                for metric in scope.get("metrics", []):
                    name = metric.get("name", "")
                    with _filter_lock:
                        allow = list(_metric_allow_list)
                    if allow and not any(f in name for f in allow if f):
                        continue
                    for key in ("gauge", "sum"):
                        if key not in metric:
                            continue
                        for dp in metric[key].get("dataPoints", []):
                            ts_nano = dp.get("timeUnixNano")
                            if not ts_nano:
                                continue
                            try:
                                ts_ms = int(ts_nano) // 1_000_000
                            except (TypeError, ValueError):
                                continue
                            val = dp.get("asDouble")
                            if val is None:
                                raw = dp.get("asInt")
                                if raw is not None:
                                    try:
                                        val = float(raw)
                                    except (TypeError, ValueError):
                                        pass
                            if val is None:
                                continue
                            rows.append((ts_ms, name, float(val), service))
        return rows

    def _append_rows_to_csv(self, rows, path):
        """线程安全地追加写入 CSV，path 为完整路径。返回写入行数"""
        if not rows or not path:
            return 0
        with _csv_lock:
            file_exists = os.path.exists(path)
            try:
                with open(path, "a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    if not file_exists:
                        w.writerow(["ts", "metric", "value", "service"])
                    for r in rows:
                        w.writerow(r)
                return len(rows)
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] 写 CSV 失败: {e}")
                return 0

    def _handle_collect_filters_get(self):
        """GET /collect/filters -> 返回当前指标过滤列表"""
        with _filter_lock:
            metrics = list(_metric_allow_list)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"metrics": metrics, "collect_all": len(metrics) == 0}).encode())

    def _handle_collect_filters_post(self, query):
        """POST /collect/filters -> 覆盖指标过滤列表。query: metric=xxx&metric=yyy，无 metric 则清空（收集全部）"""
        params = parse_qs(query)
        raw = params.get("metric") or []
        if isinstance(raw, list):
            metrics = [m.strip() for m in raw if m and str(m).strip()]
        else:
            metrics = [str(raw).strip()] if raw else []
        with _filter_lock:
            _metric_allow_list.clear()
            _metric_allow_list.extend(metrics)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "metrics": list(_metric_allow_list)}).encode())

    def _handle_collect_start(self, query):
        """GET/POST /collect/start?filename=xxx.csv"""
        global _collecting, _current_csv_path
        params = parse_qs(query)
        filename = (params.get("filename") or [None])[0]
        safe = _safe_filename(filename) if filename else None
        if not safe:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "缺少或无效的 filename（仅允许字母数字、下划线、横线、点）"}).encode())
            return
        with _collect_lock:
            _collecting = True
            _current_csv_path = os.path.join(CSV_DIR, safe)
        os.makedirs(CSV_DIR, exist_ok=True)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "message": "started", "filename": safe}).encode())
        print(f"[{datetime.now().isoformat()}] 收集已开始 -> {_current_csv_path}")

    def _handle_collect_stop(self):
        """POST /collect/stop"""
        global _collecting, _current_csv_path
        with _collect_lock:
            _collecting = False
            _current_csv_path = None
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "message": "stopped"}).encode())
        print(f"[{datetime.now().isoformat()}] 收集已停止")

    def _handle_export(self, query):
        """GET /export?filename=xxx.csv"""
        params = parse_qs(query)
        filename = (params.get("filename") or [None])[0]
        safe = _safe_filename(filename) if filename else None
        if not safe:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "缺少或无效的 filename"}).encode())
            return
        filepath = os.path.join(CSV_DIR, safe)
        abspath = os.path.abspath(filepath)
        base_abspath = os.path.abspath(CSV_DIR)
        if not abspath.startswith(base_abspath) or not os.path.isfile(abspath):
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "文件不存在"}).encode())
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{safe}"')
        self.end_headers()
        with open(abspath, "rb") as f:
            self.wfile.write(f.read())

    def _handle_export_list(self):
        """GET /export/list -> 列出可导出的 CSV 文件名"""
        if not os.path.isdir(CSV_DIR):
            files = []
        else:
            base_abspath = os.path.abspath(CSV_DIR)
            files = []
            for name in os.listdir(CSV_DIR):
                if name.startswith("."):
                    continue
                full = os.path.join(CSV_DIR, name)
                if not os.path.isfile(full):
                    continue
                # 只允许在 CSV_DIR 下的普通文件，且文件名需通过安全校验
                if os.path.abspath(full).startswith(base_abspath) and _safe_filename(name):
                    files.append(name)
            files.sort()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "files": files}).encode())
        
    def do_GET(self):
        """健康检查、收集状态、导出 CSV"""
        parsed = urlparse(self.path)
        path, query = parsed.path.rstrip("/") or "/", parsed.query

        if path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "service": "otlp-http-server"
            }
            self.wfile.write(json.dumps(response).encode())
            print(f"[{datetime.now().isoformat()}] 健康检查: {self.client_address[0]}")
            return
        if path == "/collect/status":
            with _collect_lock:
                collecting = _collecting
                filename = os.path.basename(_current_csv_path) if _current_csv_path else None
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"collecting": collecting, "filename": filename}).encode())
            return
        if path == "/collect/filters":
            self._handle_collect_filters_get()
            return
        if path == "/export/list":   # 新增：先于 /export 判断
            self._handle_export_list()
            return
        if path == "/export":
            self._handle_export(query)
            return
        self.send_response(501)
        self.end_headers()
    
    def log_message(self, format, *args):
        """禁用默认的HTTP日志输出"""
        pass

if __name__ == '__main__':
    port = int(os.getenv('PORT', '8080'))
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except:
            pass
    
    run_server(port)
