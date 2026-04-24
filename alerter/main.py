#!/usr/bin/env python3
"""
LSTM alerter pod: connect to Carve WebSocket for a target, run LSTM inference, expose prediction error to Prometheus.
Env: CARVE_URL, TARGET, MODEL_NAME, METRICS_PORT (default 9092).
Prometheus: scrape /metrics; alert when carve_prediction_error > threshold.
"""

import json
import os
import sys
import time
import urllib.parse

import requests
from prometheus_client import start_http_server, Gauge
from websocket import create_connection

from inference import StreamInference, load_checkpoint

from collections import deque
import statistics

# Prometheus gauge: current prediction error (absolute). Alert when this is above a threshold.
PREDICTION_ERROR = Gauge(
    "carve_prediction_error",
    "Absolute prediction error (|actual - predicted|) for the latest step",
    ["target", "model"],
)
LAST_UPDATE_TS = Gauge(
    "carve_prediction_error_updated_at",
    "Unix timestamp of last prediction error update",
    ["target", "model"],
)
MODEL_LOADED = Gauge("carve_alerter_model_loaded", "1 if model is loaded", ["target", "model"])


ROBUST_ZSCORE = Gauge(
    "carve_prediction_error_robust_zscore",
    "Robust z-score of prediction error using rolling median and MAD",
    ["target", "model"],
)

ERROR_MEDIAN = Gauge(
    "carve_prediction_error_median",
    "Rolling median of prediction error",
    ["target", "model"],
)

ERROR_MAD = Gauge(
    "carve_prediction_error_mad",
    "Rolling MAD of prediction error",
    ["target", "model"],
)

ROBUST_READY = Gauge(
    "carve_prediction_error_robust_ready",
    "1 if robust z-score has enough samples, else 0",
    ["target", "model"],
)


class RobustZScore:
    def __init__(self, window_size=120, min_points=30, eps=1e-6):
        self.window_size = max(10, int(window_size))
        self.min_points = max(5, int(min_points))
        self.eps = float(eps)
        self.buf = deque(maxlen=self.window_size)

    def update(self, err_value: float):
        # 返回: (ready, rz, med, mad)
        self.buf.append(float(err_value))
        if len(self.buf) < self.min_points:
            return False, 0.0, 0.0, 0.0

        data = list(self.buf)
        med = statistics.median(data)
        abs_dev = [abs(x - med) for x in data]
        mad = statistics.median(abs_dev)

        # 1.4826 是把 MAD 近似映射到“标准差尺度”的常见系数
        scale = max(1.4826 * mad, self.eps)
        rz = (err_value - med) / scale
        return True, rz, med, mad


def main():
    base = os.environ.get("CARVE_URL", "http://carve:8080").rstrip("/")
    target = os.environ.get("TARGET", "")
    model_name = os.environ.get("MODEL_NAME", "")
    metrics_port = int(os.environ.get("METRICS_PORT", "9092"))

    rz_window = int(os.environ.get("RZ_WINDOW", "120"))
    rz_min_points = int(os.environ.get("RZ_MIN_POINTS", "30"))
    rz_eps = float(os.environ.get("RZ_EPS", "1e-6"))

    if not target:
        print("TARGET env required", file=sys.stderr)
        sys.exit(1)
    display_model = model_name or target

    # 1) Download model from Carve
    model_path = "model.pt"
    try:
        params = {"target": target} if not model_name else {"name": model_name}
        r = requests.get(f"{base}/model/download", params=params, timeout=30)
        r.raise_for_status()
        with open(model_path, "wb") as f:
            f.write(r.content)
    except requests.RequestException as e:
        print(f"model download failed: {e}", file=sys.stderr)
        MODEL_LOADED.labels(target=target, model=display_model).set(0)
        sys.exit(1)

    model, scaler_min, scaler_scale, seq_len = load_checkpoint(model_path)
    MODEL_LOADED.labels(target=target, model=display_model).set(1)

    infer = StreamInference(model, scaler_min, scaler_scale, seq_len)
    rz_calc = RobustZScore(window_size=rz_window, min_points=rz_min_points, eps=rz_eps)
    # 2) WebSocket URL: CARVE_URL -> ws://host:port/alerting/stream?target=...
    parsed = urllib.parse.urlparse(base)
    host = parsed.hostname or "carve"
    port = parsed.port or (8080 if parsed.scheme != "https" else 443)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    ws_url = f"{scheme}://{host}:{port}/alerting/stream?target={urllib.parse.quote(target)}"

    # 3) Start Prometheus HTTP
    start_http_server(metrics_port)
    print(f"metrics on :{metrics_port}/metrics")

    # 4) Connect and loop (with reconnect)
    backoff = 1.0
    max_backoff = 60.0
    while True:
        try:
            ws = create_connection(ws_url)
            backoff = 1.0
            print("websocket connected")
            while True:
                raw = ws.recv()
                print(f"[alerter] ws recv len={len(raw)}", flush=True)
                rows = json.loads(raw)
                n = len(rows) if isinstance(rows, list) else -1
                print(f"[alerter] parsed rows={n}", flush=True)
                if not rows:
                    continue
                # Sort by ts; use (ts, value) as series; same ts -> keep first value
                bucket = {}
                for r in rows:
                    ts_ms = int(r.get("ts") or 0)
                    value = float(r.get("value", 0))
                    bucket[ts_ms] = bucket.get(ts_ms, 0.0) + value

                for ts_ms in sorted(bucket.keys()):
                    infer.push(ts_ms, bucket[ts_ms])

                err, reason = infer.step()
                if err is not None:
                    # 原始误差保留
                    PREDICTION_ERROR.labels(target=target, model=display_model).set(err)
                    LAST_UPDATE_TS.labels(target=target, model=display_model).set(time.time())

                    # 鲁棒标准分
                    ready, rz, med, mad = rz_calc.update(err)
                    ROBUST_READY.labels(target=target, model=display_model).set(1 if ready else 0)

                    if ready:
                        ROBUST_ZSCORE.labels(target=target, model=display_model).set(rz)
                        ERROR_MEDIAN.labels(target=target, model=display_model).set(med)
                        ERROR_MAD.labels(target=target, model=display_model).set(mad)
                    else:
                        ROBUST_ZSCORE.labels(target=target, model=display_model).set(0.0)
                        ERROR_MEDIAN.labels(target=target, model=display_model).set(0.0)
                        ERROR_MAD.labels(target=target, model=display_model).set(0.0)
        except Exception as e:
            print(f"ws error: {e}", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

if __name__ == "__main__":
    main()