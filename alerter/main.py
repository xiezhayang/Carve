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


def main():
    base = os.environ.get("CARVE_URL", "http://carve:8080").rstrip("/")
    target = os.environ.get("TARGET", "")
    model_name = os.environ.get("MODEL_NAME", "")
    metrics_port = int(os.environ.get("METRICS_PORT", "9092"))

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
                rows = json.loads(raw)
                if not rows:
                    continue
                # Sort by ts; use (ts, value) as series; same ts -> keep first value
                seen_ts = set()
                for r in rows:
                    ts_ms = r.get("ts") or 0
                    value = float(r.get("value", 0))
                    if ts_ms in seen_ts:
                        continue
                    seen_ts.add(ts_ms)
                    infer.push(ts_ms, value)
                err, reason = infer.step()
                if err is not None:
                    PREDICTION_ERROR.labels(target=target, model=display_model).set(err)
                    LAST_UPDATE_TS.labels(target=target, model=display_model).set(time.time())
        except Exception as e:
            print(f"ws error: {e}", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)