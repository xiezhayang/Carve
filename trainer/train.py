#!/usr/bin/env python3
"""
LSTM training pod: fetch CSV from Carve, train, upload model.pt back to Carve.
Uses ts for hour/dow cyclical features (value + hour_sin, hour_cos, dow_sin, dow_cos).
Env: CARVE_URL, CSV_FILENAME, MODEL_NAME (optional).
"""

import os
import sys
import io
import requests
import pandas as pd
import numpy as np
import torch
from torch.utils.data import TensorDataset, DataLoader
from sklearn.preprocessing import MinMaxScaler

from model import LSTMPredictor


def main():
    base = os.environ.get("CARVE_URL", "http://carve:8080").rstrip("/")
    filename = os.environ.get("CSV_FILENAME", "metrics.csv")
    model_name = os.environ.get("MODEL_NAME", "lstm-default")
    print(f"[trainer] start CARVE_URL={base} CSV_FILENAME={filename} MODEL_NAME={model_name}")

    # 1) Fetch CSV from Carve
    try:
        url = f"{base}/export"
        print(f"[trainer] downloading CSV: GET {url}?filename={filename}")
        r = requests.get(f"{base}/export", params={"filename": filename}, timeout=30)
        r.raise_for_status()
        body_len = len(r.content)
        print(f"[trainer] download ok status={r.status_code} size={body_len} bytes")
    except requests.RequestException as e:
        print(f"fetch csv failed: {e}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(io.StringIO(r.text), comment="#")
    print(f"[trainer] csv parsed rows={len(df)} columns={list(df.columns)}")
    if "value" not in df.columns:
        print("csv missing 'value' column", file=sys.stderr)
        sys.exit(1)
    if "ts" not in df.columns:
        print("csv missing 'ts' column", file=sys.stderr)
        sys.exit(1)

    # 只保留 ts/value，转数值，清理脏数据
    df = df[["ts", "value"]].copy()
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["ts", "value"])

    # 同一 ts 多行（不同 method/target）聚合为一个值，和告警侧保持一致
    df["ts"] = df["ts"].astype(np.int64)
    df = df.groupby("ts", as_index=False)["value"].sum().sort_values("ts").reset_index(drop=True)

    values = df["value"].astype(np.float32).values.reshape(-1, 1)
    if len(values) < 20:
        print("not enough rows for training", file=sys.stderr)
        sys.exit(1)

    # ts(ms) -> minute/second cyclical features
    ts_ms = df["ts"].to_numpy(dtype=np.int64)
    minute = ((ts_ms // 60000) % 60).astype(np.float32).reshape(-1, 1)
    second = ((ts_ms // 1000) % 60).astype(np.float32).reshape(-1, 1)

    min_sin = np.sin(2 * np.pi * minute / 60.0).astype(np.float32)
    min_cos = np.cos(2 * np.pi * minute / 60.0).astype(np.float32)
    sec_sin = np.sin(2 * np.pi * second / 60.0).astype(np.float32)
    sec_cos = np.cos(2 * np.pi * second / 60.0).astype(np.float32)

    seq_len = 10
    scaler = MinMaxScaler()
    scaled_value = scaler.fit_transform(values)
    features = np.hstack([scaled_value, min_sin, min_cos, sec_sin, sec_cos]).astype(np.float32)  # (n, 5)
    X = np.array([features[i : i + seq_len] for i in range(len(features) - seq_len)], dtype=np.float32)
    y = scaled_value[seq_len:]
    n_seq = len(X)
    print(f"[trainer] built sequences seq_len={seq_len} count={n_seq}")
    X = torch.tensor(X, dtype=torch.float32)
    y = torch.tensor(y, dtype=torch.float32)
    ds = TensorDataset(X, y)
    loader = DataLoader(ds, batch_size=32, shuffle=True)

    # 3) Train (input_size=5: value + hour_sin, hour_cos, dow_sin, dow_cos)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = LSTMPredictor(input_size=5, hidden_size=64, num_layers=1).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3)
    criterion = torch.nn.MSELoss()
    epochs = 30

    for epoch in range(epochs):
        model.train()
        loss_sum = 0.0
        n = 0
        for bx, by in loader:
            bx, by = bx.to(device), by.to(device)
            opt.zero_grad()
            out = model(bx)
            loss = criterion(out, by)
            loss.backward()
            opt.step()
            loss_sum += loss.item()
            n += 1
        if (epoch + 1) % 10 == 0:
            print(f"epoch {epoch+1}/{epochs} loss={loss_sum/max(n,1):.6f}")

    # 4) Save and upload to Carve
    path = "model.pt"
    torch.save(
        {
            "state_dict": model.state_dict(),
            "scaler_min": scaler.data_min_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
            "input_size": 5,
            "seq_len": seq_len,
        },
        path,
    )
    size_bytes = os.path.getsize(path)
    print(f"[trainer] saved model.pt size={size_bytes} bytes")

    try:
        upload_url = f"{base}/model/upload"
        print(f"[trainer] uploading to {upload_url} name={model_name}")
        with open(path, "rb") as f:
            resp = requests.post(
                f"{base}/model/upload",
                files={"model": ("model.pt", f, "application/octet-stream")},
                data={"name": model_name},
                timeout=60,
            )
        if resp.status_code == 200:
            print("model uploaded to Carve")
        else:
            print(f"upload returned {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
    except requests.RequestException as e:
        print(f"upload failed (carve may not have /model/upload yet): {e}", file=sys.stderr)
    print("[trainer] done")
    sys.exit(0)


if __name__ == "__main__":
    main()