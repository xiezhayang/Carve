#!/usr/bin/env python3
"""
LSTM training pod: fetch CSV from Carve, train, upload model.pt back to Carve.
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

    # 1) Fetch CSV from Carve
    try:
        r = requests.get(f"{base}/export", params={"filename": filename}, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"fetch csv failed: {e}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(io.StringIO(r.text))
    if "value" not in df.columns:
        print("csv missing 'value' column", file=sys.stderr)
        sys.exit(1)

    values = df["value"].astype(float).ffill().values.reshape(-1, 1)
    if len(values) < 20:
        print("not enough rows for training", file=sys.stderr)
        sys.exit(1)

    # 2) Build sequences (seq_len past -> next value)
    seq_len = 10
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(values)
    X = np.array([scaled[i : i + seq_len] for i in range(len(scaled) - seq_len)])
    y = scaled[seq_len:]

    X = torch.tensor(X, dtype=torch.float32)
    y = torch.tensor(y, dtype=torch.float32)
    ds = TensorDataset(X, y)
    loader = DataLoader(ds, batch_size=32, shuffle=True)

    # 3) Train
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = LSTMPredictor(input_size=1, hidden_size=64, num_layers=1).to(device)
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
        },
        path,
    )

    try:
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

    sys.exit(0)


if __name__ == "__main__":
    main()