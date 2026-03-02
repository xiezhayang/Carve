"""
Load LSTM checkpoint, build 5-dim features (value + hour/dow sin/cos), sliding window, predict next value, return error.
Matches trainer: ts -> hour/dow cyclical; value with checkpoint scaler.
"""

import numpy as np
import torch

from model import LSTMPredictor


def _ts_to_hour_dow(ts_ms: int):
    """ts_ms: milliseconds since epoch. Returns (hour_sin, hour_cos, dow_sin, dow_cos)."""
    s = ts_ms / 1000.0
    hour = int(s // 3600 % 24)
    days_since_epoch = int(s // (3600 * 24))
    dow = (days_since_epoch + 3) % 7  # 1970-01-01 是周四 -> 3，周一 -> 0
    hour_sin = np.sin(2 * np.pi * hour / 24).astype(np.float32)
    hour_cos = np.cos(2 * np.pi * hour / 24).astype(np.float32)
    dow_sin = np.sin(2 * np.pi * dow / 7).astype(np.float32)
    dow_cos = np.cos(2 * np.pi * dow / 7).astype(np.float32)
    return hour_sin, hour_cos, dow_sin, dow_cos


def load_checkpoint(path: str):
    """Load model.pt (from trainer). Returns (model, scaler_min, scaler_scale, seq_len)."""
    ckpt = torch.load(path, map_location="cpu", weights_only=True)
    scaler_min = np.array(ckpt["scaler_min"], dtype=np.float64).reshape(1, -1)
    scaler_scale = np.array(ckpt["scaler_scale"], dtype=np.float64).reshape(1, -1)
    input_size = ckpt["input_size"]
    seq_len = ckpt["seq_len"]
    model = LSTMPredictor(input_size=input_size, hidden_size=64, num_layers=1)
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    return model, scaler_min, scaler_scale, seq_len


class StreamInference:
    """Single time series: (ts, value) buffer, 5-dim features, sliding window, predict & error."""

    def __init__(self, model, scaler_min, scaler_scale, seq_len, device="cpu"):
        self.model = model.to(device)
        self.scaler_min = scaler_min
        self.scaler_scale = scaler_scale
        self.seq_len = seq_len
        self.device = device
        # buffer: list of (ts_ms, value) sorted by ts
        self._buffer = []

    def _scale_value(self, value: float):
        v = np.array([[value]], dtype=np.float64)
        return ((v - self.scaler_min[:, :1]) / (self.scaler_scale[:, :1] + 1e-9)).astype(np.float32).item()

    def push(self, ts_ms: int, value: float):
        self._buffer.append((ts_ms, value))
        self._buffer.sort(key=lambda x: x[0])
        # keep last N enough for seq_len + 1 (to have a "next" value)
        max_len = self.seq_len + 50
        if len(self._buffer) > max_len:
            self._buffer = self._buffer[-max_len:]

    def step(self):
        """
        If we have at least seq_len+1 points, build last full window, predict next, compare to actual next; return error.
        Returns (error, None) or (None, "need_more").
        error: absolute difference |actual - predicted| in original value space.
        """
        if len(self._buffer) < self.seq_len + 1:
            return None, "need_more"
        # use last seq_len+1 points: [0:seq_len] -> predict; actual next = [seq_len]
        start = len(self._buffer) - (self.seq_len + 1)
        window = self._buffer[start : start + self.seq_len + 1]
        features = []
        for ts_ms, val in window:
            vs = self._scale_value(val)
            h_s, h_c, d_s, d_c = _ts_to_hour_dow(ts_ms)
            features.append([vs, h_s, h_c, d_s, d_c])
        arr = np.array(features, dtype=np.float32)
        inp = arr[: self.seq_len]  # (seq_len, 5)
        actual_next_raw = window[self.seq_len][1]
        scaler_min_0 = self.scaler_min[0, 0]
        scaler_scale_0 = self.scaler_scale[0, 0]

        with torch.no_grad():
            x = torch.tensor(inp, dtype=torch.float32).unsqueeze(0).to(self.device)  # (1, seq_len, 5)
            pred_scaled = self.model(x).squeeze().item()
        pred_raw = scaler_min_0 + pred_scaled * (scaler_scale_0 + 1e-9)
        error = abs(actual_next_raw - pred_raw)
        return error, None