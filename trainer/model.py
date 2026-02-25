"""
Simple LSTM for univariate time series prediction (next-step value).
Input: (batch, seq_len, 1), Output: (batch, 1).
"""

import torch
import torch.nn as nn


class LSTMPredictor(nn.Module):
    def __init__(self, input_size=1, hidden_size=64, num_layers=1):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        # x: (batch, seq_len, 1)
        out, _ = self.lstm(x)
        last = out[:, -1, :]
        return self.fc(last)