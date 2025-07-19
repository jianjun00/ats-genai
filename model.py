import torch
import torch.nn as nn
import numpy as np
from typing import Dict

class SimpleLSTM(nn.Module):
    def __init__(self, input_size=4, hidden_size=16, num_layers=1, output_size=1):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)
    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out

def load_model():
    # For demo: randomly initialized model. In production, load from file.
    model = SimpleLSTM()
    model.eval()
    return model

def predict_return(model, signals: Dict):
    # signals: dict of latest signals (single point)
    x = np.array([[signals['close'], signals['sma_5'], signals['rsi_14'], signals['volume']]], dtype=np.float32)
    x = torch.tensor(x).unsqueeze(0)  # shape: (1, 1, 4)
    with torch.no_grad():
        pred = model(x)
    return float(pred.item())
