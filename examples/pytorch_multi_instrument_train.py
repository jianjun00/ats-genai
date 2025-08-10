"""
PyTorch Multi-Instrument Forecasting: Training/Validation Pipeline
- Uses DataLoader and model from pytorch_multi_instrument_forecast.py
- Includes masking for missing data, validation split, metrics, and checkpointing
- Assumes real-data batching via your runner framework (see TODO)
"""
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader, random_split
import numpy as np
import os

from examples.pytorch_multi_instrument_forecast import MultiInstrumentTransformer

# --- Config ---
BATCH_SIZE = 32
EPOCHS = 20
VAL_RATIO = 0.2
CHECKPOINT_PATH = 'model_checkpoint.pt'

# --- TODO: Replace with your runner framework data loader ---
def get_real_data():
    # X: [batch, step, instrument, features], y: [batch, step, instrument, 1]
    # TODO: Replace this with your runner-based batching
    raise NotImplementedError("Replace with runner framework batching!")

# --- Dataset and DataLoader ---
class MaskedMultiInstrumentDataset(Dataset):
    def __init__(self, X, y, mask=None):
        self.X = X
        self.y = y
        self.mask = mask if mask is not None else ~torch.isnan(y)
    def __len__(self):
        return self.X.shape[0]
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx], self.mask[idx]

def masked_mse_loss(pred, target, mask):
    diff = (pred - target) * mask
    return (diff ** 2).sum() / mask.sum()

# --- Main Training Pipeline ---
def main():
    # X, y = get_real_data()  # Uncomment when integrated with runner
    # For demo, use random data (remove in production)
    batch_size, lag_steps, num_instruments, num_features = 128, 30, 5, 8
    lead_steps = 7
    X = torch.randn(batch_size, lag_steps, num_instruments, num_features)
    y = torch.randn(batch_size, lead_steps, num_instruments, 1)
    mask = ~torch.isnan(y)

    dataset = MaskedMultiInstrumentDataset(X, y, mask)
    val_len = int(VAL_RATIO * len(dataset))
    train_len = len(dataset) - val_len
    train_set, val_set = random_split(dataset, [train_len, val_len])
    train_loader = DataLoader(train_set, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_set, batch_size=BATCH_SIZE)

    model = MultiInstrumentTransformer(num_instruments, num_features)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    best_val_loss = float('inf')
    for epoch in range(EPOCHS):
        model.train()
        for xb, yb, mb in train_loader:
            pred = model(xb)
            loss = masked_mse_loss(pred, yb, mb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
        model.eval()
        val_loss = 0
        n = 0
        with torch.no_grad():
            for xb, yb, mb in val_loader:
                pred = model(xb)
                loss = masked_mse_loss(pred, yb, mb)
                val_loss += loss.item() * xb.size(0)
                n += xb.size(0)
        val_loss /= n
        print(f"Epoch {epoch+1}, Val Loss: {val_loss:.4f}")
        # Checkpoint
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            torch.save(model.state_dict(), CHECKPOINT_PATH)
            print(f"Checkpoint saved at epoch {epoch+1}")
    print("Training complete. Best val loss:", best_val_loss)

if __name__ == "__main__":
    main()
