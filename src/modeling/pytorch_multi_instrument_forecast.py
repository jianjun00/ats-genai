"""
PyTorch Example: Multi-Instrument, Multi-Step Time Series Forecasting
- Input: [batch, step, instrument, features]
- Output: [batch, step, instrument, 1] (price)
- Includes: batching, storage, model skeleton
"""
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
import numpy as np

# --- Dummy Data Preparation ---
batch_size = 16
lag_steps = 30
lead_steps = 7
num_instruments = 5
num_features = 8  # e.g., open, high, low, close, etop, ebot, pldot, etc.

# Generate random data for demo
X = torch.randn(batch_size, lag_steps, num_instruments, num_features)
y = torch.randn(batch_size, lead_steps, num_instruments, 1)

# Save to disk for fast reload
torch.save({'X': X, 'y': y}, 'train_data.pt')

# --- Dataset and DataLoader ---
class MultiInstrumentDataset(Dataset):
    def __init__(self, X, y):
        self.X = X
        self.y = y
    def __len__(self):
        return self.X.shape[0]
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

dataset = MultiInstrumentDataset(X, y)
dataloader = DataLoader(dataset, batch_size=4, shuffle=True)

# --- Model Skeleton ---
class MultiInstrumentTransformer(nn.Module):
    def __init__(self, num_instruments, num_features, d_model=64, nhead=4, num_layers=2):
        super().__init__()
        self.num_instruments = num_instruments
        self.input_proj = nn.Linear(num_features, d_model)
        self.inst_embed = nn.Embedding(num_instruments, d_model)
        self.transformer = nn.Transformer(
            d_model=d_model,
            nhead=nhead,
            num_encoder_layers=num_layers,
            num_decoder_layers=num_layers,
            batch_first=True
        )
        self.output_proj = nn.Linear(d_model, 1)
    def forward(self, x):
        # x: [batch, step, instrument, features]
        b, s, n, f = x.shape
        x = self.input_proj(x)  # [b, s, n, d_model]
        # Add instrument embedding
        inst_idx = torch.arange(n, device=x.device).unsqueeze(0).unsqueeze(0).expand(b, s, n)
        inst_emb = self.inst_embed(inst_idx)  # [b, s, n, d_model]
        x = x + inst_emb
        # Merge instrument into step: [b, s*n, d_model]
        x = x.view(b, s * n, -1)
        # For demonstration, use x as both src and tgt
        out = self.transformer(x, x)  # [b, s*n, d_model]
        out = self.output_proj(out)   # [b, s*n, 1]
        out = out.view(b, s, n, 1)   # [b, s, n, 1]
        return out

# --- Training Loop Skeleton ---
model = MultiInstrumentTransformer(num_instruments, num_features)
optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
loss_fn = nn.MSELoss()

for epoch in range(2):  # demo epochs
    for xb, yb in dataloader:
        pred = model(xb)
        loss = loss_fn(pred, yb)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
    print(f"Epoch {epoch+1}, Loss: {loss.item():.4f}")

print("Demo training complete. Model and data structure ready for real data integration.")
