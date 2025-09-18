import os
import sys
import subprocess
from pathlib import Path

import torch

def test_train_from_saved_pt_runs_and_writes_checkpoint(tmp_path):
    # Prepare tiny dataset
    batch_size = 8
    lag_steps = 6
    lead_steps = 2
    num_instruments = 3
    num_features = 4
    X = torch.randn(batch_size, lag_steps, num_instruments, num_features)
    y = torch.randn(batch_size, lead_steps, num_instruments, 1)

    data_path = tmp_path / "train_data.pt"
    torch.save({"X": X, "y": y}, data_path)

    # Paths
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "src" / "modeling" / "pytorch_multi_instrument_train.py"
    checkpoint_path = tmp_path / "model_checkpoint.pt"

    # Env with PYTHONPATH=src
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    if "PYTHONPATH" in env and env["PYTHONPATH"]:
        env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = src_path

    # Run one epoch to be quick
    cmd = [
        sys.executable,
        str(script_path),
        "--data-path",
        str(data_path),
        "--epochs",
        "1",
        "--batch-size",
        "4",
        "--val-ratio",
        "0.25",
        "--checkpoint",
        str(checkpoint_path),
    ]
    result = subprocess.run(cmd, cwd=str(repo_root), env=env, capture_output=True, text=True)

    # Debug if failed
    assert result.returncode == 0, f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"

    # Check checkpoint exists and is non-empty
    assert checkpoint_path.exists(), "Checkpoint file was not created"
    assert checkpoint_path.stat().st_size > 0, "Checkpoint file is empty"
