import subprocess
import os
from pathlib import Path

def test_environment_logging_config_integration():
    project_root = Path(__file__).parent.parent.parent
    script_path = project_root / "scripts" / "print_env_logging.py"
    gin_config_path = project_root / "config" / "logging_test.gin"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root / "src")
    result = subprocess.run(
        [
            "python3",
            str(script_path),
            f"--gin_config={gin_config_path}"
        ],
        capture_output=True,
        text=True,
        env=env
    )
    assert "WARNING" in result.stdout, f"Expected 'WARNING' in stdout, got: {result.stdout}"
    assert "%(message)s" in result.stdout, f"Expected '%(message)s' in stdout, got: {result.stdout}"
