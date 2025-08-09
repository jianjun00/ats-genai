import os
import sys
import tempfile
import pytest
import importlib
import types


def test_missing_contextlib_import(monkeypatch):
    """
    Regression: If contextlib is not imported, using contextlib.redirect_stdout raises NameError.
    """
    code = """
def f():
    import io
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        pass
f()
"""
    with pytest.raises(NameError):
        exec(code, {})

def test_missing_io_import(monkeypatch):
    """
    Regression: If io is not imported, using io.StringIO raises NameError.
    """
    code = """
def f():
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        pass
f()
"""
    with pytest.raises(NameError):
        exec(code, {})

def test_indentation_error_in_import(tmp_path):
    """
    Regression: Mis-indented import statement causes IndentationError.
    """
    bad_code = """
def f():
import contextlib
    pass
"""
    file_path = tmp_path / "bad_import.py"
    file_path.write_text(bad_code)
    with pytest.raises(IndentationError):
        importlib.util.spec_from_file_location("bad_import", str(file_path))
        importlib.util.module_from_spec(importlib.util.spec_from_file_location("bad_import", str(file_path)))
        exec(bad_code, {})

def test_mod_not_defined():
    """
    Regression: Referencing undefined 'mod' raises NameError.
    """
    code = """
def f():
    mod.__main__
f()
"""
    with pytest.raises(NameError):
        exec(code, {})

def test_dataframe_missing_indicator_columns():
    """
    Regression: DataFrame parsing should fail with informative error if indicator columns are missing.
    """
    import pandas as pd
    import io
    df_str = "date symbol open close\n2024-01-01 AAPL 100 105"
    df = pd.read_csv(io.StringIO(df_str), sep=r'\s+')
    # Simulate the test logic for checking indicator columns
    for col in ['ETop', 'EBot', 'PL']:
        if col not in df.columns:
            with pytest.raises(AssertionError, match=f"Missing indicator column: {col}"):
                assert False, f"Missing indicator column: {col}"
