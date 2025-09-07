"""
Canonical training dataset path generation functions.

This module provides the single source of truth for all training dataset
file paths, directory structures, and naming conventions.

IMPORTANT: This is the ONLY place where training dataset paths are defined.
All other code must use these functions - NO DUPLICATE LOGIC.
"""

import os
from pathlib import Path
from typing import List, Optional
from datetime import datetime


class TrainingDatasetPaths:
    """
    Canonical training dataset path generator.

    This is the SINGLE SOURCE OF TRUTH for training dataset paths.
    All training data generation, loading, and management must use these functions.
    """

    # Base directory for all training datasets
    BASE_TRAINING_DATA_DIR = "/mnt/d/ats-data/training_data"

    # Supported timeframes in order
    TIMEFRAMES = ["5m", "15m", "1h", "1d", "1w"]

    @classmethod
    def get_base_dataset_dir(cls, run_id: str) -> str:
        """
        Get base directory for a training dataset run.

        Args:
            run_id: Unique identifier for the training run

        Returns:
            Base directory path: /mnt/d/ats-data/training_data/{run_id}/
        """
        return os.path.join(cls.BASE_TRAINING_DATA_DIR, run_id)

    @classmethod
    def get_symbol_dataset_dir(cls, run_id: str, symbol: str, start_date: str, end_date: str) -> str:
        """
        Get directory for a specific symbol's training dataset.

        Per PRD/DRD QR4: Directory structure is {run_id}/{SYMBOL}_{START}_{END}/

        Args:
            run_id: Unique identifier for the training run
            symbol: Stock symbol (e.g., "AAPL")
            start_date: Start date in YYYYMMDD_HHMMSS format
            end_date: End date in YYYYMMDD_HHMMSS format

        Returns:
            Symbol dataset directory path
        """
        symbol_dir_name = f"{symbol}_{start_date}_{end_date}"
        return os.path.join(cls.get_base_dataset_dir(run_id), symbol_dir_name)

    @classmethod
    def get_timeframe_dir(cls, run_id: str, symbol: str, start_date: str, end_date: str, timeframe: str) -> str:
        """
        Get directory for a specific timeframe within a symbol's dataset.

        Per PRD/DRD QR4: {run_id}/{SYMBOL}_{START}_{END}/{timeframe}/

        Args:
            run_id: Training run identifier
            symbol: Stock symbol
            start_date: Start date in YYYYMMDD_HHMMSS format
            end_date: End date in YYYYMMDD_HHMMSS format
            timeframe: Timeframe (5m, 15m, 1h, 1d, 1w)

        Returns:
            Timeframe directory path
        """
        if timeframe not in cls.TIMEFRAMES:
            raise ValueError(f"Invalid timeframe {timeframe}. Must be one of {cls.TIMEFRAMES}")

        symbol_dir = cls.get_symbol_dataset_dir(run_id, symbol, start_date, end_date)
        return os.path.join(symbol_dir, timeframe)

    @classmethod
    def get_arrayrecord_filepath(cls, run_id: str, symbol: str, start_date: str, end_date: str, timeframe: str) -> str:
        """
        Get full filepath for an ArrayRecord file.

        Per PRD/DRD QR4: {run_id}/{SYMBOL}_{START}_{END}/{timeframe}/{symbol}.arrayrecord

        Args:
            run_id: Training run identifier
            symbol: Stock symbol
            start_date: Start date in YYYYMMDD_HHMMSS format
            end_date: End date in YYYYMMDD_HHMMSS format
            timeframe: Timeframe (5m, 15m, 1h, 1d, 1w)

        Returns:
            Full ArrayRecord file path
        """
        timeframe_dir = cls.get_timeframe_dir(run_id, symbol, start_date, end_date, timeframe)
        filename = f"{symbol.lower()}.arrayrecord"  # PRD/DRD requires lowercase symbol.arrayrecord
        return os.path.join(timeframe_dir, filename)

    @classmethod
    def create_directory_structure(cls, run_id: str, symbol: str, start_date: str, end_date: str, timeframes: Optional[List[str]] = None) -> List[str]:
        """
        Create complete directory structure for a training dataset.

        Args:
            run_id: Training run identifier
            symbol: Stock symbol
            start_date: Start date in YYYYMMDD_HHMMSS format
            end_date: End date in YYYYMMDD_HHMMSS format
            timeframes: List of timeframes to create (defaults to all)

        Returns:
            List of created directory paths
        """
        if timeframes is None:
            timeframes = cls.TIMEFRAMES

        created_dirs = []

        # Create base dataset directory
        base_dir = cls.get_base_dataset_dir(run_id)
        os.makedirs(base_dir, exist_ok=True)
        created_dirs.append(base_dir)

        # Create symbol directory
        symbol_dir = cls.get_symbol_dataset_dir(run_id, symbol, start_date, end_date)
        os.makedirs(symbol_dir, exist_ok=True)
        created_dirs.append(symbol_dir)

        # Create timeframe directories
        for timeframe in timeframes:
            timeframe_dir = cls.get_timeframe_dir(run_id, symbol, start_date, end_date, timeframe)
            os.makedirs(timeframe_dir, exist_ok=True)
            created_dirs.append(timeframe_dir)

        return created_dirs

    @classmethod
    def format_date_for_path(cls, date: datetime) -> str:
        """
        Format datetime for use in training dataset paths.

        Per PRD/DRD: YYYYMMDD_HHMMSS format

        Args:
            date: Datetime to format

        Returns:
            Formatted date string: YYYYMMDD_HHMMSS
        """
        return date.strftime("%Y%m%d_%H%M%S")

    @classmethod
    def get_all_arrayrecord_files(cls, run_id: str, symbol: str, start_date: str, end_date: str, timeframes: Optional[List[str]] = None) -> dict:
        """
        Get all ArrayRecord file paths for a symbol's training dataset.

        Args:
            run_id: Training run identifier
            symbol: Stock symbol
            start_date: Start date in YYYYMMDD_HHMMSS format
            end_date: End date in YYYYMMDD_HHMMSS format
            timeframes: List of timeframes (defaults to all)

        Returns:
            Dictionary mapping timeframe -> ArrayRecord file path
        """
        if timeframes is None:
            timeframes = cls.TIMEFRAMES

        files = {}
        for timeframe in timeframes:
            files[timeframe] = cls.get_arrayrecord_filepath(run_id, symbol, start_date, end_date, timeframe)

        return files


# Convenience functions for backward compatibility
def get_training_dataset_base_dir(run_id: str) -> str:
    """Get base training dataset directory - DEPRECATED: Use TrainingDatasetPaths.get_base_dataset_dir()"""
    return TrainingDatasetPaths.get_base_dataset_dir(run_id)


def get_training_dataset_arrayrecord_path(run_id: str, symbol: str, start_date: str, end_date: str, timeframe: str) -> str:
    """Get ArrayRecord file path - DEPRECATED: Use TrainingDatasetPaths.get_arrayrecord_filepath()"""
    return TrainingDatasetPaths.get_arrayrecord_filepath(run_id, symbol, start_date, end_date, timeframe)