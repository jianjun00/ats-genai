#!/usr/bin/env python3
"""
File Operations - Unified file I/O utilities for data processing

Consolidates file handling patterns from data processing, backfill, and vendor service files.
Provides standardized file operations, path management, and data persistence utilities.

USAGE:
======

from shared.utils.file_operations import (
    ensure_directory_exists,
    safe_write_json,
    safe_read_json,
    create_backup_file,
    cleanup_temp_files
)

# Ensure directories exist before writing
ensure_directory_exists('/data/checkpoints/')

# Safe JSON operations with error handling
data = safe_read_json('/data/config.json')
safe_write_json(data, '/data/output.json')

# Create timestamped backups
backup_path = create_backup_file('/data/important.json')
"""

import os
import json
import shutil
import tempfile
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import gzip
import csv

logger = logging.getLogger(__name__)

# =============================================================================
# DIRECTORY AND PATH OPERATIONS
# =============================================================================

def ensure_directory_exists(path: Union[str, Path]) -> Path:
    """
    Ensure directory exists, creating it if necessary.

    Consolidates directory creation logic from multiple vendor services.

    Args:
        path: Directory path to ensure exists

    Returns:
        Path object for the directory

    Examples:
        >>> ensure_directory_exists('/data/checkpoints/')
        PosixPath('/data/checkpoints')
        >>> ensure_directory_exists('/mnt/d/ats-data/temp/')
        PosixPath('/mnt/d/ats-data/temp')
    """
    path_obj = Path(path)

    try:
        path_obj.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {path_obj}")
        return path_obj
    except Exception as e:
        logger.error(f"Failed to create directory {path_obj}: {e}")
        raise

def get_safe_filename(filename: str, max_length: int = 255) -> str:
    """
    Create safe filename by removing invalid characters.

    Args:
        filename: Original filename
        max_length: Maximum filename length

    Returns:
        Safe filename string
    """
    # Remove invalid characters
    invalid_chars = '<>:"/\\|?*'
    safe_name = ''.join(c for c in filename if c not in invalid_chars)

    # Replace spaces with underscores
    safe_name = safe_name.replace(' ', '_')

    # Truncate if too long
    if len(safe_name) > max_length:
        name_part, ext = os.path.splitext(safe_name)
        safe_name = name_part[:max_length-len(ext)] + ext

    return safe_name

def create_timestamped_path(base_path: Union[str, Path], prefix: str = "") -> Path:
    """
    Create timestamped path for temporary or backup files.

    Args:
        base_path: Base directory path
        prefix: Optional prefix for filename

    Returns:
        Path with timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}{timestamp}" if prefix else timestamp
    return Path(base_path) / filename

# =============================================================================
# JSON FILE OPERATIONS
# =============================================================================

def safe_write_json(
    data: Any,
    filepath: Union[str, Path],
    indent: int = 2,
    create_backup: bool = False
) -> bool:
    """
    Safely write JSON data to file with error handling.

    Args:
        data: Data to write as JSON
        filepath: File path to write to
        indent: JSON indentation
        create_backup: Create backup of existing file

    Returns:
        True if successful, False otherwise
    """
    filepath = Path(filepath)

    try:
        # Create backup if requested and file exists
        if create_backup and filepath.exists():
            backup_path = create_backup_file(filepath)
            logger.debug(f"Created backup at {backup_path}")

        # Ensure parent directory exists
        ensure_directory_exists(filepath.parent)

        # Write to temporary file first, then move (atomic operation)
        temp_path = filepath.with_suffix(filepath.suffix + '.tmp')

        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, default=str)

        # Move temp file to final location
        temp_path.rename(filepath)
        logger.debug(f"Successfully wrote JSON to {filepath}")
        return True

    except Exception as e:
        logger.error(f"Failed to write JSON to {filepath}: {e}")
        # Clean up temp file if it exists
        temp_path = filepath.with_suffix(filepath.suffix + '.tmp')
        if temp_path.exists():
            temp_path.unlink()
        return False

def safe_read_json(
    filepath: Union[str, Path],
    default: Any = None
) -> Any:
    """
    Safely read JSON data from file with error handling.

    Args:
        filepath: File path to read from
        default: Default value if file doesn't exist or can't be read

    Returns:
        Parsed JSON data or default value
    """
    filepath = Path(filepath)

    try:
        if not filepath.exists():
            logger.debug(f"JSON file not found: {filepath}")
            return default

        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.debug(f"Successfully read JSON from {filepath}")
            return data

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filepath}: {e}")
        return default
    except Exception as e:
        logger.error(f"Failed to read JSON from {filepath}: {e}")
        return default

def safe_append_jsonl(
    data: Dict[str, Any],
    filepath: Union[str, Path]
) -> bool:
    """
    Safely append JSON line to JSONL file.

    Args:
        data: Data to append as JSON line
        filepath: File path to append to

    Returns:
        True if successful, False otherwise
    """
    filepath = Path(filepath)

    try:
        # Ensure parent directory exists
        ensure_directory_exists(filepath.parent)

        with open(filepath, 'a', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, default=str)
            f.write('\n')

        return True

    except Exception as e:
        logger.error(f"Failed to append JSONL to {filepath}: {e}")
        return False

# =============================================================================
# CSV FILE OPERATIONS
# =============================================================================

def safe_write_csv(
    data: List[Dict[str, Any]],
    filepath: Union[str, Path],
    fieldnames: Optional[List[str]] = None
) -> bool:
    """
    Safely write CSV data to file.

    Args:
        data: List of dictionaries to write
        filepath: File path to write to
        fieldnames: CSV column names (auto-detected if None)

    Returns:
        True if successful, False otherwise
    """
    if not data:
        logger.warning(f"No data to write to CSV: {filepath}")
        return False

    filepath = Path(filepath)

    try:
        # Auto-detect fieldnames if not provided
        if not fieldnames:
            fieldnames = list(data[0].keys())

        # Ensure parent directory exists
        ensure_directory_exists(filepath.parent)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logger.debug(f"Successfully wrote {len(data)} rows to CSV: {filepath}")
        return True

    except Exception as e:
        logger.error(f"Failed to write CSV to {filepath}: {e}")
        return False

def safe_read_csv(
    filepath: Union[str, Path],
    default: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Safely read CSV data from file.

    Args:
        filepath: File path to read from
        default: Default value if file can't be read

    Returns:
        List of dictionaries or default value
    """
    if default is None:
        default = []

    filepath = Path(filepath)

    try:
        if not filepath.exists():
            logger.debug(f"CSV file not found: {filepath}")
            return default

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)

        logger.debug(f"Successfully read {len(data)} rows from CSV: {filepath}")
        return data

    except Exception as e:
        logger.error(f"Failed to read CSV from {filepath}: {e}")
        return default

# =============================================================================
# BACKUP AND COMPRESSION OPERATIONS
# =============================================================================

def create_backup_file(
    filepath: Union[str, Path],
    backup_dir: Optional[Union[str, Path]] = None
) -> Optional[Path]:
    """
    Create timestamped backup of file.

    Args:
        filepath: Original file path
        backup_dir: Directory for backup (same dir as original if None)

    Returns:
        Path to backup file or None if failed
    """
    filepath = Path(filepath)

    if not filepath.exists():
        logger.warning(f"Cannot backup non-existent file: {filepath}")
        return None

    try:
        # Determine backup directory
        if backup_dir:
            backup_dir = Path(backup_dir)
            ensure_directory_exists(backup_dir)
        else:
            backup_dir = filepath.parent

        # Create timestamped backup filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{filepath.stem}_{timestamp}{filepath.suffix}"
        backup_path = backup_dir / backup_name

        # Copy file to backup location
        shutil.copy2(filepath, backup_path)
        logger.debug(f"Created backup: {filepath} -> {backup_path}")

        return backup_path

    except Exception as e:
        logger.error(f"Failed to create backup of {filepath}: {e}")
        return None

def compress_file(
    filepath: Union[str, Path],
    remove_original: bool = False
) -> Optional[Path]:
    """
    Compress file using gzip.

    Args:
        filepath: File path to compress
        remove_original: Remove original file after compression

    Returns:
        Path to compressed file or None if failed
    """
    filepath = Path(filepath)

    if not filepath.exists():
        logger.warning(f"Cannot compress non-existent file: {filepath}")
        return None

    compressed_path = filepath.with_suffix(filepath.suffix + '.gz')

    try:
        with open(filepath, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        if remove_original:
            filepath.unlink()
            logger.debug(f"Compressed and removed original: {filepath} -> {compressed_path}")
        else:
            logger.debug(f"Compressed file: {filepath} -> {compressed_path}")

        return compressed_path

    except Exception as e:
        logger.error(f"Failed to compress {filepath}: {e}")
        return None

def decompress_file(
    filepath: Union[str, Path],
    remove_compressed: bool = False
) -> Optional[Path]:
    """
    Decompress gzipped file.

    Args:
        filepath: Compressed file path
        remove_compressed: Remove compressed file after decompression

    Returns:
        Path to decompressed file or None if failed
    """
    filepath = Path(filepath)

    if not filepath.exists():
        logger.warning(f"Cannot decompress non-existent file: {filepath}")
        return None

    if not filepath.suffix == '.gz':
        logger.warning(f"File is not gzipped: {filepath}")
        return None

    decompressed_path = filepath.with_suffix('')

    try:
        with gzip.open(filepath, 'rb') as f_in:
            with open(decompressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        if remove_compressed:
            filepath.unlink()
            logger.debug(f"Decompressed and removed compressed: {filepath} -> {decompressed_path}")
        else:
            logger.debug(f"Decompressed file: {filepath} -> {decompressed_path}")

        return decompressed_path

    except Exception as e:
        logger.error(f"Failed to decompress {filepath}: {e}")
        return None

# =============================================================================
# TEMPORARY FILE OPERATIONS
# =============================================================================

def create_temp_file(
    suffix: str = '',
    prefix: str = 'ats_',
    dir: Optional[Union[str, Path]] = None
) -> tempfile.NamedTemporaryFile:
    """
    Create temporary file with ATS naming convention.

    Args:
        suffix: File suffix/extension
        prefix: File prefix
        dir: Directory for temp file

    Returns:
        NamedTemporaryFile object
    """
    if dir:
        dir = Path(dir)
        ensure_directory_exists(dir)

    return tempfile.NamedTemporaryFile(
        suffix=suffix,
        prefix=prefix,
        dir=dir,
        delete=False  # Manual cleanup for better error handling
    )

def cleanup_temp_files(
    temp_dir: Union[str, Path],
    prefix: str = 'ats_',
    older_than_hours: int = 24
) -> int:
    """
    Clean up old temporary files.

    Args:
        temp_dir: Directory containing temp files
        prefix: File prefix to match
        older_than_hours: Remove files older than this many hours

    Returns:
        Number of files removed
    """
    temp_dir = Path(temp_dir)

    if not temp_dir.exists():
        return 0

    cutoff_time = datetime.now().timestamp() - (older_than_hours * 3600)
    removed_count = 0

    try:
        for file_path in temp_dir.glob(f"{prefix}*"):
            if file_path.is_file():
                try:
                    if file_path.stat().st_mtime < cutoff_time:
                        file_path.unlink()
                        removed_count += 1
                        logger.debug(f"Removed old temp file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to remove temp file {file_path}: {e}")

        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old temp files from {temp_dir}")

    except Exception as e:
        logger.error(f"Failed to cleanup temp files in {temp_dir}: {e}")

    return removed_count

# =============================================================================
# FILE SIZE AND DISK USAGE UTILITIES
# =============================================================================

def get_file_size(filepath: Union[str, Path], unit: str = 'bytes') -> float:
    """
    Get file size in specified unit.

    Args:
        filepath: File path
        unit: Size unit (bytes, kb, mb, gb)

    Returns:
        File size in specified unit
    """
    filepath = Path(filepath)

    if not filepath.exists():
        return 0.0

    size_bytes = filepath.stat().st_size

    if unit.lower() == 'kb':
        return size_bytes / 1024
    elif unit.lower() == 'mb':
        return size_bytes / (1024 * 1024)
    elif unit.lower() == 'gb':
        return size_bytes / (1024 * 1024 * 1024)
    else:
        return size_bytes

def get_directory_size(dirpath: Union[str, Path], unit: str = 'bytes') -> float:
    """
    Get total size of directory and all subdirectories.

    Args:
        dirpath: Directory path
        unit: Size unit (bytes, kb, mb, gb)

    Returns:
        Total size in specified unit
    """
    dirpath = Path(dirpath)

    if not dirpath.exists():
        return 0.0

    total_size = 0

    try:
        for file_path in dirpath.rglob('*'):
            if file_path.is_file():
                total_size += file_path.stat().st_size
    except Exception as e:
        logger.error(f"Failed to calculate directory size for {dirpath}: {e}")
        return 0.0

    if unit.lower() == 'kb':
        return total_size / 1024
    elif unit.lower() == 'mb':
        return total_size / (1024 * 1024)
    elif unit.lower() == 'gb':
        return total_size / (1024 * 1024 * 1024)
    else:
        return total_size

def check_disk_space(
    path: Union[str, Path],
    required_gb: float
) -> bool:
    """
    Check if sufficient disk space is available.

    Args:
        path: Path to check (file or directory)
        required_gb: Required space in GB

    Returns:
        True if sufficient space available
    """
    path = Path(path)

    # Get parent directory if path is a file
    if path.is_file():
        path = path.parent
    elif not path.exists():
        path = path.parent

    try:
        stat = shutil.disk_usage(path)
        available_gb = stat.free / (1024 * 1024 * 1024)

        logger.debug(f"Disk space check: {available_gb:.2f}GB available, {required_gb:.2f}GB required")

        return available_gb >= required_gb

    except Exception as e:
        logger.error(f"Failed to check disk space for {path}: {e}")
        return False