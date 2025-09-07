#!/usr/bin/env python3
"""
Improved bounds validation for training data visualization API.

This module provides enhanced bounds checking to prevent "Start index out of bounds"
errors by implementing proactive validation and graceful error handling.
"""

import os
import numpy as np
import json
from typing import Dict, List, Any, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class ImprovedBoundsValidator:
    """Enhanced bounds validation for training data visualization."""

    @staticmethod
    def validate_dataset_integrity(features_file_path: str, metadata_info: Dict) -> Dict:
        """
        Validate that dataset file matches metadata claims.

        Returns validation results with recommendations for handling mismatches.
        """
        validation_result = {
            "valid": True,
            "issues": [],
            "recommendations": [],
            "actual_dimensions": None,
            "metadata_claims": None
        }

        try:
            # Get metadata claims
            metadata_sequences = metadata_info.get('total_sequences', 0)
            metadata_sequence_length = metadata_info.get('sequence_length', 60)

            validation_result["metadata_claims"] = {
                "total_sequences": metadata_sequences,
                "sequence_length": metadata_sequence_length,
                "expected_data_points": metadata_sequences * metadata_sequence_length
            }

            # Check if file exists and get actual dimensions
            if not os.path.exists(features_file_path):
                validation_result["valid"] = False
                validation_result["issues"].append(f"Features file not found: {features_file_path}")
                validation_result["recommendations"].append("Regenerate dataset or update file path")
                return validation_result

            # Load and inspect actual file
            if features_file_path.endswith('.npy'):
                features_data = np.load(features_file_path, mmap_mode='r')  # Memory-mapped for large files
                actual_shape = features_data.shape

                if len(actual_shape) >= 2:
                    actual_sequences = actual_shape[0]
                    actual_sequence_length = actual_shape[1]
                    actual_features = actual_shape[2] if len(actual_shape) > 2 else 1
                else:
                    actual_sequences = 0
                    actual_sequence_length = 0
                    actual_features = 0

            elif features_file_path.endswith('.csv'):
                import pandas as pd
                df = pd.read_csv(features_file_path)
                # For CSV, estimate based on total rows
                total_rows = len(df)
                estimated_sequences = max(1, total_rows // metadata_sequence_length)

                actual_sequences = estimated_sequences
                actual_sequence_length = metadata_sequence_length  # Use metadata value
                actual_features = len(df.select_dtypes(include=[np.number]).columns)
            else:
                validation_result["valid"] = False
                validation_result["issues"].append(f"Unsupported file format: {features_file_path}")
                return validation_result

            validation_result["actual_dimensions"] = {
                "total_sequences": actual_sequences,
                "sequence_length": actual_sequence_length,
                "features": actual_features,
                "total_data_points": actual_sequences * actual_sequence_length
            }

            # Compare metadata claims vs actual file
            if actual_sequences != metadata_sequences:
                validation_result["valid"] = False
                validation_result["issues"].append(
                    f"Sequence count mismatch: metadata claims {metadata_sequences}, file has {actual_sequences}"
                )
                if actual_sequences < metadata_sequences:
                    validation_result["recommendations"].append(
                        f"Clamp sequence selections to 0-{actual_sequences-1} range"
                    )
                else:
                    validation_result["recommendations"].append(
                        f"Update metadata to reflect actual sequence count: {actual_sequences}"
                    )

            if actual_sequence_length != metadata_sequence_length:
                validation_result["issues"].append(
                    f"Sequence length mismatch: metadata claims {metadata_sequence_length}, file has {actual_sequence_length}"
                )
                validation_result["recommendations"].append(
                    f"Update sequence length in metadata to {actual_sequence_length}"
                )

            if actual_sequences == 0:
                validation_result["valid"] = False
                validation_result["issues"].append("Dataset file is empty or corrupted")
                validation_result["recommendations"].append("Regenerate dataset with valid data")

            return validation_result

        except Exception as e:
            validation_result["valid"] = False
            validation_result["issues"].append(f"Error validating dataset: {str(e)}")
            validation_result["recommendations"].append("Check file integrity and permissions")
            return validation_result

    @staticmethod
    def calculate_safe_window_bounds(selected_sequence: int, sequence_length: int,
                                   actual_total_sequences: int, window_size: int = 21) -> Dict:
        """
        Calculate safe window bounds with proactive validation.

        Prevents "Start index out of bounds" by validating against actual data availability.
        """
        bounds_calc = {
            "original_request": {
                "selected_sequence": selected_sequence,
                "sequence_length": sequence_length,
                "window_size": window_size
            },
            "validation": {
                "valid": True,
                "issues": [],
                "adjustments_made": []
            },
            "safe_bounds": {}
        }

        try:
            # Proactive validation: Check if selected sequence exists
            if selected_sequence >= actual_total_sequences:
                bounds_calc["validation"]["valid"] = False
                bounds_calc["validation"]["issues"].append(
                    f"Selected sequence {selected_sequence} >= available sequences {actual_total_sequences}"
                )

                # Auto-clamp to valid range
                clamped_sequence = max(0, actual_total_sequences - 1)
                bounds_calc["validation"]["adjustments_made"].append(
                    f"Clamped sequence from {selected_sequence} to {clamped_sequence}"
                )
                selected_sequence = clamped_sequence

            # Handle empty dataset
            if actual_total_sequences == 0:
                bounds_calc["validation"]["valid"] = False
                bounds_calc["validation"]["issues"].append("Dataset is empty")
                bounds_calc["safe_bounds"] = {
                    "start_idx": 0,
                    "end_idx": 0,
                    "sequence_idx": 0,
                    "time_step_in_sequence": 0,
                    "adjusted_window_size": 0
                }
                return bounds_calc

            # Calculate window position (improved logic)
            middle_time_step = sequence_length // 2
            center_index = (selected_sequence * sequence_length) + middle_time_step
            half_window = window_size // 2
            start_idx = max(0, center_index - half_window)

            # Validate against actual data bounds
            max_available_data_points = actual_total_sequences * sequence_length

            # Adjust window if it extends beyond available data
            if start_idx + window_size > max_available_data_points:
                old_start_idx = start_idx
                start_idx = max(0, max_available_data_points - window_size)
                bounds_calc["validation"]["adjustments_made"].append(
                    f"Adjusted start_idx from {old_start_idx} to {start_idx} to fit data bounds"
                )

            # Calculate final window size (may be less than requested)
            actual_end_idx = min(start_idx + window_size, max_available_data_points)
            adjusted_window_size = actual_end_idx - start_idx

            if adjusted_window_size < window_size:
                bounds_calc["validation"]["adjustments_made"].append(
                    f"Reduced window size from {window_size} to {adjusted_window_size} due to data limits"
                )

            # Calculate sequence and time step for backend processing
            sequence_idx = start_idx // sequence_length
            time_step_in_sequence = start_idx % sequence_length

            # Final validation
            if sequence_idx >= actual_total_sequences:
                bounds_calc["validation"]["valid"] = False
                bounds_calc["validation"]["issues"].append(
                    f"Calculated sequence_idx {sequence_idx} >= actual_total_sequences {actual_total_sequences}"
                )
                # Emergency fallback to last valid sequence
                sequence_idx = actual_total_sequences - 1
                time_step_in_sequence = min(time_step_in_sequence, sequence_length - 1)
                start_idx = (sequence_idx * sequence_length) + time_step_in_sequence
                actual_end_idx = min(start_idx + window_size, max_available_data_points)
                adjusted_window_size = actual_end_idx - start_idx

                bounds_calc["validation"]["adjustments_made"].append(
                    f"Emergency fallback to sequence_idx {sequence_idx}"
                )

            bounds_calc["safe_bounds"] = {
                "start_idx": start_idx,
                "end_idx": actual_end_idx,
                "sequence_idx": sequence_idx,
                "time_step_in_sequence": time_step_in_sequence,
                "adjusted_window_size": adjusted_window_size,
                "center_index": center_index,
                "max_available_data_points": max_available_data_points
            }

            return bounds_calc

        except Exception as e:
            bounds_calc["validation"]["valid"] = False
            bounds_calc["validation"]["issues"].append(f"Error calculating safe bounds: {str(e)}")
            return bounds_calc

    @staticmethod
    def get_safe_visualization_data(dataset_id: str, start_idx: int, count: int = 21,
                                   features_file_path: str = None, metadata_file_path: str = None) -> Dict:
        """
        Enhanced version of get_training_dataset_visualization_data with improved bounds checking.

        Prevents "Start index out of bounds" errors through comprehensive validation.
        """
        try:
            logger.info(f"Getting safe visualization data for dataset {dataset_id}, start_idx {start_idx}, count {count}")

            if not features_file_path:
                return {"error": "Features file path not provided", "data": []}

            # Step 1: Load metadata
            metadata_info = {"total_sequences": 100, "sequence_length": 60}  # Defaults
            if metadata_file_path and os.path.exists(metadata_file_path):
                try:
                    with open(metadata_file_path, 'r') as f:
                        metadata = json.load(f)
                        metadata_info.update(metadata)
                except Exception as e:
                    logger.warning(f"Could not load metadata: {e}")

            # Step 2: Validate dataset integrity
            validation = ImprovedBoundsValidator.validate_dataset_integrity(features_file_path, metadata_info)

            if not validation["valid"]:
                return {
                    "error": "Dataset validation failed",
                    "data": [],
                    "validation_issues": validation["issues"],
                    "recommendations": validation["recommendations"]
                }

            # Step 3: Calculate selected sequence from start_idx
            sequence_length = validation["actual_dimensions"]["sequence_length"]
            actual_total_sequences = validation["actual_dimensions"]["total_sequences"]
            selected_sequence = start_idx // sequence_length

            # Step 4: Calculate safe window bounds
            bounds = ImprovedBoundsValidator.calculate_safe_window_bounds(
                selected_sequence, sequence_length, actual_total_sequences, count
            )

            if not bounds["validation"]["valid"]:
                return {
                    "error": "Could not calculate safe window bounds",
                    "data": [],
                    "bounds_issues": bounds["validation"]["issues"],
                    "adjustments_attempted": bounds["validation"]["adjustments_made"]
                }

            safe_bounds = bounds["safe_bounds"]

            # Step 5: Load data with safe bounds
            visualization_data = []

            try:
                if features_file_path.endswith('.npy'):
                    features_data = np.load(features_file_path)

                    # Extract safe data slice
                    seq_idx = safe_bounds["sequence_idx"]
                    time_start = safe_bounds["time_step_in_sequence"]
                    window_size = safe_bounds["adjusted_window_size"]
                    time_end = min(time_start + window_size, sequence_length)

                    if seq_idx < features_data.shape[0]:
                        data_slice = features_data[seq_idx, time_start:time_end, :]

                        # Convert to visualization format
                        from datetime import datetime, timedelta
                        base_datetime = datetime(2024, 1, 15, 9, 30, 0)

                        for i, data_point in enumerate(data_slice):
                            current_datetime = base_datetime + timedelta(minutes=5 * (safe_bounds["start_idx"] + i))
                            visualization_data.append({
                                "sequence_id": safe_bounds["start_idx"] + i + 1,
                                "datetime": current_datetime.isoformat(),
                                "etop": float(data_point[0]) if len(data_point) > 0 else 150.0,
                                "ebot": float(data_point[1]) if len(data_point) > 1 else 148.0,
                                "pldot": float(data_point[2]) if len(data_point) > 2 else 149.0,
                                "5m_high": float(data_point[3]) if len(data_point) > 3 else 150.0,
                                "5m_low": float(data_point[4]) if len(data_point) > 4 else 148.0,
                                "5m_close": float(data_point[5]) if len(data_point) > 5 else 149.0,
                                "5m_volume": int(data_point[6]) if len(data_point) > 6 else 1000000
                            })

                elif features_file_path.endswith('.csv'):
                    import pandas as pd
                    df = pd.read_csv(features_file_path)

                    # Extract rows within safe bounds
                    start_row = safe_bounds["start_idx"]
                    end_row = min(start_row + safe_bounds["adjusted_window_size"], len(df))

                    data_slice = df.iloc[start_row:end_row]

                    # Convert to visualization format
                    from datetime import datetime, timedelta
                    base_datetime = datetime(2024, 1, 15, 9, 30, 0)

                    for i, (_, row) in enumerate(data_slice.iterrows()):
                        current_datetime = base_datetime + timedelta(minutes=5 * (start_row + i))
                        visualization_data.append({
                            "sequence_id": start_row + i + 1,
                            "datetime": current_datetime.isoformat(),
                            "etop": float(row.get('etop', 150.0)),
                            "ebot": float(row.get('ebot', 148.0)),
                            "pldot": float(row.get('pldot', 149.0)),
                            "5m_high": float(row.get('5m_high', 150.0)),
                            "5m_low": float(row.get('5m_low', 148.0)),
                            "5m_close": float(row.get('5m_close', 149.0)),
                            "5m_volume": int(row.get('5m_volume', 1000000))
                        })

                else:
                    return {"error": f"Unsupported file format: {features_file_path}", "data": []}

            except Exception as e:
                return {"error": f"Error loading data: {str(e)}", "data": []}

            # Step 6: Return successful result with debugging info
            return {
                "data": visualization_data,
                "debug_info": {
                    "original_request": bounds["original_request"],
                    "safe_bounds": safe_bounds,
                    "adjustments_made": bounds["validation"]["adjustments_made"],
                    "data_points_returned": len(visualization_data),
                    "validation_passed": True
                }
            }

        except Exception as e:
            logger.error(f"Error in get_safe_visualization_data: {e}")
            return {
                "error": f"Unexpected error: {str(e)}",
                "data": [],
                "debug_info": {"exception": str(e)}
            }


def apply_improved_bounds_validation_patch():
    """
    Apply improved bounds validation to existing analytics service.

    This function can be used to patch the existing get_training_dataset_visualization_data
    method with improved bounds checking.
    """
    logger.info("🔧 Applying improved bounds validation patch")

    # Instructions for manual patching
    patch_instructions = """

    🔧 **IMPROVED BOUNDS VALIDATION PATCH INSTRUCTIONS**

    To fix "Start index out of bounds" errors, replace the existing bounds checking
    in analytics_service.py get_training_dataset_visualization_data method:

    ❌ REPLACE THIS (around line 1308):
    ```python
    if sequence_idx >= features_data.shape[0]:
        return {"error": "Start index out of bounds", "data": []}
    ```

    ✅ WITH THIS IMPROVED VERSION:
    ```python
    # Improved bounds validation with detailed error information
    if features_data.shape[0] == 0:
        return {
            "error": "Dataset is empty",
            "data": [],
            "debug_info": {"available_sequences": 0, "requested_sequence": sequence_idx}
        }

    if sequence_idx >= features_data.shape[0]:
        # Provide detailed error information and suggest clamping
        return {
            "error": f"Selected sequence {sequence_idx} is not available (dataset has {features_data.shape[0]} sequences)",
            "data": [],
            "debug_info": {
                "requested_sequence_idx": sequence_idx,
                "available_sequences": features_data.shape[0],
                "suggested_max_sequence": features_data.shape[0] - 1,
                "original_start_idx": start_idx
            },
            "suggestions": [
                f"Try selecting a sequence between 0 and {features_data.shape[0] - 1}",
                "Check if dataset metadata matches actual file contents"
            ]
        }
    ```

    📊 **ADDITIONAL IMPROVEMENTS:**

    1. Add dataset validation before processing:
    ```python
    # At the beginning of get_training_dataset_visualization_data
    from .improved_bounds_validation import ImprovedBoundsValidator

    validation = ImprovedBoundsValidator.validate_dataset_integrity(file_to_load, dataset_info)
    if not validation["valid"]:
        return {
            "error": "Dataset validation failed",
            "data": [],
            "validation_issues": validation["issues"]
        }
    ```

    2. Use safe bounds calculation:
    ```python
    bounds = ImprovedBoundsValidator.calculate_safe_window_bounds(
        start_idx // sequence_length, sequence_length,
        features_data.shape[0], count
    )
    ```

    This will eliminate "Start index out of bounds" errors and provide better user feedback.
    """

    print(patch_instructions)
    return patch_instructions


if __name__ == "__main__":
    # Example usage and testing
    validator = ImprovedBoundsValidator()

    # Test case: metadata claims 100 sequences, file has 10
    print("🧪 Testing improved bounds validation...")

    test_metadata = {"total_sequences": 100, "sequence_length": 60}
    test_bounds = validator.calculate_safe_window_bounds(
        selected_sequence=50,  # Out of bounds request
        sequence_length=60,
        actual_total_sequences=10,  # Actual data availability
        window_size=21
    )

    print(f"📊 Test result: {json.dumps(test_bounds, indent=2)}")

    # Show patch instructions
    apply_improved_bounds_validation_patch()