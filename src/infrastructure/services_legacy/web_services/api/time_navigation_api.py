#!/usr/bin/env python3
"""
Time Navigation API
Provides metadata and navigation support for time series data
"""

from flask import Blueprint, request, jsonify
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from services.analytics_service import AnalyticsService

time_navigation_bp = Blueprint('time_navigation', __name__)

@time_navigation_bp.route('/api/v1/training-datasets/<int:dataset_id>/sequences/<string:sequence_id>/navigation-metadata')
def get_navigation_metadata(dataset_id, sequence_id):
    """Get navigation metadata for a sequence (available time range, etc.)."""
    try:
        analytics_service = AnalyticsService()

        # Test multiple row_index values to find available range
        available_positions = []
        max_position = 0

        # Test row indices to find the working range
        for test_index in [0, 10, 25, 50, 75, 100, 150, 200]:
            try:
                # Make a test API call to see if this position has data
                result = analytics_service.get_training_dataset_sequence_multi_timeframe(
                    dataset_id, sequence_id, test_index
                )

                if result.get('success') and result.get('table_data'):
                    table_data = result['table_data']
                    if table_data and len(table_data) > 0:
                        available_positions.append({
                            'row_index': test_index,
                            'bars': len(table_data),
                            'start_timestamp': table_data[0].get('timestamp'),
                            'end_timestamp': table_data[-1].get('timestamp'),
                            'start_price': table_data[0].get('open'),
                            'end_price': table_data[-1].get('close')
                        })
                        max_position = max(max_position, test_index)

            except Exception as e:
                # If this position fails, we've likely reached the end
                break

        # Estimate total available range based on successful positions
        if available_positions:
            # Find the actual maximum by testing a bit beyond the last successful
            for test_index in range(max_position + 1, max_position + 50, 5):
                try:
                    result = analytics_service.get_training_dataset_sequence_multi_timeframe(
                        dataset_id, sequence_id, test_index
                    )
                    if result.get('success') and result.get('table_data'):
                        max_position = test_index
                    else:
                        break
                except:
                    break

        # Convert timestamps to readable dates
        def format_timestamp(ts):
            if ts:
                try:
                    from datetime import datetime
                    return datetime.fromtimestamp(ts).isoformat()
                except:
                    return ts
            return None

        # Prepare metadata
        metadata = {
            'sequence_id': sequence_id,
            'dataset_id': dataset_id,
            'navigation': {
                'min_row_index': 0,
                'max_row_index': max_position,
                'total_positions': max_position + 1,
                'window_size': 21,  # Standard 21-bar window
                'default_position': 10  # Good starting position
            },
            'sample_positions': [
                {
                    'row_index': pos['row_index'],
                    'description': f"Position {pos['row_index']} ({pos['bars']} bars)",
                    'start_time': format_timestamp(pos['start_timestamp']),
                    'end_time': format_timestamp(pos['end_timestamp']),
                    'price_range': {
                        'start': pos['start_price'],
                        'end': pos['end_price']
                    }
                }
                for pos in available_positions[:5]  # First 5 samples
            ],
            'timeframes_available': ['5m', '15m', '1h', '1d', '1w'],
            'navigation_tips': [
                'Use row_index 0-10 for early time periods',
                f'Use row_index {max_position//2} for middle time periods',
                f'Use row_index {max_position} for latest time periods',
                'Each position shows a 21-bar window centered around that time'
            ]
        }

        return jsonify(metadata)

    except Exception as e:
        return jsonify({
            'error': f'Failed to get navigation metadata: {str(e)}',
            'sequence_id': sequence_id,
            'dataset_id': dataset_id
        }), 500

@time_navigation_bp.route('/api/v1/training-datasets/<int:dataset_id>/sequences/<string:sequence_id>/navigate')
def navigate_sequence(dataset_id, sequence_id):
    """Navigate to a specific position in the sequence."""
    try:
        # Get parameters
        row_index = request.args.get('row_index', 10, type=int)
        direction = request.args.get('direction')  # 'next', 'prev', 'first', 'last'

        analytics_service = AnalyticsService()

        # Handle navigation directions
        if direction:
            # First get current valid range
            metadata_response = get_navigation_metadata(dataset_id, sequence_id)
            if metadata_response.status_code == 200:
                metadata = metadata_response.get_json()
                nav_info = metadata.get('navigation', {})
                min_idx = nav_info.get('min_row_index', 0)
                max_idx = nav_info.get('max_row_index', 100)

                if direction == 'next':
                    row_index = min(row_index + 10, max_idx)
                elif direction == 'prev':
                    row_index = max(row_index - 10, min_idx)
                elif direction == 'first':
                    row_index = min_idx
                elif direction == 'last':
                    row_index = max_idx

        # Get the data for the specified position
        result = analytics_service.get_training_dataset_sequence_multi_timeframe(
            dataset_id, sequence_id, row_index
        )

        # Add navigation context to the response
        if result.get('success'):
            result['navigation_context'] = {
                'current_row_index': row_index,
                'direction_used': direction,
                'timestamp_range': {
                    'start': result['table_data'][0].get('timestamp') if result.get('table_data') else None,
                    'end': result['table_data'][-1].get('timestamp') if result.get('table_data') else None
                }
            }

        return jsonify(result)

    except Exception as e:
        return jsonify({
            'error': f'Navigation failed: {str(e)}',
            'sequence_id': sequence_id,
            'dataset_id': dataset_id,
            'requested_row_index': row_index
        }), 500