#!/usr/bin/env python3
"""
Test file that should be allowed to use mock data
"""
import unittest
from unittest.mock import Mock

class TestMockData(unittest.TestCase):

    def test_with_fake_data(self):
        # This should be allowed in test files
        fake_data = generate_fake_data_for_testing()
        mock_response = Mock()
        synthetic_test_data = create_synthetic_test_data()
        fallback_test_data = get_fallback_test_data()

        self.assertIsNotNone(fake_data)