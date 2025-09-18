import pytest
from unittest import mock
from datetime import date

from domains.market_data.services.agent.llm_assistant import LLMAssistant
from domains.market_data.services.agent.models import EODPrice

class TestLLMAssistant:
    @pytest.fixture
    def assistant(self):
        """Create an LLM assistant with a mock API key"""
        with mock.patch.dict('os.environ', {"OPENAI_API_KEY": "test_key"}):
            return LLMAssistant()

    @mock.patch('src.market_data.agent.llm_assistant.LLMAssistant._generate')
    def test_select_best_source(self, mock_generate, assistant):
        """Test selecting the best data source"""
        # Setup mock response
        mock_generate.return_value = "tiingo is the best source for this data point."

        # Call the method
        data_point = {"symbol": "AAPL", "date": date(2023, 1, 1)}
        available_sources = ["tiingo", "polygon"]
        result = assistant.select_best_source(data_point, available_sources)

        # Assertions
        assert result == "tiingo"
        mock_generate.assert_called_once()

    @mock.patch('src.market_data.agent.llm_assistant.LLMAssistant._generate')
    def test_select_best_source_single_option(self, mock_generate, assistant):
        """Test selecting the best data source when only one is available"""
        # Should not call the LLM if only one source is available
        data_point = {"symbol": "AAPL", "date": date(2023, 1, 1)}
        available_sources = ["polygon"]
        result = assistant.select_best_source(data_point, available_sources)

        # Assertions
        assert result == "polygon"
        mock_generate.assert_not_called()

    @mock.patch('src.market_data.agent.llm_assistant.LLMAssistant._generate')
    def test_get_recommended_sources(self, mock_generate, assistant):
        """Test getting recommended data sources in priority order"""
        # Setup mock response
        mock_generate.return_value = "tiingo, polygon"

        # Call the method
        data_point = {"symbol": "AAPL", "date": date(2023, 1, 1)}
        available_sources = ["tiingo", "polygon"]
        result = assistant.get_recommended_sources(data_point, available_sources)

        # Assertions
        assert result == ["tiingo", "polygon"]
        mock_generate.assert_called_once()

    @mock.patch('src.market_data.agent.llm_assistant.LLMAssistant._generate')
    def test_reconcile_data_conflicts(self, mock_generate, assistant):
        """Test reconciling data conflicts"""
        # Setup mock response
        mock_generate.return_value = """
        After analyzing the data, here's the reconciled values:

        {
            "open": 150.25,
            "high": 155.5,
            "low": 149.0,
            "close": 153.25,
            "adj_close": 153.25,
            "volume": 1005000
        }
        """

        # Create sample EOD records
        records = [
            EODPrice(
                instrument_id="AAPL",
                date=date(2023, 1, 1),
                open=150.0,
                high=155.0,
                low=149.0,
                close=153.0,
                adj_close=None,
                volume=1000000,
                vendor="polygon",
                quality_score=0.9
            ),
            EODPrice(
                instrument_id="AAPL",
                date=date(2023, 1, 1),
                open=150.5,
                high=156.0,
                low=149.5,
                close=153.5,
                adj_close=153.5,
                volume=1010000,
                vendor="tiingo",
                quality_score=0.95
            )
        ]

        # Call the method
        result = assistant.reconcile_data_conflicts(records)

        # Assertions
        assert isinstance(result, dict)
        assert result["open"] == 150.25
        assert result["high"] == 155.5
        assert result["low"] == 149.0
        assert result["close"] == 153.25
        assert result["adj_close"] == 153.25
        assert result["volume"] == 1005000
        mock_generate.assert_called_once()

    @mock.patch('src.market_data.agent.llm_assistant.LLMAssistant._generate')
    def test_reconcile_data_conflicts_single_record(self, mock_generate, assistant):
        """Test reconciling data conflicts with a single record"""
        # Should not call the LLM if only one record is provided
        record = EODPrice(
            instrument_id="AAPL",
            date=date(2023, 1, 1),
            open=150.0,
            high=155.0,
            low=149.0,
            close=153.0,
            adj_close=None,
            volume=1000000,
            vendor="polygon",
            quality_score=0.9
        )

        # Call the method
        result = assistant.reconcile_data_conflicts([record])

        # Assertions
        assert isinstance(result, dict)
        assert result["open"] == 150.0
        assert result["high"] == 155.0
        assert result["low"] == 149.0
        assert result["close"] == 153.0
        assert result["adj_close"] is None
        assert result["volume"] == 1000000
        mock_generate.assert_not_called()

    @mock.patch('src.market_data.agent.llm_assistant.LLMAssistant._generate')
    def test_detect_anomalies(self, mock_generate, assistant):
        """Test anomaly detection"""
        # Setup mock response
        mock_generate.return_value = """
        {
            "is_anomaly": true,
            "anomaly_type": "price_spike",
            "confidence": 0.85,
            "explanation": "Close price is 20% higher than previous day with no corresponding volume increase."
        }
        """

        # Create sample record and history
        record = {
            "date": date(2023, 1, 5),
            "open": 150.0,
            "high": 180.0,
            "low": 149.0,
            "close": 175.0,
            "volume": 1000000
        }

        historical_records = [
            {
                "date": date(2023, 1, 4),
                "open": 145.0,
                "high": 150.0,
                "low": 144.0,
                "close": 148.0,
                "volume": 900000
            },
            {
                "date": date(2023, 1, 3),
                "open": 146.0,
                "high": 148.0,
                "low": 145.0,
                "close": 147.0,
                "volume": 850000
            }
        ]

        # Call the method
        result = assistant.detect_anomalies(record, historical_records)

        # Assertions
        assert isinstance(result, dict)
        assert result["is_anomaly"] is True
        assert result["anomaly_type"] == "price_spike"
        assert result["confidence"] == 0.85
        assert "explanation" in result
        mock_generate.assert_called_once()

    @mock.patch('requests.post')
    def test_generate(self, mock_post, assistant):
        """Test the _generate method that calls the LLM API"""
        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "This is a test response"
                    }
                }
            ]
        }
        mock_post.return_value = mock_response

        # Call the method directly
        result = assistant._generate("Test prompt")

        # Assertions
        assert result == "This is a test response"
        mock_post.assert_called_once()

        # Verify request format
        call_args = mock_post.call_args
        assert "api.openai.com" in call_args[0][0]
        assert "Bearer test_key" in call_args[1]["headers"]["Authorization"]
        assert "Test prompt" in call_args[1]["json"]["messages"][0]["content"]

    @mock.patch('requests.post')
    def test_generate_error(self, mock_post, assistant):
        """Test handling API errors in _generate"""
        # Setup mock response to raise an exception
        mock_post.side_effect = Exception("API error")

        # Call the method
        result = assistant._generate("Test prompt")

        # Should return default response on error
        assert result == "Unable to generate response"
        mock_post.assert_called_once()
