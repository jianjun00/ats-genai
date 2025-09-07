import os
import logging
import json
from typing import List, Dict, Any, Optional
import requests
from .models import EODPrice

logger = logging.getLogger(__name__)

class LLMAssistant:
    """
    LLM-powered assistant for the data agent to make decisions about
    data sources, reconciliation strategies, and anomaly detection.
    """

    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4"):
        """
        Initialize the LLM assistant.

        Args:
            api_key: API key for the LLM service
            model: Model name to use
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise Exception("Please set your OPENAI_API_KEY environment variable or pass api_key explicitly.")
        self.model = model

    def select_best_source(self, data_point: Dict[str, Any], available_sources: List[str]) -> str:
        """
        Select the best data source for a specific data point.

        Args:
            data_point: Dictionary with symbol and date information
            available_sources: List of available data sources

        Returns:
            Name of the best data source to use
        """
        if not available_sources:
            raise ValueError("No available sources provided")

        if len(available_sources) == 1:
            return available_sources[0]

        prompt = self._create_source_selection_prompt(data_point, available_sources)
        response = self._generate(prompt)
        return self._parse_source_selection(response, available_sources)

    def get_recommended_sources(self, data_point: Dict[str, Any], available_sources: List[str]) -> List[str]:
        """
        Get recommended data sources in priority order.

        Args:
            data_point: Dictionary with symbol and date information
            available_sources: List of available data sources

        Returns:
            List of data sources in priority order
        """
        if not available_sources:
            return []

        if len(available_sources) == 1:
            return available_sources

        prompt = self._create_source_ranking_prompt(data_point, available_sources)
        response = self._generate(prompt)
        return self._parse_source_ranking(response, available_sources)

    def reconcile_data_conflicts(self, records: List[EODPrice]) -> Dict[str, Any]:
        """
        Resolve conflicts between multiple data sources.

        Args:
            records: List of EODPrice records from different sources

        Returns:
            Dictionary with reconciled values
        """
        if not records:
            raise ValueError("No records provided for reconciliation")

        if len(records) == 1:
            # No conflict to resolve with a single record
            record = records[0]
            return {
                "open": record.open,
                "high": record.high,
                "low": record.low,
                "close": record.close,
                "adj_close": record.adj_close,
                "volume": record.volume
            }

        prompt = self._create_conflict_resolution_prompt(records)
        response = self._generate(prompt)
        return self._parse_reconciliation_result(response)

    def detect_anomalies(self, record: Dict[str, Any], historical_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Detect anomalies in a record compared to historical data.

        Args:
            record: Current price record
            historical_records: List of historical price records

        Returns:
            Dictionary with anomaly information
        """
        prompt = self._create_anomaly_detection_prompt(record, historical_records)
        response = self._generate(prompt)
        return self._parse_anomaly_detection(response)

    def _generate(self, prompt: str) -> str:
        """
        Generate a response from the LLM.

        Args:
            prompt: Prompt to send to the LLM

        Returns:
            LLM response text
        """
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            payload = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1  # Low temperature for more deterministic responses
            }

            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"Error generating LLM response: {e}")
            # Fallback to a simple default response
            return "Unable to generate response"

    def _create_source_selection_prompt(self, data_point: Dict[str, Any], available_sources: List[str]) -> str:
        """Create prompt for source selection"""
        return f"""
        You are a financial data expert. I need to select the best data source for the following data point:

        Symbol: {data_point['symbol']}
        Date: {data_point['date']}

        Available sources: {', '.join(available_sources)}

        Please select the best source considering:
        1. Data quality and reliability
        2. Timeliness of updates
        3. Historical accuracy

        Respond with just the name of the best source.
        """

    def _create_source_ranking_prompt(self, data_point: Dict[str, Any], available_sources: List[str]) -> str:
        """Create prompt for source ranking"""
        return f"""
        You are a financial data expert. I need to rank data sources for the following data point:

        Symbol: {data_point['symbol']}
        Date: {data_point['date']}

        Available sources: {', '.join(available_sources)}

        Please rank the sources in order of preference considering:
        1. Data quality and reliability
        2. Timeliness of updates
        3. Historical accuracy

        Respond with a comma-separated list of sources in priority order.
        """

    def _create_conflict_resolution_prompt(self, records: List[EODPrice]) -> str:
        """Create prompt for conflict resolution"""
        records_info = []
        for r in records:
            records_info.append(f"""
            Source: {r.vendor}
            Date: {r.date}
            Open: {r.open}
            High: {r.high}
            Low: {r.low}
            Close: {r.close}
            Adjusted Close: {r.adj_close}
            Volume: {r.volume}
            """)

        records_text = "\n".join(records_info)

        return f"""
        You are a financial data expert. I have conflicting price data from different sources for the same security and date.
        Please reconcile these values into a single set of values.

        {records_text}

        Analyze the data and provide the reconciled values. Consider:
        1. Source reliability
        2. Data consistency (e.g., high >= open, high >= close, low <= open, low <= close)
        3. Outlier detection

        Respond with a JSON object containing the reconciled values for open, high, low, close, adj_close, and volume.
        Example: {{"open": 150.25, "high": 152.75, "low": 149.50, "close": 151.80, "adj_close": 151.80, "volume": 5000000}}
        """

    def _create_anomaly_detection_prompt(self, record: Dict[str, Any], historical_records: List[Dict[str, Any]]) -> str:
        """Create prompt for anomaly detection"""
        historical_summary = []
        for i, h in enumerate(historical_records[-5:]):  # Use last 5 records for context
            historical_summary.append(f"""
            Date: {h.get('date')}
            Open: {h.get('open')}
            High: {h.get('high')}
            Low: {h.get('low')}
            Close: {h.get('close')}
            Volume: {h.get('volume')}
            """)

        history_text = "\n".join(historical_summary)

        return f"""
        You are a financial data expert. I need to detect anomalies in the following price record compared to recent history:

        Current record:
        Date: {record.get('date')}
        Open: {record.get('open')}
        High: {record.get('high')}
        Low: {record.get('low')}
        Close: {record.get('close')}
        Volume: {record.get('volume')}

        Recent history:
        {history_text}

        Analyze the data and identify any anomalies. Consider:
        1. Unusual price movements (gaps, spikes)
        2. Volume anomalies
        3. Inconsistent OHLC values

        Respond with a JSON object containing:
        1. "is_anomaly": boolean indicating if an anomaly was detected
        2. "anomaly_type": description of the anomaly type if detected
        3. "confidence": confidence score (0-1)
        4. "explanation": brief explanation of the anomaly

        Example: {{"is_anomaly": true, "anomaly_type": "price_spike", "confidence": 0.85, "explanation": "Close price is 20% higher than previous day with no corresponding news or market movement."}}
        """

    def _parse_source_selection(self, response: str, available_sources: List[str]) -> str:
        """Parse source selection response"""
        response = response.strip().lower()
        for source in available_sources:
            if source.lower() in response:
                return source
        # Default to first source if parsing fails
        return available_sources[0]

    def _parse_source_ranking(self, response: str, available_sources: List[str]) -> List[str]:
        """Parse source ranking response"""
        response = response.strip().lower()
        ranked_sources = []

        # Try to parse comma-separated list
        for item in response.split(','):
            item = item.strip()
            for source in available_sources:
                if source.lower() in item and source not in ranked_sources:
                    ranked_sources.append(source)

        # Add any missing sources
        for source in available_sources:
            if source not in ranked_sources:
                ranked_sources.append(source)

        return ranked_sources

    def _parse_reconciliation_result(self, response: str) -> Dict[str, Any]:
        """Parse reconciliation result"""
        try:
            # Try to extract JSON from the response
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response[start_idx:end_idx]
                return json.loads(json_str)
        except Exception as e:
            logger.error(f"Error parsing reconciliation result: {e}")

        # Return empty dict if parsing fails
        return {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "adj_close": None,
            "volume": None
        }

    def _parse_anomaly_detection(self, response: str) -> Dict[str, Any]:
        """Parse anomaly detection result"""
        try:
            # Try to extract JSON from the response
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response[start_idx:end_idx]
                return json.loads(json_str)
        except Exception as e:
            logger.error(f"Error parsing anomaly detection result: {e}")

        # Return default result if parsing fails
        return {
            "is_anomaly": False,
            "anomaly_type": None,
            "confidence": 0,
            "explanation": "Failed to analyze"
        }
