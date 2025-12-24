"""
Tests for the broker_crawler module.

These tests verify parsing, retry logic, and data validation
without making actual network requests.
"""

from unittest.mock import patch
from datetime import datetime
from broker_crawler import (
    BrokerCrawler,
    BrokerCrawlerConfig,
    BrokerDataRow,
    BROKER_CODES,
)


class TestBrokerDataRow:
    """Tests for BrokerDataRow dataclass."""

    def test_to_dict_returns_correct_structure(self):
        """to_dict should return all fields as a dictionary."""
        row = BrokerDataRow(
            broker_code="AD",
            broker_name="OSO Sekuritas",
            table_type="buy",
            symbol="BBNI",
            netval=100.5,
            bval=200.0,
            sval=99.5,
            bavg=5000.0,
            savg=5100.0,
            crawl_date="2025-12-24",
            crawl_timestamp="2025-12-24T18:00:00",
        )

        result = row.to_dict()

        assert result["broker_code"] == "AD"
        assert result["symbol"] == "BBNI"
        assert result["netval"] == 100.5
        assert result["table_type"] == "buy"

    def test_to_dict_preserves_numeric_precision(self):
        """Numeric values should maintain precision."""
        row = BrokerDataRow(
            broker_code="AD",
            broker_name="OSO",
            table_type="sell",
            symbol="BBRI",
            netval=-1.2345,
            bval=0.0,
            sval=1.2345,
            bavg=0.0,
            savg=4567.89,
            crawl_date="2025-12-24",
            crawl_timestamp="2025-12-24T18:00:00",
        )

        result = row.to_dict()

        assert result["netval"] == -1.2345
        assert result["savg"] == 4567.89


class TestBrokerCrawlerConfig:
    """Tests for BrokerCrawlerConfig."""

    def test_default_config_values(self):
        """Default config should have sensible values."""
        config = BrokerCrawlerConfig()

        assert config.base_url == "https://neobdm.tech"
        assert config.rate_limit_seconds == 1.0
        assert config.max_retries == 3
        assert config.session_max_age_minutes == 25

    def test_login_url_property(self):
        """login_url should combine base_url and login_path."""
        config = BrokerCrawlerConfig()
        assert config.login_url == "https://neobdm.tech/accounts/login/"

    @patch.dict("os.environ", {"NEOBDM_USERNAME": "testuser", "NEOBDM_PASSWORD": "testpass"})
    def test_credentials_from_environment(self):
        """Should read credentials from environment variables."""
        config = BrokerCrawlerConfig()
        assert config.username == "testuser"
        assert config.password == "testpass"


class TestBrokerCrawler:
    """Tests for BrokerCrawler class."""

    def test_init_creates_session(self):
        """Crawler should create a requests session on init."""
        crawler = BrokerCrawler()

        assert crawler.session is not None
        assert "User-Agent" in crawler.session.headers

    def test_init_sets_logged_in_false(self):
        """Crawler should start in logged-out state."""
        crawler = BrokerCrawler()
        assert crawler._logged_in is False

    def test_login_fails_without_credentials(self):
        """Login should fail if no credentials provided."""
        config = BrokerCrawlerConfig()
        config.username = ""
        config.password = ""
        crawler = BrokerCrawler(config)

        result = crawler.login()

        assert result is False

    def test_is_session_expired_true_when_no_session(self):
        """Session should be considered expired if never created."""
        crawler = BrokerCrawler()
        crawler._session_created_at = None

        assert crawler._is_session_expired() is True

    def test_is_session_expired_false_when_recent(self):
        """Session should not be expired if recently created."""
        crawler = BrokerCrawler()
        crawler._session_created_at = datetime.now()

        assert crawler._is_session_expired() is False

    def test_build_fetch_payload_structure(self):
        """Payload should have correct structure for Dash callback."""
        crawler = BrokerCrawler()
        payload = crawler._build_fetch_payload("AD", "Today")

        assert "output" in payload
        assert "inputs" in payload
        assert "state" in payload

        # Check broker code is in state
        state = payload["state"]
        assert len(state) > 0
        assert state[0]["value"] == ["AD"]


class TestParseTableData:
    """Tests for table data parsing."""

    def test_parse_valid_data(self):
        """Should correctly parse valid table data."""
        crawler = BrokerCrawler()

        raw_data = [
            {
                "symbol": "[BBNI](/stock_detail/BBNI)",
                "netval": 100.5,
                "bval": 200.0,
                "sval": 99.5,
                "bavg": 5000,
                "savg": 5100,
            }
        ]

        result = crawler._parse_table_data(
            raw_data,
            broker_code="AD",
            broker_name="OSO",
            table_type="buy",
            crawl_date="2025-12-24",
            crawl_timestamp="2025-12-24T18:00:00",
        )

        assert len(result) == 1
        assert result[0].symbol == "BBNI"  # Extracted from markdown link
        assert result[0].netval == 100.5

    def test_parse_symbol_without_markdown(self):
        """Should handle plain symbol without markdown link."""
        crawler = BrokerCrawler()

        raw_data = [{"symbol": "BBRI", "netval": 50, "bval": 50, "sval": 0, "bavg": 0, "savg": 0}]

        result = crawler._parse_table_data(
            raw_data, "AD", "OSO", "buy", "2025-12-24", "2025-12-24T18:00:00"
        )

        assert result[0].symbol == "BBRI"

    def test_parse_skips_invalid_rows(self):
        """Should skip rows with invalid data."""
        crawler = BrokerCrawler()

        raw_data = [
            {"symbol": "[BBNI](/stock)", "netval": "invalid", "bval": 0, "sval": 0, "bavg": 0, "savg": 0},
            {"symbol": "[BBRI](/stock)", "netval": 100, "bval": 50, "sval": 50, "bavg": 0, "savg": 0},
        ]

        result = crawler._parse_table_data(
            raw_data, "AD", "OSO", "buy", "2025-12-24", "2025-12-24T18:00:00"
        )

        # First row skipped due to invalid netval, second row parsed
        assert len(result) == 1
        assert result[0].symbol == "BBRI"


class TestBrokerCodes:
    """Tests for the BROKER_CODES constant."""

    def test_broker_codes_has_90_entries(self):
        """Should have exactly 90 broker codes."""
        assert len(BROKER_CODES) == 90

    def test_broker_codes_structure(self):
        """Each entry should have code and name keys."""
        for broker in BROKER_CODES:
            assert "code" in broker
            assert "name" in broker
            assert len(broker["code"]) == 2  # All codes are 2 letters

    def test_broker_codes_unique(self):
        """All broker codes should be unique."""
        codes = [b["code"] for b in BROKER_CODES]
        assert len(codes) == len(set(codes))
