"""
Tests for the database module.

These tests verify database operations, validation, and connection management.
Note: These are unit tests with mocked connections, not integration tests.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch
from db import Database, DatabaseConfig, get_database


class TestDatabaseConfig:
    """Tests for DatabaseConfig."""

    @patch.dict("os.environ", {"DATABASE_URL": "postgresql://user:pass@host:5432/db"})
    def test_config_from_database_url(self):
        """Should use DATABASE_URL when available."""
        config = DatabaseConfig()
        assert config.database_url == "postgresql://user:pass@host:5432/db"

    @patch.dict("os.environ", {
        "DATABASE_URL": "",
        "DB_HOST": "myhost",
        "DB_PORT": "5433",
        "DB_NAME": "mydb",
        "DB_USER": "myuser",
        "DB_PASSWORD": "mypass"
    }, clear=True)
    def test_config_from_individual_vars(self):
        """Should build URL from individual vars when DATABASE_URL not set."""
        config = DatabaseConfig()
        assert "myhost" in config.database_url
        assert "5433" in config.database_url
        assert "mydb" in config.database_url


class TestDatabaseConnection:
    """Tests for Database connection management."""

    def test_init_creates_config(self):
        """Database should create default config if none provided."""
        db = Database()
        assert db.config is not None
        assert db._conn is None

    def test_connect_returns_false_on_failure(self):
        """connect() should return False when connection fails."""
        db = Database()
        db.config.database_url = "invalid://connection"

        result = db.connect()

        assert result is False

    def test_disconnect_clears_connection(self):
        """disconnect() should clear the connection to None."""
        db = Database()
        mock_conn = MagicMock()
        db._conn = mock_conn

        db.disconnect()

        # Connection should be closed and set to None
        mock_conn.close.assert_called_once()
        assert db._conn is None


class TestTradeValidation:
    """Tests for trade data validation."""

    def test_validate_trade_requires_broker_code(self):
        """Validation should fail if broker_code missing."""
        db = Database()
        trade = {"symbol": "BBNI", "table_type": "buy", "crawl_date": "2025-01-01"}

        assert db._validate_trade(trade) is False

    def test_validate_trade_requires_symbol(self):
        """Validation should fail if symbol missing."""
        db = Database()
        trade = {"broker_code": "AD", "table_type": "buy", "crawl_date": "2025-01-01"}

        assert db._validate_trade(trade) is False

    def test_validate_trade_requires_table_type(self):
        """Validation should fail if table_type missing."""
        db = Database()
        trade = {"broker_code": "AD", "symbol": "BBNI", "crawl_date": "2025-01-01"}

        assert db._validate_trade(trade) is False

    def test_validate_trade_rejects_invalid_table_type(self):
        """Validation should fail if table_type not 'buy' or 'sell'."""
        db = Database()
        trade = {
            "broker_code": "AD",
            "symbol": "BBNI",
            "table_type": "invalid",
            "crawl_date": "2025-01-01"
        }

        assert db._validate_trade(trade) is False

    def test_validate_trade_accepts_valid_data(self):
        """Validation should pass for valid trade data."""
        db = Database()
        trade = {
            "broker_code": "AD",
            "symbol": "BBNI",
            "table_type": "buy",
            "crawl_date": "2025-01-01",
            "netval": 100.5,
            "bval": 100.5,
            "sval": 0,
            "bavg": 5000,
            "savg": 0,
        }

        assert db._validate_trade(trade) is True

    def test_validate_trade_rejects_negative_bval(self):
        """Validation should fail if bval is negative."""
        db = Database()
        trade = {
            "broker_code": "AD",
            "symbol": "BBNI",
            "table_type": "buy",
            "crawl_date": "2025-01-01",
            "bval": -100,  # Invalid
            "sval": 0,
        }

        assert db._validate_trade(trade) is False

    def test_validate_trade_allows_negative_netval(self):
        """netval can be negative (sell > buy)."""
        db = Database()
        trade = {
            "broker_code": "AD",
            "symbol": "BBNI",
            "table_type": "sell",
            "crawl_date": "2025-01-01",
            "netval": -50.5,  # Valid (net selling)
            "bval": 0,
            "sval": 50.5,
        }

        assert db._validate_trade(trade) is True

    def test_validate_trade_rejects_invalid_broker_code_length(self):
        """Broker code should be 2-4 characters."""
        db = Database()
        trade = {
            "broker_code": "A",  # Too short
            "symbol": "BBNI",
            "table_type": "buy",
            "crawl_date": "2025-01-01",
            "bval": 0,
            "sval": 0,
        }

        assert db._validate_trade(trade) is False

    def test_validate_trade_rejects_numeric_broker_code(self):
        """Broker code should be alphabetic only."""
        db = Database()
        trade = {
            "broker_code": "A1",  # Contains number
            "symbol": "BBNI",
            "table_type": "buy",
            "crawl_date": "2025-01-01",
            "bval": 0,
            "sval": 0,
        }

        assert db._validate_trade(trade) is False


class TestInsertBrokerTrades:
    """Tests for insert_broker_trades method."""

    def test_insert_empty_list_returns_zero(self):
        """Should return 0 when given empty list."""
        db = Database()
        result = db.insert_broker_trades([], datetime.now())
        assert result == 0

    def test_validate_filters_out_invalid_trades(self):
        """_validate_trade should correctly filter invalid trades."""
        db = Database()

        valid_trade = {
            "broker_code": "AD",
            "symbol": "BBNI",
            "table_type": "buy",
            "crawl_date": "2025-01-01",
            "bval": 100,
            "sval": 0
        }
        invalid_trade = {
            "broker_code": "INVALID_LONG",  # Too long
            "symbol": "BBRI",
            "table_type": "buy",
            "crawl_date": "2025-01-01"
        }

        # Valid trade should pass, invalid should fail
        assert db._validate_trade(valid_trade) is True
        assert db._validate_trade(invalid_trade) is False


class TestGetDatabase:
    """Tests for get_database singleton."""

    def test_returns_database_instance(self):
        """Should return a Database instance."""
        with patch('db._db_instance', None):
            db = get_database()
            assert isinstance(db, Database)

    def test_returns_same_instance(self):
        """Should return the same instance on subsequent calls."""
        with patch('db._db_instance', None):
            db1 = get_database()
            db2 = get_database()
            assert db1 is db2
