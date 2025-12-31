"""
Tests for the aggregates module.

These tests verify date range calculations and aggregation logic.
"""

from datetime import date
from unittest.mock import MagicMock, patch
from aggregates import get_period_trade_dates, get_latest_crawl_date, AggregationComputer


class TestGetLatestCrawlDate:
    """Tests for get_latest_crawl_date function."""

    def test_returns_date_from_crawl_log(self):
        """Should return the max crawl_date from successful crawls."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (date(2025, 12, 30),)
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        result = get_latest_crawl_date(mock_db)

        assert result == date(2025, 12, 30)

    def test_returns_none_if_no_crawls(self):
        """Should return None if no successful crawls exist."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        result = get_latest_crawl_date(mock_db)

        assert result is None


class TestGetPeriodTradeDates:
    """Tests for get_period_trade_dates function."""

    def test_today_returns_single_date(self):
        """'today' should return the latest crawl date as a single-element list."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (date(2025, 12, 30),)
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        result = get_period_trade_dates(mock_db, "today")

        assert result == [date(2025, 12, 30)]

    def test_2d_returns_two_dates(self):
        """'2d' should return last 2 distinct trade dates."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(date(2025, 12, 30),), (date(2025, 12, 29),)]
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        result = get_period_trade_dates(mock_db, "2d")

        assert len(result) == 2
        assert date(2025, 12, 30) in result
        assert date(2025, 12, 29) in result

    def test_invalid_period_returns_empty(self):
        """Invalid period format should return empty list."""
        mock_db = MagicMock()

        result = get_period_trade_dates(mock_db, "invalid")

        assert result == []


class TestAggregationComputer:
    """Tests for AggregationComputer class."""

    def test_init_with_db(self):
        """Should accept a database instance."""
        mock_db = MagicMock()
        computer = AggregationComputer(mock_db)

        assert computer.db is mock_db

    def test_periods_constant(self):
        """Should have all expected periods defined."""
        assert "today" in AggregationComputer.PERIODS
        assert "2d" in AggregationComputer.PERIODS
        assert "3d" in AggregationComputer.PERIODS
        assert "5d" in AggregationComputer.PERIODS
        assert "10d" in AggregationComputer.PERIODS
        assert "20d" in AggregationComputer.PERIODS
        assert "60d" in AggregationComputer.PERIODS

    def test_compute_all_calls_sub_methods(self):
        """compute_all should call all compute methods."""
        mock_db = MagicMock()
        mock_db._conn = True
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (date(2025, 12, 30),)
        mock_cursor.fetchall.return_value = [(date(2025, 12, 30),), (date(2025, 12, 29),)]
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        computer = AggregationComputer(mock_db)

        # Mock all the compute methods and get_trade_date_count
        with patch.object(computer, 'compute_daily_totals') as mock_daily, \
             patch.object(computer, 'compute_broker_aggregates') as mock_broker, \
             patch.object(computer, 'compute_ticker_aggregates') as mock_ticker, \
             patch.object(computer, 'compute_broker_symbol_aggregates'), \
             patch.object(computer, 'compute_top_netval_insights'), \
             patch.object(computer, '_update_pct_calculations'), \
             patch('aggregates.get_trade_date_count', return_value=60):

            computer.compute_all(date(2025, 12, 24))

            # Should call daily totals
            mock_daily.assert_called()

            # Should call aggregates for each period
            assert mock_broker.call_count >= 1
            assert mock_ticker.call_count >= 1


class TestAggregationComputerEdgeCases:
    """Edge case tests for aggregation."""

    def test_today_returns_empty_if_no_crawls(self):
        """Today period should return empty list if no crawls exist."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (None,)
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        result = get_period_trade_dates(mock_db, "today")

        assert result == []

    def test_10d_parses_correctly(self):
        """'10d' should correctly parse to 10 days."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(date(2025, 12, i),) for i in range(30, 20, -1)]
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        result = get_period_trade_dates(mock_db, "10d")

        assert len(result) == 10

    def test_60d_parses_correctly(self):
        """'60d' should correctly parse to 60 days."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        # Simulate having 60 trade dates
        mock_cursor.fetchall.return_value = [(date(2025, i % 12 + 1, (i % 28) + 1),) for i in range(60)]
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        result = get_period_trade_dates(mock_db, "60d")

        assert len(result) == 60
