"""
Tests for the aggregates module.

These tests verify date range calculations and aggregation logic.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch
from aggregates import get_period_dates, AggregationComputer


class TestGetPeriodDates:
    """Tests for get_period_dates function."""

    def test_today_period(self):
        """'today' should return same date for start and end."""
        ref_date = date(2025, 12, 24)
        start, end = get_period_dates("today", ref_date)

        assert start == ref_date
        assert end == ref_date

    def test_week_period(self):
        """'week' should return 7 days back from reference date."""
        ref_date = date(2025, 12, 24)
        start, end = get_period_dates("week", ref_date)

        assert end == ref_date
        assert start == ref_date - timedelta(days=7)

    def test_month_period(self):
        """'month' should return 30 days back from reference date."""
        ref_date = date(2025, 12, 24)
        start, end = get_period_dates("month", ref_date)

        assert end == ref_date
        assert start == ref_date - timedelta(days=30)

    def test_ytd_period(self):
        """'ytd' should return from January 1st of the year."""
        ref_date = date(2025, 12, 24)
        start, end = get_period_dates("ytd", ref_date)

        assert end == ref_date
        assert start == date(2025, 1, 1)

    def test_all_period(self):
        """'all' should return an early start date."""
        ref_date = date(2025, 12, 24)
        start, end = get_period_dates("all", ref_date)

        assert end == ref_date
        # Start date should be at least 1 year before end
        assert start < ref_date

    def test_defaults_to_today(self):
        """Should default to today if no reference date provided."""
        today = date.today()
        start, end = get_period_dates("today")

        assert end == today
        assert start == today


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
        assert "week" in AggregationComputer.PERIODS
        assert "month" in AggregationComputer.PERIODS
        assert "ytd" in AggregationComputer.PERIODS
        assert "all" in AggregationComputer.PERIODS

    def test_compute_all_calls_sub_methods(self):
        """compute_all should call all compute methods."""
        mock_db = MagicMock()
        mock_db._conn = True
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=None)

        computer = AggregationComputer(mock_db)

        # Mock all the compute methods
        with patch.object(computer, 'compute_daily_totals') as mock_daily, \
             patch.object(computer, 'compute_broker_aggregates') as mock_broker, \
             patch.object(computer, 'compute_ticker_aggregates') as mock_ticker, \
             patch.object(computer, 'compute_broker_symbol_aggregates'), \
             patch.object(computer, 'compute_top_netval_insights'):

            computer.compute_all(date(2025, 12, 24))

            # Should call daily totals
            mock_daily.assert_called()

            # Should call aggregates for each period
            assert mock_broker.call_count >= 1
            assert mock_ticker.call_count >= 1


class TestAggregationComputerEdgeCases:
    """Edge case tests for aggregation."""

    def test_ytd_on_january_first(self):
        """YTD on Jan 1 should have start == end."""
        ref_date = date(2025, 1, 1)
        start, end = get_period_dates("ytd", ref_date)

        assert start == end == date(2025, 1, 1)

    def test_week_period_crosses_month(self):
        """Week period should work when crossing month boundaries."""
        ref_date = date(2025, 1, 3)  # January 3rd
        start, end = get_period_dates("week", ref_date)

        assert start == date(2024, 12, 27)  # December 27th
        assert end == date(2025, 1, 3)

    def test_week_period_crosses_year(self):
        """Week period should work when crossing year boundaries."""
        ref_date = date(2025, 1, 5)
        start, end = get_period_dates("week", ref_date)

        assert start.year == 2024
        assert end.year == 2025
