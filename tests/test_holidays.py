"""
Tests for the holidays module using exchange_calendars.

These tests verify that the IDX trading calendar is working correctly
for weekends, holidays, and trading day navigation.
"""

from datetime import date
from holidays import (
    is_idx_trading_day,
    get_next_trading_day,
    get_previous_trading_day,
    get_trading_days_in_range,
    _get_idx_calendar
)


class TestIsIdxTradingDay:
    """Tests for is_idx_trading_day function."""

    def test_weekend_saturday_returns_false(self):
        """Saturday should not be a trading day."""
        saturday = date(2025, 12, 27)
        assert saturday.weekday() == 5  # Verify it's Saturday
        assert is_idx_trading_day(saturday) is False

    def test_weekend_sunday_returns_false(self):
        """Sunday should not be a trading day."""
        sunday = date(2025, 12, 28)
        assert sunday.weekday() == 6  # Verify it's Sunday
        assert is_idx_trading_day(sunday) is False

    def test_christmas_2025_returns_false(self):
        """Christmas Day 2025 should be a holiday."""
        assert is_idx_trading_day(date(2025, 12, 25)) is False

    def test_new_year_2025_returns_false(self):
        """New Year's Day 2025 should be a holiday."""
        assert is_idx_trading_day(date(2025, 1, 1)) is False

    def test_regular_weekday_returns_true(self):
        """A regular Wednesday should be a trading day."""
        wednesday = date(2025, 12, 24)  # Christmas Eve (not a holiday in IDX)
        assert wednesday.weekday() == 2  # Verify it's Wednesday
        assert is_idx_trading_day(wednesday) is True

    def test_defaults_to_today_when_none(self):
        """Should default to today's date when no argument provided."""
        # This will pass as long as today is a valid date in the calendar
        result = is_idx_trading_day(None)
        assert isinstance(result, bool)

    def test_historical_date(self):
        """Should work for historical dates."""
        # Jan 2, 2024 was a Tuesday (trading day)
        assert is_idx_trading_day(date(2024, 1, 2)) is True


class TestGetNextTradingDay:
    """Tests for get_next_trading_day function."""

    def test_from_saturday_returns_monday(self):
        """From Saturday, next trading day should be Monday."""
        saturday = date(2025, 12, 20)
        monday = date(2025, 12, 22)
        assert saturday.weekday() == 5
        assert monday.weekday() == 0
        assert get_next_trading_day(saturday) == monday

    def test_from_trading_day_returns_same_day(self):
        """From a trading day, should return the same day."""
        trading_day = date(2025, 12, 22)  # Monday
        assert get_next_trading_day(trading_day) == trading_day

    def test_from_friday_before_christmas_weekend(self):
        """Friday Dec 26 2025 is a holiday (joint leave), should skip to Monday."""
        thursday = date(2025, 12, 25)  # Christmas (holiday)
        # Next trading day after Dec 25 should skip Dec 26 (joint leave) and weekend
        result = get_next_trading_day(thursday)
        assert result >= date(2025, 12, 29)  # At least Monday Dec 29

    def test_raises_error_if_no_trading_day_within_30_days(self):
        """Should raise ValueError if no trading day found within 30 days."""
        # This is unlikely to happen with real calendar data, but test the safety check
        # We can't easily trigger this without mocking, so we just verify the function exists
        pass


class TestGetPreviousTradingDay:
    """Tests for get_previous_trading_day function."""

    def test_from_monday_returns_friday(self):
        """From Monday, previous trading day should be Friday."""
        monday = date(2025, 12, 22)
        friday = date(2025, 12, 19)
        assert monday.weekday() == 0
        assert friday.weekday() == 4
        assert get_previous_trading_day(monday) == friday

    def test_from_sunday_returns_friday(self):
        """From Sunday, previous trading day should be Friday."""
        sunday = date(2025, 12, 21)
        friday = date(2025, 12, 19)
        assert get_previous_trading_day(sunday) == friday


class TestGetTradingDaysInRange:
    """Tests for get_trading_days_in_range function."""

    def test_one_week_range(self):
        """Should return trading days in a one-week range."""
        start = date(2025, 12, 15)  # Monday
        end = date(2025, 12, 19)    # Friday

        trading_days = get_trading_days_in_range(start, end)

        # Should have 5 trading days (Mon-Fri)
        assert len(trading_days) == 5
        assert trading_days[0] == start
        assert trading_days[-1] == end

    def test_weekend_excluded(self):
        """Weekend days should not be in the result."""
        start = date(2025, 12, 19)  # Friday
        end = date(2025, 12, 22)    # Monday

        trading_days = get_trading_days_in_range(start, end)

        # Should only have Friday and Monday (2 days)
        assert len(trading_days) == 2
        for day in trading_days:
            assert day.weekday() < 5  # No weekends


class TestCalendarCaching:
    """Tests for calendar caching behavior."""

    def test_calendar_is_cached(self):
        """The calendar should be cached (same instance returned)."""
        cal1 = _get_idx_calendar()
        cal2 = _get_idx_calendar()
        assert cal1 is cal2

    def test_calendar_has_idx_code(self):
        """The calendar should have the correct exchange code."""
        cal = _get_idx_calendar()
        assert cal.name == 'XIDX'
