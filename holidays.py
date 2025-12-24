"""
IDX (Indonesia Stock Exchange) Holiday Calendar.

This module uses the exchange_calendars library to determine IDX trading days.
The library is maintained by the open-source community and includes:
- All IDX holidays (national holidays, joint leaves)
- Weekend detection
- Historical data back to 1977

Usage:
    from holidays import is_idx_trading_day, get_next_trading_day

    if is_idx_trading_day():
        print("IDX is open today!")
"""

from datetime import date, timedelta
from functools import lru_cache

import pandas as pd
from exchange_calendars import get_calendar


@lru_cache(maxsize=1)
def _get_idx_calendar():
    """
    Get the IDX exchange calendar (cached singleton).

    Returns:
        ExchangeCalendar for Indonesia Stock Exchange (XIDX).
    """
    return get_calendar('XIDX')


def is_idx_trading_day(check_date: date | None = None) -> bool:
    """
    Check if the given date is an IDX trading day.

    Uses the exchange_calendars library which maintains an up-to-date
    calendar of IDX holidays including weekends, national holidays,
    and joint leave days.

    Args:
        check_date: Date to check. Defaults to today.

    Returns:
        True if IDX is open for trading, False otherwise.
    """
    if check_date is None:
        check_date = date.today()

    calendar = _get_idx_calendar()
    timestamp = pd.Timestamp(check_date)

    try:
        return calendar.is_session(timestamp)
    except ValueError:
        # Date is outside calendar range (unlikely for current dates)
        # Fall back to simple weekend check
        return check_date.weekday() < 5


def get_next_trading_day(from_date: date | None = None) -> date:
    """
    Get the next trading day from the given date.

    If from_date is already a trading day, returns from_date.
    Otherwise, finds the next day when IDX is open.

    Args:
        from_date: Starting date. Defaults to today.

    Returns:
        The next date when IDX is open for trading.

    Raises:
        ValueError: If no trading day found within 30 days.
    """
    if from_date is None:
        from_date = date.today()

    calendar = _get_idx_calendar()
    timestamp = pd.Timestamp(from_date)

    try:
        # If it's already a session, return it
        if calendar.is_session(timestamp):
            return from_date

        # Find next valid session
        next_session = calendar.next_open(timestamp)
        result_date = next_session.date()

        # Safety check
        if (result_date - from_date).days > 30:
            raise ValueError("Could not find trading day within 30 days")

        return result_date
    except Exception:
        # Fallback: iterate until we find a trading day
        check_date = from_date
        for _ in range(31):
            if is_idx_trading_day(check_date):
                return check_date
            check_date += timedelta(days=1)
        raise ValueError("Could not find trading day within 30 days")


def get_previous_trading_day(from_date: date | None = None) -> date:
    """
    Get the previous trading day from the given date.

    Args:
        from_date: Starting date. Defaults to today.

    Returns:
        The most recent date when IDX was open for trading.

    Raises:
        ValueError: If no trading day found within 30 days.
    """
    if from_date is None:
        from_date = date.today()

    calendar = _get_idx_calendar()
    timestamp = pd.Timestamp(from_date)

    try:
        # Find previous valid session (excludes from_date)
        prev_session = calendar.previous_close(timestamp)
        result_date = prev_session.date()

        # Safety check
        if (from_date - result_date).days > 30:
            raise ValueError("Could not find trading day within 30 days")

        return result_date
    except Exception:
        # Fallback: iterate backwards until we find a trading day
        check_date = from_date - timedelta(days=1)
        for _ in range(31):
            if is_idx_trading_day(check_date):
                return check_date
            check_date -= timedelta(days=1)
        raise ValueError("Could not find trading day within 30 days")


def get_trading_days_in_range(start_date: date, end_date: date) -> list[date]:
    """
    Get all trading days in a date range.

    Args:
        start_date: Start of range (inclusive).
        end_date: End of range (inclusive).

    Returns:
        List of dates when IDX was/will be open.
    """
    calendar = _get_idx_calendar()

    sessions = calendar.sessions_in_range(
        pd.Timestamp(start_date),
        pd.Timestamp(end_date)
    )

    return [session.date() for session in sessions]
