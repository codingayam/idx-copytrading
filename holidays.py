"""
IDX (Indonesia Stock Exchange) Holiday Calendar.

This module tracks IDX trading holidays to skip crawls on non-trading days.
"""

from datetime import date

# IDX Holidays for 2024 and 2025
# Source: https://www.idx.co.id/en/about-idx/exchange-holiday/
IDX_HOLIDAYS = {
    # 2024 Holidays
    "2024-01-01",  # New Year's Day
    "2024-02-08",  # Chinese New Year (Imlek)
    "2024-02-10",  # Chinese New Year (Imlek) - Additional
    "2024-03-11",  # Nyepi (Balinese Day of Silence)
    "2024-03-28",  # Joint Leave
    "2024-03-29",  # Good Friday
    "2024-04-10",  # Eid ul-Fitr (Idul Fitri)
    "2024-04-11",  # Eid ul-Fitr (Idul Fitri)
    "2024-04-12",  # Eid ul-Fitr (Idul Fitri) - Joint Leave
    "2024-04-15",  # Eid ul-Fitr (Idul Fitri) - Joint Leave
    "2024-05-01",  # Labor Day
    "2024-05-09",  # Ascension Day of Jesus Christ
    "2024-05-10",  # Joint Leave
    "2024-05-23",  # Waisak Day (Buddha's Birthday)
    "2024-05-24",  # Joint Leave
    "2024-06-17",  # Eid ul-Adha (Idul Adha)
    "2024-06-18",  # Joint Leave
    "2024-07-07",  # Islamic New Year (Hijri)
    "2024-08-17",  # Indonesia Independence Day
    "2024-09-16",  # Maulid Nabi (Prophet Muhammad's Birthday)
    "2024-12-25",  # Christmas Day
    "2024-12-26",  # Joint Leave
    
    # 2025 Holidays
    "2025-01-01",  # New Year's Day
    "2025-01-27",  # Isra Mi'raj (Prophet's Ascension)
    "2025-01-28",  # Joint Leave
    "2025-01-29",  # Chinese New Year (Imlek)
    "2025-01-30",  # Chinese New Year (Imlek) - Day 2
    "2025-03-28",  # Hari Raya Nyepi (Balinese New Year)
    "2025-03-31",  # Eid ul-Fitr (Idul Fitri)
    "2025-04-01",  # Eid ul-Fitr (Idul Fitri)
    "2025-04-02",  # Joint Leave
    "2025-04-03",  # Joint Leave
    "2025-04-04",  # Joint Leave
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Labor Day
    "2025-05-12",  # Waisak Day (Buddha's Birthday)
    "2025-05-29",  # Ascension Day of Jesus Christ
    "2025-05-30",  # Joint Leave
    "2025-06-06",  # Eid ul-Adha (Idul Adha)
    "2025-06-27",  # Islamic New Year (Hijri)
    "2025-08-17",  # Indonesia Independence Day
    "2025-08-18",  # Joint Leave
    "2025-09-05",  # Maulid Nabi (Prophet Muhammad's Birthday)
    "2025-12-25",  # Christmas Day
    "2025-12-26",  # Joint Leave
}


def is_idx_trading_day(check_date: date | None = None) -> bool:
    """
    Check if the given date is an IDX trading day.
    
    Args:
        check_date: Date to check. Defaults to today.
        
    Returns:
        True if IDX is open for trading, False otherwise.
    """
    if check_date is None:
        check_date = date.today()
    
    # Check if weekend (Saturday=5, Sunday=6)
    if check_date.weekday() >= 5:
        return False
    
    # Check if holiday
    date_str = check_date.strftime("%Y-%m-%d")
    if date_str in IDX_HOLIDAYS:
        return False
    
    return True


def get_next_trading_day(from_date: date | None = None) -> date:
    """
    Get the next trading day from the given date.
    
    Args:
        from_date: Starting date. Defaults to today.
        
    Returns:
        The next date when IDX is open for trading.
    """
    from datetime import timedelta
    
    if from_date is None:
        from_date = date.today()
    
    check_date = from_date
    while not is_idx_trading_day(check_date):
        check_date += timedelta(days=1)
        # Safety: don't look more than 30 days ahead
        if (check_date - from_date).days > 30:
            raise ValueError("Could not find trading day within 30 days")
    
    return check_date


def get_previous_trading_day(from_date: date | None = None) -> date:
    """
    Get the previous trading day from the given date.
    
    Args:
        from_date: Starting date. Defaults to today.
        
    Returns:
        The most recent date when IDX was open for trading.
    """
    from datetime import timedelta
    
    if from_date is None:
        from_date = date.today()
    
    check_date = from_date - timedelta(days=1)
    while not is_idx_trading_day(check_date):
        check_date -= timedelta(days=1)
        # Safety: don't look more than 30 days back
        if (from_date - check_date).days > 30:
            raise ValueError("Could not find trading day within 30 days")
    
    return check_date
