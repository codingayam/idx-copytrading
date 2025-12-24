from datetime import date
from holidays import is_idx_trading_day, get_next_trading_day, get_previous_trading_day

def test_is_idx_trading_day_weekend():
    # Saturday
    assert is_idx_trading_day(date(2025, 12, 27)) is False
    # Sunday
    assert is_idx_trading_day(date(2025, 12, 28)) is False

def test_is_idx_trading_day_holiday():
    # Christmas Day 2025
    assert is_idx_trading_day(date(2025, 12, 25)) is False
    # New Year 2025
    assert is_idx_trading_day(date(2025, 1, 1)) is False

def test_is_idx_trading_day_normal():
    # A random Wednesday (today for you)
    assert is_idx_trading_day(date(2025, 12, 24)) is True

def test_get_next_trading_day():
    # From Saturday to Monday
    dec_20_2025 = date(2025, 12, 20) # Saturday
    dec_22_2025 = date(2025, 12, 22) # Monday
    assert get_next_trading_day(dec_20_2025) == dec_22_2025

def test_get_previous_trading_day():
    # From Monday to Friday
    dec_22_2025 = date(2025, 12, 22) # Monday
    dec_19_2025 = date(2025, 12, 19) # Friday
    assert get_previous_trading_day(dec_22_2025) == dec_19_2025
