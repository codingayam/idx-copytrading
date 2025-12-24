"""
Aggregation computation for IDX Copytrading System.

This module computes aggregate statistics from raw broker trade data
after each successful crawl:
- Aggregates by broker (across all symbols)
- Aggregates by ticker (across all brokers)
- Aggregates by broker-symbol pairs
- Daily market totals
- Daily insights (top movers)
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any

from db import Database, get_database

logger = logging.getLogger(__name__)


def get_period_dates(period: str, reference_date: date | None = None) -> tuple[date, date]:
    """
    Get start and end dates for a given period.
    
    Args:
        period: One of "today", "week", "month", "ytd", "all"
        reference_date: Reference date (defaults to today)
        
    Returns:
        Tuple of (start_date, end_date)
    """
    if reference_date is None:
        reference_date = date.today()
    
    if period == "today":
        return (reference_date, reference_date)
    elif period == "week":
        return (reference_date - timedelta(days=7), reference_date)
    elif period == "month":
        return (reference_date - timedelta(days=30), reference_date)
    elif period == "ytd":
        year_start = date(reference_date.year, 1, 1)
        return (year_start, reference_date)
    elif period == "all":
        # Use a very early date for "all time"
        return (date(2024, 1, 1), reference_date)
    else:
        raise ValueError(f"Unknown period: {period}")


class AggregationComputer:
    """Computes and stores aggregate statistics."""
    
    PERIODS = ["today", "week", "month", "ytd", "all"]
    
    def __init__(self, db: Database | None = None):
        self.db = db or get_database()
    
    def compute_all(self, reference_date: date | None = None):
        """
        Compute all aggregates for all periods.
        
        Args:
            reference_date: Reference date for computations (defaults to today)
        """
        if reference_date is None:
            reference_date = date.today()
        
        logger.info(f"Computing aggregates for {reference_date}...")
        start_time = datetime.now()
        
        # 1. Compute daily totals
        self.compute_daily_totals(reference_date)
        
        # 2. Compute aggregates for each period
        for period in self.PERIODS:
            start_date, end_date = get_period_dates(period, reference_date)
            
            logger.info(f"  Computing {period} aggregates ({start_date} to {end_date})...")
            
            self.compute_broker_aggregates(period, start_date, end_date)
            self.compute_ticker_aggregates(period, start_date, end_date)
            self.compute_broker_symbol_aggregates(period, start_date, end_date)
        
        # 3. Update percentage calculations
        self._update_pct_calculations()
        
        # 4. Compute insights
        logger.info("  Computing daily insights...")
        self.compute_top_netval_insights(reference_date, lookback_days=5, insight_type="top_netval_5d")
        self.compute_top_netval_insights(reference_date, lookback_days=30, insight_type="top_netval_month")
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Aggregation completed in {duration:.1f}s")
    
    def compute_daily_totals(self, trade_date: date):
        """Compute and store daily market totals."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_totals (
                    trade_date, total_market_bval, total_market_sval, 
                    total_market_volume, active_symbols, active_brokers, computed_at
                )
                SELECT 
                    %s as trade_date,
                    COALESCE(SUM(bval), 0) as total_market_bval,
                    COALESCE(SUM(sval), 0) as total_market_sval,
                    COALESCE(SUM(bval + sval), 0) as total_market_volume,
                    COUNT(DISTINCT symbol) as active_symbols,
                    COUNT(DISTINCT broker_code) as active_brokers,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date = %s
                ON CONFLICT (trade_date)
                DO UPDATE SET
                    total_market_bval = EXCLUDED.total_market_bval,
                    total_market_sval = EXCLUDED.total_market_sval,
                    total_market_volume = EXCLUDED.total_market_volume,
                    active_symbols = EXCLUDED.active_symbols,
                    active_brokers = EXCLUDED.active_brokers,
                    computed_at = EXCLUDED.computed_at
                """,
                (trade_date, trade_date)
            )
    
    def compute_broker_aggregates(self, period: str, start_date: date, end_date: date):
        """Compute aggregates grouped by broker (all symbols combined)."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aggregates_by_broker (
                    broker_code, period, period_start, period_end,
                    total_netval, total_bval, total_sval,
                    weighted_bavg, weighted_savg, trade_count, computed_at
                )
                SELECT 
                    broker_code,
                    %s as period,
                    %s as period_start,
                    %s as period_end,
                    COALESCE(SUM(netval), 0) as total_netval,
                    COALESCE(SUM(bval), 0) as total_bval,
                    COALESCE(SUM(sval), 0) as total_sval,
                    CASE 
                        WHEN SUM(bval) > 0 THEN SUM(bavg * bval) / SUM(bval)
                        ELSE 0 
                    END as weighted_bavg,
                    CASE 
                        WHEN SUM(sval) > 0 THEN SUM(savg * sval) / SUM(sval)
                        ELSE 0 
                    END as weighted_savg,
                    COUNT(*) as trade_count,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY broker_code
                ON CONFLICT (broker_code, period)
                DO UPDATE SET
                    period_start = EXCLUDED.period_start,
                    period_end = EXCLUDED.period_end,
                    total_netval = EXCLUDED.total_netval,
                    total_bval = EXCLUDED.total_bval,
                    total_sval = EXCLUDED.total_sval,
                    weighted_bavg = EXCLUDED.weighted_bavg,
                    weighted_savg = EXCLUDED.weighted_savg,
                    trade_count = EXCLUDED.trade_count,
                    computed_at = EXCLUDED.computed_at
                """,
                (period, start_date, end_date, start_date, end_date)
            )
    
    def compute_ticker_aggregates(self, period: str, start_date: date, end_date: date):
        """Compute aggregates grouped by symbol/ticker (all brokers combined)."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aggregates_by_ticker (
                    symbol, period, period_start, period_end,
                    total_netval, total_bval, total_sval,
                    weighted_bavg, weighted_savg, trade_count, computed_at
                )
                SELECT 
                    symbol,
                    %s as period,
                    %s as period_start,
                    %s as period_end,
                    COALESCE(SUM(netval), 0) as total_netval,
                    COALESCE(SUM(bval), 0) as total_bval,
                    COALESCE(SUM(sval), 0) as total_sval,
                    CASE 
                        WHEN SUM(bval) > 0 THEN SUM(bavg * bval) / SUM(bval)
                        ELSE 0 
                    END as weighted_bavg,
                    CASE 
                        WHEN SUM(sval) > 0 THEN SUM(savg * sval) / SUM(sval)
                        ELSE 0 
                    END as weighted_savg,
                    COUNT(*) as trade_count,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY symbol
                ON CONFLICT (symbol, period)
                DO UPDATE SET
                    period_start = EXCLUDED.period_start,
                    period_end = EXCLUDED.period_end,
                    total_netval = EXCLUDED.total_netval,
                    total_bval = EXCLUDED.total_bval,
                    total_sval = EXCLUDED.total_sval,
                    weighted_bavg = EXCLUDED.weighted_bavg,
                    weighted_savg = EXCLUDED.weighted_savg,
                    trade_count = EXCLUDED.trade_count,
                    computed_at = EXCLUDED.computed_at
                """,
                (period, start_date, end_date, start_date, end_date)
            )
    
    def compute_broker_symbol_aggregates(self, period: str, start_date: date, end_date: date):
        """Compute aggregates for broker-symbol pairs (for drill-down views)."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aggregates_broker_symbol (
                    broker_code, symbol, table_type, period, period_start, period_end,
                    netval_sum, bval_sum, sval_sum,
                    weighted_bavg, weighted_savg, computed_at
                )
                SELECT 
                    broker_code,
                    symbol,
                    table_type,
                    %s as period,
                    %s as period_start,
                    %s as period_end,
                    COALESCE(SUM(netval), 0) as netval_sum,
                    COALESCE(SUM(bval), 0) as bval_sum,
                    COALESCE(SUM(sval), 0) as sval_sum,
                    CASE 
                        WHEN SUM(bval) > 0 THEN SUM(bavg * bval) / SUM(bval)
                        ELSE 0 
                    END as weighted_bavg,
                    CASE 
                        WHEN SUM(sval) > 0 THEN SUM(savg * sval) / SUM(sval)
                        ELSE 0 
                    END as weighted_savg,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY broker_code, symbol, table_type
                ON CONFLICT (broker_code, symbol, table_type, period)
                DO UPDATE SET
                    period_start = EXCLUDED.period_start,
                    period_end = EXCLUDED.period_end,
                    netval_sum = EXCLUDED.netval_sum,
                    bval_sum = EXCLUDED.bval_sum,
                    sval_sum = EXCLUDED.sval_sum,
                    weighted_bavg = EXCLUDED.weighted_bavg,
                    weighted_savg = EXCLUDED.weighted_savg,
                    computed_at = EXCLUDED.computed_at
                """,
                (period, start_date, end_date, start_date, end_date)
            )
    
    def _update_pct_calculations(self):
        """Update percentage of symbol volume for broker-symbol aggregates."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE aggregates_broker_symbol abs
                SET pct_of_symbol_volume = 
                    (abs.bval_sum + abs.sval_sum) / NULLIF(
                        (SELECT total_bval + total_sval 
                         FROM aggregates_by_ticker 
                         WHERE symbol = abs.symbol AND period = abs.period), 0
                    ) * 100
                """
            )
    
    def compute_top_netval_insights(
        self, 
        reference_date: date, 
        lookback_days: int = 5,
        insight_type: str = "top_netval_5d",
        top_n: int = 20
    ):
        """
        Compute top netval movers for the insights tab.
        
        Args:
            reference_date: End date for lookback period
            lookback_days: Number of days to look back
            insight_type: Type identifier for the insight
            top_n: Number of top entries to keep
        """
        start_date = reference_date - timedelta(days=lookback_days)
        
        with self.db.cursor() as cur:
            # Delete old insights of this type for this date
            cur.execute(
                "DELETE FROM daily_insights WHERE insight_date = %s AND insight_type = %s",
                (reference_date, insight_type)
            )
            
            # Insert new insights
            cur.execute(
                """
                INSERT INTO daily_insights (
                    insight_date, insight_type, symbol, broker_code,
                    netval, bval, sval, bavg, savg, rank, computed_at
                )
                SELECT 
                    %s as insight_date,
                    %s as insight_type,
                    symbol,
                    broker_code,
                    SUM(netval) as netval,
                    SUM(bval) as bval,
                    SUM(sval) as sval,
                    CASE WHEN SUM(bval) > 0 THEN SUM(bavg * bval) / SUM(bval) ELSE 0 END as bavg,
                    CASE WHEN SUM(sval) > 0 THEN SUM(savg * sval) / SUM(sval) ELSE 0 END as savg,
                    ROW_NUMBER() OVER (ORDER BY SUM(netval) DESC) as rank,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date BETWEEN %s AND %s
                GROUP BY symbol, broker_code
                ORDER BY SUM(netval) DESC
                LIMIT %s
                """,
                (reference_date, insight_type, start_date, reference_date, top_n)
            )


def compute_aggregates(reference_date: date | None = None):
    """
    Convenience function to compute all aggregates.
    
    Args:
        reference_date: Reference date (defaults to today)
    """
    db = get_database()
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    try:
        computer = AggregationComputer(db)
        computer.compute_all(reference_date)
        return True
    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        return False
    finally:
        db.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    compute_aggregates()
