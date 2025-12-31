"""
Aggregation computation for IDX Copytrading System.

This module computes aggregate statistics from raw broker trade data
after each successful crawl:
- Aggregates by broker (across all symbols)
- Aggregates by ticker (across all brokers)
- Aggregates by broker-symbol pairs
- Daily market totals
- Daily insights (top movers)

Optimization: Only recomputes periods that need updating based on
the number of available trade dates.
"""

import logging
from datetime import date, datetime

from db import Database, get_database

logger = logging.getLogger(__name__)


def get_latest_crawl_date(db: Database) -> date | None:
    """Get the latest successful crawl date from crawl_log."""
    with db.cursor() as cur:
        cur.execute("SELECT MAX(crawl_date) FROM crawl_log WHERE status = 'success'")
        result = cur.fetchone()
        return result[0] if result and result[0] else None


def get_trade_date_count(db: Database) -> int:
    """Get the total number of distinct trade dates in the database."""
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT trade_date) FROM broker_trades")
        result = cur.fetchone()
        return result[0] if result else 0


def get_period_trade_dates(db: Database, period: str) -> list[date]:
    """
    Get the list of trade dates for a period.

    - "today": Returns [latest_crawl_date]
    - "Nd": Returns last N distinct trade dates

    Args:
        db: Database connection
        period: One of "today", "2d", "3d", "5d", "10d", "20d", "60d"

    Returns:
        List of trade dates (descending order)
    """
    with db.cursor() as cur:
        if period == "today":
            # Get latest successful crawl date
            cur.execute("SELECT MAX(crawl_date) FROM crawl_log WHERE status = 'success'")
            result = cur.fetchone()
            return [result[0]] if result and result[0] else []

        # Parse "Nd" format (e.g., "2d" -> 2, "10d" -> 10)
        try:
            n = int(period.replace("d", ""))
        except ValueError:
            logger.error(f"Invalid period format: {period}")
            return []

        # Get last N distinct trade dates
        cur.execute("""
            SELECT DISTINCT trade_date
            FROM broker_trades
            ORDER BY trade_date DESC
            LIMIT %s
        """, (n,))
        return [row[0] for row in cur.fetchall()]


def get_period_days(period: str) -> int:
    """Get the number of days for a period."""
    if period == "today":
        return 1
    try:
        return int(period.replace("d", ""))
    except ValueError:
        return 0


class AggregationComputer:
    """Computes and stores aggregate statistics."""

    PERIODS = ["today", "2d", "3d", "5d", "10d", "20d", "60d"]

    def __init__(self, db: Database | None = None):
        self.db = db or get_database()

    def compute_all(self, reference_date: date | None = None, force_all: bool = False):
        """
        Compute all aggregates for all periods.

        Args:
            reference_date: Reference date for computations (defaults to latest crawl date)
            force_all: If True, recompute all periods regardless of data availability
        """
        if reference_date is None:
            reference_date = get_latest_crawl_date(self.db)
            if not reference_date:
                logger.error("No successful crawl found, cannot compute aggregates")
                return

        logger.info(f"Computing aggregates for {reference_date}...")
        start_time = datetime.now()

        # 1. Compute daily totals for latest date
        self.compute_daily_totals(reference_date)

        # 2. Get the number of available trade dates for smart recomputation
        available_days = get_trade_date_count(self.db)
        logger.info(f"  Available trade dates: {available_days}")

        # 3. Compute aggregates for each period (with smart skipping)
        for period in self.PERIODS:
            period_days = get_period_days(period)

            # Skip periods that can't have new data
            # Only recompute if: force_all OR period_days <= available_days
            if not force_all and period_days > available_days:
                logger.info(f"  Skipping {period} (requires {period_days} days, only {available_days} available)")
                continue

            trade_dates = get_period_trade_dates(self.db, period)

            if not trade_dates:
                logger.warning(f"  No trade dates for period {period}, skipping")
                continue

            period_start = min(trade_dates)
            period_end = max(trade_dates)

            logger.info(f"  Computing {period} aggregates ({len(trade_dates)} days: {period_start} to {period_end})...")

            self.compute_broker_aggregates(period, trade_dates, period_start, period_end)
            self.compute_ticker_aggregates(period, trade_dates, period_start, period_end)
            self.compute_broker_symbol_aggregates(period, trade_dates, period_start, period_end)

        # 4. Update percentage calculations
        self._update_pct_calculations()

        # 5. Compute insights
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

    def compute_broker_aggregates(self, period: str, trade_dates: list[date], period_start: date, period_end: date):
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
                    COALESCE(AVG(bavg), 0) as weighted_bavg,
                    COALESCE(AVG(savg), 0) as weighted_savg,
                    COUNT(*) as trade_count,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date = ANY(%s)
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
                (period, period_start, period_end, trade_dates)
            )

    def compute_ticker_aggregates(self, period: str, trade_dates: list[date], period_start: date, period_end: date):
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
                    COALESCE(AVG(bavg), 0) as weighted_bavg,
                    COALESCE(AVG(savg), 0) as weighted_savg,
                    COUNT(*) as trade_count,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date = ANY(%s)
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
                (period, period_start, period_end, trade_dates)
            )

    def compute_broker_symbol_aggregates(self, period: str, trade_dates: list[date], period_start: date, period_end: date):
        """Compute aggregates for broker-symbol pairs (for drill-down views)."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aggregates_broker_symbol (
                    broker_code, symbol, period, period_start, period_end,
                    netval_sum, bval_sum, sval_sum,
                    weighted_bavg, weighted_savg, computed_at
                )
                SELECT
                    broker_code,
                    symbol,
                    %s as period,
                    %s as period_start,
                    %s as period_end,
                    COALESCE(SUM(netval), 0) as netval_sum,
                    COALESCE(SUM(bval), 0) as bval_sum,
                    COALESCE(SUM(sval), 0) as sval_sum,
                    COALESCE(AVG(bavg), 0) as weighted_bavg,
                    COALESCE(AVG(savg), 0) as weighted_savg,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date = ANY(%s)
                GROUP BY broker_code, symbol
                ON CONFLICT (broker_code, symbol, period)
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
                (period, period_start, period_end, trade_dates)
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
        # Get the last N trade dates
        trade_dates = get_period_trade_dates(self.db, f"{lookback_days}d")

        if not trade_dates:
            logger.warning(f"No trade dates for insights {insight_type}")
            return

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
                    AVG(bavg) as bavg,
                    AVG(savg) as savg,
                    ROW_NUMBER() OVER (ORDER BY SUM(netval) DESC) as rank,
                    NOW() as computed_at
                FROM broker_trades
                WHERE trade_date = ANY(%s)
                GROUP BY symbol, broker_code
                ORDER BY SUM(netval) DESC
                LIMIT %s
                """,
                (reference_date, insight_type, trade_dates, top_n)
            )


def compute_aggregates(reference_date: date | None = None, force_all: bool = False):
    """
    Convenience function to compute all aggregates.

    Args:
        reference_date: Reference date (defaults to latest crawl date)
        force_all: If True, recompute all periods regardless of data availability
    """
    db = get_database()
    if not db.connect():
        logger.error("Failed to connect to database")
        return False

    try:
        computer = AggregationComputer(db)
        computer.compute_all(reference_date, force_all=force_all)
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
