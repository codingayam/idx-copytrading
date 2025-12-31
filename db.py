"""
Database connection and operations for IDX Copytrading System.

This module provides PostgreSQL database connectivity and helper functions
for inserting crawled data and managing aggregations.
"""

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Generator

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration from environment variables."""
    database_url: str = ""

    def __post_init__(self):
        self.database_url = os.getenv("DATABASE_URL", "")
        if not self.database_url:
            # Fallback to individual components
            host = os.getenv("DB_HOST", "localhost")
            port = os.getenv("DB_PORT", "5432")
            name = os.getenv("DB_NAME", "idx_copytrading")
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "")
            self.database_url = f"postgresql://{user}:{password}@{host}:{port}/{name}"


class Database:
    """PostgreSQL database connection manager."""

    def __init__(self, config: DatabaseConfig | None = None):
        self.config = config or DatabaseConfig()
        self._conn = None

    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self._conn = psycopg2.connect(self.config.database_url)
            self._conn.autocommit = False
            logger.info("Database connection established")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def disconnect(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Database connection closed")

    @contextmanager
    def transaction(self) -> Generator[Any, None, None]:
        """Context manager for database transactions with automatic rollback on error."""
        if not self._conn:
            raise RuntimeError("Database not connected")

        cursor = self._conn.cursor()
        try:
            yield cursor
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise
        finally:
            cursor.close()

    @contextmanager
    def cursor(self) -> Generator[Any, None, None]:
        """Context manager for simple cursor operations (auto-commit)."""
        if not self._conn:
            raise RuntimeError("Database not connected")

        cursor = self._conn.cursor()
        try:
            yield cursor
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cursor.close()

    # ==========================================
    # Crawl Log Operations
    # ==========================================

    def has_successful_crawl_today(self, crawl_date: date) -> bool:
        """Check if there's already a successful crawl for the given date."""
        with self.cursor() as cur:
            cur.execute(
                "SELECT id FROM crawl_log WHERE crawl_date = %s AND status = 'success'",
                (crawl_date,)
            )
            return cur.fetchone() is not None

    def start_crawl_log(self, crawl_date: date) -> int:
        """Create a new crawl log entry and return its ID."""
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crawl_log (crawl_date, crawl_start, status)
                VALUES (%s, %s, 'running')
                RETURNING id
                """,
                (crawl_date, datetime.now())
            )
            return cur.fetchone()[0]

    def update_crawl_log(
        self,
        log_id: int,
        status: str,
        total_rows: int | None = None,
        successful_brokers: int | None = None,
        failed_brokers: int | None = None,
        error_message: str | None = None
    ):
        """Update a crawl log entry."""
        with self.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl_log
                SET status = %s,
                    crawl_end = %s,
                    total_rows = COALESCE(%s, total_rows),
                    successful_brokers = COALESCE(%s, successful_brokers),
                    failed_brokers = COALESCE(%s, failed_brokers),
                    error_message = COALESCE(%s, error_message)
                WHERE id = %s
                """,
                (status, datetime.now(), total_rows, successful_brokers,
                 failed_brokers, error_message, log_id)
            )

    # ==========================================
    # Broker Trades Operations
    # ==========================================

    def insert_broker_trades(self, trades: list[dict], crawl_timestamp: datetime) -> int:
        """
        Insert broker trade data with UPSERT behavior.

        Args:
            trades: List of trade dictionaries with keys matching broker_trades columns
            crawl_timestamp: Timestamp of the crawl

        Returns:
            Number of rows inserted/updated
        """
        if not trades:
            return 0

        # Data validation
        valid_trades = []
        for trade in trades:
            if self._validate_trade(trade):
                valid_trades.append(trade)
            else:
                logger.warning(f"Invalid trade data skipped: {trade}")

        if not valid_trades:
            return 0

        # Parse crawl_date from the first trade
        crawl_date = valid_trades[0].get("crawl_date")
        if isinstance(crawl_date, str):
            crawl_date = datetime.strptime(crawl_date, "%Y-%m-%d").date()

        # Prepare data for bulk insert
        values = [
            (
                trade["broker_code"],
                trade["symbol"],
                crawl_date,
                Decimal(str(trade.get("netval", 0))),
                Decimal(str(trade.get("bval", 0))),
                Decimal(str(trade.get("sval", 0))),
                Decimal(str(trade.get("bavg", 0))),
                Decimal(str(trade.get("savg", 0))),
                crawl_timestamp,
            )
            for trade in valid_trades
        ]

        with self.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO broker_trades
                    (broker_code, symbol, trade_date, netval, bval, sval, bavg, savg, crawl_timestamp)
                VALUES %s
                ON CONFLICT (broker_code, symbol, trade_date)
                DO UPDATE SET
                    netval = EXCLUDED.netval,
                    bval = EXCLUDED.bval,
                    sval = EXCLUDED.sval,
                    bavg = EXCLUDED.bavg,
                    savg = EXCLUDED.savg,
                    crawl_timestamp = EXCLUDED.crawl_timestamp
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            return len(valid_trades)

    def _validate_trade(self, trade: dict) -> bool:
        """Validate trade data before insertion."""
        required_fields = ["broker_code", "symbol", "crawl_date"]

        # Check required fields
        for field in required_fields:
            if not trade.get(field):
                logger.debug(f"Missing required field: {field}")
                return False

        # Validate broker code format (2-4 uppercase letters)
        broker_code = trade.get("broker_code", "")
        if not (2 <= len(broker_code) <= 4 and broker_code.isalpha()):
            logger.debug(f"Invalid broker code format: {broker_code}")
            return False

        # Validate symbol format (1-10 alphanumeric characters)
        symbol = trade.get("symbol", "")
        if not (1 <= len(symbol) <= 10 and (symbol.isalnum() or '-' in symbol)):
            logger.debug(f"Invalid symbol format: {symbol}")
            return False

        # Validate numeric fields are non-negative
        for field in ["bval", "sval"]:
            value = trade.get(field, 0)
            try:
                if float(value) < 0:
                    logger.debug(f"Negative {field}: {value}")
                    return False
            except (ValueError, TypeError):
                logger.debug(f"Invalid {field}: {value}")
                return False

        return True

    # ==========================================
    # Symbol Operations
    # ==========================================

    def update_symbols(self, trades: list[dict], trade_date: date):
        """Update symbols table based on crawled trades."""
        if not trades:
            return

        # Get unique valid symbols from trades
        symbols = {
            trade["symbol"]
            for trade in trades
            if trade.get("symbol") and len(trade["symbol"]) <= 10
        }

        with self.cursor() as cur:
            for symbol in symbols:
                cur.execute(
                    """
                    INSERT INTO symbols (symbol, first_seen, last_seen, is_active)
                    VALUES (%s, %s, %s, true)
                    ON CONFLICT (symbol)
                    DO UPDATE SET
                        last_seen = GREATEST(symbols.last_seen, EXCLUDED.last_seen),
                        is_active = true
                    """,
                    (symbol, trade_date, trade_date)
                )

    # ==========================================
    # Health Check
    # ==========================================

    def get_health_status(self) -> dict:
        """Get database health status for API endpoint."""
        try:
            with self.cursor() as cur:
                # Check connection
                cur.execute("SELECT 1")

                # Get last successful crawl
                cur.execute(
                    """
                    SELECT crawl_date, crawl_end, total_rows
                    FROM crawl_log
                    WHERE status = 'success'
                    ORDER BY crawl_end DESC
                    LIMIT 1
                    """
                )
                last_crawl = cur.fetchone()

                return {
                    "dbConnected": True,
                    "lastCrawl": {
                        "date": last_crawl[0].isoformat() if last_crawl else None,
                        "completedAt": last_crawl[1].isoformat() if last_crawl else None,
                        "totalRows": last_crawl[2] if last_crawl else None,
                    } if last_crawl else None,
                    "status": "healthy"
                }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "dbConnected": False,
                "lastCrawl": None,
                "status": "unhealthy",
                "error": str(e)
            }


# Global database instance (singleton pattern)
_db_instance: Database | None = None


def get_database() -> Database:
    """Get or create the global database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance
