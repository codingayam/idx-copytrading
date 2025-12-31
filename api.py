"""
FastAPI REST API for IDX Copytrading System.

This module provides REST endpoints for the frontend to consume
broker trading data and aggregated statistics.

Endpoints:
- /api/health - Health check with last crawl info
- /api/brokers - List all brokers
- /api/brokers/{code}/aggregates - Broker aggregates by period
- /api/brokers/{code}/trades - Broker trades with pagination
- /api/tickers - List all active tickers
- /api/tickers/{symbol}/aggregates - Ticker aggregates by period
- /api/tickers/{symbol}/brokers - Brokers trading this ticker
- /api/insights - Top movers and market stats
"""

import logging
import os
from datetime import date, datetime
from enum import Enum
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from db import Database, get_database

# Caching imports
from cachetools import TTLCache
from datetime import timedelta
import pytz
from functools import wraps
import hashlib
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="IDX Copytrading API",
    description="API for broker trading data visualization",
    version="1.0.0",
)

# CORS middleware (for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
# Enums and Models
# ==========================================

class Period(str, Enum):
    today = "today"
    d2 = "2d"
    d3 = "3d"
    d5 = "5d"
    d10 = "10d"
    d20 = "20d"
    d60 = "60d"


class SortField(str, Enum):
    netval = "netval"
    bval = "bval"
    sval = "sval"
    bavg = "bavg"
    savg = "savg"
    pct_volume = "pct_volume"


class SortOrder(str, Enum):
    asc = "asc"
    desc = "desc"


class HealthResponse(BaseModel):
    status: str
    dbConnected: bool
    lastCrawl: dict | None


class BrokerResponse(BaseModel):
    code: str
    name: str


class PaginatedResponse(BaseModel):
    data: list[dict]
    total: int
    page: int
    limit: int
    pages: int


# ==========================================
# Database Connection Helper
# ==========================================

def get_db() -> Database:
    """Get database connection."""
    db = get_database()
    if not db._conn:
        db.connect()
    return db


# ==========================================
# Caching Infrastructure
# ==========================================

def get_seconds_until_next_crawl() -> int:
    """
    Calculate seconds until next 1pm UTC on a weekday.
    Crawl runs Mon-Fri at 13:00 UTC.
    """
    utc = pytz.UTC
    now = datetime.now(utc)

    # Start with next 1pm UTC today
    next_crawl = now.replace(hour=13, minute=0, second=0, microsecond=0)

    # If past 1pm today, move to tomorrow
    if now >= next_crawl:
        next_crawl += timedelta(days=1)

    # Skip weekends (5=Saturday, 6=Sunday)
    while next_crawl.weekday() >= 5:
        next_crawl += timedelta(days=1)

    seconds = int((next_crawl - now).total_seconds())
    # Minimum 60 seconds, maximum 5 days (to handle edge cases)
    return max(60, min(seconds, 5 * 24 * 3600))


# API response cache - stores cached endpoint responses
# maxsize=2000 handles all broker/ticker/period combinations
# TTL is dynamically set to expire at next crawl time
_api_cache: TTLCache = TTLCache(maxsize=2000, ttl=get_seconds_until_next_crawl())


def _make_cache_key(*args, **kwargs) -> str:
    """Create a unique cache key from function arguments."""
    key_data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
    return hashlib.md5(key_data.encode()).hexdigest()


def cached_endpoint(func):
    """
    Decorator to cache endpoint responses until next crawl.
    Cache key is based on function name and all arguments.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        cache_key = f"{func.__name__}:{_make_cache_key(*args, **kwargs)}"

        # Check cache
        if cache_key in _api_cache:
            logger.debug(f"Cache hit: {cache_key}")
            return _api_cache[cache_key]

        # Execute function and cache result
        result = await func(*args, **kwargs)
        _api_cache[cache_key] = result
        logger.debug(f"Cache miss, stored: {cache_key}")
        return result

    return wrapper


def clear_api_cache():
    """Clear the API cache. Called after successful crawl."""
    _api_cache.clear()
    logger.info("API cache cleared")


def refresh_cache_ttl():
    """Refresh the cache with new TTL based on next crawl time."""
    global _api_cache
    new_ttl = get_seconds_until_next_crawl()
    # Keep existing cached items, just update TTL for new items
    _api_cache = TTLCache(maxsize=2000, ttl=new_ttl)
    logger.info(f"Cache TTL refreshed: {new_ttl} seconds until next crawl")


# Admin secret for protected endpoints (set in environment)
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "idx-admin-2025")


# ==========================================
# Admin Endpoints
# ==========================================

@app.post("/api/admin/clear-cache")
async def admin_clear_cache(secret: str = Query(..., description="Admin secret key")):
    """
    Clear the API cache manually.

    Usage: POST /api/admin/clear-cache?secret=your-secret-key
    """
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")

    clear_api_cache()
    refresh_cache_ttl()

    return {
        "status": "success",
        "message": "API cache cleared and TTL refreshed",
        "newTtlSeconds": get_seconds_until_next_crawl()
    }


# ==========================================
# Health Endpoint
# ==========================================

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint with database status and last crawl info."""
    try:
        db = get_db()
        status = db.get_health_status()
        return HealthResponse(
            status=status.get("status", "unknown"),
            dbConnected=status.get("dbConnected", False),
            lastCrawl=status.get("lastCrawl"),
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="error",
            dbConnected=False,
            lastCrawl=None,
        )


# ==========================================
# Broker Endpoints
# ==========================================

@app.get("/api/brokers", response_model=list[BrokerResponse])
@cached_endpoint
async def list_brokers():
    """Get list of all brokers."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT code, name FROM brokers ORDER BY code")
        rows = cur.fetchall()
        return [BrokerResponse(code=row[0], name=row[1]) for row in rows]


@app.get("/api/brokers/{code}/aggregates")
@cached_endpoint
async def get_broker_aggregates(
    code: str,
    period: Period = Period.today,
):
    """Get aggregated data for a specific broker."""
    db = get_db()
    code = code.upper()

    with db.cursor() as cur:
        # Get broker info
        cur.execute("SELECT code, name FROM brokers WHERE code = %s", (code,))
        broker = cur.fetchone()
        if not broker:
            raise HTTPException(status_code=404, detail=f"Broker {code} not found")

        # Get aggregates
        cur.execute(
            """
            SELECT
                total_netval, total_bval, total_sval,
                weighted_bavg, weighted_savg, trade_count,
                period_start, period_end
            FROM aggregates_by_broker
            WHERE broker_code = %s AND period = %s
            """,
            (code, period.value)
        )
        agg = cur.fetchone()

        if not agg:
            return {
                "broker": {"code": broker[0], "name": broker[1]},
                "period": period.value,
                "aggregates": None,
            }

        return {
            "broker": {"code": broker[0], "name": broker[1]},
            "period": period.value,
            "aggregates": {
                "totalNetval": float(agg[0]) if agg[0] else 0,
                "totalBval": float(agg[1]) if agg[1] else 0,
                "totalSval": float(agg[2]) if agg[2] else 0,
                "weightedBavg": float(agg[3]) if agg[3] else 0,
                "weightedSavg": float(agg[4]) if agg[4] else 0,
                "tradeCount": agg[5] or 0,
                "periodStart": agg[6].isoformat() if agg[6] else None,
                "periodEnd": agg[7].isoformat() if agg[7] else None,
            },
        }


@app.get("/api/brokers/{code}/trades", response_model=PaginatedResponse)
@cached_endpoint
async def get_broker_trades(
    code: str,
    period: Period = Period.today,
    sort: SortField = SortField.netval,
    order: SortOrder = SortOrder.desc,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """Get trades for a specific broker with pagination."""
    db = get_db()
    code = code.upper()
    offset = (page - 1) * limit

    # Map sort field to column
    sort_col = {
        SortField.netval: "netval",
        SortField.bval: "bval",
        SortField.sval: "sval",
        SortField.bavg: "bavg",
        SortField.savg: "savg",
    }.get(sort, "netval")

    order_dir = "DESC" if order == SortOrder.desc else "ASC"

    with db.cursor() as cur:
        # Get total count
        cur.execute(
            """
            SELECT COUNT(DISTINCT symbol)
            FROM aggregates_broker_symbol
            WHERE broker_code = %s AND period = %s
            """,
            (code, period.value)
        )
        total = cur.fetchone()[0]

        # Get paginated data
        cur.execute(
            f"""
            SELECT
                symbol,
                SUM(netval_sum) as netval,
                SUM(bval_sum) as bval,
                SUM(sval_sum) as sval,
                AVG(weighted_bavg) as bavg,
                AVG(weighted_savg) as savg
            FROM aggregates_broker_symbol
            WHERE broker_code = %s AND period = %s
            GROUP BY symbol
            ORDER BY {sort_col} {order_dir}
            LIMIT %s OFFSET %s
            """,
            (code, period.value, limit, offset)
        )
        rows = cur.fetchall()

        data = [
            {
                "symbol": row[0],
                "netval": float(row[1]) if row[1] else 0,
                "bval": float(row[2]) if row[2] else 0,
                "sval": float(row[3]) if row[3] else 0,
                "bavg": float(row[4]) if row[4] else 0,
                "savg": float(row[5]) if row[5] else 0,
            }
            for row in rows
        ]

        return PaginatedResponse(
            data=data,
            total=total,
            page=page,
            limit=limit,
            pages=(total + limit - 1) // limit if total > 0 else 1,
        )


# ==========================================
# Ticker Endpoints
# ==========================================

@app.get("/api/tickers")
@cached_endpoint
async def list_tickers(
    active_only: bool = True,
    limit: int = Query(100, ge=1, le=2000),
):
    """Get list of all tickers."""
    db = get_db()

    with db.cursor() as cur:
        if active_only:
            cur.execute(
                """
                SELECT symbol, company_name, last_seen
                FROM symbols
                WHERE is_active = true
                ORDER BY symbol
                LIMIT %s
                """,
                (limit,)
            )
        else:
            cur.execute(
                "SELECT symbol, company_name, last_seen FROM symbols ORDER BY symbol LIMIT %s",
                (limit,)
            )

        rows = cur.fetchall()
        return [
            {
                "symbol": row[0],
                "companyName": row[1],
                "lastSeen": row[2].isoformat() if row[2] else None,
            }
            for row in rows
        ]


@app.get("/api/tickers/{symbol}/aggregates")
@cached_endpoint
async def get_ticker_aggregates(
    symbol: str,
    period: Period = Period.today,
):
    """Get aggregated data for a specific ticker."""
    db = get_db()
    symbol = symbol.upper()

    with db.cursor() as cur:
        # Get ticker info
        cur.execute(
            "SELECT symbol, company_name FROM symbols WHERE symbol = %s",
            (symbol,)
        )
        ticker = cur.fetchone()
        if not ticker:
            raise HTTPException(status_code=404, detail=f"Ticker {symbol} not found")

        # Get aggregates
        cur.execute(
            """
            SELECT
                total_netval, total_bval, total_sval,
                weighted_bavg, weighted_savg, trade_count,
                period_start, period_end
            FROM aggregates_by_ticker
            WHERE symbol = %s AND period = %s
            """,
            (symbol, period.value)
        )
        agg = cur.fetchone()

        if not agg:
            return {
                "ticker": {"symbol": ticker[0], "companyName": ticker[1]},
                "period": period.value,
                "aggregates": None,
            }

        return {
            "ticker": {"symbol": ticker[0], "companyName": ticker[1]},
            "period": period.value,
            "aggregates": {
                "totalNetval": float(agg[0]) if agg[0] else 0,
                "totalBval": float(agg[1]) if agg[1] else 0,
                "totalSval": float(agg[2]) if agg[2] else 0,
                "weightedBavg": float(agg[3]) if agg[3] else 0,
                "weightedSavg": float(agg[4]) if agg[4] else 0,
                "tradeCount": agg[5] or 0,
                "periodStart": agg[6].isoformat() if agg[6] else None,
                "periodEnd": agg[7].isoformat() if agg[7] else None,
            },
        }


@app.get("/api/tickers/{symbol}/brokers", response_model=PaginatedResponse)
@cached_endpoint
async def get_ticker_brokers(
    symbol: str,
    period: Period = Period.today,
    sort: SortField = SortField.netval,
    order: SortOrder = SortOrder.desc,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """Get brokers trading a specific ticker with their volume percentages."""
    db = get_db()
    symbol = symbol.upper()
    offset = (page - 1) * limit

    sort_col = {
        SortField.netval: "netval",
        SortField.bval: "bval",
        SortField.sval: "sval",
        SortField.bavg: "bavg",
        SortField.savg: "savg",
        SortField.pct_volume: "pct_volume",
    }.get(sort, "netval")

    order_dir = "DESC" if order == SortOrder.desc else "ASC"

    with db.cursor() as cur:
        # Get total count
        cur.execute(
            """
            SELECT COUNT(DISTINCT broker_code)
            FROM aggregates_broker_symbol
            WHERE symbol = %s AND period = %s
            """,
            (symbol, period.value)
        )
        total = cur.fetchone()[0]

        # Get paginated data
        cur.execute(
            f"""
            SELECT
                abs.broker_code,
                b.name as broker_name,
                SUM(abs.netval_sum) as netval,
                SUM(abs.bval_sum) as bval,
                SUM(abs.sval_sum) as sval,
                AVG(abs.pct_of_symbol_volume) as pct_volume,
                AVG(abs.weighted_bavg) as bavg,
                AVG(abs.weighted_savg) as savg
            FROM aggregates_broker_symbol abs
            JOIN brokers b ON abs.broker_code = b.code
            WHERE abs.symbol = %s AND abs.period = %s
            GROUP BY abs.broker_code, b.name
            ORDER BY {sort_col} {order_dir}
            LIMIT %s OFFSET %s
            """,
            (symbol, period.value, limit, offset)
        )
        rows = cur.fetchall()

        data = [
            {
                "brokerCode": row[0],
                "brokerName": row[1],
                "netval": float(row[2]) if row[2] else 0,
                "bval": float(row[3]) if row[3] else 0,
                "sval": float(row[4]) if row[4] else 0,
                "pctVolume": float(row[5]) if row[5] else 0,
                "bavg": float(row[6]) if row[6] else 0,
                "savg": float(row[7]) if row[7] else 0,
            }
            for row in rows
        ]

        return PaginatedResponse(
            data=data,
            total=total,
            page=page,
            limit=limit,
            pages=(total + limit - 1) // limit if total > 0 else 1,
        )


# ==========================================
# Insights Endpoint
# ==========================================

@app.get("/api/insights")
@cached_endpoint
async def get_insights(
    period: Period = Period.d5,
    limit: int = Query(20, ge=1, le=50),
):
    """Get top movers and market insights."""
    db = get_db()

    # Map period to insight type (5d for short periods, month for longer)
    insight_type = "top_netval_5d" if period in [Period.today, Period.d2, Period.d3, Period.d5] else "top_netval_month"

    with db.cursor() as cur:
        # Get latest insights (filtered to most recent date)
        cur.execute(
            """
            SELECT
                di.symbol, di.broker_code, b.name as broker_name,
                di.netval, di.bval, di.sval, di.bavg, di.savg, di.rank
            FROM daily_insights di
            JOIN brokers b ON di.broker_code = b.code
            WHERE di.insight_type = %s
              AND di.insight_date = (
                  SELECT MAX(insight_date)
                  FROM daily_insights
                  WHERE insight_type = %s
              )
            ORDER BY di.rank
            LIMIT %s
            """,
            (insight_type, insight_type, limit)
        )
        rows = cur.fetchall()

        top_movers = [
            {
                "rank": row[8],
                "symbol": row[0],
                "brokerCode": row[1],
                "brokerName": row[2],
                "netval": float(row[3]) if row[3] else 0,
                "bval": float(row[4]) if row[4] else 0,
                "sval": float(row[5]) if row[5] else 0,
                "bavg": float(row[6]) if row[6] else 0,
                "savg": float(row[7]) if row[7] else 0,
            }
            for row in rows
        ]

        # Get market stats for the selected period
        cur.execute(
            """
            SELECT
                MIN(period_start),
                MAX(period_end),
                SUM(total_bval),
                SUM(total_sval)
            FROM aggregates_by_broker
            WHERE period = %s
            """,
            (period.value,)
        )
        stats = cur.fetchone()

        market_stats = None
        if stats and stats[0]:
            start_date = stats[0]
            end_date = stats[1]

            # Format date: "YYYY-MM-DD" or "YYYY-MM-DD to YYYY-MM-DD"
            if start_date == end_date:
                date_str = start_date.isoformat()
            else:
                date_str = f"{start_date.isoformat()} to {end_date.isoformat()}"

            market_stats = {
                "date": date_str,
                "totalBval": float(stats[2]) if stats[2] else 0,
                "totalSval": float(stats[3]) if stats[3] else 0,
            }

        # Get top brokers by netval (activity)
        cur.execute(
            """
            SELECT
                abb.broker_code, b.name,
                abb.total_netval, abb.total_bval, abb.total_sval
            FROM aggregates_by_broker abb
            JOIN brokers b ON abb.broker_code = b.code
            WHERE abb.period = %s
            ORDER BY ABS(abb.total_netval) DESC
            LIMIT 50
            """,
            (period.value,)
        )
        broker_rows = cur.fetchall()

        top_brokers = [
            {
                "rank": i + 1,
                "brokerCode": row[0],
                "brokerName": row[1],
                "netval": float(row[2]) if row[2] else 0,
                "bval": float(row[3]) if row[3] else 0,
                "sval": float(row[4]) if row[4] else 0,
            }
            for i, row in enumerate(broker_rows)
        ]

        return {
            "period": period.value,
            "topMovers": top_movers,
            "topBrokers": top_brokers,
            "marketStats": market_stats,
        }


# ==========================================
# Pivot Table Endpoint
# ==========================================

@app.get("/api/pivot")
@cached_endpoint
async def get_pivot_data(
    period: Period = Period.d5,
    metric: str = Query("netval", pattern="^(netval|bval|sval)$"),
    brokers: str = Query(None, description="Comma-separated broker codes"),
    symbols: str = Query(None, description="Comma-separated symbols"),
):
    """
    Get pivot table data for Data Analysis tab.

    - period: time period filter
    - metric: value to aggregate (netval, bval, sval)
    - brokers: comma-separated list of broker codes (e.g., "AD,CC,YP")
    - symbols: comma-separated list of symbols (e.g., "BBCA,BBRI,TLKM")

    Returns:
        - brokers: list of broker codes (rows)
        - symbols: list of symbols (columns)
        - data: nested dict {broker: {symbol: value}}
        - totals: {broker: {brokerCode: total}, symbol: {symbol: total}}
    """
    db = get_db()

    # Parse broker and symbol lists
    broker_list = [b.strip().upper() for b in brokers.split(",")] if brokers else []
    symbol_list = [s.strip().upper() for s in symbols.split(",")] if symbols else []

    # If no brokers or symbols selected, return empty
    if not broker_list or not symbol_list:
        return {
            "brokers": broker_list,
            "symbols": symbol_list,
            "data": {},
            "totals": {"broker": {}, "symbol": {}},
            "metric": metric,
            "period": period.value,
        }

    # Map metric to column
    metric_col = {
        "netval": "netval_sum",
        "bval": "bval_sum",
        "sval": "sval_sum",
    }.get(metric, "netval_sum")

    with db.cursor() as cur:
        # Get pivot data for selected brokers and symbols
        cur.execute(
            f"""
            SELECT broker_code, symbol, SUM({metric_col}) as val
            FROM aggregates_broker_symbol
            WHERE period = %s
              AND broker_code = ANY(%s)
              AND symbol = ANY(%s)
            GROUP BY broker_code, symbol
            """,
            (period.value, broker_list, symbol_list)
        )

        # Build nested data structure
        data = {b: {} for b in broker_list}
        for row in cur.fetchall():
            broker_code, symbol, val = row
            if broker_code in data:
                data[broker_code][symbol] = float(val) if val else 0

        # Calculate broker totals (row totals)
        broker_totals = {}
        for b in broker_list:
            broker_totals[b] = sum(data[b].values())

        # Calculate symbol totals (column totals)
        symbol_totals = {}
        for s in symbol_list:
            symbol_totals[s] = sum(data[b].get(s, 0) for b in broker_list)

        return {
            "brokers": broker_list,
            "symbols": symbol_list,
            "data": data,
            "totals": {
                "broker": broker_totals,
                "symbol": symbol_totals,
            },
            "metric": metric,
            "period": period.value,
        }


# ==========================================
# Cache Management Endpoints
# ==========================================

@app.get("/api/cache/status")
async def cache_status():
    """Get cache status information."""
    return {
        "size": len(_api_cache),
        "maxsize": _api_cache.maxsize,
        "ttl": _api_cache.ttl,
        "secondsUntilNextCrawl": get_seconds_until_next_crawl(),
    }


@app.post("/api/cache/clear")
async def clear_cache():
    """
    Clear the API cache.
    Called after successful crawl to ensure fresh data is served.
    """
    clear_api_cache()
    refresh_cache_ttl()
    return {"status": "cleared", "newTtl": get_seconds_until_next_crawl()}


# ==========================================
# Helper Functions
# ==========================================

def _get_period_filter(period: str) -> str:
    """Get SQL date filter for period."""
    from datetime import timedelta
    today = date.today()

    if period == "today":
        return f"trade_date = '{today}'"
    elif period == "week":
        start = today - timedelta(days=7)
        return f"trade_date BETWEEN '{start}' AND '{today}'"
    elif period == "month":
        start = today - timedelta(days=30)
        return f"trade_date BETWEEN '{start}' AND '{today}'"
    elif period == "ytd":
        start = date(today.year, 1, 1)
        return f"trade_date BETWEEN '{start}' AND '{today}'"
    else:  # all
        return "1=1"


# ==========================================
# Static Files (React Frontend)
# ==========================================

# Check if frontend build exists
FRONTEND_DIR = Path(__file__).parent / "frontend" / "dist"

if FRONTEND_DIR.exists():
    # Serve static files
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.get("/{path:path}")
    async def serve_frontend(path: str):
        """Serve React frontend for all non-API routes."""
        # Check if file exists
        file_path = FRONTEND_DIR / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        # Return index.html for SPA routing
        return FileResponse(FRONTEND_DIR / "index.html")


# ==========================================
# Startup/Shutdown Events
# ==========================================

@app.on_event("startup")
async def startup():
    """Initialize database connection on startup."""
    logger.info("Starting IDX Copytrading API...")
    db = get_database()
    if db.connect():
        logger.info("Database connected successfully")
    else:
        logger.warning("Database connection failed - API will have limited functionality")


@app.on_event("shutdown")
async def shutdown():
    """Close database connection on shutdown."""
    logger.info("Shutting down IDX Copytrading API...")
    db = get_database()
    db.disconnect()


# ==========================================
# Main (for development)
# ==========================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
