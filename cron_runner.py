"""
Cron Runner for IDX Copytrading System.

This script is designed to be run as a Railway cron job that:
1. Checks if today is an IDX trading day
2. Verifies no successful crawl exists for today
3. Runs the crawler with database integration
4. Computes aggregates after successful crawl
5. Logs all operations for monitoring

Railway cron configuration: "0 21 * * 1-5" (9 PM Jakarta, Mon-Fri)
"""

import logging
import os
import sys
from datetime import date, datetime
from typing import Any

from dotenv import load_dotenv

load_dotenv()

# Configure logging for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def run_daily_crawl() -> dict[str, Any]:
    """
    Run the daily crawl and aggregation pipeline.
    
    Returns:
        Dictionary with status and metadata about the run.
    """
    from holidays import is_idx_trading_day
    from db import Database
    from broker_crawler import BrokerCrawler, BrokerCrawlerConfig, BROKER_CODES
    from aggregates import AggregationComputer
    
    today = date.today()
    result = {
        "date": today.isoformat(),
        "status": "unknown",
        "message": "",
        "rows_crawled": 0,
        "successful_brokers": 0,
        "failed_brokers": 0,
    }
    
    # Step 1: Check if today is a trading day
    logger.info(f"Checking if {today} is an IDX trading day...")
    if not is_idx_trading_day(today):
        result["status"] = "skipped"
        result["message"] = "Not an IDX trading day (weekend or holiday)"
        logger.info(result["message"])
        return result
    
    # Step 2: Connect to database
    logger.info("Connecting to database...")
    db = Database()
    if not db.connect():
        result["status"] = "error"
        result["message"] = "Failed to connect to database"
        logger.error(result["message"])
        return result
    
    try:
        # Step 3: Check for existing successful crawl
        if db.has_successful_crawl_today(today):
            result["status"] = "skipped"
            result["message"] = "Successful crawl already exists for today"
            logger.info(result["message"])
            return result
        
        # Step 4: Start crawl log
        crawl_log_id = db.start_crawl_log(today)
        logger.info(f"Started crawl log entry: {crawl_log_id}")
        
        # Step 5: Run crawler
        logger.info("Starting broker crawl...")
        config = BrokerCrawlerConfig()
        crawler = BrokerCrawler(config)
        
        if not crawler.login():
            error_msg = "Failed to authenticate with NeoBDM"
            db.update_crawl_log(crawl_log_id, "failed", error_message=error_msg)
            result["status"] = "error"
            result["message"] = error_msg
            logger.error(error_msg)
            return result
        
        # Crawl all brokers
        crawl_result = crawler.crawl_all_brokers(BROKER_CODES)
        
        # Step 6: Insert data into database
        if crawl_result.get("data"):
            logger.info(f"Inserting {len(crawl_result['data'])} rows into database...")
            crawl_timestamp = datetime.now()
            rows_inserted = db.insert_broker_trades(crawl_result["data"], crawl_timestamp)
            
            # Update symbols table
            db.update_symbols(crawl_result["data"], today)
            
            result["rows_crawled"] = rows_inserted
            result["successful_brokers"] = len(crawl_result.get("successful_broker_codes", []))
            result["failed_brokers"] = len(crawl_result.get("failed_broker_codes", []))
            
            logger.info(f"Inserted {rows_inserted} rows")
        else:
            logger.warning("No data returned from crawl")
        
        # Step 7: Compute aggregates (only if crawl was successful)
        success_rate = result["successful_brokers"] / len(BROKER_CODES) if BROKER_CODES else 0
        if success_rate >= 0.8:  # At least 80% success rate
            logger.info("Computing aggregates...")
            computer = AggregationComputer(db)
            computer.compute_all(today)
            
            # Mark crawl as successful
            db.update_crawl_log(
                crawl_log_id,
                status="success",
                total_rows=result["rows_crawled"],
                successful_brokers=result["successful_brokers"],
                failed_brokers=result["failed_brokers"]
            )
            
            # Clear API cache to serve fresh data
            try:
                from api import clear_api_cache, refresh_cache_ttl
                clear_api_cache()
                refresh_cache_ttl()
                logger.info("API cache cleared after successful crawl")
            except Exception as cache_err:
                logger.warning(f"Failed to clear API cache: {cache_err}")
            
            result["status"] = "success"
            result["message"] = "Crawl and aggregation completed successfully"
        else:
            # Mark as failed due to low success rate
            error_msg = f"Low success rate: {success_rate:.1%} ({result['successful_brokers']}/{len(BROKER_CODES)} brokers)"
            db.update_crawl_log(
                crawl_log_id,
                status="failed",
                total_rows=result["rows_crawled"],
                successful_brokers=result["successful_brokers"],
                failed_brokers=result["failed_brokers"],
                error_message=error_msg
            )
            result["status"] = "partial_failure"
            result["message"] = error_msg
            logger.warning(error_msg)
        
        logger.info(f"Daily crawl completed: {result['status']}")
        return result
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.exception(error_msg)
        result["status"] = "error"
        result["message"] = error_msg
        
        # Try to update crawl log with error
        try:
            db.update_crawl_log(crawl_log_id, "failed", error_message=error_msg)
        except:
            pass
        
        return result
        
    finally:
        db.disconnect()


def main():
    """Main entry point for the cron runner."""
    logger.info("=" * 60)
    logger.info("IDX Copytrading Cron Runner - Starting")
    logger.info(f"Timezone: {os.environ.get('TZ', 'UTC')}")
    logger.info(f"Current time: {datetime.now().isoformat()}")
    logger.info("=" * 60)
    
    result = run_daily_crawl()
    
    logger.info("=" * 60)
    logger.info(f"Result: {result}")
    logger.info("IDX Copytrading Cron Runner - Finished")
    logger.info("=" * 60)
    
    # Exit with appropriate code for Railway
    if result["status"] == "success":
        sys.exit(0)
    elif result["status"] == "skipped":
        sys.exit(0)  # Skipped is not an error
    else:
        sys.exit(1)  # Non-zero exit for failures


if __name__ == "__main__":
    main()
