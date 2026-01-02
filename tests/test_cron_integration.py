"""
Integration tests for the cron runner pipeline.

These tests verify the complete flow from crawling to aggregation,
including the critical ordering of operations.

IMPORTANT: These tests require a real database connection.
They are marked with @pytest.mark.integration and skipped if no DB is available.
"""

import pytest
import os


# Skip all tests if DATABASE_URL is not set
pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="Integration tests require DATABASE_URL environment variable"
)


@pytest.fixture
def test_db():
    """Create a test database connection."""
    from db import Database
    db = Database()
    if not db.connect():
        pytest.skip("Could not connect to database")
    yield db
    db.disconnect()


class TestCronRunnerIntegration:
    """Integration tests for the cron runner pipeline."""

    def test_aggregation_uses_current_crawl_date(self, test_db):
        """
        Critical test: Aggregation should use today's date after marking crawl success.

        This test verifies the fix for the bug where aggregation ran BEFORE the
        crawl was marked as success, causing it to use the previous day's date.

        The correct order is:
        1. Insert trade data
        2. Mark crawl as success in crawl_log
        3. Run aggregation (which queries crawl_log for latest success date)
        """
        from aggregates import get_period_trade_dates

        # Get the latest successful crawl date
        with test_db.cursor() as cur:
            cur.execute("SELECT MAX(crawl_date) FROM crawl_log WHERE status = 'success'")
            latest_crawl_date = cur.fetchone()[0]

        if not latest_crawl_date:
            pytest.skip("No successful crawls in database")

        # Verify get_period_trade_dates("today") returns the latest crawl date
        today_dates = get_period_trade_dates(test_db, "today")

        assert len(today_dates) == 1, "Expected exactly one date for 'today' period"
        assert today_dates[0] == latest_crawl_date, (
            f"'today' period date {today_dates[0]} does not match "
            f"latest successful crawl date {latest_crawl_date}"
        )

    def test_aggregates_match_crawl_log_date(self, test_db):
        """
        Verify that aggregates' period_end matches the latest crawl_log date.

        This catches the scenario where aggregation was computed with stale data.
        """
        with test_db.cursor() as cur:
            # Get latest crawl date from crawl_log
            cur.execute("SELECT MAX(crawl_date) FROM crawl_log WHERE status = 'success'")
            latest_crawl_date = cur.fetchone()[0]

            if not latest_crawl_date:
                pytest.skip("No successful crawls in database")

            # Get latest period_end from aggregates_by_broker for 'today' period
            cur.execute("""
                SELECT MAX(period_end)
                FROM aggregates_by_broker
                WHERE period = 'today'
            """)
            aggregates_date = cur.fetchone()[0]

            if not aggregates_date:
                pytest.skip("No 'today' aggregates in database")

        assert aggregates_date == latest_crawl_date, (
            f"Aggregates period_end ({aggregates_date}) does not match "
            f"crawl_log date ({latest_crawl_date}). "
            "This indicates aggregation ran before crawl was marked as success."
        )

    def test_cron_flow_marks_success_before_aggregation(self):
        """
        Unit test: Verify cron_runner.py marks success BEFORE computing aggregates.

        This is a code inspection test that verifies the correct ordering
        by checking the source code structure.
        """
        import inspect
        from cron_runner import run_daily_crawl

        source = inspect.getsource(run_daily_crawl)

        # Find the position of key operations
        update_crawl_log_pos = source.find('db.update_crawl_log')
        compute_all_pos = source.find('computer.compute_all')

        # Both should exist
        assert update_crawl_log_pos != -1, "update_crawl_log not found in run_daily_crawl"
        assert compute_all_pos != -1, "compute_all not found in run_daily_crawl"

        # The first update_crawl_log with status="success" should come BEFORE compute_all
        # Find the success update specifically
        success_update_pos = source.find('status="success"')

        assert success_update_pos != -1, "status='success' not found"
        assert success_update_pos < compute_all_pos, (
            "Critical: update_crawl_log(status='success') must come BEFORE compute_all(). "
            "Otherwise, aggregation will use stale dates."
        )


class TestAggregationDateConsistency:
    """Tests for date consistency across the aggregation pipeline."""

    def test_all_periods_have_consistent_end_date(self, test_db):
        """
        All periods should have period_end <= latest crawl date.
        """
        with test_db.cursor() as cur:
            # Get latest crawl date
            cur.execute("SELECT MAX(crawl_date) FROM crawl_log WHERE status = 'success'")
            latest_crawl_date = cur.fetchone()[0]

            if not latest_crawl_date:
                pytest.skip("No successful crawls in database")

            # Check all periods
            cur.execute("""
                SELECT DISTINCT period, MAX(period_end) as max_end
                FROM aggregates_by_broker
                GROUP BY period
            """)

            for row in cur.fetchall():
                period, max_end = row
                assert max_end <= latest_crawl_date, (
                    f"Period '{period}' has period_end ({max_end}) after "
                    f"latest crawl date ({latest_crawl_date})"
                )

    def test_daily_totals_matches_crawl_log(self, test_db):
        """
        daily_totals should have entry for latest crawl date.
        """
        with test_db.cursor() as cur:
            # Get latest crawl date
            cur.execute("SELECT MAX(crawl_date) FROM crawl_log WHERE status = 'success'")
            latest_crawl_date = cur.fetchone()[0]

            if not latest_crawl_date:
                pytest.skip("No successful crawls in database")

            # Check daily_totals
            cur.execute("""
                SELECT trade_date FROM daily_totals WHERE trade_date = %s
            """, (latest_crawl_date,))
            result = cur.fetchone()

            assert result is not None, (
                f"daily_totals missing entry for latest crawl date {latest_crawl_date}"
            )
