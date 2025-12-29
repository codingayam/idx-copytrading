
import pytest
from playwright.sync_api import Page, expect

# Configuration
BASE_URL = "https://idx-copytrading-production.up.railway.app"

@pytest.mark.e2e
def test_homepage_loads(page: Page):
    """Test that the homepage loads successfully."""
    page.goto(BASE_URL)

    # Check title (adjust based on your actual React app title)
    # expect(page).to_have_title("IDX Copytrading")

    # Check for a key element that should be present
    # e.g., the Brokers table or a header
    expect(page.get_by_text("IDX Copytrading")).to_be_visible()

@pytest.mark.e2e
@pytest.mark.skip(reason="Depends on production deployment - run manually after deploy")
def test_broker_tab_displays_table(page: Page):
    """
    Test navigating to the Broker tab and verifying the table loads.
    Uses tab navigation since the app is tab-based, not URL-routed.
    """
    page.goto(BASE_URL)

    # Click on the Broker tab
    page.get_by_role("button", name="Broker").click()

    # Wait for the table to load (use .first since page may have multiple tables)
    expect(page.get_by_role("table").first).to_be_visible(timeout=10000)

    # Verify data rows exist (at least one)
    expect(page.locator("tbody tr").first).to_be_visible()
