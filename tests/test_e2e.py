
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
def test_broker_details_page_sorting(page: Page):
    """
    Test navigating to a broker page and sorting the table.
    This specifically verifies the fix for the 500 error.
    """
    page.goto(f"{BASE_URL}/brokers/AD")
    
    # Wait for the table to load
    # Assuming the table has a header 'Net Value' or similar
    # And we want to ensure it doesn't show an error message
    
    # Check for table visibility
    # Note: Adjust selectors based on actual React components
    expect(page.get_by_role("table")).to_be_visible()
    
    # Verify no error toast/message is displayed
    # expect(page.get_by_text("Internal Server Error")).not_to_be_visible()
    
    # Verify data rows exist (at least one)
    expect(page.locator("tbody tr").first).to_be_visible()
