
import pytest
from unittest.mock import MagicMock
from db import Database

@pytest.fixture
def mock_db_cursor():
    """Returns a mock cursor that behaves like a context manager."""
    mock_cursor = MagicMock()
    
    # Setup context manager behavior for the cursor
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    
    return mock_cursor

@pytest.fixture
def mock_db(mock_db_cursor):
    """
    Returns a mock Database instance with a mocked cursor.
    It patches get_database globally so the API uses this mock.
    """
    mock_db_instance = MagicMock(spec=Database)
    mock_db_instance._conn = True # Bypass "if not db._conn" check
    mock_db_instance.cursor.return_value = mock_db_cursor
    # Mocking connection status properties
    mock_db_instance.get_health_status.return_value = {
        "status": "ok",
        "dbConnected": True,
        "lastCrawl": None
    }
    
    return mock_db_instance
