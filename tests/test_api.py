
from fastapi.testclient import TestClient
from unittest.mock import patch
from api import app, SortField, SortOrder

client = TestClient(app)

def test_health_check(mock_db):
    """Test the health check endpoint."""
    with patch("api.get_database", return_value=mock_db):
        response = client.get("/api/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

def test_get_broker_trades_sorting(mock_db, mock_db_cursor):
    """
    Test that the broker trades endpoint uses the correct column aliases
    for sorting to avoid GroupingError.
    """
    # Setup mock return values to avoid errors (structure matches SQL query)
    mock_db_cursor.fetchone.return_value = (5,) # Total count
    mock_db_cursor.fetchall.return_value = [
        ("BBNI", 100.0, 50.0, 50.0, 5000.0, 5100.0) # A sample row
    ]

    with patch("api.get_database", return_value=mock_db):
        # Request with netval sorting (the one that caused the error)
        response = client.get(
            "/api/brokers/AD/trades", 
            params={"sort": "netval", "order": "desc"}
        )
        
        assert response.status_code == 200
        
        # Verify the SQL query used the alias 'netval' not 'netval_sum' in ORDER BY
        # We check the call arguments of cursor.execute
        # The query is the first argument
        calls = mock_db_cursor.execute.call_args_list
        api_call = calls[1] # 0 is count query, 1 is data query
        query_sql = api_call[0][0]
        
        # Check against the raw column name to ensure we FIXED it
        assert "ORDER BY netval DESC" in query_sql
        assert "ORDER BY netval_sum DESC" not in query_sql

def test_get_brokers_list(mock_db, mock_db_cursor):
    """Test fetching the broker list."""
    mock_db_cursor.fetchall.return_value = [("AD", "OSO Sekuritas"), ("YP", "Mirae Asset")]
    
    with patch("api.get_database", return_value=mock_db):
        response = client.get("/api/brokers")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["code"] == "AD"
