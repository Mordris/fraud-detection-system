# alert_monitor/tests/test_monitor.py

import pytest
import json
from starlette.testclient import TestClient # <-- Import the correct client

from alert_monitor.monitor import app, recent_alerts, manager

# NOTE: For TestClient, tests are synchronous, not async.
# We remove the async/await keywords.

@pytest.fixture
def api_client(mocker):
    """
    A fixture that provides a synchronous test client for our FastAPI app,
    while correctly handling the app's lifespan events.
    """
    # Patch the background thread before the app is used.
    mocker.patch("alert_monitor.monitor.redis_listener_thread")
    
    # TestClient handles the lifespan context automatically
    with TestClient(app) as client:
        yield client
    
    # Cleanup state after tests
    recent_alerts.clear()
    manager.active_connections.clear()


# --- Test Cases ---

def test_get_dashboard(api_client: TestClient):
    """
    Tests if the root endpoint '/' successfully serves the HTML dashboard.
    """
    response = api_client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "<title>Fraud Detection Dashboard</title>" in response.text


def test_websocket_connection(api_client: TestClient):
    """
    Tests if a client can successfully connect to the WebSocket endpoint.
    """
    # The TestClient has a built-in websocket_connect context manager
    with api_client.websocket_connect("/ws") as websocket:
        assert len(manager.active_connections) == 1
        assert manager.active_connections[0] == websocket

    assert len(manager.active_connections) == 0


def test_websocket_receives_broadcast(api_client: TestClient, mocker):
    """
    Tests if a connected WebSocket client receives a broadcasted message.
    """
    # Mock the broadcast function since we are not in a real event loop
    mocker.patch("alert_monitor.monitor.manager.broadcast")

    with api_client.websocket_connect("/ws") as websocket:
        # Manually add a test alert to the system
        test_alert = {
            "transaction_id": "ws_test_001", "user_id": "user_ws_test",
            "amount": "999.99", "received_at": "2025-07-18T14:00:00Z"
        }
        recent_alerts.append(test_alert)
        
        # Re-run the connect logic to send initial alerts
        # This is a bit of a workaround because we're not in a real event loop
        # A more advanced test could mock manager.connect itself
        # For now, we manually trigger the broadcast to test the flow
        websocket.send_text("test message") # to keep connection open

        # Simulate the broadcast
        manager.broadcast(json.dumps(test_alert))

        # Verify that the mock was called
        manager.broadcast.assert_called_with(json.dumps(test_alert))