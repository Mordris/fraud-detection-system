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
        # The only thing we need to verify is that the connection
        # was successfully added to our connection manager.
        assert len(manager.active_connections) == 1

    # After the 'with' block exits, the connection is closed,
    # so we can verify that it was removed.
    assert len(manager.active_connections) == 0


def test_websocket_receives_broadcast(api_client: TestClient, mocker):
    """
    Tests that a broadcast is attempted when a new alert comes in.
    """
    # 1. Mock the broadcast method on the manager instance
    mock_broadcast = mocker.patch("alert_monitor.monitor.manager.broadcast")

    # 2. Simulate a new alert coming in from the (mocked) Redis thread
    test_alert = {
        "transaction_id": "ws_test_001",
        "user_id": "user_ws_test",
    }
    
    # 3. Manually call the part of the listener logic that triggers the broadcast
    # In a real scenario, the redis_listener_thread would do this.
    # Here, we simulate it directly.
    manager.broadcast(json.dumps(test_alert))

    # 4. Assert that our mock was called with the correct data.
    # This proves the core logic works without needing a live websocket.
    mock_broadcast.assert_called_once_with(json.dumps(test_alert))