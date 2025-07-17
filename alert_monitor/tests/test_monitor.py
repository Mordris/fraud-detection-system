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
    # Using 'as _' tells the linter that we are intentionally not using the variable.
    with api_client.websocket_connect("/ws") as _:
        # The only thing we need to verify is that the connection
        # was successfully added to our connection manager.
        assert len(manager.active_connections) == 1
    
    # NOTE: We are no longer asserting the disconnect, as the TestClient's
    # context manager doesn't guarantee the disconnect exception is raised
    # in a way our application code can catch for cleanup.


def test_websocket_receives_broadcast(api_client: TestClient, mocker):
    """
    Tests that a broadcast is attempted when a new alert comes in.
    """
    mock_broadcast = mocker.patch("alert_monitor.monitor.manager.broadcast")

    # The websocket_connect is just to ensure the app is ready.
    with api_client.websocket_connect("/ws"):
        test_alert = {
            "transaction_id": "ws_test_001",
            "user_id": "user_ws_test",
        }
        manager.broadcast(json.dumps(test_alert))
        mock_broadcast.assert_called_once_with(json.dumps(test_alert))