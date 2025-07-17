# alert_monitor/tests/test_monitor.py

import pytest
import json
from httpx import AsyncClient, ASGITransport

# Import the FastAPI app instance from your main application file
from alert_monitor.monitor import app, recent_alerts, manager

# Mark all tests in this file as asynchronous
pytestmark = pytest.mark.asyncio


@pytest.fixture
async def api_client(mocker):
    """
    A pytest fixture that:
    1. Mocks the background Redis listener thread to prevent real connections.
    2. Correctly handles the FastAPI lifespan events.
    3. Provides a test client to make requests.
    """
    # We don't want the real Redis listener thread to start during tests.
    mocker.patch("alert_monitor.monitor.redis_listener_thread")

    # Use the app's own lifespan context manager
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    # Clear any state between tests
    recent_alerts.clear()
    manager.active_connections.clear()


# --- Test Cases ---

async def test_get_dashboard(api_client: AsyncClient):
    """
    Tests if the root endpoint '/' successfully serves the HTML dashboard.
    """
    response = await api_client.get("/")
    assert response.status_code == 200
    # Check if the response content is HTML and contains the title
    assert "text/html" in response.headers["content-type"]
    assert "<title>Fraud Detection Dashboard</title>" in response.text


async def test_websocket_connection(api_client: AsyncClient):
    """
    Tests if a client can successfully connect to the WebSocket endpoint.
    """
    # The 'websocket_connect' context manager handles opening and closing the connection
    async with api_client.websocket_connect("/ws") as websocket:
        # Check that the connection was successful by seeing if it was added to the manager
        assert len(manager.active_connections) == 1
        # Ensure the connected object is the one we have
        assert manager.active_connections[0] == websocket

    # Check that the connection was cleaned up after disconnecting
    assert len(manager.active_connections) == 0


async def test_websocket_receives_broadcast(api_client: AsyncClient, mocker):
    """
    Tests if a connected WebSocket client receives a broadcasted message.
    """
    # We need to mock the `run_coroutine_threadsafe` because our test
    # event loop is different from the application's.
    # We can replace it with a simple awaitable call.
    async def mock_broadcast(coro):
        await coro

    mocker.patch("asyncio.run_coroutine_threadsafe", side_effect=mock_broadcast)

    async with api_client.websocket_connect("/ws") as websocket:
        # Manually add a test alert to the system
        test_alert = {
            "transaction_id": "ws_test_001",
            "user_id": "user_ws_test",
            "amount": "999.99",
            "received_at": "2025-07-18T14:00:00Z"
        }
        # Manually put an alert into the cache for initial send
        recent_alerts.append(test_alert)

        # The app should send recent alerts upon connection
        received_data = await websocket.receive_json()
        assert received_data["transaction_id"] == "ws_test_001"

        # Now, simulate a new alert being broadcasted by the system
        new_alert = {"transaction_id": "ws_test_002"}
        manager.broadcast(json.dumps(new_alert))

        # Check if the client received the new broadcasted alert
        received_broadcast = await websocket.receive_json()
        assert received_broadcast["transaction_id"] == "ws_test_002"