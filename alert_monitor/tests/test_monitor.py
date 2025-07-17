# alert_monitor/tests/test_monitor.py

import pytest
import json
from httpx import AsyncClient, ASGITransport
from httpx_ws import aconnect_ws # Import the websocket connector

from alert_monitor.monitor import app, recent_alerts, manager

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def api_client(mocker):
    """
    Fixture for testing standard HTTP endpoints.
    """
    mocker.patch("alert_monitor.monitor.redis_listener_thread")

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    
    recent_alerts.clear()
    manager.active_connections.clear()


@pytest.fixture
async def websocket_client(mocker):
    """
    A separate fixture specifically for testing WebSocket endpoints using httpx-ws.
    """
    mocker.patch("alert_monitor.monitor.redis_listener_thread")
    
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        # Yield the transport to be used by the websocket client
        yield transport

    recent_alerts.clear()
    manager.active_connections.clear()


# --- Test Cases ---

async def test_get_dashboard(api_client: AsyncClient):
    """
    Tests if the root endpoint '/' successfully serves the HTML dashboard.
    """
    response = await api_client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "<title>Fraud Detection Dashboard</title>" in response.text


async def test_websocket_connection(websocket_client: ASGITransport):
    """
    Tests if a client can successfully connect to the WebSocket endpoint.
    """
    # Use the aconnect_ws context manager from httpx-ws
    async with aconnect_ws("/ws", transport=websocket_client) as websocket:
        assert len(manager.active_connections) == 1
        assert manager.active_connections[0] == websocket

    assert len(manager.active_connections) == 0


async def test_websocket_receives_broadcast(websocket_client: ASGITransport, mocker):
    """
    Tests if a connected WebSocket client receives a broadcasted message.
    """
    async def mock_broadcast(coro):
        await coro

    mocker.patch("asyncio.run_coroutine_threadsafe", side_effect=mock_broadcast)

    # Use the aconnect_ws context manager from httpx-ws
    async with aconnect_ws("/ws", transport=websocket_client) as websocket:
        test_alert = {
            "transaction_id": "ws_test_001", "user_id": "user_ws_test",
            "amount": "999.99", "received_at": "2025-07-18T14:00:00Z"
        }
        recent_alerts.append(test_alert)

        received_data = await websocket.receive_json()
        assert received_data["transaction_id"] == "ws_test_001"

        new_alert = {"transaction_id": "ws_test_002"}
        manager.broadcast(json.dumps(new_alert))

        received_broadcast = await websocket.receive_json()
        assert received_broadcast["transaction_id"] == "ws_test_002"