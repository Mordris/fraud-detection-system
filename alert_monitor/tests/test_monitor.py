# alert_monitor/tests/test_monitor.py

import pytest
import json
from httpx import AsyncClient, ASGITransport
from httpx_ws import aconnect_ws

from alert_monitor.monitor import app, recent_alerts, manager

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def api_client(mocker):
    """
    A single, robust fixture that:
    1. Mocks external services (Redis listener).
    2. Correctly handles the FastAPI lifespan events.
    3. Provides a fully configured test client for both HTTP and WebSocket tests.
    """
    mocker.patch("alert_monitor.monitor.redis_listener_thread")

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    
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


async def test_websocket_connection(api_client: AsyncClient):
    """
    Tests if a client can successfully connect to the WebSocket endpoint.
    """
    # aconnect_ws uses the client object directly
    async with aconnect_ws("/ws", client=api_client) as websocket:
        assert len(manager.active_connections) == 1
        assert manager.active_connections[0] == websocket

    assert len(manager.active_connections) == 0


async def test_websocket_receives_broadcast(api_client: AsyncClient, mocker):
    """
    Tests if a connected WebSocket client receives a broadcasted message.
    """
    async def mock_broadcast(coro):
        await coro

    mocker.patch("asyncio.run_coroutine_threadsafe", side_effect=mock_broadcast)

    async with aconnect_ws("/ws", client=api_client) as websocket:
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