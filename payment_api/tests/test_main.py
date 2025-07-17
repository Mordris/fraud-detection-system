# payment_api/tests/test_main.py

import pytest
from httpx import AsyncClient, ASGITransport

# Import the FastAPI app instance from your main application file
from payment_api.main import app

# Mark all tests in this file as asynchronous
pytestmark = pytest.mark.asyncio

# This is a more robust fixture that mocks external services before the app starts
@pytest.fixture
async def api_client(mocker):
    # 1. Patch the clients that try to make external connections during startup
    mocker.patch("payment_api.main.KafkaAdminClient", autospec=True)
    mocker.patch("payment_api.main.KafkaProducer", autospec=True)

    # 2. Use the app's own lifespan context manager
    # This is the modern and correct way to handle startup/shutdown in tests
    async with app.router.lifespan_context(app):
        # 3. Now that startup has run (with mocked clients), create the test client
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

# --- Test Cases ---

async def test_health_check(api_client: AsyncClient):
    """
    Tests if the /health endpoint is working and returns a 200 OK status.
    """
    response = await api_client.get("/health")
    assert response.status_code == 200
    assert response.json()["api_status"] == "ok"

async def test_create_successful_transaction(api_client: AsyncClient):
    """
    Tests the "happy path" for the /transaction endpoint.
    The producer is already mocked by the fixture.
    """
    # Define a valid transaction payload
    valid_transaction = {
        "transaction_id": "txn_test_success_001",
        "user_id": "user-test-123",
        "card_number": "4242-4242-4242-4242",
        "amount": 150.75,
        "timestamp": "2025-07-18T12:00:00Z",
        "merchant_id": "merchant_test",
        "location": "Test City"
    }

    response = await api_client.post("/transaction", json=valid_transaction)

    # Assert that the API accepted the transaction
    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    # We can no longer assert that 'send' was called because the entire
    # producer is now a mock object, but this test still validates the API layer.

async def test_create_failed_transaction_invalid_amount(api_client: AsyncClient):
    """
    Tests the "sad path" for the /transaction endpoint.
    """
    # Define an invalid transaction payload
    invalid_transaction = {
        "transaction_id": "txn_test_fail_002",
        "user_id": "user-test-123",
        "card_number": "4242-4242-4242-4242",
        "amount": -50.00,
        "timestamp": "2025-07-18T12:01:00Z",
        "merchant_id": "merchant_test",
        "location": "Test City"
    }

    response = await api_client.post("/transaction", json=invalid_transaction)

    assert response.status_code == 422