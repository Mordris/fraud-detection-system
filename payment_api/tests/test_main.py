# payment_api/tests/test_main.py

import pytest
from httpx import AsyncClient, ASGITransport

# Import the FastAPI app instance from your main application file
from payment_api.main import app

# Mark all tests in this file as asynchronous
pytestmark = pytest.mark.asyncio

@pytest.fixture
async def api_client(mocker):
    """
    A robust pytest fixture that:
    1. Mocks external services (Kafka) before the app starts.
    2. Correctly handles the FastAPI lifespan events for startup/shutdown.
    3. Provides a test client to make requests.
    """
    # Patch the clients that try to make external connections during startup.
    # We mock them at the source where they are imported in main.py.
    mocker.patch("payment_api.main.KafkaAdminClient", autospec=True)
    producer_mock = mocker.patch("payment_api.main.KafkaProducer", autospec=True)

    # Use the app's own lifespan context manager. This is the modern and
    # correct way to handle startup/shutdown events during testing.
    async with app.router.lifespan_context(app):
        # Now that startup has run (with mocked clients), create the test client.
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Attach the producer mock to the client so we can access it in tests
            client.producer_mock = producer_mock
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

    # Get the mock instance that was created during startup and assert that
    # its 'send' method was called exactly once.
    producer_instance = api_client.producer_mock.return_value
    producer_instance.send.assert_called_once()


async def test_create_failed_transaction_invalid_amount(api_client: AsyncClient):
    """
    Tests the "sad path" where a request with invalid data is rejected.
    """
    # Define an invalid transaction payload
    invalid_transaction = {
        "transaction_id": "txn_test_fail_002",
        "user_id": "user-test-123",
        "card_number": "4242-4242-4242-4242",
        "amount": -50.00,  # Invalid amount
        "timestamp": "2025-07-18T12:01:00Z",
        "merchant_id": "merchant_test",
        "location": "Test City"
    }

    response = await api_client.post("/transaction", json=invalid_transaction)

    # FastAPI automatically returns a 422 Unprocessable Entity for Pydantic validation errors.
    assert response.status_code == 422