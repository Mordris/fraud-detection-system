# payment_api/tests/test_main.py

import pytest
from httpx import AsyncClient, ASGITransport

# Import the FastAPI app instance from your main application file
from payment_api.main import app, startup_event, shutdown_event

# Mark all tests in this file as asynchronous
pytestmark = pytest.mark.asyncio

@pytest.fixture(scope="session")
async def api_client():
    # Manually run the startup event before the tests
    startup_event()
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    
    # Manually run the shutdown event after the tests are done
    shutdown_event()

# --- Test Cases ---

async def test_health_check(api_client: AsyncClient):
    """
    Tests if the /health endpoint is working and returns a 200 OK status.
    """
    response = await api_client.get("/health")
    assert response.status_code == 200
    # Check that the response body is what we expect
    assert response.json()["api_status"] == "ok"

async def test_create_successful_transaction(api_client: AsyncClient, mocker):
    """
    Tests the "happy path" for the /transaction endpoint.
    It ensures a valid transaction is accepted and mocks the Kafka producer.
    """
    # Use pytest-mock to "patch" the KafkaProducer's 'send' method.
    # This prevents the test from needing a real Kafka connection.
    mock_send = mocker.patch("payment_api.main.producer.send")
    # Also mock the 'get' method that blocks for the result.
    mock_send.return_value.get.return_value = "Mocked Kafka Response"

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
    assert response.json()["transaction_id"] == "txn_test_success_001"

    # Assert that our mock Kafka producer's 'send' method was called exactly once
    mock_send.assert_called_once()

async def test_create_failed_transaction_invalid_amount(api_client: AsyncClient):
    """
    Tests the "sad path" for the /transaction endpoint.
    It ensures a transaction with an invalid amount (negative) is rejected.
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

    # FastAPI automatically returns a 422 Unprocessable Entity for validation errors.
    assert response.status_code == 422