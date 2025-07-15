# alert_monitor/monitor.py

# --- Standard Library Imports ---
import os
import json
import logging
import asyncio
import time
import threading
from datetime import datetime
from typing import List, Dict, Any

# --- Third-Party Imports ---
import redis  # The standard synchronous Python client for Redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# --- Configuration ---
# Load configuration from environment variables, with sensible defaults for local development.
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_KEY = "fraud_alerts"  # The Redis list key where Flink pushes alerts.

# --- Logging Setup ---
# Configure basic logging to see application events in the console.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- FastAPI Application Initialization ---
# Create the main FastAPI application instance.
app = FastAPI(title="Fraud Alert Monitor", description="Real-time fraud detection alert monitoring system")

# --- WebSocket Connection Manager ---
# This class manages all active WebSocket connections from browser clients.
class ConnectionManager:
    """Manages active WebSocket connections and broadcasting messages."""
    def __init__(self):
        # A list to hold all active WebSocket connection objects.
        self.active_connections: List[WebSocket] = []
        # Get a reference to the main asyncio event loop.
        # This is crucial for safely sending messages from a background thread.
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:  # 'get_running_loop' fails if no loop is running
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    async def connect(self, websocket: WebSocket):
        """Accepts a new WebSocket connection and adds it to the list of active connections."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Removes a WebSocket connection when it is closed."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    def broadcast(self, message: str):
        """
        Broadcasts a message to all active WebSocket connections.
        This method is thread-safe.
        """
        # Since this method is called from a different thread (the Redis listener),
        # we must use `run_coroutine_threadsafe` to schedule the async `_broadcast`
        # coroutine to run on the main FastAPI/Uvicorn event loop.
        asyncio.run_coroutine_threadsafe(self._broadcast(message), self.loop)

    async def _broadcast(self, message: str):
        """The async part of the broadcast operation."""
        for connection in self.active_connections:
            # Send the message to each connected client.
            await connection.send_text(message)

# --- Global Application State ---
# A single instance of the ConnectionManager to be used by the whole application.
manager = ConnectionManager()
# An in-memory list to cache the most recent alerts.
# This allows new clients to get a history of recent events upon connecting.
recent_alerts: List[Dict[str, Any]] = []
MAX_RECENT_ALERTS = 100

# --- Background Task for Redis ---
def redis_listener_thread():
    """
    This function runs in a dedicated background thread.
    Its only job is to block and listen for new messages from the Redis queue.
    """
    logger.info("Starting Redis listener thread...")
    # This outer loop makes the listener resilient. If the connection to Redis
    # is ever lost, it will wait and then try to reconnect.
    while True:
        try:
            # Connect to Redis. `decode_responses=True` ensures we get strings, not bytes.
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            redis_client.ping() # Verify the connection is successful.
            logger.info("Redis listener connected.")
            
            # This inner loop continuously waits for new alerts.
            while True:
                # `blpop` is a blocking call that waits until an item is available in the list.
                # This is highly efficient as it doesn't waste CPU cycles polling.
                result = redis_client.blpop([REDIS_KEY])
                if result:
                    _, alert_data = result  # `blpop` returns a tuple: (key, value)
                    try:
                        # Parse the JSON string received from Redis into a Python dictionary.
                        alert = json.loads(alert_data)
                        # Enrich the alert with a timestamp of when it was received by the monitor.
                        alert['received_at'] = datetime.now().isoformat()
                        
                        # Add the alert to our in-memory cache.
                        recent_alerts.insert(0, alert)
                        # Trim the cache to prevent it from growing indefinitely.
                        if len(recent_alerts) > MAX_RECENT_ALERTS:
                            recent_alerts.pop()
                        
                        # Use the manager to broadcast the new alert to all connected dashboards.
                        manager.broadcast(json.dumps(alert))
                        logger.info(f"Broadcasted new fraud alert: {alert.get('transaction_id')}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing alert JSON: {e}")
        except Exception as e:
            # If any error occurs (e.g., Redis connection fails), log it and retry.
            logger.error(f"Redis listener thread error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

# --- FastAPI Event Handlers ---
@app.on_event("startup")
async def startup_event():
    """
    This function is called by FastAPI when the application starts up.
    It's the perfect place to start background tasks.
    """
    # Create and start the background thread for listening to Redis.
    # `daemon=True` ensures the thread will exit when the main application exits.
    listener_thread = threading.Thread(target=redis_listener_thread, daemon=True)
    listener_thread.start()

# --- FastAPI Endpoints ---
@app.get("/")
async def get_dashboard():
    """Serves the main HTML dashboard page."""
    # The HTML, CSS, and JavaScript for the frontend are all contained in this string.
    return HTMLResponse(content="""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Fraud Detection Dashboard</title>
        <link rel="icon" href="data:;base64,iVBORw0KGgo=">
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; text-align: center; }
            .stats { display: flex; gap: 20px; margin-bottom: 20px; }
            .stat-card { background: white; padding: 20px; border-radius: 8px; flex: 1; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }
            .stat-value { font-size: 2em; font-weight: bold; color: #3498db; }
            .stat-label { color: #7f8c8d; }
            #connection-status.disconnected { color: #e74c3c; }
            #connection-status.connected { color: #2ecc71; }
            .alert { background: #fff; border-left: 4px solid #e74c3c; padding: 15px; margin: 10px 0; border-radius: 4px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .alert-header { font-weight: bold; color: #e74c3c; }
            .alert-details { margin-top: 10px; color: #2c3e50; }
            .alert-time { color: #7f8c8d; font-size: 0.9em; margin-top: 5px; }
            #status { padding: 10px; margin-bottom: 20px; border-radius: 4px; text-align: center; font-weight: bold; }
            .status.connected { background: #d4edda; color: #155724; }
            .status.disconnected { background: #f8d7da; color: #721c24; }
            #alerts-container { max-height: 600px; overflow-y: auto; background: #fff; padding: 10px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header"><h1>Fraud Detection Dashboard</h1><p>Real-time monitoring of suspicious transactions</p></div>
            <div class="stats">
                <div class="stat-card"><div class="stat-value" id="total-alerts">0</div><div class="stat-label">Total Alerts</div></div>
                <div class="stat-card"><div class="stat-value" id="recent-alerts">0</div><div class="stat-label">Last Hour</div></div>
                <div class="stat-card"><div class="stat-value disconnected" id="connection-status">Disconnected</div><div class="stat-label">Status</div></div>
            </div>
            <div id="status" class="status disconnected">Disconnected from alert system</div>
            <div id="alerts-container"><p style="text-align: center; color: #7f8c8d; padding: 40px;">Waiting for fraud alerts...</p></div>
        </div>
        <script>
            let totalAlerts = 0;
            let recentAlerts = [];
            const alertsContainer = document.getElementById('alerts-container');
            const statusDiv = document.getElementById('status');
            const connectionStatus = document.getElementById('connection-status');
            function connect() {
                const ws = new WebSocket(`ws://${window.location.host}/ws`);
                ws.onopen = function(event) {
                    statusDiv.textContent = 'Connected to fraud alert system';
                    statusDiv.className = 'status connected';
                    connectionStatus.textContent = 'Connected';
                    connectionStatus.className = 'stat-value connected';
                };
                ws.onclose = function(event) {
                    statusDiv.textContent = 'Disconnected from alert system. Retrying...';
                    statusDiv.className = 'status disconnected';
                    connectionStatus.textContent = 'Disconnected';
                    connectionStatus.className = 'stat-value disconnected';
                    setTimeout(connect, 3000);
                };
                ws.onmessage = function(event) {
                    const alert = JSON.parse(event.data);
                    addAlert(alert);
                };
                ws.onerror = function(err) { console.error('WebSocket Error:', err); ws.close(); };
            }
            function addAlert(alert) {
                totalAlerts++;
                const now = new Date();
                recentAlerts.push(now);
                recentAlerts = recentAlerts.filter(time => now - time < 3600000);
                document.getElementById('total-alerts').textContent = totalAlerts;
                document.getElementById('recent-alerts').textContent = recentAlerts.length;
                if (totalAlerts === 1) { alertsContainer.innerHTML = ''; }
                const alertDiv = document.createElement('div');
                alertDiv.className = 'alert';
                alertDiv.innerHTML = `<div class="alert-header">🚨 FRAUD ALERT</div><div class="alert-details"><strong>Transaction ID:</strong> ${alert.transaction_id}<br><strong>User ID:</strong> ${alert.user_id}<br><strong>Amount:</strong> $${parseFloat(alert.amount).toFixed(2)}</div><div class="alert-time">Received: ${new Date(alert.received_at).toLocaleString()}</div>`;
                alertsContainer.insertBefore(alertDiv, alertsContainer.firstChild);
                while (alertsContainer.children.length > 100) { alertsContainer.removeChild(alertsContainer.lastChild); }
            }
            connect();
            setInterval(() => {
                const now = new Date();
                recentAlerts = recentAlerts.filter(time => now - time < 3600000);
                document.getElementById('recent-alerts').textContent = recentAlerts.length;
            }, 1000);
        </script>
    </body>
    </html>
    """)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """The endpoint that handles incoming WebSocket connections."""
    # Register the new client with the connection manager.
    await manager.connect(websocket)
    try:
        # Send a brief history of recent alerts to the newly connected client.
        if recent_alerts:
            for alert in reversed(recent_alerts):
                await websocket.send_text(json.dumps(alert))
        
        # This loop keeps the connection open. It waits for messages from the client,
        # which we don't use in this app, but it's necessary to prevent the
        # connection from being closed immediately.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        # If the client disconnects, remove them from the manager.
        manager.disconnect(websocket)