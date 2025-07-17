# alert_monitor/monitor.py

# --- Standard Library Imports ---
import os
import json
import logging
import asyncio
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Dict, Any

# --- Third-Party Imports ---
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# --- Configuration ---
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_KEY = "fraud_alerts"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- WebSocket Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    def broadcast(self, message: str):
        asyncio.run_coroutine_threadsafe(self._broadcast(message), self.loop)

    async def _broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

# --- Global Application State ---
manager = ConnectionManager()
recent_alerts: List[Dict[str, Any]] = []
MAX_RECENT_ALERTS = 100

# --- Background Task for Redis ---
def redis_listener_thread():
    logger.info("Starting Redis listener thread...")
    while True:
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            redis_client.ping()
            logger.info("Redis listener connected.")
            
            while True:
                result = redis_client.blpop([REDIS_KEY])
                if result:
                    _, alert_data = result
                    try:
                        alert = json.loads(alert_data)
                        alert['received_at'] = datetime.now().isoformat()
                        
                        recent_alerts.insert(0, alert)
                        if len(recent_alerts) > MAX_RECENT_ALERTS:
                            recent_alerts.pop()
                        
                        manager.broadcast(json.dumps(alert))
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing alert JSON: {e}")
        except Exception as e:
            logger.error(f"Redis listener thread error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

# --- Lifespan Context Manager (Modern Replacement for on_event) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the application's startup and shutdown logic.
    """
    # --- Code to run on startup ---
    logger.info("Application startup: Starting Redis listener thread...")
    listener_thread = threading.Thread(target=redis_listener_thread, daemon=True)
    listener_thread.start()
    
    yield # The application runs here

    # --- Code to run on shutdown ---
    logger.info("Application shutdown.")
    # No specific shutdown logic needed for the daemon thread

# --- FastAPI App Initialization ---
app = FastAPI(title="Fraud Alert Monitor", lifespan=lifespan)


# --- FastAPI Endpoints ---
@app.get("/")
async def get_dashboard():
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
    await manager.connect(websocket)
    try:
        if recent_alerts:
            for alert in reversed(recent_alerts):
                await websocket.send_text(json.dumps(alert))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Note: I have truncated the long HTML string for brevity. 
# Make sure to copy the full HTML from your original file into the """...""" block.