import json
import sqlite3

import paho.mqtt.client as mqtt
from neo4j import GraphDatabase
from pymongo import MongoClient

from config import (MONGO_URI, NEO4J_URI, NEO4J_USER,
                    NEO4J_PASSWORD, NEO4J_DATABASE)

BROKER, PORT = "localhost", 1883

# ---------- MongoDB (Atlas) ----------
mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo["home_security"]
events = mongo_db["intrusion_events"]
alerts = mongo_db["alerts"]

# ---------- Neo4j (AuraDB) ----------
graph = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD),
                             max_transaction_retry_time=3)

# ---------- SQL database (SQLite) ----------
db = sqlite3.connect("home_security.db", check_same_thread=False)
db.execute("""
CREATE TABLE IF NOT EXISTS readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    house TEXT, room TEXT, sensor TEXT,
    value REAL, timestamp TEXT
)""")
db.commit()

# ---------- Alert rules ----------
TEMP_LIMIT = 60      # degrees C
GAS_LIMIT = 300      # ppm
last_temp = {}       # last temperature seen per room, used for the fire rule


def check_alerts(house, room, sensor, data):
    """Return an alert name, or None if everything is normal."""
    if sensor == "temperature":
        last_temp[room] = data["value"]
        if data["value"] > TEMP_LIMIT:
            return "HIGH_TEMPERATURE"
    elif sensor == "smoke" and data["detected"]:
        if last_temp.get(room, 0) > TEMP_LIMIT:
            return "FIRE"           # smoke + high temperature
        return "SMOKE"
    elif sensor == "gas" and data["value"] > GAS_LIMIT:
        return "GAS_LEAK"
    elif sensor == "intrusion" and data.get("opened"):
        return "INTRUSION"
    return None


# ---------- Safe database functions ----------
# If one database fails, the others must keep working.
def safe_mongo_insert(collection, document):
    """Save to MongoDB, but never crash the subscriber if it fails."""
    try:
        collection.insert_one(document)
    except Exception as error:
        print("MongoDB error:", str(error)[:120])


def save_alert_in_graph(house, room, alert_type, timestamp):
    """Create an Alert node and link it to the room where it happened."""
    try:
        graph.execute_query(
            """
            MATCH (r:Room {id: $house + '/' + $room})
            CREATE (a:Alert {type: $type, timestamp: $ts})
            CREATE (r)-[:HAD_ALERT]->(a)
            """,
            house=house, room=room, type=alert_type, ts=timestamp,
            database_=NEO4J_DATABASE,
        )
    except Exception as error:
        print("Neo4j error:", str(error)[:120])


# ---------- MQTT callbacks ----------
def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected to broker:", reason_code)
    client.subscribe("home/+/+/+", qos=1)   # house / room / sensor


def on_message(client, userdata, msg):
    try:
        _, house, room, sensor = msg.topic.split("/")
        data = json.loads(msg.payload.decode())
    except (ValueError, UnicodeDecodeError):
        print("Invalid message ignored:", msg.topic)
        return

    # Ignore our own alert messages coming back from the broker
    if room == "alerts":
        return

    # Store numeric readings in SQL
    if sensor in ("temperature", "gas"):
        db.execute(
            "INSERT INTO readings (house, room, sensor, value, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (house, room, sensor, data["value"], data.get("timestamp")),
        )
        db.commit()

    # Store intrusion events in MongoDB
    if sensor == "intrusion":
        safe_mongo_insert(events, {"house": house, "room": room, **data})

    # Check alert rules
    alert = check_alerts(house, room, sensor, data)
    if alert:
        print(f"*** ALERT {alert} in {house}/{room} -> {data}")
        safe_mongo_insert(alerts, {"house": house, "room": room,
                                   "type": alert, "data": data})
        save_alert_in_graph(house, room, alert, data.get("timestamp"))
        client.publish(f"home/{house}/alerts/{alert}",
                       json.dumps({"room": room, "data": data}), qos=1)


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT)

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("Stopped by user")
    client.disconnect()
    db.close()
    graph.close()