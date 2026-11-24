import json
import sqlite3
import threading
import time

import paho.mqtt.client as mqtt

BROKER, PORT = "localhost", 1883
QOS = 1
COMMIT_EVERY = 100               # 1 = save every message, 100 = save in batches
HOUSES_LIST = [1, 5, 20, 50]    # number of simulated houses to test
DURATION = 10                   # seconds of sending in each test
GRACE = 60                      # extra seconds to let the system catch up
MSGS_PER_HOUSE_PER_SEC = 4.5    # 3 rooms x 3 sensors every 2 seconds
OK_LATENCY_MS = 500             # "keeps up" means average latency below this
ROOMS = ["kitchen", "living_room", "bedroom"]
SENSORS = ["temperature", "smoke", "gas"]

# ---------- Separate test database ----------
db = sqlite3.connect("benchmark.db", check_same_thread=False)
db.execute("DROP TABLE IF EXISTS readings")
db.execute("""CREATE TABLE readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT, value REAL, sent_at REAL)""")
db.commit()

# State of the current test (reset before every test)
state = {"run": 0, "expected": 0, "received": 0, "latencies": []}
ready = threading.Event()   # set when the listener is subscribed
done = threading.Event()    # set when all messages of a test have arrived


# ---------- Listener (plays the role of the subscriber) ----------
def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe("bench/#", qos=QOS)


def on_subscribe(client, userdata, mid, reason_codes, properties):
    ready.set()


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        if data["run"] != state["run"]:
            return   # late message from an earlier test: ignore it
        state["latencies"].append(time.time() - data["sent_at"])
        db.execute("INSERT INTO readings (value, sent_at) VALUES (?, ?)",
                   (data["value"], data["sent_at"]))
        state["received"] += 1
        if state["received"] % COMMIT_EVERY == 0:
            db.commit()
        if state["received"] >= state["expected"]:
            db.commit()
            done.set()
    except Exception as error:
        print("Listener error:", error)


listener = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
listener.on_connect = on_connect
listener.on_subscribe = on_subscribe
listener.on_message = on_message
listener.connect(BROKER, PORT)
listener.loop_start()

if not ready.wait(timeout=10):
    print("PROBLEM: the listener never subscribed. Is Mosquitto running?")
    raise SystemExit

# ---------- Sender (plays the role of the sensors in all houses) ----------
sender = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
sender.connect(BROKER, PORT)
sender.loop_start()


def run_test(houses):
    offered = houses * MSGS_PER_HOUSE_PER_SEC     # messages per second
    n = int(offered * DURATION)                   # messages in this test

    state["run"] += 1
    state["expected"] = n
    state["received"] = 0
    state["latencies"] = []
    done.clear()
    db.execute("DELETE FROM readings")
    db.commit()

    print(f"\nTest with {houses} house(s): sending {n} messages "
          f"at {offered:.1f} msg/s for {DURATION} s...")
    start = time.time()
    for i in range(n):
        # Message i is due at start + i/offered: this spreads them evenly
        wait = start + i / offered - time.time()
        if wait > 0:
            time.sleep(wait)
        house = (i // 9) % houses + 1
        room = ROOMS[i % 3]
        sensor = SENSORS[(i // 3) % 3]
        payload = json.dumps({"run": state["run"], "value": 20 + (i % 10),
                              "sent_at": time.time()})
        sender.publish(f"bench/house{house}/{room}/{sensor}", payload, qos=QOS)

    # Wait until everything is processed, or give up after GRACE seconds
    deadline = time.time() + GRACE
    while not done.is_set() and time.time() < deadline:
        done.wait(timeout=0.5)
    total_time = time.time() - start

    received = state["received"]
    lat = state["latencies"]
    average_ms = sum(lat) / len(lat) * 1000 if lat else 0
    maximum_ms = max(lat) * 1000 if lat else 0
    keeps_up = received == n and average_ms < OK_LATENCY_MS
    return {
        "houses": houses, "offered": offered, "sent": n,
        "received": received, "throughput": received / total_time,
        "avg": average_ms, "max": maximum_ms,
        "verdict": "OK" if keeps_up else "FALLING BEHIND",
    }


# ---------- Run all tests ----------
print(f"QoS {QOS}, commit every {COMMIT_EVERY} message(s)")
results = [run_test(h) for h in HOUSES_LIST]

print("\n=========== RESULTS ===========")
print(f"QoS {QOS}, commit every {COMMIT_EVERY} message(s)\n")
print(f"{'Houses':>6} {'Offered':>9} {'Sent':>6} {'Processed':>10} "
      f"{'Throughput':>11} {'Avg lat':>9} {'Max lat':>9}  Verdict")
for r in results:
    print(f"{r['houses']:>6} {r['offered']:>7.1f}/s {r['sent']:>6} "
          f"{r['received']:>10} {r['throughput']:>9.0f}/s "
          f"{r['avg']:>7.0f}ms {r['max']:>7.0f}ms  {r['verdict']}")

listener.loop_stop()
sender.loop_stop()
listener.disconnect()
sender.disconnect()
db.close()