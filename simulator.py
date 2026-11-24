import json
import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
HOUSE = "house1"
ROOMS = ["kitchen", "living_room", "bedroom"]

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT)
client.loop_start()


def publish(topic, payload):
    payload["timestamp"] = datetime.now().isoformat()
    client.publish(topic, json.dumps(payload), qos=1)
    print(topic, payload)


try:
    while True:
        for room in ROOMS:
            # Normal readings, with a small chance of a dangerous value
            temp = random.gauss(24, 2)
            if random.random() < 0.02:
                temp = random.uniform(60, 90)   # simulated fire
            publish(f"home/{HOUSE}/{room}/temperature",
                    {"value": round(temp, 1), "unit": "C"})

            smoke = random.random() < 0.01
            publish(f"home/{HOUSE}/{room}/smoke", {"detected": smoke})

            gas = random.gauss(50, 10)
            if random.random() < 0.01:
                gas = random.uniform(400, 800)  # simulated gas leak
            publish(f"home/{HOUSE}/{room}/gas",
                    {"value": round(gas, 1), "unit": "ppm"})

            if random.random() < 0.05:
                publish(f"home/{HOUSE}/{room}/intrusion",
                        {"sensor": "door_contact", "opened": True})
        time.sleep(2)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()