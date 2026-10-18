"""«Жертва» для эксперимента LWT: подключается с Last Will и работает, пока её не убьют kill -9.

python lwt_victim.py --keepalive 10 --port 41010
"""
import argparse
import time

import paho.mqtt.client as mqtt

from common import BROKER, PORT, status_topic

ap = argparse.ArgumentParser()
ap.add_argument("--keepalive", type=int, default=10)
ap.add_argument("--port", type=int, default=0, help="фиксированный исходящий порт (для глухого обрыва)")
ap.add_argument("--dev", default="dev-lwt")
a = ap.parse_args()

topic = status_topic("astana", a.dev)
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"lwt-{a.dev}")
c.will_set(topic, "offline", qos=1, retain=True)
c.connect(BROKER, PORT, keepalive=a.keepalive, bind_port=a.port)
c.loop_start()
c.publish(topic, "online", qos=1, retain=True).wait_for_publish(5)
print("READY", flush=True)
while True:
    time.sleep(1)
