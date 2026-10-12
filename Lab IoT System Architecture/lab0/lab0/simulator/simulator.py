"""IoT-симулятор одного устройства: публикует температуру в MQTT раз в N секунд.

Топики:
  iot/<DEVICE_ID>/telemetry  — JSON с измерением (QoS из MQTT_QOS)
  iot/<DEVICE_ID>/status     — online/offline (retained, offline = Last Will)
"""
import json
import math
import os
import random
import signal
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
DEVICE_ID = os.getenv("DEVICE_ID", "sensor-01")
INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "5"))
QOS = int(os.getenv("MQTT_QOS", "1"))
# false = постоянная сессия: брокер помнит подписки/очередь QoS1 (нужно persistence true)
CLEAN = os.getenv("MQTT_CLEAN_SESSION", "false").lower() == "true"

TOPIC_DATA = f"iot/{DEVICE_ID}/telemetry"
TOPIC_STATUS = f"iot/{DEVICE_ID}/status"

running = True


def log(msg: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} [{DEVICE_ID}] {msg}", flush=True)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log(f"connected to {HOST}:{PORT}")
        client.publish(TOPIC_STATUS, "online", qos=1, retain=True)
    else:
        log(f"connect failed: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    log(f"disconnected ({reason_code}); paho will retry with backoff 1..30 s")


def read_temperature(t0: float) -> float:
    # плавная «суточная» синусоида + шум
    base = 22 + 3 * math.sin((time.time() - t0) / 120)
    return round(base + random.gauss(0, 0.2), 2)


def stop(*_):
    global running
    running = False


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=DEVICE_ID, clean_session=CLEAN)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.will_set(TOPIC_STATUS, "offline", qos=1, retain=True)   # Last Will
    client.reconnect_delay_set(min_delay=1, max_delay=30)           # повтор — задача клиента
    client.max_queued_messages_set(1000)                            # буфер на время обрыва

    # первое подключение: ждём брокер, если он ещё не поднялся
    while running:
        try:
            client.connect(HOST, PORT, keepalive=30)
            break
        except OSError as e:
            log(f"broker unavailable ({e}), retry in 3 s")
            time.sleep(3)

    client.loop_start()   # сетевой поток: keepalive + автопереподключение
    t0, seq = time.time(), 0
    next_tick = time.monotonic()

    while running:
        seq += 1
        payload = json.dumps({
            "device_id": DEVICE_ID,
            "seq": seq,
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "temperature": read_temperature(t0),
            "unit": "C",
        })
        info = client.publish(TOPIC_DATA, payload, qos=QOS)
        state = "sent" if client.is_connected() else "queued (offline)"
        log(f"#{seq} {state} rc={info.rc} {len(payload.encode())} B -> {TOPIC_DATA}")

        next_tick += INTERVAL                        # без накопления дрейфа
        time.sleep(max(0, next_tick - time.monotonic()))

    if client.is_connected():
        try:
            client.publish(TOPIC_STATUS, "offline", qos=1, retain=True).wait_for_publish(2)
        except (RuntimeError, ValueError):
            pass
    client.loop_stop()
    client.disconnect()
    log("stopped")


if __name__ == "__main__":
    main()
