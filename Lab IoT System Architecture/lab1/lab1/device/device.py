"""Виртуальное IoT-устройство (Lab 1, SW-вариант).

Каждые SAMPLE_INTERVAL секунд: опрос 4 каналов -> валидация -> кодирование
(JSON или CBOR) -> запись в локальный кольцевой буфер -> отправка из буфера.

Буфер — своя реализация (RingBuffer), а не очередь paho: устройство само
решает, что хранить при обрыве и что выбрасывать при переполнении.
Отправка «at-least-once»: запись удаляется из буфера только после PUBACK.
Возможный дубль (PUBACK потерян при обрыве) отсеивает коллектор по seq.
"""
import os
import signal
import threading
import time
from collections import deque
from datetime import datetime

import paho.mqtt.client as mqtt

from model import Device, encode

HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
DEVICE_ID = os.getenv("DEVICE_ID", "env-node-01")
INTERVAL = float(os.getenv("SAMPLE_INTERVAL", "5"))
ENCODING = os.getenv("ENCODING", "json").lower()          # json | cbor
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", "500"))
OVERFLOW = os.getenv("BUFFER_OVERFLOW", "drop_oldest")    # drop_oldest | drop_newest
DAY_SECONDS = float(os.getenv("DAY_SECONDS", "86400"))
FAULTS = os.getenv("FAULTS", "true").lower() == "true"
STATS_EVERY = int(os.getenv("STATS_EVERY", "12"))         # сводка каждые N отсчётов
FLUSH_DELAY = float(os.getenv("FLUSH_DELAY", "2"))        # пауза после переподключения перед выгрузкой

TOPIC = f"iot/{DEVICE_ID}/telemetry/{ENCODING}"
TOPIC_STATUS = f"iot/{DEVICE_ID}/status"


def log(msg):
    print(f"{datetime.now().isoformat(timespec='seconds')} [{DEVICE_ID}] {msg}", flush=True)


class RingBuffer:
    """Кольцевой буфер фиксированной ёмкости с явной политикой переполнения."""

    def __init__(self, capacity, policy):
        self.q, self.capacity, self.policy = deque(), capacity, policy
        self.lock = threading.Lock()
        self.dropped = 0          # сколько записей потеряно из-за переполнения
        self.high_water = 0       # максимальное заполнение за время работы

    def push(self, item):
        with self.lock:
            if len(self.q) >= self.capacity:
                self.dropped += 1
                if self.policy == "drop_newest":
                    return False            # новую запись не принимаем
                self.q.popleft()            # drop_oldest: вытесняем самую старую
            self.q.append(item)
            self.high_water = max(self.high_water, len(self.q))
            return True

    def peek(self):
        with self.lock:
            return self.q[0] if self.q else None

    def pop(self):
        with self.lock:
            return self.q.popleft() if self.q else None

    def __len__(self):
        return len(self.q)


running = True
connected = threading.Event()
connected_at = [0.0]


def on_connect(client, userdata, flags, rc, props):
    if rc == 0:
        connected_at[0] = time.monotonic()
        connected.set()
        client.publish(TOPIC_STATUS, "online", qos=1, retain=True)
        log(f"connected to {HOST}:{PORT}, buffered={len(buffer)}")


def on_disconnect(client, userdata, flags, rc, props):
    if connected.is_set():
        log(f"disconnected ({rc}); buffering locally, capacity={BUFFER_SIZE}")
    connected.clear()


def sender(client):
    """Поток отправки: выгружает буфер, пока есть связь. Порядок FIFO."""
    sent_total = 0
    while running or (len(buffer) and connected.is_set()):
        item = buffer.peek()
        if item is None or not connected.is_set() or time.monotonic() - connected_at[0] < FLUSH_DELAY:
            time.sleep(0.2)
            continue
        seq, payload = item
        info = client.publish(TOPIC, payload, qos=1)
        try:
            info.wait_for_publish(timeout=3)
        except (RuntimeError, ValueError):
            pass
        if info.is_published():
            buffer.pop()
            sent_total += 1
        else:
            time.sleep(0.5)  # нет PUBACK — запись остаётся в буфере, повторим


def stop(*_):
    global running
    running = False


buffer = RingBuffer(BUFFER_SIZE, OVERFLOW)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    dev = Device(DEVICE_ID, day_seconds=DAY_SECONDS, faults=FAULTS)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=DEVICE_ID, clean_session=True)
    client.on_connect, client.on_disconnect = on_connect, on_disconnect
    client.will_set(TOPIC_STATUS, "offline", qos=1, retain=True)
    client.reconnect_delay_set(1, 10)
    client.connect_async(HOST, PORT, keepalive=15)
    client.loop_start()
    threading.Thread(target=sender, args=(client,), daemon=True).start()

    log(f"start: encoding={ENCODING}, interval={INTERVAL}s, buffer={BUFFER_SIZE} ({OVERFLOW}), topic={TOPIC}")
    t0, next_tick = time.monotonic(), time.monotonic()
    while running:
        msg = dev.sample(time.monotonic() - t0)
        payload = encode(msg, ENCODING)
        size_json, size_cbor = len(encode(msg, "json")), len(encode(msg, "cbor"))
        accepted = buffer.push((msg["seq"], payload))
        state = "online" if connected.is_set() else "OFFLINE"
        flags = f" quality={msg['quality']}" if "quality" in msg else ""
        log(f"seq={msg['seq']} {state} buf={len(buffer)}/{BUFFER_SIZE} "
            f"json={size_json}B cbor={size_cbor}B{'' if accepted else ' REJECTED'}{flags}")
        if msg["seq"] and msg["seq"] % STATS_EVERY == 0:
            log(f"stats: buffered={len(buffer)} high_water={buffer.high_water} dropped={buffer.dropped}")
        next_tick += INTERVAL
        time.sleep(max(0.0, next_tick - time.monotonic()))

    time.sleep(1)
    log(f"stopped: buffered={len(buffer)} high_water={buffer.high_water} dropped={buffer.dropped}")
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
