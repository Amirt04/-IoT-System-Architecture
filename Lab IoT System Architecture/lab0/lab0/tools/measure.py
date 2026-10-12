"""Замер интервала и размера MQTT-сообщений для отчёта Lab 0.

Запуск (с хоста, брокер на localhost:1883):
    pip install paho-mqtt
    python tools/measure.py --count 60          # ~5 минут при интервале 5 с
Результат: таблица в консоли + measurements.csv
"""
import argparse
import csv
import statistics
import time

import paho.mqtt.client as mqtt

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=1883)
ap.add_argument("--topic", default="iot/+/telemetry")
ap.add_argument("--count", type=int, default=60)
ap.add_argument("--csv", default="measurements.csv")
args = ap.parse_args()

rows = []  # (recv_time, topic, size)


def on_message(client, userdata, msg):
    rows.append((time.time(), msg.topic, len(msg.payload)))
    n = len(rows)
    dt = rows[-1][0] - rows[-2][0] if n > 1 else 0.0
    print(f"{n:4d}  {msg.topic}  {len(msg.payload)} B  dt={dt:.3f} s")
    if n >= args.count:
        client.disconnect()


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="lab0-measure")
client.on_connect = lambda c, u, f, rc, p: c.subscribe(args.topic, qos=1)
client.on_message = on_message
client.connect(args.host, args.port, 30)
print(f"Subscribed to {args.topic}, collecting {args.count} messages...")
client.loop_forever()

intervals = [b[0] - a[0] for a, b in zip(rows, rows[1:])]
sizes = [r[2] for r in rows]

with open(args.csv, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["n", "recv_unix", "topic", "size_bytes", "interval_s"])
    for i, r in enumerate(rows):
        w.writerow([i + 1, f"{r[0]:.3f}", r[1], r[2],
                    f"{intervals[i-1]:.3f}" if i else ""])

print("\n===== ИТОГ =====")
print(f"Сообщений:        {len(rows)}")
print(f"Интервал, с:      ср={statistics.mean(intervals):.3f}  "
      f"σ={statistics.pstdev(intervals):.3f}  "
      f"min={min(intervals):.3f}  max={max(intervals):.3f}")
print(f"Размер payload, Б: ср={statistics.mean(sizes):.1f}  "
      f"min={min(sizes)}  max={max(sizes)}")
print(f"Пропускная способность: {sum(sizes)/ (rows[-1][0]-rows[0][0]):.1f} Б/с (только payload)")
print(f"CSV сохранён: {args.csv}")
