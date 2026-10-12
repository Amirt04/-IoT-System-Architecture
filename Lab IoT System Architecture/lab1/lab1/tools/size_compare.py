"""Сравнение размера сообщения в JSON и CBOR + трафик в сутки (Lab 1).

Генерирует N сообщений той же моделью, что и устройство, кодирует каждое
в разные форматы и считает средний размер payload, размер MQTT-пакета
PUBLISH (QoS 1) и трафик в сутки при заданном интервале.

Запуск:  python tools/size_compare.py [--n 2000] [--interval 5] [--fleet 1000]
Результат: таблица в консоли + data/size_compare.csv
"""
import argparse
import csv
import json
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone

import cbor2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "device"))
from model import Device, encode  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--n", type=int, default=2000)
ap.add_argument("--interval", type=float, default=5.0, help="период отправки, с")
ap.add_argument("--fleet", type=int, default=1000, help="число устройств для оценки парка")
ap.add_argument("--device-id", default="env-node-01")
args = ap.parse_args()

SHORT = {"schema_version": "v", "device_id": "d", "ts": "t", "seq": "s", "metrics": "m",
         "temperature": "T", "humidity": "H", "pressure": "P", "motion": "M",
         "battery": "b", "rssi": "r", "quality": "q"}


def compact(msg):
    """CBOR-компакт: короткие ключи + время как целое число миллисекунд."""
    def short(o):
        if isinstance(o, dict):
            return {SHORT.get(k, k): short(v) for k, v in o.items()}
        return o
    m = dict(msg)
    m["ts"] = int(datetime.fromisoformat(m["ts"].replace("Z", "+00:00")).timestamp() * 1000)
    return cbor2.dumps(short(m))


def mqtt_publish_size(payload_len, topic):
    """PUBLISH QoS1: 1 Б тип + длина остатка (1–4 Б) + 2 Б длина топика + топик + 2 Б packet id."""
    rem = 2 + len(topic) + 2 + payload_len
    rem_len_bytes = 1 if rem < 128 else 2 if rem < 16384 else 3
    return 1 + rem_len_bytes + rem


dev = Device(args.device_id, day_seconds=86400, seed=42)
start = datetime(2026, 10, 12, tzinfo=timezone.utc)
sizes = {"JSON": [], "CBOR": [], "CBOR компакт": []}
for i in range(args.n):
    msg = dev.sample(i * args.interval, now=start + timedelta(seconds=i * args.interval))
    sizes["JSON"].append(len(encode(msg, "json")))
    sizes["CBOR"].append(len(encode(msg, "cbor")))
    sizes["CBOR компакт"].append(len(compact(msg)))

per_day = 86400 / args.interval
topic = f"iot/{args.device_id}/telemetry/json"
PUBACK = 4
rows = []
base = statistics.mean(sizes["JSON"])
for fmt, s in sizes.items():
    avg = statistics.mean(s)
    pkt = mqtt_publish_size(round(avg), topic)
    day_payload = avg * per_day
    day_wire = (pkt + PUBACK) * per_day
    rows.append({
        "format": fmt,
        "payload_avg_B": round(avg, 1),
        "payload_min_max_B": f"{min(s)}–{max(s)}",
        "mqtt_packet_B": pkt,
        "saving_vs_json_pct": round(100 * (1 - avg / base), 1),
        "payload_per_day_MB": round(day_payload / 1e6, 2),
        "wire_per_day_MB": round(day_wire / 1e6, 2),
        "fleet_wire_per_day_GB": round(day_wire * args.fleet / 1e9, 2),
        "fleet_wire_per_month_GB": round(day_wire * args.fleet * 30 / 1e9, 1),
    })

print(f"Сообщений: {args.n}, интервал {args.interval} с -> {per_day:.0f} сообщений/сутки на устройство\n")
hdr = ["Формат", "Payload, Б (ср.)", "min–max", "Пакет MQTT, Б", "Экономия, %",
       "Payload/сутки, МБ", "С заголовками/сутки, МБ", f"Парк {args.fleet}: ГБ/сутки", "ГБ/месяц"]
print(" | ".join(hdr))
for r in rows:
    print(" | ".join(str(v) for v in r.values()))

sample = dev.sample(0, now=start)
print("\nПример сообщения (JSON):\n" + json.dumps(sample, ensure_ascii=False, indent=2))
print("CBOR hex:", encode(sample, "cbor").hex()[:120], "...")

os.makedirs(os.path.join(os.path.dirname(__file__), "..", "data"), exist_ok=True)
out = os.path.join(os.path.dirname(__file__), "..", "data", "size_compare.csv")
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)
print(f"\nCSV: {os.path.normpath(out)}")
