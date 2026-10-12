"""Коллектор (приёмная сторона) для проверки Lab 1.

* декодирует JSON/CBOR (формат — последний уровень топика);
* валидирует сообщение по JSON Schema;
* ставит ingest_ts и считает задержку ingest_ts - device_ts;
* проверяет непрерывность seq: пропуски, дубли, нарушение порядка;
* пишет каждую запись в data/received.csv и сводку в data/summary.json.
"""
import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import cbor2
import paho.mqtt.client as mqtt
from jsonschema import Draft202012Validator, FormatChecker

HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
SCHEMA = os.getenv("SCHEMA_PATH", "/schema/telemetry.schema.json")
OUT = os.getenv("DATA_DIR", "/data")
SUMMARY_EVERY = float(os.getenv("SUMMARY_EVERY", "60"))

validator = Draft202012Validator(json.load(open(SCHEMA, encoding="utf-8")),
                                 format_checker=FormatChecker())
os.makedirs(OUT, exist_ok=True)
csv_file = open(os.path.join(OUT, "received.csv"), "a", newline="", encoding="utf-8")
writer = csv.writer(csv_file)
if csv_file.tell() == 0:
    writer.writerow(["ingest_ts", "device_ts", "latency_s", "device_id", "seq", "fmt", "bytes",
                     "valid", "status", "temperature", "humidity", "pressure", "motion",
                     "battery", "rssi", "quality"])

state = {}   # device_id -> {expected, seen, gaps, dup, ooo, ...}
totals = {"received": 0, "invalid": 0, "bytes": {"json": [], "cbor": []}}


def log(msg):
    print(f"{datetime.now().isoformat(timespec='seconds')} [collector] {msg}", flush=True)


def check_seq(dev, seq):
    old = state.get(dev)
    if old and seq == 0 and old["max"] > 0:
        # seq начался заново -> устройство перезагрузилось (счётчик не сохраняется в flash)
        log(f"{dev}: seq reset to 0 after {old['max']} -> device reboot, new session")
        state.pop(dev)
    s = state.setdefault(dev, {"first": seq, "expected": seq, "max": seq - 1, "seen": set(),
                               "missing": set(), "dup": 0, "ooo": 0, "late_fill": 0})
    if seq in s["seen"]:
        s["dup"] += 1
        return "duplicate"
    s["seen"].add(seq)
    if seq > s["max"] + 1:
        s["missing"].update(range(s["max"] + 1, seq))   # дырка в нумерации
        status = f"gap({seq - s['max'] - 1})"
    elif seq <= s["max"]:
        s["ooo"] += 1
        if seq in s["missing"]:
            s["missing"].discard(seq)
            s["late_fill"] += 1
        status = "out_of_order"
    else:
        status = "ok"
    s["max"] = max(s["max"], seq)
    return status


def on_message(client, userdata, m):
    ingest = datetime.now(timezone.utc)
    fmt = m.topic.rsplit("/", 1)[-1]
    try:
        msg = json.loads(m.payload) if fmt == "json" else cbor2.loads(m.payload)
    except Exception as e:  # noqa: BLE001
        totals["invalid"] += 1
        log(f"decode error on {m.topic}: {e}")
        return
    errors = sorted(validator.iter_errors(msg), key=lambda e: e.path)
    valid = not errors
    if not valid:
        totals["invalid"] += 1
        log(f"SCHEMA INVALID seq={msg.get('seq')}: {errors[0].message}")
    totals["received"] += 1
    totals["bytes"].setdefault(fmt, []).append(len(m.payload))
    dev, seq = msg.get("device_id", "?"), msg.get("seq", -1)
    status = check_seq(dev, seq)
    try:
        dts = datetime.fromisoformat(msg["ts"].replace("Z", "+00:00"))
        latency = (ingest - dts).total_seconds()
    except Exception:  # noqa: BLE001
        latency = None
    met = msg.get("metrics", {})
    writer.writerow([ingest.isoformat(timespec="milliseconds"), msg.get("ts"),
                     f"{latency:.3f}" if latency is not None else "", dev, seq, fmt, len(m.payload),
                     valid, status, met.get("temperature"), met.get("humidity"), met.get("pressure"),
                     met.get("motion"), msg.get("battery"), msg.get("rssi"),
                     json.dumps(msg.get("quality", {}))])
    csv_file.flush()
    if status != "ok" or latency is None or latency > 2 or "quality" in msg:
        extra = f" quality={msg['quality']}" if "quality" in msg else ""
        log(f"{dev} seq={seq} {status} latency={latency:.1f}s valid={valid}{extra}")


def summary():
    out = {"generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "received": totals["received"], "schema_invalid": totals["invalid"], "devices": {}}
    for fmt, sizes in totals["bytes"].items():
        if sizes:
            out[f"avg_bytes_{fmt}"] = round(sum(sizes) / len(sizes), 1)
    for dev, s in state.items():
        span = s["max"] - s["first"] + 1
        out["devices"][dev] = {
            "seq_first": s["first"], "seq_last": s["max"], "expected": span,
            "unique_received": len(s["seen"]), "missing": len(s["missing"]),
            "missing_ranges": ranges(sorted(s["missing"])), "duplicates": s["dup"],
            "out_of_order": s["ooo"], "loss_pct": round(100 * len(s["missing"]) / span, 2) if span else 0,
        }
    json.dump(out, open(os.path.join(OUT, "summary.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    for dev, d in out["devices"].items():
        log(f"SUMMARY {dev}: seq {d['seq_first']}..{d['seq_last']} expected={d['expected']} "
            f"unique={d['unique_received']} missing={d['missing']} {d['missing_ranges']} "
            f"dup={d['duplicates']} loss={d['loss_pct']}% invalid={out['schema_invalid']}")


def ranges(nums):
    res, start = [], None
    for i, n in enumerate(nums):
        if start is None:
            start = n
        if i == len(nums) - 1 or nums[i + 1] != n + 1:
            res.append(f"{start}-{n}" if n != start else str(n))
            start = None
    return res


running = True


def stop(*_):
    global running
    running = False


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="lab1-collector", clean_session=False)
    # постоянная сессия: пока коллектор офлайн, брокер копит для него QoS1 (нужно persistence true)
    client.on_connect = lambda c, u, f, rc, p: (c.subscribe("iot/+/telemetry/#", qos=1),
                                                log("subscribed iot/+/telemetry/#"))
    client.on_message = on_message
    client.reconnect_delay_set(1, 10)
    client.connect_async(HOST, PORT, 30)
    client.loop_start()
    last = time.monotonic()
    while running:
        time.sleep(0.5)
        if time.monotonic() - last >= SUMMARY_EVERY:
            summary()
            last = time.monotonic()
    summary()
    client.loop_stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
