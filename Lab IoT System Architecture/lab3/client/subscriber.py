"""Subscriber с wildcard-подписками: вывод, подсчёт сообщений и детекция пропусков/дублей по seq.

Пример:  python subscriber.py --duration 60
         python subscriber.py --sub "iot/v1/+/+/telemetry/temperature" --sub "iot/v1/#"
Подписки по умолчанию показывают разницу между '+' и '#'.
"""
import argparse
import collections
import json
import time

import paho.mqtt.client as mqtt

from common import BROKER, PORT, log, parse, save_json, uid

DEFAULT_SUBS = ["iot/v1/+/+/telemetry/temperature",   # '+' — один уровень: температура всех устройств
                "iot/v1/astana/+/telemetry/+",         # все метрики устройств площадки astana
                "iot/v1/#"]                            # '#' — всё дерево, включая status


class SeqTracker:
    """Учёт непрерывности seq по каждому потоку (топику)."""

    def __init__(self):
        self.last, self.seen = {}, collections.defaultdict(set)
        self.gaps = collections.Counter()
        self.dups = collections.Counter()

    def add(self, key, seq):
        if seq in self.seen[key]:
            self.dups[key] += 1
            return "dup"
        self.seen[key].add(seq)
        last = self.last.get(key)
        self.last[key] = seq if last is None else max(last, seq)
        if last is not None and seq > last + 1:
            self.gaps[key] += seq - last - 1
            return f"gap {seq - last - 1}"
        return "ok"

    def missing(self, key):
        s = self.seen[key]
        return (max(s) - min(s) + 1 - len(s)) if s else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sub", action="append", help="фильтр подписки (можно несколько)")
    ap.add_argument("--qos", type=int, default=1)
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    subs = a.sub or DEFAULT_SUBS

    per_filter = collections.Counter()
    per_topic = collections.Counter()
    tracker = SeqTracker()
    lat = []
    # каждая подписка — отдельный клиент, чтобы было видно, что ловит каждый фильтр
    clients = []
    for f in subs:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("sub"))

        def on_message(cl, u, m, flt=f):
            per_filter[flt] += 1
            if flt != subs[-1]:
                return
            per_topic[m.topic] += 1
            p = parse(m.payload)
            if isinstance(p, dict) and "seq" in p:
                st = tracker.add(m.topic, p["seq"])
                lat.append((time.time() - p["ts"]) * 1000)
                if not a.quiet or st != "ok":
                    log(f"{m.topic}  seq={p['seq']} v={p['v']}  {st}")
            else:
                log(f"{m.topic}  {m.payload.decode(errors='replace')}  retain={m.retain}")

        c.on_message = on_message
        c.on_connect = lambda cl, u, fl, rc, p, flt=f: cl.subscribe(flt, qos=a.qos)
        c.connect(BROKER, PORT, 30)
        c.loop_start()
        clients.append(c)
    log(f"подписки: {subs}; слушаю {a.duration:.0f} с")
    try:
        time.sleep(a.duration)
    except KeyboardInterrupt:
        pass
    for c in clients:
        c.loop_stop()
        c.disconnect()

    print("\n===== ИТОГ =====")
    for f in subs:
        print(f"{per_filter[f]:6d}  сообщений  <- {f}")
    tel = [t for t in per_topic if "/telemetry/" in t]
    print(f"Потоков телеметрии: {len(tel)}; всего пропусков по seq: {sum(tracker.missing(t) for t in tel)}; "
          f"дублей: {sum(tracker.dups.values())}")
    if lat:
        lat.sort()
        print(f"Задержка, мс: средняя {sum(lat) / len(lat):.1f}, p50 {lat[len(lat) // 2]:.1f}, max {lat[-1]:.1f}")
    save_json("subscriber.json", {"filters": dict(per_filter), "topics": dict(per_topic),
                                  "missing": {t: tracker.missing(t) for t in tel},
                                  "duplicates": dict(tracker.dups)})


if __name__ == "__main__":
    main()
