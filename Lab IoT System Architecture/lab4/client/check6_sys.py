"""Проверка 6. Метрики $SYS/#: подключения, сообщения, нагрузка.

Снимок без нагрузки и под нагрузкой (5 клиентов, суммарно ~200 сообщений/с в течение 15 с).
"""
import json
import threading
import time

from common import A, Check, Client, RESULTS, log, pw, read_sys

KEYS = [
    ("version", "$SYS/broker/version"),
    ("uptime", "$SYS/broker/uptime"),
    ("clients/connected", "$SYS/broker/clients/connected"),
    ("clients/disconnected", "$SYS/broker/clients/disconnected"),
    ("clients/maximum", "$SYS/broker/clients/maximum"),
    ("subscriptions/count", "$SYS/broker/subscriptions/count"),
    ("retained messages/count", "$SYS/broker/retained messages/count"),
    ("store/messages/count", "$SYS/broker/store/messages/count"),
    ("messages/received", "$SYS/broker/messages/received"),
    ("messages/sent", "$SYS/broker/messages/sent"),
    ("publish/messages/dropped", "$SYS/broker/publish/messages/dropped"),
    ("load/messages/received/1min", "$SYS/broker/load/messages/received/1min"),
    ("load/messages/sent/1min", "$SYS/broker/load/messages/sent/1min"),
    ("load/bytes/received/1min", "$SYS/broker/load/bytes/received/1min"),
    ("load/connections/1min", "$SYS/broker/load/connections/1min"),
    ("bridge state", "$SYS/broker/connection/bridge-a-to-b/state"),
]


def load(stop, rate=40):
    c = Client(A, "gateway01", pw("gateway01")).connect()
    i = 0
    while not stop.is_set():
        c.c.publish(f"iot/v1/astana/gw-load-{threading.get_ident() % 1000}/telemetry/t", json.dumps({"i": i}), qos=0)
        i += 1
        time.sleep(1 / rate)
    c.close()


def main():
    ch = Check(6, "Метрики $SYS")
    idle = read_sys(A, "dashboard", pw("dashboard"), seconds=6)
    sink = Client(A, "dashboard", pw("dashboard")).connect()
    sink.subscribe("iot/v1/astana/+/telemetry/#", 0)
    stop = threading.Event()
    th = [threading.Thread(target=load, args=(stop,)) for _ in range(5)]
    for t in th:
        t.start()
    log("нагрузка: 5 клиентов × 40 сообщений/с, жду 16 с")
    time.sleep(10)
    busy = read_sys(A, "dashboard", pw("dashboard"), seconds=6)
    stop.set()
    for t in th:
        t.join()
    sink.close()

    rows = []
    print(f"\n  {'метрика':<32} {'без нагрузки':>16} {'под нагрузкой':>16}")
    for name, topic in KEYS:
        a, b = idle.get(topic, "—"), busy.get(topic, "—")
        rows.append({"metric": name, "topic": topic, "idle": a, "load": b})
        print(f"  {name:<32} {a:>16} {b:>16}")
    with open(f"{RESULTS}/sys_metrics.json", "w", encoding="utf-8") as f:
        json.dump({"rows": rows, "all_idle": idle, "all_load": busy}, f, ensure_ascii=False, indent=2)

    ch.row("число метрик в $SYS/#", "> 40 топиков", f"{len(busy)}", len(busy) > 40)
    c0 = int(idle.get("$SYS/broker/clients/connected", "0") or 0)
    c1 = int(busy.get("$SYS/broker/clients/connected", "0") or 0)
    ch.row("clients/connected растёт под нагрузкой", "+6 клиентов (5 нагрузка + приёмник)", f"{c0} -> {c1}", c1 >= c0 + 5)
    l0 = float(idle.get("$SYS/broker/load/messages/received/1min", "0") or 0)
    l1 = float(busy.get("$SYS/broker/load/messages/received/1min", "0") or 0)
    ch.row("load/messages/received/1min растёт", "заметный рост (скользящее среднее за 1 мин)", f"{l0:.1f} -> {l1:.1f}",
           l1 > l0 + 50)
    bstate = next((v for k, v in busy.items() if k.startswith("$SYS/broker/connection/") and k.endswith("/state")), "—")
    ch.row("состояние моста видно в $SYS", "1", bstate, bstate == "1")
    return ch.save()


if __name__ == "__main__":
    main()
