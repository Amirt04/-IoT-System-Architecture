"""Проверка 5. Мост A -> B: передача в нужных направлениях и отсутствие петель.

Настройки моста на брокере A:
  iot/v1/+/+/telemetry/#  out   (A -> B)
  iot/v1/+/+/status       out
  iot/v1/+/+/cmd/#        in    (B -> A)
  bridge/test/#           both  (в обе стороны — проверка петли)
"""
import json
import time

from common import A, B, Check, Client, pw, read_sys


def main():
    ch = Check(5, "Bridge между брокерами A и B")
    st = read_sys(A, "dashboard", pw("dashboard"), seconds=1.5, prefix="$SYS/broker/connection/#")
    state = next((v for k, v in st.items() if k.endswith("/state")), "?")
    ch.row("состояние моста ($SYS/broker/connection/<id>/state)", "1 (подключён)", state, state == "1")

    on_b = Client(B, "viewer", pw("viewer", "B")).connect()
    on_b.subscribe("#", 1)
    on_a = Client(A, "gateway01", pw("gateway01")).connect()
    on_a.subscribe("iot/v1/astana/#", 1)
    on_a.subscribe("bridge/test/#", 1)
    dev = Client(A, "device01", pw("device01")).connect()
    time.sleep(0.8)

    # A -> B: телеметрия
    lat = []
    for i in range(20):
        dev.publish("iot/v1/astana/device01/telemetry/temperature", json.dumps({"seq": i, "ts": time.time()}))
    time.sleep(1.5)
    for m in on_b.received("iot/v1/astana/device01/telemetry"):
        lat.append((m["t"] - json.loads(m["payload"])["ts"]) * 1000)
    n = len(lat)
    ch.row("телеметрия device01 на A -> брокер B", "20 из 20", f"{n} из 20, задержка моста средн. "
           f"{sum(lat) / n:.1f} мс" if n else "0", n == 20)

    # B -> A: команда
    on_b.publish("iot/v1/astana/device01/cmd/led", "on")
    time.sleep(1.0)
    n_cmd = len(on_a.received("iot/v1/astana/device01/cmd/led"))
    ch.row("команда, опубликованная на B -> брокер A", "1 сообщение", f"{n_cmd}", n_cmd == 1)

    # направление out: команда, опубликованная на A, не должна уходить в B
    svc = Client(A, "svc-ingest", pw("svc-ingest")).connect()
    svc.publish("iot/v1/astana/device01/cmd/fw", "update")
    time.sleep(1.0)
    n_back = len(on_b.received("iot/v1/astana/device01/cmd/fw"))
    ch.row("команда, опубликованная на A, в брокер B (направление in only)", "0 — не передаётся", f"{n_back}",
           n_back == 0)

    # петля: bridge/test/# в обе стороны
    on_a.publish("bridge/test/ping", "from-A")
    time.sleep(3)
    a_ping, b_ping = len(on_a.received("bridge/test/ping")), len(on_b.received("bridge/test/ping"))
    ch.row("ping с A по both-топику: копий на A / на B за 3 с", "1 / 1 — эхо не возвращается",
           f"{a_ping} / {b_ping}", a_ping == 1 and b_ping == 1)
    on_b.publish("bridge/test/pong", "from-B")
    time.sleep(3)
    a_pong, b_pong = len(on_a.received("bridge/test/pong")), len(on_b.received("bridge/test/pong"))
    ch.row("pong с B по both-топику: копий на A / на B за 3 с", "1 / 1 — петли нет", f"{a_pong} / {b_pong}",
           a_pong == 1 and b_pong == 1)

    for c in (on_a, on_b, dev, svc):
        c.close()
    ch.info(bridge_latency_ms=round(sum(lat) / len(lat), 2) if lat else None)
    return ch.save()


if __name__ == "__main__":
    main()
