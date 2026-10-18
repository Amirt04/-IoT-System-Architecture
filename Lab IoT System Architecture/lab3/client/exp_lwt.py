"""Эксперимент LWT: аварийное завершение клиента (kill -9) и время появления статуса offline.

Сценарий A — kill -9 при живой сети: процесс убит, но ядро ОС закрывает его TCP-сокет (FIN/RST),
             брокер сразу видит обрыв без DISCONNECT и публикует Will.
Сценарий B — «потеря питания / пропал Wi-Fi»: сначала пакеты клиента перестают доходить
             (netem 100 % на его порт), затем kill -9. FIN не доходит — брокер узнаёт о гибели
             клиента только по истечении keepalive × 1,5. Проверяется для keepalive 5, 10 и 20 с.
"""
import os
import random
import signal
import subprocess
import sys
import threading
import time

import paho.mqtt.client as mqtt

from common import BROKER, PORT, log, save_json, status_topic, uid
from netctl import Net

HERE = os.path.dirname(os.path.abspath(__file__))


def observe(topic):
    ev, box = threading.Event(), {}
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("obs"))

    def on_msg(cl, u, m):
        if m.payload == b"offline" and not m.retain:
            box["t"] = time.time()
            ev.set()

    c.on_message = on_msg
    c.on_connect = lambda cl, u, f, rc, p: cl.subscribe(topic, 1)
    c.connect(BROKER, PORT, 30)
    c.loop_start()
    time.sleep(0.5)
    return c, ev, box


def case(net, name, keepalive, silent):
    dev = f"dev-lwt-{name}"
    port = random.randint(41000, 48999)              # свой исходящий порт: по нему режется связь в сценарии B
    topic = status_topic("astana", dev)
    victim = subprocess.Popen([sys.executable, os.path.join(HERE, "lwt_victim.py"), "--keepalive", str(keepalive),
                               "--port", str(port), "--dev", dev], stdout=subprocess.PIPE, text=True)
    if victim.stdout.readline().strip() != "READY":
        raise RuntimeError("жертва не подключилась")
    obs, ev, box = observe(topic)
    time.sleep(keepalive * 0.4)                     # клиент поработал, обменялся PINGREQ
    if silent:
        net.block_port(port)
        log(f"[{name}] keepalive {keepalive} с: связь клиента оборвана (пакеты не доходят), затем kill -9")
    else:
        log(f"[{name}] keepalive {keepalive} с: kill -9 при работающей сети")
    t_kill = time.time()
    os.kill(victim.pid, signal.SIGKILL)             # то же, что kill -9 <pid>
    victim.wait()
    ok = ev.wait(keepalive * 2 + 15)
    if silent:
        net.clear()
    obs.loop_stop()
    obs.disconnect()
    dt = round(box["t"] - t_kill, 3) if ok else None
    log(f"   offline через {dt} с" + (f" (теория: до 1,5 × {keepalive} = {1.5 * keepalive:g} с)" if silent else ""))
    return {"case": name, "scenario": "глухой обрыв + kill -9" if silent else "kill -9, сеть работает",
            "keepalive_s": keepalive, "limit_1_5k_s": 1.5 * keepalive, "lwt_delay_s": dt}


def main():
    net = Net()
    rows = []
    try:
        rows.append(case(net, "a-k10", 10, silent=False))
        for k in (5, 10, 20):
            rows.append(case(net, f"b-k{k}", k, silent=True))
    finally:
        net.close()
    save_json("lwt.json", rows)
    print("\n===== ИТОГ LWT =====")
    print(f"{'сценарий':<26} {'keepalive, с':>12} {'1,5×K, с':>9} {'LWT через, с':>13}")
    for r in rows:
        print(f"{r['scenario']:<26} {r['keepalive_s']:>12} {r['limit_1_5k_s']:>9g} {r['lwt_delay_s']!s:>13}")


if __name__ == "__main__":
    main()
