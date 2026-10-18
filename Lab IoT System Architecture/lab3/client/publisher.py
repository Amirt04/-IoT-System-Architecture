"""Publisher × N: каждое виртуальное устройство публикует 4 метрики с порядковым номером seq.

Пример:  python publisher.py --devices 3 --interval 2 --count 30 --qos 1
Каждое устройство — отдельное MQTT-подключение со своим LWT (status = offline).
"""
import argparse
import math
import random
import threading
import time

import paho.mqtt.client as mqtt

from common import BROKER, METRICS, PORT, log, payload, status_topic, t_topic


def value(metric, t):
    if metric == "temperature":
        return round(21 + 3 * math.sin(t / 60) + random.gauss(0, 0.1), 2)
    if metric == "humidity":
        return round(45 + 8 * math.cos(t / 90) + random.gauss(0, 0.5), 1)
    if metric == "pressure":
        return round(965 + random.gauss(0, 0.2), 1)
    return random.random() < 0.1


def device(site, dev, a, stop):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{site}-{dev}")
    c.will_set(status_topic(site, dev), "offline", qos=1, retain=True)
    c.connect(BROKER, PORT, keepalive=a.keepalive)
    c.loop_start()
    c.publish(status_topic(site, dev), "online", qos=1, retain=True)
    t0 = time.time()
    for seq in range(a.count):
        if stop.is_set():
            break
        for m in METRICS:
            c.publish(t_topic(site, dev, m), payload(seq, value(m, time.time() - t0)), qos=a.qos)
        log(f"{site}/{dev} seq={seq} -> 4 метрики (QoS {a.qos})")
        stop.wait(a.interval)
    c.publish(status_topic(site, dev), "offline", qos=1, retain=True).wait_for_publish(3)
    c.loop_stop()
    c.disconnect()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--devices", type=int, default=3)
    ap.add_argument("--sites", default="astana,almaty")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--count", type=int, default=30)
    ap.add_argument("--qos", type=int, default=1, choices=(0, 1, 2))
    ap.add_argument("--keepalive", type=int, default=10)
    a = ap.parse_args()
    sites = a.sites.split(",")
    stop = threading.Event()
    th = [threading.Thread(target=device, args=(sites[i % len(sites)], f"dev-{i + 1:03d}", a, stop))
          for i in range(a.devices)]
    for t in th:
        t.start()
        time.sleep(0.1)
    try:
        for t in th:
            t.join()
    except KeyboardInterrupt:
        stop.set()


if __name__ == "__main__":
    main()
