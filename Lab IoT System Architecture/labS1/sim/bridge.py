"""MQTT-мост: доставка результатов симуляции на стенд курса (ветка «Network Server -> MQTT -> стенд»).

Моделирует сеть (по умолчанию N=50, SF7, период 60 с), разрешает коллизии той же моделью,
что и run_all.py, и воспроизводит события в реальном времени (с ускорением --speed):
  * доставленные пакеты -> application/labs1/device/<devEui>/event/up  (JSON в стиле ChirpStack v4)
  * сводка раз в 10 с     -> labs1/stats  (отправлено, доставлено, PDR)
Смотреть: MQTT Explorer (localhost:1883) или  mosquitto_sub -t 'application/#' -t 'labs1/#' -v
"""
import argparse
import base64
import json
import os
import random
import time
from datetime import datetime, timezone

import numpy as np
import paho.mqtt.client as mqtt

import network as nw
from airtime import time_on_air

FREQ = [868_100_000, 868_300_000, 868_500_000]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--sf", type=int, default=7)
    ap.add_argument("--period", type=float, default=60.0)
    ap.add_argument("--minutes", type=float, default=30.0, help="длительность модельного времени")
    ap.add_argument("--speed", type=float, default=10.0, help="ускорение воспроизведения")
    a = ap.parse_args()

    rng = np.random.default_rng(int(time.time()))
    periods = max(1, int(a.minutes * 60 / a.period))
    dist = nw.place_devices(a.n, rng)
    dev = np.repeat(np.arange(a.n), periods)
    k = np.tile(np.arange(periods), a.n)
    start = (k + rng.random(len(dev))) * a.period
    toa = time_on_air(a.sf, 20)
    dur = np.full(len(dev), toa)
    chan = rng.integers(0, nw.CHANNELS, size=len(dev))
    power = nw.PTX_DBM - nw.path_loss(dist[dev], rng)
    ok = nw.resolve(start, dur, chan, np.full(len(dev), a.sf), power)
    order = np.argsort(start)

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="labs1-bridge")
    c.connect(a.host, 1883, 30)
    c.loop_start()
    print(f"N={a.n} SF{a.sf} T={a.period}s ToA={toa * 1000:.1f} ms, {len(dev)} uplinks, speed x{a.speed}", flush=True)

    t0, sent, recv, fcnt, last_stat = time.monotonic(), 0, 0, {}, 0.0
    for i in order:
        wait = start[i] / a.speed - (time.monotonic() - t0)
        if wait > 0:
            time.sleep(wait)
        d = int(dev[i])
        fcnt[d] = fcnt.get(d, -1) + 1
        sent += 1
        if ok[i]:
            recv += 1
            dev_eui = f"70b3d57ed0{d:06x}"
            temp = round(20 + 3 * random.random(), 2)
            up = {
                "deduplicationId": f"{d}-{fcnt[d]}",
                "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "deviceInfo": {"applicationName": "labs1", "deviceName": f"node-{d:04d}", "devEui": dev_eui},
                "devAddr": f"{0x26000000 + d:08x}", "dr": 12 - a.sf, "fCnt": fcnt[d], "fPort": 1,
                "data": base64.b64encode(json.dumps({"t": temp}).encode()).decode(),
                "object": {"temperature": temp},
                "rxInfo": [{"gatewayId": "0016c001ff1e0001", "rssi": round(float(power[i]), 1),
                            "snr": round(float(power[i]) + 117, 1)}],
                "txInfo": {"frequency": FREQ[int(chan[i])],
                           "modulation": {"lora": {"bandwidth": 125000, "spreadingFactor": a.sf, "codeRate": "CR_4_5"}}},
            }
            c.publish(f"application/labs1/device/{dev_eui}/event/up", json.dumps(up), qos=0)
        if time.monotonic() - last_stat > 10:
            last_stat = time.monotonic()
            st = {"sent": sent, "delivered": recv, "pdr": round(recv / sent, 4)}
            c.publish("labs1/stats", json.dumps(st), retain=True)
            print(time.strftime("%H:%M:%S"), st, flush=True)
    c.publish("labs1/stats", json.dumps({"sent": sent, "delivered": recv, "pdr": round(recv / sent, 4), "done": True}), retain=True)
    time.sleep(1)
    c.loop_stop()
    print(f"Done: sent {sent}, delivered {recv}, PDR {recv / sent * 100:.1f} %")


if __name__ == "__main__":
    main()
