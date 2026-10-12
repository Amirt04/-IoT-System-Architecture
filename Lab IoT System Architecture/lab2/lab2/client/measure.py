"""Lab 2 — измерение характеристик канала (SW-режим, tc netem).

Для каждой серии и каждого профиля канала:
  1. iperf3 -c   -> throughput (TCP), ретрансмиты
  2. ping -c 200 -> RTT, jitter, packet loss
  3. MQTT 1000 сообщений при QoS 0 и QoS 1 -> доля доставки, задержка доставки, время PUBACK
  4. Время подключения: TCP connect + (TLS handshake) + MQTT CONNECT/CONNACK
Сырые результаты: /results/raw/s<серия>_<профиль>.json

Запуск в контейнере:  python measure.py            (3 серии, все профили)
                       python measure.py --quick    (1 серия, укороченные тесты)
"""
import argparse
import json
import os
import re
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from profiles import PROFILES, netem_args

BROKER = os.getenv("BROKER", "mosquitto")
IPERF = os.getenv("IPERF", "iperf")
IFACE = os.getenv("IFACE", "eth0")
OUT = os.getenv("RESULTS", "/results")


def log(msg):
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


def sh(cmd, timeout=120, check=False):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}: {r.stderr.strip()}")
    return r


# ---------------------------------------------------------------- netem
def netem_apply(p):
    sh(["tc", "qdisc", "replace", "dev", IFACE, "root", "netem", *netem_args(p)], check=True)


def netem_clear():
    sh(["tc", "qdisc", "del", "dev", IFACE, "root"])


def netem_selftest():
    try:
        sh(["tc", "qdisc", "replace", "dev", IFACE, "root", "netem", "delay", "1ms"], check=True)
        netem_clear()
    except Exception as e:  # noqa: BLE001
        sys.exit(f"tc netem недоступен: {e}\nПроверь cap_add: NET_ADMIN в docker-compose.yml.")


# ---------------------------------------------------------------- iperf3
def run_iperf(seconds):
    for attempt in range(3):
        r = sh(["iperf3", "-c", IPERF, "-t", str(seconds), "-J"], timeout=seconds + 60)
        try:
            j = json.loads(r.stdout)
            if "error" in j:
                raise ValueError(j["error"])
            return {"throughput_mbit": j["end"]["sum_received"]["bits_per_second"] / 1e6,
                    "retransmits": j["end"]["sum_sent"].get("retransmits"),
                    "seconds": seconds}
        except Exception as e:  # noqa: BLE001
            log(f"  iperf3 attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    return {"throughput_mbit": None, "retransmits": None, "seconds": seconds}


# ---------------------------------------------------------------- ping
def run_ping(count, interval):
    r = sh(["ping", "-n", "-c", str(count), "-i", str(interval), "-W", "2", BROKER],
           timeout=count * interval + 60)
    rtts = {int(m.group(1)): float(m.group(2))
            for m in re.finditer(r"icmp_seq=(\d+) .*?time=([\d.]+) ms", r.stdout)}
    seq_sorted = [rtts[k] for k in sorted(rtts)]
    jitter = (sum(abs(b - a) for a, b in zip(seq_sorted, seq_sorted[1:])) / (len(seq_sorted) - 1)
              if len(seq_sorted) > 1 else None)
    return {"sent": count, "received": len(rtts), "loss_pct": 100 * (1 - len(rtts) / count),
            "rtt_ms": seq_sorted, "jitter_ms": jitter}


def run_mtr(count=20):
    r = sh(["mtr", "-r", "-n", "-c", str(count), "-i", "0.2", BROKER], timeout=120)
    return r.stdout


# ---------------------------------------------------------------- MQTT delivery
def run_mqtt(qos, n, rate, tag):
    topic = f"lab2/{tag}/q{qos}"
    recv, dup, acks = {}, [0], {}
    sent_at, mid_sent = {}, {}
    sub_ready = threading.Event()

    def on_msg(c, u, m):
        t = time.time()
        seq, ts = m.payload.split(b",", 2)[:2]
        seq = int(seq)
        if seq in recv:
            dup[0] += 1
        else:
            recv[seq] = (t - float(ts)) * 1000

    def on_pub(c, u, mid, rc, props):
        if mid in mid_sent:
            acks[mid] = (time.time() - mid_sent[mid]) * 1000

    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sub-{tag}-{qos}")
    sub.on_message = on_msg
    sub.on_subscribe = lambda *a: sub_ready.set()
    sub.on_connect = lambda c, u, f, rc, p: c.subscribe(topic, qos=qos)
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"pub-{tag}-{qos}")
    pub.on_publish = on_pub
    for c in (sub, pub):
        c.reconnect_delay_set(1, 5)
        c.connect(BROKER, 1883, keepalive=10)
        c.loop_start()
    sub_ready.wait(30)
    time.sleep(0.5)

    period, nxt, t_start = 1.0 / rate, time.monotonic(), time.time()
    for i in range(n):
        now = time.time()
        payload = f"{i},{now:.6f},".encode().ljust(100, b"x")   # 100 Б полезной нагрузки
        info = pub.publish(topic, payload, qos=qos)
        sent_at[i] = now
        if qos:
            mid_sent[info.mid] = now
        nxt += period
        time.sleep(max(0.0, nxt - time.monotonic()))
    t_sent = time.time()
    deadline = time.time() + 20
    while len(recv) < n and time.time() < deadline:
        time.sleep(0.2)
    for c in (pub, sub):
        c.loop_stop()
        c.disconnect()
    return {"qos": qos, "sent": n, "delivered": len(recv), "duplicates": dup[0],
            "loss_pct": 100 * (1 - len(recv) / n), "rate_msg_s": rate,
            "send_duration_s": t_sent - t_start,
            "latency_ms": [recv[k] for k in sorted(recv)],
            "puback_ms": list(acks.values())}


# ---------------------------------------------------------------- connect time
def mqtt_connect_packet(client_id):
    cid = client_id.encode()
    var = b"\x00\x04MQTT\x04\x02\x00\x3c"          # MQTT 3.1.1, clean session, keepalive 60
    payload = struct.pack("!H", len(cid)) + cid
    rem = var + payload
    return b"\x10" + bytes([len(rem)]) + rem


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf


def connect_once(ip, tls, i):
    port = 8883 if tls else 1883
    t0 = time.perf_counter()
    s = socket.create_connection((ip, port), timeout=15)
    t1 = time.perf_counter()
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)   # без задержки Нейгла, как в MQTT-клиентах
    t2 = t1
    if tls:
        ctx = ssl.create_default_context()
        ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
        s = ctx.wrap_socket(s)
        t2 = time.perf_counter()
    s.sendall(mqtt_connect_packet(f"ct-{i}-{int(t0 * 1e6) % 10**9}"))
    connack = recv_exact(s, 4)
    t3 = time.perf_counter()
    try:
        s.sendall(b"\xe0\x00")      # DISCONNECT
        s.close()
    except OSError:
        pass
    if connack[0] != 0x20 or connack[3] != 0:
        raise ConnectionError(f"bad CONNACK {connack.hex()}")
    return {"tcp_ms": (t1 - t0) * 1000, "tls_ms": (t2 - t1) * 1000,
            "mqtt_ms": (t3 - t2) * 1000, "total_ms": (t3 - t0) * 1000}


def run_connect(reps):
    ip = socket.gethostbyname(BROKER)       # DNS вне замера: на устройстве IP обычно кэширован
    out = {"plain": [], "tls": [], "failures": {"plain": 0, "tls": 0}}
    for tls in (False, True):
        key = "tls" if tls else "plain"
        for i in range(reps):
            try:
                out[key].append(connect_once(ip, tls, i))
            except Exception as e:  # noqa: BLE001
                out["failures"][key] += 1
                log(f"  connect {key} #{i} failed: {e}")
            time.sleep(0.2)
    return out


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", type=int, default=3)
    ap.add_argument("--profiles", default="all", help="например d01,d10")
    ap.add_argument("--quick", action="store_true", help="1 серия, короткие тесты (проверка стенда)")
    a = ap.parse_args()

    cfg = dict(iperf_s=5, ping_n=200, ping_i=0.05, mqtt_n=1000, mqtt_rate=100, conn_reps=10)
    if a.quick:
        a.series = 1
        cfg.update(iperf_s=2, ping_n=50, mqtt_n=200, conn_reps=3)
    profiles = PROFILES if a.profiles == "all" else [p for p in PROFILES if p["name"] in a.profiles.split(",")]

    netem_selftest()
    raw = os.path.join(OUT, "raw")
    os.makedirs(raw, exist_ok=True)
    total = a.series * len(profiles)
    log(f"Start: {a.series} series x {len(profiles)} profiles, config={cfg}")
    t_all, k = time.time(), 0
    try:
        for s in range(1, a.series + 1):
            for p in profiles:
                k += 1
                log(f"[{k}/{total}] series {s}, {p['name']}: d={p['distance_m']} m, RSSI={p['rssi_dbm']} dBm, "
                    f"netem {' '.join(netem_args(p))}")
                netem_apply(p)
                time.sleep(1)
                res = {"series": s, "profile": p, "config": cfg,
                       "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                       "netem": sh(["tc", "qdisc", "show", "dev", IFACE]).stdout.strip()}
                res["iperf"] = run_iperf(cfg["iperf_s"])
                log(f"  throughput {res['iperf']['throughput_mbit'] or 0:.2f} Mbit/s, retr={res['iperf']['retransmits']}")
                res["ping"] = run_ping(cfg["ping_n"], cfg["ping_i"])
                rt = sorted(res["ping"]["rtt_ms"]) or [0]
                log(f"  ping loss {res['ping']['loss_pct']:.1f}%, RTT p50 {rt[len(rt) // 2]:.1f} ms, "
                    f"jitter {res['ping']['jitter_ms'] or 0:.1f} ms")
                if s == 1:
                    res["mtr"] = run_mtr()
                res["mqtt"] = []
                for q in (0, 1):
                    m = run_mqtt(q, cfg["mqtt_n"], cfg["mqtt_rate"], f"s{s}{p['name']}")
                    lat = sorted(m["latency_ms"]) or [0]
                    log(f"  MQTT QoS{q}: delivered {m['delivered']}/{m['sent']}, dup {m['duplicates']}, "
                        f"p50 {lat[len(lat) // 2]:.1f} ms, p99 {lat[int(len(lat) * .99) - 1]:.1f} ms")
                    res["mqtt"].append(m)
                res["connect"] = run_connect(cfg["conn_reps"])
                med = sorted(x["total_ms"] for x in res["connect"]["tls"]) or [0]
                log(f"  connect TLS+MQTT median {med[len(med) // 2]:.1f} ms")
                with open(os.path.join(raw, f"s{s}_{p['name']}.json"), "w") as f:
                    json.dump(res, f)
    finally:
        netem_clear()
    log(f"Done in {(time.time() - t_all) / 60:.1f} min. Raw data: {raw}")


if __name__ == "__main__":
    main()
