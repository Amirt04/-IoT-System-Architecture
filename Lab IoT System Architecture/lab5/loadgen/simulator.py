"""Симулятор N устройств: у каждого своё MQTT-подключение, публикация телеметрии QoS 1.

python simulator.py --devices 1000 --rate 1 --duration 60 [--procs 4] [--follow-ctrl] [--json out.json]
python simulator.py --devices 100 --count 300 --rate 0            # «залп»: 30 000 сообщений как можно быстрее

Топик: iot/v1/<site>/<dev-NNNN>/telemetry.  --follow-ctrl: слушать iot/v1/ctrl/backpressure
и умножать частоту на factor (режим throttle у ingest).
"""
import argparse
import asyncio
import json
import multiprocessing as mp
import os
import random
import sys
import time

import aiomqtt

BROKER = os.getenv("BROKER", "mosquitto")
SITES = ["astana", "almaty", "karaganda", "shymkent"]
CTRL_TOPIC = "iot/v1/ctrl/backpressure"


def payload(dev, seq, invalid):
    m = {"temperature": round(random.gauss(55, 4), 2), "vibration_rms": round(abs(random.gauss(2.2, 0.4)), 3),
         "current": round(random.gauss(14.5, 0.8), 2), "rpm": round(random.gauss(1460, 5), 1)}
    if invalid:                                   # ~0.5 %: битые данные для проверки валидации
        m["temperature"] = 999.0
    return json.dumps({"device_id": dev, "ts": int(time.time() * 1000), "seq": seq, "fw": "1.4.2",
                       "metrics": m}, separators=(",", ":"))


async def worker(idx, devs, a, ready_q, go, start_ts, out_q):
    st = {"published": 0, "acked": 0, "failed": 0, "invalid_sent": 0, "late": 0, "connect_fail": 0,
          "per_sec": {}}
    factor = [1.0]
    sem = asyncio.Semaphore(64)
    clients = {}

    async def connect(n):
        dev = f"dev-{n:04d}"
        c = aiomqtt.Client(BROKER, 1883, identifier=f"sim-{dev}", protocol=aiomqtt.ProtocolVersion.V5,
                           keepalive=60, timeout=30)
        async with sem:
            for _ in range(5):
                try:
                    await c.__aenter__()
                    clients[n] = c
                    return
                except aiomqtt.MqttError:
                    await asyncio.sleep(1 + random.random())
            st["connect_fail"] += 1

    t0 = time.time()
    await asyncio.gather(*(connect(n) for n in devs))
    ctrl = None
    if a.follow_ctrl:
        ctrl = aiomqtt.Client(BROKER, 1883, identifier=f"sim-ctrl-{os.getpid()}", protocol=aiomqtt.ProtocolVersion.V5)
        await ctrl.__aenter__()
        await ctrl.subscribe(CTRL_TOPIC, qos=1)

        async def listen():
            try:
                async for m in ctrl.messages:
                    try:
                        factor[0] = max(0.01, float(json.loads(m.payload)["factor"]))
                    except (ValueError, KeyError):
                        pass
            except aiomqtt.MqttError:
                pass
        asyncio.create_task(listen())
    ready_q.put((idx, len(clients), round(time.time() - t0, 1)))
    await asyncio.get_running_loop().run_in_executor(None, go.wait)
    start = start_ts.value
    stop_at = start + a.duration if a.duration else float("inf")
    interval = 1 / a.rate if a.rate > 0 else 0

    async def device(n, c):
        dev = f"dev-{n:04d}"
        topic = f"iot/v1/{SITES[n % len(SITES)]}/{dev}/telemetry"
        nxt = start + (random.random() * interval)
        seq = 0
        while True:
            if interval:
                d = nxt - time.time()
                if d > 0:
                    await asyncio.sleep(d)
            now = time.time()
            if now >= stop_at or (a.count and seq >= a.count):
                break
            bad = random.random() < a.invalid
            st["published"] += 1
            try:
                await c.publish(topic, payload(dev, seq, bad), qos=a.qos)
                st["acked"] += 1
                st["invalid_sent"] += bad
                sec = int(time.time() - start)
                st["per_sec"][sec] = st["per_sec"].get(sec, 0) + 1
            except aiomqtt.MqttError:
                st["failed"] += 1
                await asyncio.sleep(0.5)
            seq += 1
            if interval:
                nxt += interval / factor[0]
                if nxt < time.time() - interval:      # отстали больше чем на период — не «догоняем» залпом
                    st["late"] += 1
                    nxt = time.time()

    await asyncio.gather(*(device(n, c) for n, c in clients.items()))
    st["elapsed"] = time.time() - start
    for c in list(clients.values()) + ([ctrl] if ctrl else []):
        try:
            await c.__aexit__(None, None, None)
        except Exception:  # noqa: BLE001
            pass
    out_q.put(st)


def proc_main(idx, devs, a, ready_q, go, start_ts, out_q):
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except Exception:  # noqa: BLE001
        pass
    asyncio.run(worker(idx, devs, a, ready_q, go, start_ts, out_q))


def run(a):
    procs = a.procs or max(1, min(4, (os.cpu_count() or 2) - 1, a.devices // 100 or 1))
    ids = list(range(a.first, a.first + a.devices))
    slices = [ids[i::procs] for i in range(procs)]
    ready_q, out_q = mp.Queue(), mp.Queue()
    go, start_ts = mp.Event(), mp.Value("d", 0.0)
    ps = [mp.Process(target=proc_main, args=(i, s, a, ready_q, go, start_ts, out_q)) for i, s in enumerate(slices)]
    for p in ps:
        p.start()
    connected, ctime = 0, 0.0
    for _ in ps:
        _, n, t = ready_q.get()
        connected += n
        ctime = max(ctime, t)
    print(f"[sim] подключено {connected}/{a.devices} устройств за {ctime} с ({procs} проц.), старт", flush=True)
    start_ts.value = time.time() + 0.3
    go.set()
    t_start = start_ts.value
    res = [out_q.get() for _ in ps]
    for p in ps:
        p.join()
    agg = {k: sum(r[k] for r in res) for k in ("published", "acked", "failed", "invalid_sent", "late", "connect_fail")}
    per_sec = {}
    for r in res:
        for s, v in r["per_sec"].items():
            per_sec[s] = per_sec.get(s, 0) + v
    elapsed = max(r["elapsed"] for r in res)
    agg.update(devices=a.devices, connected=connected, connect_seconds=ctime, rate=a.rate, procs=procs,
               target_rate=a.devices * a.rate, elapsed=round(elapsed, 2), start=t_start,
               achieved_rate=round(agg["acked"] / (a.duration or elapsed), 1) if (a.duration or elapsed) else None,
               per_sec=[per_sec.get(s, 0) for s in range(int(max(per_sec, default=-1)) + 1)])
    print(f"[sim] отправлено {agg['acked']} (PUBACK), ошибок {agg['failed']}, "
          f"факт. темп {agg['achieved_rate']} сообщ./с (цель {agg['target_rate']:g}), опозданий {agg['late']}",
          flush=True)
    return agg


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--devices", type=int, default=100)
    ap.add_argument("--rate", type=float, default=1.0, help="сообщений/с на устройство (0 = без пауз)")
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--count", type=int, default=0, help="сообщений на устройство (0 = без ограничения)")
    ap.add_argument("--qos", type=int, default=1)
    ap.add_argument("--invalid", type=float, default=0.005)
    ap.add_argument("--first", type=int, default=1)
    ap.add_argument("--procs", type=int, default=0)
    ap.add_argument("--follow-ctrl", action="store_true")
    ap.add_argument("--json")
    a = ap.parse_args(argv)
    if a.count and not a.rate:
        a.duration = 0
    agg = run(a)
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(agg, f)
    return agg


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
