"""Lab 5 — все эксперименты конвейера телеметрии.

python run_lab5.py                    # всё: loads, saturation, backpressure, batching (~20–25 мин)
python run_lab5.py loads              # шаг 4: 100 / 500 / 1000 / 2000 устройств
python run_lab5.py saturation         # шаг 5: наращиваем темп до насыщения
python run_lab5.py backpressure       # шаг 6: перегрузка в режимах none / inflight / throttle
python run_lab5.py batching           # шаг 7: батч 1 / 50 / 500 — задержка и пропускная способность
Опции: --duration 60 --rate 1 --quick (короткие прогоны для проверки стенда)

Результаты: /results/*.json, затем  python plots.py  -> /results/plots/*.png и summary.md
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

import paho.mqtt.client as mqtt

INGEST = os.getenv("INGEST", "http://ingest:8000")
BROKER = os.getenv("BROKER", "mosquitto")
RESULTS = os.getenv("RESULTS", "/results")
HERE = os.path.dirname(os.path.abspath(__file__))
os.makedirs(RESULTS, exist_ok=True)


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def http(path, data=None, timeout=10):
    req = urllib.request.Request(INGEST + path, method="POST" if data is not None else "GET",
                                 data=json.dumps(data).encode() if data is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def stats(full=False, since=0, until=0):
    try:
        return http(f"/stats?full=1&since={since}&until={until}" if full else "/stats", timeout=30 if full else 5)
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


class Sys:
    """Метрики брокера из $SYS (байты, сообщения, отброшенные, хранимые в очередях)."""
    KEYS = {"$SYS/broker/bytes/received": "bytes_in", "$SYS/broker/bytes/sent": "bytes_out",
            "$SYS/broker/publish/messages/received": "pub_in", "$SYS/broker/publish/messages/sent": "pub_out",
            "$SYS/broker/publish/messages/dropped": "dropped", "$SYS/broker/store/messages/count": "stored_msgs",
            "$SYS/broker/clients/connected": "clients", "$SYS/broker/retained messages/count": "retained"}

    def __init__(self):
        self.v = {}
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="lab5-runner-sys")
        self.c.on_connect = lambda c, *_: c.subscribe([(k, 0) for k in self.KEYS])
        self.c.on_message = self._msg
        self.c.connect(BROKER, 1883)
        self.c.loop_start()
        time.sleep(1.5)

    def _msg(self, c, u, m):
        try:
            self.v[self.KEYS[m.topic]] = float(m.payload)
        except ValueError:
            pass

    def snap(self):
        d = dict(self.v)
        # store/messages/count включает retained-сообщения ($SYS и др.) — очередь = разница
        d["stored_msgs"] = max(0.0, d.get("stored_msgs", 0) - d.get("retained", 0))
        return d


SYS = None


def wait_idle(timeout=120, quiet=False):
    """Ждём, пока сервис всё запишет и брокер отдаст очередь."""
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        s = stats()
        if "error" in s:
            time.sleep(1)
            continue
        cur = (s["received"], s["stored"])
        if s["queue_depth"] == 0 and (s["inflight"] or 0) == 0 and cur == last and SYS.snap()["stored_msgs"] < 50:
            return True
        last = cur
        time.sleep(1)
    if not quiet:
        log("  ! очередь не опустела за", timeout, "с")
    return False


def configure(**kw):
    for _ in range(30):
        try:
            return http("/config", kw, timeout=60)
        except Exception:  # noqa: BLE001
            time.sleep(2)
    raise RuntimeError("ingest недоступен")


def slope(ts, ys):
    n = len(ts)
    if n < 3:
        return 0.0
    mt, my = sum(ts) / n, sum(ys) / n
    den = sum((t - mt) ** 2 for t in ts)
    return sum((t - mt) * (y - my) for t, y in zip(ts, ys)) / den if den else 0.0


def run_load(name, devices, rate, duration, ingest_cfg=None, follow_ctrl=False, drain_timeout=90, procs=0):
    """Один прогон: настраиваем ingest, запускаем симулятор, раз в секунду снимаем /stats и $SYS."""
    configure(**(ingest_cfg or {}))
    wait_idle(60, quiet=True)
    http("/reset", {})
    log(f"▶ {name}: {devices} устр. × {rate:g} Гц = {devices * rate:g} сообщ./с, {duration} с, cfg={ingest_cfg}")
    out = os.path.join(RESULTS, f"_sim_{name}.json")
    cmd = [sys.executable, "-u", os.path.join(HERE, "simulator.py"), "--devices", str(devices), "--rate", str(rate),
           "--duration", str(duration), "--json", out] + (["--follow-ctrl"] if follow_ctrl else []) + \
          (["--procs", str(procs)] if procs else [])
    sim = subprocess.Popen(cmd)
    series = []
    sys0 = None
    t_end_pub = None
    crashed = False
    while True:
        time.sleep(1)
        s = stats()
        b = SYS.snap()
        if sys0 is None and sim.poll() is None:
            sys0 = b
        p = {"t": time.time(), "broker_queue": b.get("stored_msgs", 0), "dropped": b.get("dropped", 0),
             "bytes_in": b.get("bytes_in", 0), "pub_in": b.get("pub_in", 0)}
        if "error" in s:
            crashed = True
            p["error"] = s["error"]
        else:
            p.update({k: s[k] for k in ("received", "stored", "queue_depth", "backlog", "inflight", "cpu_percent", "rss_mb",
                                        "factor", "invalid")})
        series.append(p)
        if sim.poll() is not None:
            if t_end_pub is None:
                t_end_pub = time.time()
                sys1 = b
                log("  публикация закончена, ждём, пока конвейер дообработает очередь")
            if "error" not in s and s["queue_depth"] == 0 and (s["inflight"] or 0) == 0 \
                    and p["broker_queue"] < 50 and len(series) > 2 and series[-2].get("stored") == s["stored"]:
                break
            if time.time() - t_end_pub > drain_timeout:
                log(f"  ! не дообработано за {drain_timeout} с — прерываем ожидание")
                break
    sim_res = json.load(open(out, encoding="utf-8"))
    os.remove(out)
    t0 = sim_res["start"]
    # задержка — по сообщениям, измеренным с 2-й секунды до (конец − max_delay): без краевых эффектов прогона
    md = (ingest_cfg or {}).get("max_delay", 2.0)
    full = stats(full=True, since=t0 + 2, until=t0 + max(duration - md, 3))
    for p in series:
        p["t"] = round(p["t"] - t0, 2)
    # установившийся участок: с 5-й секунды до конца публикации; скорости — наклон регрессии (батчи дают «ступеньки»)
    pub_end = t_end_pub - t0
    w = [p for p in series if 5 <= p["t"] <= pub_end and "error" not in p] or \
        [p for p in series if p["t"] <= pub_end and "error" not in p]
    tt = [p["t"] for p in w]
    thr = slope(tt, [p["stored"] for p in w])
    in_rate = slope(tt, [p["received"] for p in w])
    q_slope = slope(tt, [p["backlog"] for p in w])
    bq_slope = slope(tt, [p["broker_queue"] for p in w])
    tail = [p["backlog"] + p["broker_queue"] for p in w[-5:]]
    tail_backlog = sum(tail) / len(tail) if tail else 0
    pub = sim_res["acked"]
    valid_pub = pub - sim_res["invalid_sent"]
    rows = full.get("db_rows", 0) if "error" not in full else None
    bytes_in = (sys1.get("bytes_in", 0) - sys0.get("bytes_in", 0)) if sys0 else 0
    pub_in = (sys1.get("pub_in", 0) - sys0.get("pub_in", 0)) if sys0 else 0
    target = devices * rate
    backlog_slope = q_slope + bq_slope
    r = {"name": name, "devices": devices, "rate": rate, "target_rate": target, "duration": duration,
         "cfg": full.get("cfg", ingest_cfg), "sim": {k: v for k, v in sim_res.items() if k != "per_sec"},
         "source_rate_series": sim_res["per_sec"],
         "achieved_rate": sim_res["achieved_rate"], "throughput": round(thr, 1), "ingest_in_rate": round(in_rate, 1),
         "queue_slope": round(q_slope, 1), "broker_queue_slope": round(bq_slope, 1),
         "max_queue": max((p.get("queue_depth", 0) for p in series), default=0),
         "max_broker_queue": max((p["broker_queue"] for p in series), default=0),
         "cpu_avg": round(sum(p["cpu_percent"] for p in w) / len(w), 1) if w else None,
         "cpu_max": max((p["cpu_percent"] for p in w), default=None),
         "rss_max": max((p.get("rss_mb", 0) for p in series), default=None),
         "rss_start": series[0].get("rss_mb") if series else None,
         "latency": full.get("latency"), "broker_wait": full.get("broker_wait"), "batch": full.get("batch"),
         "flush_reasons": full.get("flush_reasons"), "stage_seconds": full.get("stage_seconds"),
         "published_valid": valid_pub, "db_rows": rows, "invalid": full.get("invalid"),
         "duplicates": full.get("duplicates"),
         "lost": (valid_pub - rows) if rows is not None else None,
         "broker_dropped": (series[-1]["dropped"] - series[0]["dropped"]) if series else 0,
         "bytes_in_per_s": round(bytes_in / sim_res["elapsed"], 1) if sim_res["elapsed"] else 0,
         "bytes_per_msg": round(bytes_in / pub_in, 1) if pub_in else None,
         "crashed": crashed, "series": series}
    r["tail_backlog"] = round(tail_backlog)
    # насыщение: очередь (сервис + брокер) устойчиво растёт и к концу прогона больше ~0,25 с нагрузки
    r["saturated"] = bool((backlog_slope > max(10.0, 0.01 * target) and tail_backlog > 0.25 * target)
                          or crashed or (r["achieved_rate"] and thr < 0.85 * r["achieved_rate"]))
    r["generator_limited"] = bool(r["achieved_rate"] and r["achieved_rate"] < 0.9 * target and not r["saturated"])
    lat = r["latency"] or {}
    log(f"  ✔ вход {r['achieved_rate']} → запись {r['throughput']} сообщ./с | p50/p95/p99 = "
        f"{lat.get('50')}/{lat.get('95')}/{lat.get('99')} с | очередь max {r['max_queue']} "
        f"(рост {q_slope:+.0f}/с), брокер {r['max_broker_queue']} | CPU {r['cpu_avg']}% RAM {r['rss_max']} МБ | "
        f"потери {r['lost']}, dropped {r['broker_dropped']} {'| НАСЫЩЕНИЕ' if r['saturated'] else ''}"
        f"{' | УПАЛ' if crashed else ''}")
    return r


def save(name, data):
    with open(os.path.join(RESULTS, f"{name}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    log(f"  сохранено {RESULTS}/{name}.json")


BASE = {"mode": "none", "max_batch_size": 500, "max_delay": 2.0}


def sc_loads(a):
    res = [run_load(f"loads_{n}", n, a.rate, a.duration, BASE) for n in (100, 500, 1000, 2000)]
    save("loads", res)
    return res


def sc_saturation(a):
    """Наращиваем темп (2000 устройств, частота растёт), пока очередь не начнёт расти неограниченно."""
    devices = 2000
    res = []
    hits = 0
    for hz in a.sat_steps:
        r = run_load(f"sat_{int(devices * hz)}", devices, hz, a.sat_duration, BASE, drain_timeout=60)
        res.append(r)
        if r["generator_limited"]:
            log("  ! генератор не выдаёт заданный темп — дальше увеличивать бессмысленно (добавь --procs)")
            break
        hits += r["saturated"]
        if hits >= 2 or r["crashed"]:
            break
    ok = [r for r in res if not r["saturated"]]
    sat = [r for r in res if r["saturated"]]
    capacity = max((r["throughput"] for r in res), default=0)
    summary = {"capacity_msg_s": capacity,
               "last_ok_load": ok[-1]["target_rate"] if ok else None,
               "first_saturated_load": sat[0]["target_rate"] if sat else None, "runs": res}
    save("saturation", summary)
    log(f"  ёмкость конвейера ≈ {capacity:.0f} сообщ./с; насыщение между "
        f"{summary['last_ok_load']} и {summary['first_saturated_load']} сообщ./с")
    return summary


def sc_backpressure(a):
    cap = a.capacity
    if not cap:
        try:
            cap = json.load(open(os.path.join(RESULTS, "saturation.json"), encoding="utf-8"))["capacity_msg_s"]
        except FileNotFoundError:
            cap = 4000
    devices = 2000
    hz = round(cap * a.overload / devices, 2)
    log(f"перегрузка: {devices} × {hz} Гц = {devices * hz:.0f} сообщ./с (≈{a.overload}× ёмкости {cap:.0f})")
    res = []
    # «наивный» режим последним: он раздувает память процесса (и может уронить контейнер по OOM)
    for mode in ("inflight", "throttle", "none"):
        cfg = dict(BASE, mode=mode, inflight=1000)
        res.append(run_load(f"bp_{mode}", devices, hz, a.bp_duration, cfg, follow_ctrl=(mode == "throttle"),
                            drain_timeout=120))
    configure(**BASE)
    save("backpressure", {"capacity": cap, "overload": a.overload, "load": devices * hz, "runs": res})
    return res


def drain_test(batch, n_total=30000):
    """Максимальная пропускная способность: копим n сообщений в брокере (сервис на паузе), потом «сливаем»."""
    configure(mode="none", max_batch_size=batch, max_delay=2.0)
    wait_idle(60, quiet=True)
    http("/pause", {})
    time.sleep(1)
    sim = subprocess.run([sys.executable, os.path.join(HERE, "simulator.py"), "--devices", "100", "--count",
                          str(n_total // 100), "--rate", "0", "--invalid", "0"], capture_output=True, text=True)
    print(sim.stdout.strip(), flush=True)
    http("/reset", {})
    t0 = time.time()
    http("/resume", {})
    cpu = []
    while time.time() - t0 < 300:
        time.sleep(0.5)
        s = stats()
        if "error" in s:
            continue
        cpu.append(s["cpu_percent"])
        if s["stored"] >= n_total:
            break
    dt = time.time() - t0
    s = stats(full=True)
    return {"batch": batch, "messages": s["stored"], "seconds": round(dt, 2), "throughput": round(s["stored"] / dt, 1),
            "cpu_avg": round(sum(cpu) / len(cpu), 1) if cpu else None, "stage_seconds": s.get("stage_seconds"),
            "batch_stats": s.get("batch")}


def sc_batching(a):
    out = {"load_devices": a.batch_devices, "rate": 1.0, "runs": []}
    for b in (1, 50, 500):
        r = run_load(f"batch_{b}", a.batch_devices, 1.0, a.batch_duration, dict(BASE, max_batch_size=b))
        d = drain_test(b, a.drain_messages)
        log(f"  батч {b}: макс. пропускная способность {d['throughput']} сообщ./с ({d['messages']} за {d['seconds']} с)")
        out["runs"].append({"batch": b, "latency_run": r, "drain": d})
    configure(**BASE)
    save("batching", out)
    return out


def main():
    global SYS
    ap = argparse.ArgumentParser()
    ap.add_argument("scenarios", nargs="*", default=["loads", "saturation", "backpressure", "batching"])
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--rate", type=float, default=1.0, help="Гц на устройство в шаге 4")
    ap.add_argument("--sat-duration", type=int, default=40)
    ap.add_argument("--sat-steps", type=float, nargs="+", default=[1.5, 2, 3, 4, 5, 6, 8, 10, 12])
    ap.add_argument("--capacity", type=float, default=0)
    ap.add_argument("--overload", type=float, default=1.5)
    ap.add_argument("--bp-duration", type=int, default=60)
    ap.add_argument("--batch-devices", type=int, default=500)
    ap.add_argument("--batch-duration", type=int, default=40)
    ap.add_argument("--drain-messages", type=int, default=30000)
    ap.add_argument("--quick", action="store_true", help="короткие прогоны (проверка стенда)")
    a = ap.parse_args()
    if a.quick:
        a.duration, a.sat_duration, a.bp_duration, a.batch_duration, a.drain_messages = 15, 15, 20, 15, 10000
    for _ in range(30):
        try:
            http("/health")
            break
        except Exception:  # noqa: BLE001
            log("жду ingest…")
            time.sleep(2)
    SYS = Sys()
    t0 = time.time()
    for sc in a.scenarios:
        log(f"===== сценарий {sc} =====")
        globals()[f"sc_{sc}"](a)
    log(f"готово за {(time.time() - t0) / 60:.1f} мин. Графики:  python plots.py")


if __name__ == "__main__":
    main()
