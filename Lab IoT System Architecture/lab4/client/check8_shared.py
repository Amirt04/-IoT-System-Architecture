"""Проверка 8. MQTT 5 shared subscription: 3 consumer'а на $share/g1/... делят поток между собой.

Для сравнения — 3 обычных подписчика на тот же фильтр: каждый получает ВСЁ (обработка ×3).
Проверяется на кластере EMQX (consumer'ы на разных узлах) и на Mosquitto A.
"""
import argparse
import json
import os
import statistics
import time

from common import A, EMQX1, EMQX2, RESULTS, Check, Client, log, pw

N = 3000
FILTER = "iot/v1/+/+/telemetry/#"


RATE = 300   # сообщений/с; переопределяется --rate


def run(label, servers, pub_server, pub_auth, sub_auth, topic_fn):
    shared = [Client(s, *sub_auth, client_id=f"{label}-share-{i}").connect() for i, s in enumerate(servers)]
    if not all(c.ok for c in shared):
        for c in shared:
            c.close()
        raise RuntimeError(f"нет подключения: {[c.connack_str() for c in shared]}")
    for c in shared:
        c.subscribe(f"$share/g1/{FILTER}", 1)
    plain = [Client(s, *sub_auth, client_id=f"{label}-plain-{i}").connect() for i, s in enumerate(servers)]
    for c in plain:
        c.subscribe(FILTER, 1)
    pub = Client(pub_server, *pub_auth, client_id=f"{label}-pub").connect()
    time.sleep(1)
    t0 = time.time()
    nxt = time.monotonic()
    for i in range(N):
        pub.c.publish(topic_fn(i), json.dumps({"seq": i}), qos=1)
        if i % 10 == 9:                 # темп RATE сообщений/с
            nxt += 10 / RATE
            time.sleep(max(0.0, nxt - time.monotonic()))
    end = time.time() + 30
    while time.time() < end and (sum(len(c.received()) for c in shared) < N
                                 or min(len(c.received()) for c in plain) < N):
        time.sleep(0.3)
    time.sleep(1.5)
    dur = time.time() - t0
    s_counts = [len(c.received()) for c in shared]
    p_counts = [len(c.received()) for c in plain]
    seqs = [json.loads(m["payload"])["seq"] for c in shared for m in c.received()]
    for c in shared + plain + [pub]:
        c.close()
    return {"shared": s_counts, "plain": p_counts, "shared_unique": len(set(seqs)), "shared_total": len(seqs),
            "seconds": round(dur, 1)}


def main():
    global RATE
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=RATE, help="темп публикации, сообщений/с")
    RATE = ap.parse_args().rate
    ch = Check(8, f"MQTT 5 shared subscriptions: распределение нагрузки ({RATE:g} сообщ./с)")
    results = {}
    try:
        results["EMQX"] = run("emqx", [EMQX1, EMQX2, EMQX1], EMQX1, (), (),
                              lambda i: f"iot/v1/astana/dev-{i % 10:02d}/telemetry/temperature")
    except Exception as e:  # noqa: BLE001
        log(f"EMQX недоступен: {e}")
    results["Mosquitto"] = run("mosq", [A, A, A], A, ("gateway01", pw("gateway01")),
                               ("svc-ingest", pw("svc-ingest")),
                               lambda i: f"iot/v1/astana/dev-{i % 10:02d}/telemetry/temperature")
    for name, r in results.items():
        s = r["shared"]
        dev = max(abs(x - N / 3) for x in s) / (N / 3) * 100
        cv = statistics.pstdev(s) / statistics.mean(s) * 100 if sum(s) else 0
        r.update(max_deviation_pct=round(dev, 1), cv_pct=round(cv, 1))
        r["rate"] = RATE
        log(f"{name}: shared {s} (сумма {sum(s)}, уникальных {r['shared_unique']}), обычные {r['plain']}")
        ch.row(f"{name}: 3 consumer'а $share/g1, {N} сообщений", f"сумма {N}, каждому ≈{N // 3}, без дублей",
               f"{s} -> сумма {sum(s)}, уникальных {r['shared_unique']}, отклонение до {dev:.1f} %",
               sum(s) == N and r["shared_unique"] == N and dev <= 15)
        ch.row(f"{name}: 3 обычных подписчика на тот же фильтр", f"каждый получает все {N}",
               f"{r['plain']}", all(x == N for x in r["plain"]))
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, f"shared_{int(RATE)}.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    ch.info(**results)
    return ch.save()


if __name__ == "__main__":
    main()
