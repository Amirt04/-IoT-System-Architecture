"""Проверка 3. Persistence: persistent-сессия и её очередь переживают рестарт брокера.

1. svc-ingest подключается с постоянной сессией (MQTT 5, Session Expiry 1 ч), подписывается QoS 1 и уходит.
2. device01 публикует 50 сообщений QoS 1 — они ложатся в очередь сессии svc-ingest.
3. Брокер A штатно перезапускается (SIGTERM -> mosquitto сохраняет mosquitto.db).
4. svc-ingest подключается снова: сессия должна восстановиться (Session Present = 1) и отдать все 50.
Контроль: клиент с чистой сессией (Clean Start) после рестарта ничего не получает.
"""
import json
import time

from common import A, Check, Client, log, pw, restart_broker_a

TOPIC = "iot/v1/astana/device01/telemetry/humidity"
N = 50


def main():
    ch = Check(3, "Persistence: сессия и очередь после рестарта брокера")
    cid = "svc-ingest-persistent"
    s = Client(A, "svc-ingest", pw("svc-ingest"), client_id=cid, clean=False).connect()
    s.subscribe(TOPIC, 1)
    tmp = Client(A, "svc-ingest", pw("svc-ingest"), client_id="svc-ingest-clean", clean=True).connect()
    tmp.subscribe(TOPIC, 1)
    time.sleep(0.3)
    s.close()
    tmp.close()
    log("svc-ingest отключился, сессия осталась на брокере")

    dev = Client(A, "device01", pw("device01")).connect()
    acks = [dev.publish(TOPIC, json.dumps({"seq": i, "v": 40 + i % 10})) for i in range(N)]
    dev.close()
    ch.row(f"device01 публикует {N} сообщений QoS 1, пока подписчик офлайн", f"{N} × PUBACK Success",
           f"{acks.count('Success')} × Success", acks.count("Success") == N)

    log("перезапускаю брокер A ...")
    dt = restart_broker_a()
    log(f"брокер A снова принимает подключения через {dt:.1f} с")

    s2 = Client(A, "svc-ingest", pw("svc-ingest"), client_id=cid, clean=False).connect()
    time.sleep(3)
    got = sorted({json.loads(m["payload"])["seq"] for m in s2.received(TOPIC)})
    ch.row("переподключение с тем же client id после рестарта", "Session Present = 1",
           f"Session Present = {int(bool(s2.session_present))}", s2.session_present)
    ch.row("сообщения из очереди persistent-сессии", f"получено {N}/{N}, seq 0..{N - 1}",
           f"получено {len(got)}/{N}" + (f", seq {got[0]}..{got[-1]}" if got else ""), len(got) == N)
    s2.close()

    c2 = Client(A, "svc-ingest", pw("svc-ingest"), client_id="svc-ingest-clean", clean=True).connect()
    time.sleep(2)
    n_clean = len(c2.received(TOPIC))
    ch.row("контроль: клиент с Clean Start после рестарта", "Session Present = 0, 0 сообщений",
           f"Session Present = {int(bool(c2.session_present))}, {n_clean} сообщений",
           not c2.session_present and n_clean == 0)
    c2.close()
    ch.info(restart_seconds=round(dt, 1))
    return ch.save()


if __name__ == "__main__":
    main()
