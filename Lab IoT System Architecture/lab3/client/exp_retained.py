"""Эксперимент retained: подписка ПОСЛЕ публикации.

1. Очищаем retained на топике config.
2. Публикуем обычное сообщение (retain=False) и retained-конфиг (retain=True).
3. Через 2 с подключается НОВЫЙ подписчик -> должен сразу получить только retained (флаг retain=1).
4. Публикуем «живое» сообщение -> подписчик получает его с retain=0.
5. Удаляем retained (пустой payload с retain=True) -> ещё один новый подписчик ничего не получает.
"""
import json
import threading
import time

import paho.mqtt.client as mqtt

from common import BROKER, PORT, config_topic, log, save_json, status_topic, uid

SITE, DEV = "astana", "dev-001"


def publish(topic, payload, retain):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("pub"))
    c.connect(BROKER, PORT, 30)
    c.loop_start()
    c.publish(topic, payload, qos=1, retain=retain).wait_for_publish(5)
    c.loop_stop()
    c.disconnect()


def late_subscriber(topics, listen=3.0):
    got, t_suback = [], {}
    ready = threading.Event()
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("late"))

    def on_sub(cl, u, mid, rc, p):
        t_suback["t"] = time.time()
        ready.set()

    c.on_subscribe = on_sub
    c.on_message = lambda cl, u, m: got.append({"topic": m.topic, "payload": m.payload.decode(errors="replace"),
                                                "retain": bool(m.retain), "t": time.time()})
    c.on_connect = lambda cl, u, f, rc, p: cl.subscribe([(t, 1) for t in topics])
    t0 = time.time()
    c.connect(BROKER, PORT, 30)
    c.loop_start()
    ready.wait(5)
    time.sleep(listen)
    c.loop_stop()
    c.disconnect()
    for g in got:
        g["after_connect_ms"] = round((g.pop("t") - t0) * 1000, 1)
    return got, c


def main():
    cfg, st = config_topic(SITE, DEV), status_topic(SITE, DEV)
    steps = []

    publish(cfg, "", retain=True)                                 # 1. очистка
    publish(cfg, json.dumps({"note": "обычное сообщение до подписки"}, ensure_ascii=False), retain=False)
    publish(cfg, json.dumps({"interval_s": 5, "fw": "1.4.2", "ver": 7}), retain=True)
    publish(st, "online", retain=True)
    log("опубликовано: обычное сообщение + retained-конфиг + retained-статус; жду 2 с")
    time.sleep(2)

    got, _ = late_subscriber([cfg, st])                           # 3. поздний подписчик
    log(f"поздний подписчик получил {len(got)} сообщ.:")
    for g in got:
        log(f"   {g['topic']}  retain={int(g['retain'])}  через {g['after_connect_ms']} мс после connect: {g['payload']}")
    steps.append({"step": "подписка после публикации", "received": got})

    # 4. живое сообщение при активной подписке
    live = []
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("live"))
    c.on_message = lambda cl, u, m: live.append({"payload": m.payload.decode(), "retain": bool(m.retain)})
    c.on_connect = lambda cl, u, f, rc, p: cl.subscribe(cfg, 1)
    c.connect(BROKER, PORT, 30)
    c.loop_start()
    time.sleep(1)
    publish(cfg, json.dumps({"interval_s": 10, "fw": "1.4.3", "ver": 8}), retain=True)
    time.sleep(1)
    c.loop_stop()
    c.disconnect()
    log(f"активный подписчик: {[(x['payload'], 'retain=' + str(int(x['retain']))) for x in live]}")
    steps.append({"step": "активная подписка (первое — retained из хранилища, второе — живое)", "received": live})

    publish(cfg, "", retain=True)                                 # 5. удаление retained
    got2, _ = late_subscriber([cfg], listen=2)
    log(f"после удаления retained новый подписчик получил: {len(got2)} сообщ.")
    steps.append({"step": "после удаления retained (пустой payload)", "received": got2})

    save_json("retained.json", steps)
    print("\n===== ИТОГ retained =====")
    print(f"1) Подписка после публикации: получено {len(got)} (ожидалось 2 retained, обычное — нет): "
          f"{'OK' if len(got) == 2 and all(g['retain'] for g in got) else 'проверь'}")
    print(f"2) При активной подписке: первое сообщение retain=1 (из хранилища), новое — retain=0: "
          f"{[int(x['retain']) for x in live]}")
    print(f"3) После удаления retained: получено {len(got2)} (ожидалось 0)")


if __name__ == "__main__":
    main()
