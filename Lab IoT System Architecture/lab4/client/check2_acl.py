"""Проверка 2. ACL по схеме топиков: разрешённое проходит, публикация в чужой топик и лишние подписки блокируются.

MQTT 5 возвращает причину отказа (PUBACK/SUBACK reason code «Not authorized»),
MQTT 3.1.1 — нет: брокер молча выбрасывает запрещённую публикацию, клиент об этом не узнаёт.
"""
import time

from common import A, V3, V5, Check, Client, pw

OWN = "iot/v1/astana/device01/telemetry/temperature"
FOREIGN = "iot/v1/astana/device02/telemetry/temperature"
OTHER_SITE = "iot/v1/almaty/device01/telemetry/temperature"
CMD = "iot/v1/astana/device01/cmd/reboot"


def main():
    ch = Check(2, "ACL: разграничение доступа к топикам")
    obs = Client(A, "dashboard", pw("dashboard")).connect()          # наблюдатель видит всё дерево iot/v1
    obs.subscribe("iot/v1/#", 1)
    dev = Client(A, "device01", pw("device01")).connect()
    dev3 = Client(A, "device01", pw("device01"), version=V3).connect()
    time.sleep(0.5)

    rc = dev.publish(OWN, '{"seq":1,"v":21.5}')
    time.sleep(0.5)
    got = len(obs.received(OWN))
    ch.row("device01 -> свой топик telemetry", "PUBACK Success, доставлено", f"PUBACK {rc}, доставлено {got}",
           rc == "Success" and got == 1)

    rc = dev.publish(FOREIGN, '{"seq":1,"v":99}')
    time.sleep(0.5)
    got = len(obs.received(FOREIGN))
    ch.row("device01 -> чужой топик device02 (MQTT 5)", "PUBACK Not authorized, не доставлено",
           f"PUBACK {rc}, доставлено {got}", rc == "Not authorized" and got == 0)

    rc = dev3.publish(FOREIGN, '{"seq":2,"v":99}')
    time.sleep(0.5)
    got = len(obs.received(FOREIGN))
    ch.row("device01 -> чужой топик (MQTT 3.1.1)", "не доставлено; клиент получает обычный PUBACK",
           f"PUBACK {rc}, доставлено {got}", got == 0)

    rc = dev.publish(OTHER_SITE, '{"seq":1,"v":1}')
    time.sleep(0.5)
    got = len(obs.received(OTHER_SITE))
    ch.row("device01 -> свой id на чужой площадке almaty", "Not authorized", f"PUBACK {rc}, доставлено {got}",
           rc == "Not authorized" and got == 0)

    rc = dev.publish(CMD, "reboot")
    ch.row("device01 публикует команду себе (cmd — только чтение)", "Not authorized", f"PUBACK {rc}",
           rc == "Not authorized")

    # mosquitto принимает SUBSCRIBE с широким фильтром, но фильтрует доставку по правам чтения
    spy = Client(A, "device01", pw("device01")).connect()
    r = spy.subscribe("iot/v1/#", 1)
    gw = Client(A, "gateway01", pw("gateway01")).connect()
    gw.publish("iot/v1/astana/device02/telemetry/temperature", '{"seq":7,"v":20.1}')
    gw.publish("iot/v1/astana/gw01/status", "online")
    time.sleep(0.7)
    got = len(spy.received("iot/v1/astana/device02")) + len(spy.received("iot/v1/astana/gw01"))
    ch.row("device01 подписан на iot/v1/#, публикуются чужие данные",
           "чужие сообщения не доставлены (SUBACK может быть Granted)", f"SUBACK {r}, получено чужих {got}", got == 0)

    r = dev.subscribe("iot/v1/astana/device01/cmd/#", 1)
    ch.row("device01 подписывается на свои команды", "SUBACK Granted QoS 1", f"SUBACK {r}", r == ["Granted QoS 1"])

    svc = Client(A, "svc-ingest", pw("svc-ingest")).connect()
    rc = svc.publish(CMD, "reboot")
    time.sleep(0.5)
    got = len(dev.received(CMD))
    ch.row("svc-ingest отправляет команду device01", "Success, устройство получило", f"PUBACK {rc}, получено {got}",
           rc == "Success" and got == 1)

    dash = Client(A, "dashboard", pw("dashboard")).connect()
    rc = dash.publish(OWN, "fake")
    ch.row("dashboard пытается публиковать (только чтение)", "Not authorized", f"PUBACK {rc}", rc == "Not authorized")
    r = dash.subscribe("$SYS/broker/#", 0)
    svc.subscribe("$SYS/broker/#", 0)
    time.sleep(6)                                   # sys_interval = 5 с
    d_sys, s_sys = len(dash.received("$SYS")), len(svc.received("$SYS"))
    ch.row("dashboard читает $SYS", "метрики приходят", f"получено {d_sys} метрик", d_sys > 0)
    ch.row("svc-ingest читает $SYS (не разрешено)", "0 метрик", f"получено {s_sys}", s_sys == 0)

    for c in (spy, gw):
        c.close()
    for c in (obs, dev, dev3, svc, dash):
        c.close()
    return ch.save()


if __name__ == "__main__":
    main()
