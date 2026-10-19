"""Проверка 4. Лимиты брокера: message_size_limit, max_inflight_messages, max_queued_messages.

a) message_size_limit 1024: payload 512 Б проходит, 2048 Б отбрасывается.
b) max_inflight_messages 20: «медленный» подписчик (сырой сокет, PUBACK не отправляет) получает
   ровно 20 сообщений QoS 1 — остальные ждут на брокере, пока окно не освободится.
c) max_queued_messages 100: офлайн-подписчик с persistent-сессией, публикуется 300 сообщений ->
   после подключения приходит ~100, остальные отброшены. Диагностика: $SYS/broker/publish/messages/dropped
   и строка в логе брокера «Outgoing messages are being dropped for client ...».
"""
import json
import socket
import struct
import time

from common import A, Check, Client, broker_a_logs, log, pw, read_sys

TOPIC_SIZE = "iot/v1/astana/device01/telemetry/pressure"
TOPIC_FLOW = "iot/v1/astana/device01/telemetry/motion"


# ---------------------------------------------------------------- минимальный MQTT 3.1.1 клиент без PUBACK
def _str(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def _pkt(ptype, body):
    rl, enc = len(body), b""
    while True:
        d, rl = rl % 128, rl // 128
        enc += bytes([d | (0x80 if rl else 0)])
        if not rl:
            break
    return bytes([ptype]) + enc + body


def raw_v5_publish(user, password, topic, payload):
    """Сырой MQTT 5: CONNECT + PUBLISH QoS 1, возвращает код причины из PUBACK (или описание разрыва)."""
    s = socket.create_connection(A, timeout=5)
    var = _str("MQTT") + bytes([5, 0xC2]) + struct.pack("!H", 30) + b"\x00"     # v5, user+pass+clean, без свойств
    s.sendall(_pkt(0x10, var + _str("raw-" + str(int(time.time() * 1000) % 100000)) + _str(user) + _str(password)))
    s.recv(16)                                                                     # CONNACK
    body = _str(topic) + struct.pack("!H", 1) + b"\x00" + payload
    s.sendall(_pkt(0x32, body))
    s.settimeout(3)
    try:
        r = s.recv(16)
    except socket.timeout:
        r = b""
    s.close()
    if not r:
        return "соединение без ответа"
    if r[0] >> 4 == 4:                                                            # PUBACK
        code = r[4] if len(r) > 4 and r[1] >= 3 else 0
        return f"PUBACK reason 0x{code:02X}"
    if r[0] >> 4 == 14:                                                           # DISCONNECT
        return f"DISCONNECT reason 0x{r[2]:02X}" if len(r) > 2 else "DISCONNECT"
    return f"пакет 0x{r[0]:02X}"


def raw_subscriber_no_ack(user, password, topic, listen=3.0):
    """Подключается, подписывается QoS 1 и НЕ подтверждает PUBLISH. Возвращает число полученных PUBLISH."""
    s = socket.create_connection(A, timeout=5)
    var = _str("MQTT") + bytes([4, 0xC2]) + struct.pack("!H", 60)          # user+pass+clean session
    s.sendall(_pkt(0x10, var + _str("slow-" + str(int(time.time() * 1000) % 100000)) + _str(user) + _str(password)))
    s.recv(4)                                                                   # CONNACK
    s.sendall(_pkt(0x82, struct.pack("!H", 1) + _str(topic) + bytes([1])))
    buf, count, end = b"", 0, time.time() + listen
    s.settimeout(0.3)
    while time.time() < end:
        try:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
        except socket.timeout:
            continue
        while len(buf) >= 2:
            mult, rl, i = 1, 0, 1
            while i < len(buf):
                rl += (buf[i] & 0x7F) * mult
                mult *= 128
                i += 1
                if buf[i - 1] & 0x80 == 0:
                    break
            else:
                break
            if len(buf) < i + rl:
                break
            if buf[0] >> 4 == 3:
                count += 1
            buf = buf[i + rl:]
    return s, count


def main():
    ch = Check(4, "Лимиты: message_size_limit, max_inflight_messages, max_queued_messages")

    # ---------------- a) размер сообщения
    obs = Client(A, "dashboard", pw("dashboard")).connect()
    obs.subscribe(TOPIC_SIZE, 1)
    dev = Client(A, "device01", pw("device01")).connect()
    rc_small = dev.publish(TOPIC_SIZE, "x" * 512)
    # для превышения — сырой MQTT 5: paho 2.1 не умеет разобрать код причины, который вернёт mosquitto
    rc_big = raw_v5_publish("device01", pw("device01"), TOPIC_SIZE, b"y" * 2048)
    time.sleep(0.7)
    sizes = [len(m["payload"]) for m in obs.received(TOPIC_SIZE)]
    ch.row("payload 512 Б (лимит 1024)", "доставлено", f"PUBACK {rc_small}, доставлены размеры {sizes}", 512 in sizes)
    ch.row("payload 2048 Б (больше лимита)", "отброшено брокером", f"{rc_big}, доставлены размеры {sizes}",
           2048 not in sizes)
    obs.close()

    # ---------------- b) max_inflight_messages
    sock, before = raw_subscriber_no_ack("svc-ingest", pw("svc-ingest"), TOPIC_FLOW, listen=0.5)
    for i in range(60):
        dev.publish(TOPIC_FLOW, json.dumps({"seq": i}), qos=1)
    sock2_count = 0
    end = time.time() + 3
    buf = b""
    sock.settimeout(0.3)
    while time.time() < end:
        try:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
        except socket.timeout:
            continue
    # считаем PUBLISH в накопленном потоке
    i = 0
    while i + 2 <= len(buf):
        mult, rl, j = 1, 0, i + 1
        while j < len(buf):
            rl += (buf[j] & 0x7F) * mult
            mult *= 128
            j += 1
            if buf[j - 1] & 0x80 == 0:
                break
        if buf[i] >> 4 == 3:
            sock2_count += 1
        i = j + rl
    sock.close()
    ch.row("60 сообщений QoS 1 подписчику, который не шлёт PUBACK", "получено ровно 20 (окно inflight)",
           f"получено {sock2_count}", sock2_count == 20)

    # ---------------- c) max_queued_messages
    sys0 = read_sys(A, "dashboard", pw("dashboard"), seconds=6, prefix="$SYS/broker/publish/messages/dropped")
    dropped0 = int(sys0.get("$SYS/broker/publish/messages/dropped", "0") or 0)
    cid = "svc-ingest-queue"
    s = Client(A, "svc-ingest", pw("svc-ingest"), client_id=cid, clean=False).connect()
    s.subscribe(TOPIC_FLOW, 1)
    time.sleep(0.3)
    s.close()
    t_log = time.time() - 2
    for i in range(300):
        dev.publish(TOPIC_FLOW, json.dumps({"seq": i}), qos=1)
    dev.close()
    log("опубликовано 300 сообщений для офлайн-подписчика (лимит очереди 100)")
    s2 = Client(A, "svc-ingest", pw("svc-ingest"), client_id=cid, clean=False).connect()
    time.sleep(3)
    got = sorted({json.loads(m["payload"])["seq"] for m in s2.received(TOPIC_FLOW)})
    s2.close()
    ch.row("300 сообщений офлайн-подписчику при max_queued_messages 100", "доставлено ≈100, остальное отброшено",
           f"доставлено {len(got)}" + (f" (seq {got[0]}..{got[-1]})" if got else ""), 90 <= len(got) <= 125)
    sys1 = read_sys(A, "dashboard", pw("dashboard"), seconds=6, prefix="$SYS/broker/publish/messages/dropped")
    dropped1 = int(sys1.get("$SYS/broker/publish/messages/dropped", "0") or 0)
    ch.row("метрика $SYS/broker/publish/messages/dropped", "выросла на число отброшенных",
           f"{dropped0} -> {dropped1} (+{dropped1 - dropped0})", dropped1 > dropped0)
    lines = [ln for ln in broker_a_logs(t_log) if "dropped" in ln.lower()]
    ch.row("лог брокера", "предупреждение об отбрасывании для клиента", lines[-1].strip() if lines else "строк нет",
           bool(lines))
    ch.info(delivered_after_queue=len(got), dropped_delta=dropped1 - dropped0, inflight_received=sock2_count)
    return ch.save()


if __name__ == "__main__":
    main()
