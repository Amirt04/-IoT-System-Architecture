"""Захват трафика и разбор пакетов MQTT (то же, что в Wireshark) + накладные расходы протокола.

На стороне брокера запускается tcpdump -> /captures/mqtt.pcap (откройте его в Wireshark, фильтр: mqtt).
Сценарий: CONNECT -> SUBSCRIBE -> по 4 метрики с QoS 0, 1, 2 -> пауза (PINGREQ) -> DISCONNECT.
Для каждого QoS считается: сколько байт уходит «на провод» на одно сообщение (все кадры TCP-потока
издателя: PUBLISH, подтверждения MQTT, чистые TCP ACK) и какая доля из них — полезная нагрузка.
"""
import csv
import json
import os
import time

import paho.mqtt.client as mqtt

from common import BROKER, CAPTURES, METRICS, PORT, RESULTS, log, save_json, t_topic, uid
from netctl import Net
from pcap_mqtt import analyze

SITE, DEV = "astana", "dev-001"
PCAP = "mqtt.pcap"


def main():
    os.makedirs(CAPTURES, exist_ok=True)
    net = Net()
    phases = {}
    try:
        net.clear()
        net.capture_start(PCAP)
        log("tcpdump запущен на стороне брокера")

        sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("sub"))
        sub.connect(BROKER, PORT, 60)
        sub.loop_start()
        sub.subscribe(f"iot/v1/{SITE}/+/telemetry/+", qos=2)
        time.sleep(0.5)

        pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=DEV)
        t = time.time()
        pub.connect(BROKER, PORT, keepalive=10)
        pub.loop_start()
        time.sleep(0.8)
        pub_port = pub.socket().getsockname()[1]
        phases["connect"] = (t, time.time())

        sizes = {}
        for qos in (0, 1, 2):
            t = time.time()
            for seq, m in enumerate(METRICS):
                body = json.dumps({"seq": seq, "ts": round(time.time(), 3), "v": 21.37 if m != "motion" else False},
                                  separators=(",", ":"))
                sizes.setdefault(qos, []).append((len(t_topic(SITE, DEV, m)), len(body)))
                pub.publish(t_topic(SITE, DEV, m), body, qos=qos)
                time.sleep(0.4)
            time.sleep(0.6)
            phases[f"qos{qos}"] = (t, time.time())
            log(f"фаза QoS {qos}: 4 сообщения")
        t = time.time()
        time.sleep(12.0)                                  # keepalive 10 с -> PINGREQ/PINGRESP
        phases["ping"] = (t, time.time())
        t = time.time()
        pub.disconnect()
        pub.loop_stop()
        time.sleep(0.5)
        phases["disconnect"] = (t, time.time())
        sub.loop_stop()
        sub.disconnect()
        net.capture_stop()
    finally:
        net.close()

    path = os.path.join(CAPTURES, PCAP)
    frames = analyze(path)
    pf = [f for f in frames if f["client_port"] == pub_port]
    log(f"pcap: {path}, кадров всего {len(frames)}, в потоке издателя {len(pf)}")

    # --- таблица кадров
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, "capture_frames.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "flow", "dir", "frame_len", "eth", "ip", "tcp", "tcp_payload", "mqtt"])
        t0 = frames[0]["t"] if frames else 0
        for fr in frames:
            w.writerow([f"{fr['t'] - t0:.4f}", "publisher" if fr["client_port"] == pub_port else "subscriber",
                        fr["dir"], fr["frame_len"], fr["l2"], fr["ip"], fr["tcp"], fr["tcp_payload"],
                        " + ".join(p["type"] + (f"(QoS{p['qos']})" if p["type"] == "PUBLISH" else "") for p in fr["mqtt"])])

    # --- разбор отдельных пакетов
    examples = {}
    for fr in pf:
        for p in fr["mqtt"]:
            key = p["type"] + (f"_QoS{p['qos']}" if p["type"] == "PUBLISH" else "")
            if key not in examples:
                ex = {k: v for k, v in p.items() if k != "raw"}
                ex.update(frame_len=fr["frame_len"], eth=fr["l2"], ip=fr["ip"], tcp=fr["tcp"],
                          mqtt_hex=p["raw"].hex(" "), dir=fr["dir"])
                examples[key] = ex

    # --- накладные расходы на сообщение по QoS (поток издателя: оба направления, все кадры)
    rows = []
    for qos in (0, 1, 2):
        a, b = phases[f"qos{qos}"]
        fs = [f for f in pf if a <= f["t"] <= b]
        n = 4
        total = sum(f["frame_len"] for f in fs)
        payload = sum(s[1] for s in sizes[qos])
        topic = sum(s[0] for s in sizes[qos])
        mqtt_bytes = sum(p["size"] for f in fs for p in f["mqtt"])
        types = {}
        for f in fs:
            for p in f["mqtt"]:
                types[p["type"]] = types.get(p["type"], 0) + 1
            if not f["mqtt"]:
                types["TCP ACK"] = types.get("TCP ACK", 0) + 1
        rows.append({"qos": qos, "frames_per_msg": round(len(fs) / n, 2), "wire_bytes_per_msg": round(total / n, 1),
                     "payload_bytes_per_msg": round(payload / n, 1), "topic_bytes_per_msg": round(topic / n, 1),
                     "mqtt_bytes_per_msg": round(mqtt_bytes / n, 1),
                     "payload_share_pct": round(100 * payload / total, 1) if total else None,
                     "overhead_pct": round(100 * (total - payload) / total, 1) if total else None,
                     "packets": types})
    conn = [f for f in pf if phases["connect"][0] - 0.5 <= f["t"] <= phases["connect"][1]]
    summary = {"pcap": path, "publisher_port": pub_port, "per_message": rows, "examples": examples,
               "connect_bytes": sum(f["frame_len"] for f in conn), "connect_frames": len(conn)}
    save_json("capture_summary.json", summary)

    print("\n===== Разбор пакетов (как в Wireshark) =====")
    for k in ("CONNECT", "CONNACK", "PUBLISH_QoS1", "PUBACK", "PUBLISH_QoS2", "PUBREC", "PUBREL", "PUBCOMP",
              "PINGREQ", "DISCONNECT"):
        e = examples.get(k)
        if not e:
            continue
        extra = ""
        if k.startswith("PUBLISH"):
            extra = f" topic='{e['topic']}' ({e['topic_len']} Б), id={e['packet_id']}, payload {e['payload_len']} Б"
        elif k == "CONNECT":
            extra = f" {e['protocol']} v{e['level']}, keepalive {e['keepalive']} с, clean={e['clean_session']}"
        print(f"{k:<13} кадр {e['frame_len']:>4} Б = Ethernet {e['eth']} + IP {e['ip']} + TCP {e['tcp']} + MQTT {e['size']}"
              f" (заголовок {e['fixed_header']} Б){extra}")
    print("\n===== Накладные расходы на одно сообщение (поток издателя) =====")
    print(f"{'QoS':>3} {'кадров':>7} {'байт на проводе':>16} {'MQTT, Б':>8} {'полезная нагрузка':>18} {'доля нагрузки':>14}")
    for r in rows:
        print(f"{r['qos']:>3} {r['frames_per_msg']:>7} {r['wire_bytes_per_msg']:>16} {r['mqtt_bytes_per_msg']:>8} "
              f"{r['payload_bytes_per_msg']:>18} {str(r['payload_share_pct']) + ' %':>14}   {r['packets']}")
    print(f"\nПодключение (TCP handshake + CONNECT/CONNACK): {summary['connect_frames']} кадров, {summary['connect_bytes']} Б")
    print(f"Файл для Wireshark: {path}  (фильтр: mqtt)")


if __name__ == "__main__":
    main()
