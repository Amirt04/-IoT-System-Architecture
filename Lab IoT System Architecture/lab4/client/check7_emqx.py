"""Проверка 7. EMQX: кластер из 2 узлов, маршрутизация между узлами, REST API/дашборд.

EMQX 5.8.9 (последняя версия под Apache 2.0; начиная с 5.9 кластеризация требует лицензии).
"""
import json
import os
import time
import urllib.request

from common import EMQX1, EMQX2, EMQX_API, Check, Client, log

ADMIN, ADMIN_PW = os.getenv("EMQX_USER", "admin"), os.getenv("EMQX_PASSWORD", "public")


def api(path, token=None, data=None):
    req = urllib.request.Request(f"http://{EMQX_API}{path}", method="POST" if data else "GET",
                                 data=json.dumps(data).encode() if data else None,
                                 headers={"Content-Type": "application/json",
                                          **({"Authorization": f"Bearer {token}"} if token else {})})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def main():
    ch = Check(7, "EMQX: кластер, дашборд, сравнение с Mosquitto")
    nodes = []
    try:
        token = api("/api/v5/login", data={"username": ADMIN, "password": ADMIN_PW})["token"]
        nodes = api("/api/v5/nodes", token)
        for n in nodes:
            log(f"  узел {n.get('node')}: {n.get('node_status')}, версия {n.get('version')}, "
                f"подключений {n.get('connections')}, память {n.get('memory_used')}")
        running = [n for n in nodes if n.get("node_status") == "running"]
        ch.row("REST API: узлы кластера (GET /api/v5/nodes)", "2 узла в статусе running",
               ", ".join(f"{n.get('node')}={n.get('node_status')}" for n in nodes), len(running) == 2)
    except Exception as e:  # noqa: BLE001
        ch.row("REST API: узлы кластера", "2 узла running",
               f"API недоступен ({e}); посмотри Cluster Overview в дашборде http://localhost:18083", False)

    sub = Client(EMQX2, client_id="lab4-sub-node2").connect()
    r = sub.subscribe("iot/v1/+/+/telemetry/#", 1)
    pub = Client(EMQX1, client_id="lab4-pub-node1").connect()
    time.sleep(0.5)
    lat = []
    for i in range(50):
        pub.publish("iot/v1/astana/device01/telemetry/temperature", json.dumps({"seq": i, "ts": time.time()}))
    time.sleep(1.5)
    for m in sub.received():
        lat.append((m["t"] - json.loads(m["payload"])["ts"]) * 1000)
    ch.row("публикация на узле emqx1 -> подписчик на узле emqx2", "50 из 50 (маршрутизация внутри кластера)",
           f"{len(lat)} из 50, задержка средн. {sum(lat) / len(lat):.1f} мс" if lat else "0", len(lat) == 50)
    rc = pub.publish("iot/v1/x", "y" * 2000)
    ch.row("EMQX по умолчанию: payload 2000 Б", "принимается (лимит по умолчанию 1 МБ)", f"PUBACK {rc}",
           rc.startswith("Success") or rc == "No matching subscribers")
    pub.close()
    sub.close()
    ch.info(nodes=nodes, cross_node_latency_ms=round(sum(lat) / len(lat), 2) if lat else None, subscribe=r)
    return ch.save()


if __name__ == "__main__":
    main()
