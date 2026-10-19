"""Общие функции Lab 4: подключение пользователей, MQTT v3.1.1 / v5, учёт результатов проверок,
перезапуск брокера и чтение его логов через Docker Engine API."""
import http.client
import json
import os
import socket
import subprocess
import threading
import time
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

A = (os.getenv("BROKER_A", "broker-a"), int(os.getenv("BROKER_A_PORT", "1883")))
B = (os.getenv("BROKER_B", "broker-b"), int(os.getenv("BROKER_B_PORT", "1883")))
EMQX1 = (os.getenv("EMQX1", "emqx1"), int(os.getenv("EMQX1_PORT", "1883")))
EMQX2 = (os.getenv("EMQX2", "emqx2"), int(os.getenv("EMQX2_PORT", "1883")))
EMQX_API = os.getenv("EMQX_API", "emqx1:18083")
CONTAINER_A = os.getenv("CONTAINER_A", "lab4-broker-a")
RESULTS = os.getenv("RESULTS", "/results")
USERS_FILE = os.getenv("USERS_FILE", "/config/users.txt")


def _load_users():
    users = {}
    with open(USERS_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                broker, user, pw = line.split(":", 2)
                users[(broker, user)] = pw
    return users


USERS = _load_users()


def pw(user, broker="A"):
    return USERS[(broker, user)]


def uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


V3, V5 = mqtt.MQTTv311, mqtt.MQTTv5


class Client:
    """Тонкая обёртка над paho: синхронное подключение, сбор сообщений, ожидание PUBACK/SUBACK с кодами."""

    def __init__(self, server, user=None, password=None, version=V5, client_id=None, clean=True, keepalive=30):
        self.server, self.version = server, version
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id or uid(user or "anon"),
                             protocol=version, clean_session=None if version == V5 else clean)
        self.clean = clean
        if user:
            self.c.username_pw_set(user, password)
        self.msgs, self.lock = [], threading.Lock()
        self.connack, self.session_present = None, None
        self.acks, self.subacks = {}, {}
        self._conn_ev = threading.Event()
        self.c.on_connect = self._on_connect
        self.c.on_message = self._on_message
        self.c.on_publish = self._on_publish
        self.c.on_subscribe = self._on_subscribe
        self.keepalive = keepalive

    # ------------------------------------------------------------ callbacks
    def _on_connect(self, c, u, flags, rc, props):
        self.connack = rc
        self.session_present = bool(flags.session_present)
        self._conn_ev.set()

    def _on_message(self, c, u, m):
        with self.lock:
            self.msgs.append({"topic": m.topic, "payload": m.payload, "qos": m.qos, "retain": m.retain,
                              "t": time.time()})

    def _on_publish(self, c, u, mid, rc, props):
        self.acks[mid] = rc

    def _on_subscribe(self, c, u, mid, rcs, props):
        self.subacks[mid] = rcs

    # ------------------------------------------------------------ API
    def connect(self, timeout=8, wait=True):
        kw = {}
        if self.version == V5:
            p = Properties(PacketTypes.CONNECT)
            if not self.clean:
                p.SessionExpiryInterval = 3600
            kw = {"clean_start": self.clean, "properties": p}
        try:
            self.c.connect(self.server[0], self.server[1], keepalive=self.keepalive, **kw)
        except OSError as e:
            self.connack = f"socket error: {e}"
            return self
        self.c.loop_start()
        if wait:
            self._conn_ev.wait(timeout)
        return self

    @property
    def ok(self):
        return self.connack is not None and not isinstance(self.connack, str) and not self.connack.is_failure

    def connack_str(self):
        if self.connack is None:
            return "нет ответа (соединение закрыто брокером)"
        return str(self.connack)

    def subscribe(self, topic, qos=1, timeout=5):
        rc, mid = self.c.subscribe(topic, qos)
        end = time.time() + timeout
        while mid not in self.subacks and time.time() < end:
            time.sleep(0.02)
        r = self.subacks.get(mid)
        return [str(x) for x in r] if r else ["нет SUBACK"]

    def publish(self, topic, payload, qos=1, retain=False, timeout=5):
        info = self.c.publish(topic, payload, qos=qos, retain=retain)
        if qos == 0:
            return "QoS 0: подтверждения нет"
        end = time.time() + timeout
        while info.mid not in self.acks and time.time() < end:
            time.sleep(0.02)
        rc = self.acks.get(info.mid)
        return str(rc) if rc is not None else "нет PUBACK"

    def received(self, topic_prefix=None):
        with self.lock:
            return [m for m in self.msgs if topic_prefix is None or m["topic"].startswith(topic_prefix)]

    def close(self):
        try:
            self.c.disconnect()
        finally:
            self.c.loop_stop()


# ---------------------------------------------------------------- результаты проверок
class Check:
    def __init__(self, num, title):
        self.num, self.title, self.rows = num, title, []
        print(f"\n######## Проверка {num}: {title} ########", flush=True)

    def row(self, action, expected, actual, ok):
        self.rows.append({"action": action, "expected": expected, "actual": actual, "pass": bool(ok)})
        print(f"  [{'PASS' if ok else 'FAIL'}] {action}\n         ожидалось: {expected}\n         получено:  {actual}",
              flush=True)

    def info(self, **kw):
        self.__dict__.setdefault("extra", {}).update(kw)

    def save(self):
        os.makedirs(RESULTS, exist_ok=True)
        res = {"check": self.num, "title": self.title, "rows": self.rows,
               "passed": sum(r["pass"] for r in self.rows), "total": len(self.rows),
               "extra": self.__dict__.get("extra", {})}
        with open(os.path.join(RESULTS, f"check{self.num}.json"), "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2, default=str)
        status = "PASS" if res["passed"] == res["total"] else "FAIL"
        print(f"  ИТОГ проверки {self.num}: {res['passed']}/{res['total']} -> {status}", flush=True)
        return res


# ---------------------------------------------------------------- Docker Engine API (через /var/run/docker.sock)
class _UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=60)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.path)


DOCKER_SOCK = "/var/run/docker.sock"


def docker_api(method, url):
    c = _UnixHTTP(DOCKER_SOCK)
    c.request(method, url)
    r = c.getresponse()
    return r.status, r.read()


def wait_port(server, down=False, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(server, timeout=1):
                if not down:
                    return True
        except OSError:
            if down:
                return True
        time.sleep(0.3)
    return False


def restart_broker_a():
    """Штатный перезапуск брокера A (SIGTERM -> mosquitto сохраняет базу -> старт)."""
    t0 = time.time()
    if os.getenv("LOCAL_RESTART_CMD"):
        subprocess.run(os.getenv("LOCAL_RESTART_CMD"), shell=True, check=True)
    elif os.path.exists(DOCKER_SOCK):
        st, body = docker_api("POST", f"/containers/{CONTAINER_A}/restart?t=10")
        if st not in (204, 200):
            raise RuntimeError(f"Docker API restart: {st} {body[:200]}")
    else:
        print(f"\n>>> Выполни в другом терминале:  docker restart {CONTAINER_A}\n>>> жду, пока брокер остановится...")
        wait_port(A, down=True, timeout=300)
    time.sleep(1)
    wait_port(A, timeout=60)
    time.sleep(1.5)
    return time.time() - t0


def broker_a_logs(since_ts):
    """Строки лога брокера A с момента since_ts (Docker API или локальный файл)."""
    if os.getenv("LOCAL_LOG_FILE"):
        with open(os.getenv("LOCAL_LOG_FILE"), encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()[-400:]
    if not os.path.exists(DOCKER_SOCK):
        return []
    st, body = docker_api("GET", f"/containers/{CONTAINER_A}/logs?stdout=1&stderr=1&since={int(since_ts)}")
    out, i = [], 0
    while i + 8 <= len(body):                      # мультиплексированный поток Docker: 8-байтный заголовок
        n = int.from_bytes(body[i + 4:i + 8], "big")
        out += body[i + 8:i + 8 + n].decode(errors="replace").splitlines()
        i += 8 + n
    return out


def read_sys(server, user, password, seconds=7, prefix="$SYS/#"):
    """Снимок $SYS: последние значения всех метрик за seconds секунд."""
    c = Client(server, user, password, version=V3).connect()
    c.subscribe(prefix, 0)
    time.sleep(seconds)
    c.close()
    vals = {}
    for m in c.received("$SYS"):
        vals[m["topic"]] = m["payload"].decode(errors="replace")
    return vals
