"""Lab 5 — ingest-сервис телеметрии.

MQTT (aiomqtt, MQTT 5, QoS 1) -> validate (JSON Schema) -> enrich (метаданные) -> batch
   -> очередь записи -> SQLite (WAL, synchronous=FULL) ;  /metrics -> Prometheus

Батч сбрасывается по правилу  max_batch_size OR max_delay.
Режимы перегрузки (MODE / POST /config):
  none      — авто-PUBACK сразу при приёме, очереди без ограничений (как «наивный» сервис);
  inflight  — backpressure: PUBACK только после записи в БД, окно Receive Maximum = INFLIGHT,
              очередь записи ограничена -> сервис не берёт больше, чем успевает; остаток ждёт в брокере;
  throttle  — inflight + сигнал источникам: retained iot/v1/ctrl/backpressure {"factor": 0..1},
              симуляторы снижают частоту (AIMD-регулятор).
"""
import asyncio
import json
import logging
import os
import socket
import sqlite3
import time
from array import array
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import aiomqtt
import psutil
from fastapi import Body, FastAPI
from jsonschema import Draft202012Validator
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

BROKER = os.getenv("BROKER", "mosquitto")
BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
TOPIC = os.getenv("TOPIC", "iot/v1/+/+/telemetry")
CTRL_TOPIC = "iot/v1/ctrl/backpressure"
DB_PATH = os.getenv("DB_PATH", "/data/telemetry.db")
NODE = os.getenv("NODE_NAME", socket.gethostname())
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema", "telemetry.schema.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

cfg = {
    "max_batch_size": int(os.getenv("MAX_BATCH_SIZE", "500")),
    "max_delay": float(os.getenv("MAX_DELAY", "2.0")),
    "mode": os.getenv("MODE", "none"),          # none | inflight | throttle
    "inflight": int(os.getenv("INFLIGHT", "1000")),
    "write_queue": int(os.getenv("WRITE_QUEUE", "2")),   # батчей в очереди записи (для inflight/throttle)
}

# ---------------------------------------------------------------- метрики Prometheus
RECEIVED = Counter("messages_received_total", "Принято сообщений из MQTT")
INVALID = Counter("messages_invalid_total", "Отброшено при валидации", ["reason"])
STORED = Counter("messages_stored_total", "Записано в хранилище (commit)")
DUPS = Counter("messages_duplicate_total", "Повторные доставки (QoS 1), отброшены INSERT OR IGNORE")
BATCH = Histogram("batch_size", "Фактический размер батча при сбросе",
                  buckets=[1, 2, 5, 10, 25, 50, 100, 200, 300, 400, 500, 750, 1000])
FLUSH = Counter("batch_flush_total", "Сбросы батча по причине", ["reason"])
LAT = Histogram("processing_latency_seconds", "Сквозная задержка: ts устройства -> commit в БД",
                buckets=[.005, .01, .025, .05, .1, .25, .5, .75, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 15, 20, 30, 60])
BWAIT = Histogram("broker_wait_seconds", "Ожидание до приёма сервисом: ts устройства -> приём",
                  buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 20, 30, 60])
QDEPTH = Gauge("queue_depth", "Сообщений внутри сервиса: входная очередь + батч + очередь записи")
INFL = Gauge("inflight_messages", "Принято, но ещё не подтверждено брокеру (PUBACK)")
FACTOR = Gauge("backpressure_factor", "Множитель частоты источников (режим throttle)")
STAGE = Counter("stage_seconds_total", "Время по этапам конвейера", ["stage"])
CPU = Gauge("ingest_cpu_percent", "CPU процесса ingest, % одного ядра")
RSS = Gauge("ingest_rss_bytes", "Память процесса ingest (RSS)")


class State:
    def __init__(self):
        self.client = None
        self.gen = 0
        self.consumer_task = None
        self.batch = []
        self.batch_t0 = None
        self.write_q = None
        self.writing = 0
        self.unacked = 0
        self.factor = 1.0
        self.recent_bw = deque(maxlen=20000)
        self.reset()

    def reset(self):
        self.t_reset = time.time()
        self.received = self.invalid = self.stored = self.dups = 0
        self.lat = array("d")
        self.ts = array("d")
        self.bw = array("d")
        self.batches = array("i")
        self.flush_reasons = {"size": 0, "delay": 0}
        self.stage = {"parse": 0.0, "validate": 0.0, "enrich": 0.0, "db_write": 0.0}
        self.max_q = 0
        self.cpu = 0.0
        self.rss = 0


st = State()
validator = Draft202012Validator(json.load(open(SCHEMA_PATH, encoding="utf-8")))
proc = psutil.Process()
DBX = ThreadPoolExecutor(1, thread_name_prefix="db")
_db = None

# реестр устройств (в реальной системе — справочник/CMDB); здесь детерминированно по номеру
MODELS = ["АИР80В2", "АИР100L4", "АИР132М4", "5АМ250М4"]
LINES = ["насосная-1", "вентиляция-2", "конвейер-3", "компрессорная-4"]


def device_meta(dev):
    n = int(dev.split("-")[1])
    return MODELS[n % 4], f"{LINES[(n // 4) % 4]}/M{n:05d}"


_meta_cache = {}


# ---------------------------------------------------------------- хранилище
def db_init():
    global _db
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _db = sqlite3.connect(DB_PATH, check_same_thread=False)
    _db.execute("PRAGMA journal_mode=WAL")
    _db.execute("PRAGMA synchronous=FULL")      # каждый commit = fsync -> цена батча честная
    _db.execute("""CREATE TABLE IF NOT EXISTS telemetry(
        device_id TEXT, site TEXT, ts INTEGER, seq INTEGER,
        temperature REAL, vibration_rms REAL, current REAL, rpm REAL,
        model TEXT, asset TEXT, received_at REAL, node TEXT, stored_at REAL,
        PRIMARY KEY(device_id, ts, seq)) WITHOUT ROWID""")
    _db.commit()


def db_write(rows):
    now = time.time()
    cur = _db.executemany("INSERT OR IGNORE INTO telemetry VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                          [r + (now,) for r in rows])
    _db.commit()
    return cur.rowcount


def db_clear():
    _db.execute("DELETE FROM telemetry")
    _db.commit()
    _db.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def db_count():
    return _db.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0]


# ---------------------------------------------------------------- конвейер
def manual_ack():
    return cfg["mode"] != "none"


def ack(gen, mid):
    if gen == st.gen and st.client is not None and manual_ack():
        try:
            st.client._client.ack(mid, 1)
        except Exception:  # noqa: BLE001 — соединение уже закрыто, брокер передоставит
            pass
        st.unacked -= 1


def handle(m, gen):
    t_recv = time.time()
    st.received += 1
    RECEIVED.inc()
    if manual_ack():
        st.unacked += 1
    p0 = time.perf_counter()
    try:
        d = json.loads(m.payload)
    except ValueError:
        INVALID.labels("json").inc()
        st.invalid += 1
        ack(gen, m.mid)
        return
    p1 = time.perf_counter()
    ok = validator.is_valid(d)
    p2 = time.perf_counter()
    st.stage["parse"] += p1 - p0
    st.stage["validate"] += p2 - p1
    if not ok:
        INVALID.labels("schema").inc()
        st.invalid += 1
        ack(gen, m.mid)
        return
    parts = m.topic.value.split("/")
    dev = d["device_id"]
    if len(parts) != 5 or parts[3] != dev:
        INVALID.labels("topic").inc()
        st.invalid += 1
        ack(gen, m.mid)
        return
    meta = _meta_cache.get(dev)
    if meta is None:
        meta = _meta_cache[dev] = device_meta(dev)
    mt = d["metrics"]
    ts = d["ts"] / 1000
    row = (dev, parts[2], d["ts"], d["seq"], mt["temperature"], mt["vibration_rms"], mt["current"],
           mt.get("rpm"), meta[0], meta[1], t_recv, NODE)
    st.stage["enrich"] += time.perf_counter() - p2
    st.recent_bw.append((t_recv, t_recv - ts))
    if not st.batch:
        st.batch_t0 = time.monotonic()
    st.batch.append((row, gen, m.mid, ts, t_recv))


async def flush(reason):
    if not st.batch:
        return
    b, st.batch, st.batch_t0 = st.batch, [], None
    st.flush_reasons[reason] += 1
    FLUSH.labels(reason).inc()
    BATCH.observe(len(b))
    st.batches.append(len(b))
    await st.write_q.put(b)          # в режимах backpressure очередь ограничена -> здесь ждём


async def consumer():
    gen = st.gen
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = 3600          # сессия переживает паузу/переподключение
    if manual_ack():
        props.ReceiveMaximum = cfg["inflight"]  # окно неподтверждённых QoS1 от брокера к нам
    client = aiomqtt.Client(BROKER, BROKER_PORT, identifier=f"ingest-{NODE}",
                            protocol=aiomqtt.ProtocolVersion.V5, clean_start=False,
                            properties=props, keepalive=30)
    client._client.manual_ack_set(manual_ack())
    while True:
        try:
            async with client:
                st.client = client
                await client.subscribe(TOPIC, qos=1)
                log.info("подключено к %s, режим=%s, inflight=%s, batch=%s/%ss", BROKER, cfg["mode"],
                         cfg["inflight"] if manual_ack() else "∞", cfg["max_batch_size"], cfg["max_delay"])
                async for m in client.messages:
                    handle(m, gen)
                    if len(st.batch) >= cfg["max_batch_size"]:
                        await flush("size")
        except aiomqtt.MqttError as e:
            st.client = None
            log.warning("MQTT: %s — переподключение через 1 с", e)
            await asyncio.sleep(1)


async def timer():
    while True:
        await asyncio.sleep(0.01)
        if st.batch and time.monotonic() - st.batch_t0 >= cfg["max_delay"]:
            await flush("delay")


async def writer():
    loop = asyncio.get_running_loop()
    while True:
        b = await st.write_q.get()
        st.writing = len(b)
        t = time.perf_counter()
        new = await loop.run_in_executor(DBX, db_write, [x[0] for x in b])
        st.stage["db_write"] += time.perf_counter() - t
        now = time.time()
        for _, gen, mid, ts, t_recv in b:
            e2e = now - ts
            LAT.observe(e2e)
            BWAIT.observe(t_recv - ts)
            st.lat.append(e2e)
            st.ts.append(ts)
            st.bw.append(t_recv - ts)
            ack(gen, mid)
        n = len(b)
        st.stored += n
        STORED.inc(n)
        if n - new:
            st.dups += n - new
            DUPS.inc(n - new)
        st.writing = 0
        st.write_q.task_done()


def backlog():
    """Ожидают обработки: входная очередь клиента + очередь записи (без формируемого батча)."""
    inq = st.client._queue.qsize() if st.client is not None else 0
    return inq + sum(len(b) for b in st.write_q._queue) + st.writing


def queue_depth():
    return backlog() + len(st.batch)


async def sampler():
    proc.cpu_percent(None)
    last = time.time()
    while True:
        await asyncio.sleep(0.25)
        q = queue_depth()
        st.max_q = max(st.max_q, q)
        QDEPTH.set(q)
        INFL.set(st.unacked if manual_ack() else 0)
        if time.time() - last >= 1:
            last = time.time()
            st.cpu = proc.cpu_percent(None)
            st.rss = proc.memory_info().rss
            CPU.set(st.cpu)
            RSS.set(st.rss)
            if len(st.batches) != getattr(st, "_logged", -1):
                st._logged = len(st.batches)
                if not st.batches:
                    continue
                tail = st.batches[-200:]
                log.info("батчи: последний=%d, ср.(200)=%.0f, причины size/delay=%d/%d, очередь=%d",
                         st.batches[-1], sum(tail) / len(tail), st.flush_reasons["size"],
                         st.flush_reasons["delay"], q)


async def controller():
    """AIMD-регулятор для режима throttle: перегрузка -> factor×0.7, норма -> +0.05."""
    published = None
    while True:
        await asyncio.sleep(1)
        if cfg["mode"] != "throttle":
            target = 1.0
        else:
            now = time.time()
            w = sorted(b for t, b in st.recent_bw if now - t <= 1.0)
            p95 = w[int(len(w) * 0.95)] if w else 0.0
            util = st.unacked / cfg["inflight"]
            if p95 > 0.5 or util > 0.9:
                st.factor = max(0.05, st.factor * 0.7)
            elif p95 < 0.2 and util < 0.5:
                st.factor = min(1.0, st.factor + 0.05)
            target = st.factor
        st.factor = target
        FACTOR.set(target)
        if st.client is not None and (published is None or abs(published - target) > 1e-3):
            try:
                await st.client.publish(CTRL_TOPIC, json.dumps({"factor": round(target, 3), "ts": time.time()}),
                                        qos=1, retain=True)
                published = target
            except aiomqtt.MqttError:
                pass


def start_consumer():
    st.gen += 1
    st.unacked = 0
    st.consumer_task = asyncio.create_task(consumer())


async def stop_consumer():
    if st.consumer_task:
        st.consumer_task.cancel()
        try:
            await st.consumer_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    st.consumer_task = None
    st.client = None


def new_write_q():
    return asyncio.Queue(maxsize=cfg["write_queue"] if manual_ack() else 0)


@asynccontextmanager
async def lifespan(_app):
    await asyncio.get_running_loop().run_in_executor(DBX, db_init)
    st.write_q = new_write_q()
    tasks = [asyncio.create_task(f()) for f in (writer, timer, sampler, controller)]
    start_consumer()
    yield
    await stop_consumer()
    for t in tasks:
        t.cancel()


app = FastAPI(title="Lab 5 ingest", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
def health():
    return {"ok": True, "mqtt": st.client is not None, "node": NODE}


def pct(a, p):
    if not a:
        return None
    s = sorted(a)
    return round(s[min(len(s) - 1, int(len(s) * p / 100))], 4)


@app.get("/stats")
async def stats(full: int = 0, since: float = 0, until: float = 0):
    d = {"t": time.time(), "cfg": cfg, "received": st.received, "invalid": st.invalid, "stored": st.stored,
         "duplicates": st.dups, "queue_depth": queue_depth(), "backlog": backlog(), "max_queue_depth": st.max_q,
         "inflight": st.unacked if manual_ack() else None, "factor": round(st.factor, 3),
         "cpu_percent": st.cpu, "rss_mb": round(st.rss / 2**20, 1), "connected": st.client is not None,
         "batches": len(st.batches), "flush_reasons": st.flush_reasons}
    if full:
        b = list(st.batches)
        lat, bw = st.lat, st.bw
        if since or until:      # только сообщения, измеренные в окне [since, until] (без «хвостов» прогона)
            hi = until or float("inf")
            idx = [i for i, t in enumerate(st.ts) if since <= t <= hi]
            lat, bw = [st.lat[i] for i in idx], [st.bw[i] for i in idx]
        d.update(
            latency={p: pct(lat, p) for p in (50, 95, 99)} | {"max": round(max(lat), 4) if lat else None,
                                                              "n": len(lat)},
            broker_wait={p: pct(bw, p) for p in (50, 95, 99)},
            batch={"count": len(b), "avg": round(sum(b) / len(b), 1) if b else None,
                   "min": min(b) if b else None, "max": max(b) if b else None},
            stage_seconds={k: round(v, 3) for k, v in st.stage.items()},
            db_rows=await asyncio.get_running_loop().run_in_executor(DBX, db_count))
    return d


@app.post("/reset")
async def reset():
    await asyncio.get_running_loop().run_in_executor(DBX, db_clear)
    st.reset()
    return {"ok": True}


@app.post("/config")
async def config(body: dict = Body(...)):
    reconnect = any(k in body and body[k] != cfg[k] for k in ("mode", "inflight"))
    for k in cfg:
        if k in body:
            cfg[k] = type(cfg[k])(body[k])
    if reconnect:
        await stop_consumer()
        await flush("delay")
        await st.write_q.join()
        st.write_q._maxsize = cfg["write_queue"] if manual_ack() else 0   # писатель ждёт на этой же очереди
        st.factor = 1.0
        start_consumer()
        await asyncio.sleep(0.5)
    log.info("конфигурация: %s", cfg)
    return cfg


@app.post("/pause")
async def pause():
    """Отключиться от брокера, сохранив сессию: сообщения копятся в брокере (тест «слива» очереди)."""
    await stop_consumer()
    return {"paused": True}


@app.post("/resume")
async def resume():
    if st.consumer_task is None:
        start_consumer()
    return {"paused": False}
