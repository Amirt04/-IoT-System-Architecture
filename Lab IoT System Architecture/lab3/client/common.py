"""Общие настройки Lab 3: схема топиков, формат сообщения, вспомогательные функции.

Схема топиков (спроектирована по принципу «от общего к частному», Practice 6):
    iot/v1/<site>/<device_id>/telemetry/<metric>     — телеметрия, metric ∈ {temperature, humidity, pressure, motion}
    iot/v1/<site>/<device_id>/status                 — online / offline (retained, offline = LWT)
    iot/v1/<site>/<device_id>/config                 — конфигурация устройства (retained)
Примеры подписок:
    iot/v1/+/+/telemetry/temperature   — температура со всех площадок и устройств  (+ = один уровень)
    iot/v1/astana/dev-001/telemetry/+  — все метрики одного устройства
    iot/v1/#                           — всё дерево (# = любое число уровней)
"""
import json
import os
import time
import uuid

BROKER = os.getenv("BROKER", "mosquitto")
PORT = int(os.getenv("BROKER_PORT", "1883"))
RESULTS = os.getenv("RESULTS", "/results")
CAPTURES = os.getenv("CAPTURES", "/captures")

ROOT = "iot/v1"
METRICS = ("temperature", "humidity", "pressure", "motion")


def t_topic(site, dev, metric):
    return f"{ROOT}/{site}/{dev}/telemetry/{metric}"


def status_topic(site, dev):
    return f"{ROOT}/{site}/{dev}/status"


def config_topic(site, dev):
    return f"{ROOT}/{site}/{dev}/config"


def payload(seq, value):
    """Компактный JSON: порядковый номер, время отправки (для задержки), значение."""
    return json.dumps({"seq": seq, "ts": round(time.time(), 6), "v": value}, separators=(",", ":"))


def parse(raw):
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def pct(values, q):
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * q / 100
    f = int(k)
    c = min(f + 1, len(v) - 1)
    return v[f] + (v[c] - v[f]) * (k - f)


def save_json(name, obj):
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, name), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
