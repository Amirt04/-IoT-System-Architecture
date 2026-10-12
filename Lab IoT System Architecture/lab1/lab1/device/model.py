"""Модели датчиков, валидация и сборка сообщения (Lab 1, Device Layer).

Конвейер: [sensor models] -> [sampling] -> [validation] -> [encoding] -> [ring buffer] -> [publish]
Этот модуль отвечает за первые три шага и формирование сообщения.
"""
import json
import math
import random
from datetime import datetime, timezone

import cbor2

SCHEMA_VERSION = "1.0"


class AnalogSensor:
    """Значение = база + суточный цикл + шум + дрейф + редкие сбои.

    Сбои (fault injection) нужны, чтобы было что ловить валидатору:
      * outlier — одиночный выброс (иногда за пределы диапазона);
      * nan     — датчик вернул NaN (обрыв линии, ошибка CRC);
      * stuck   — «залипание»: несколько отсчётов подряд одно и то же значение.
    """

    def __init__(self, name, base, amplitude, noise, drift_per_hour, decimals,
                 day_seconds, phase=0.0, p_outlier=0.01, outlier_size=15.0,
                 p_nan=0.005, p_stuck=0.003, stuck_len=6, rng=None):
        self.name, self.base, self.amp, self.noise = name, base, amplitude, noise
        self.drift = drift_per_hour / 3600.0
        self.dec, self.day, self.phase = decimals, day_seconds, phase
        self.p_outlier, self.outlier_size = p_outlier, outlier_size
        self.p_nan, self.p_stuck, self.stuck_len = p_nan, p_stuck, stuck_len
        self.rng = rng or random.Random()
        self.stuck_left, self.stuck_value = 0, None

    def read(self, t):
        if self.stuck_left > 0:
            self.stuck_left -= 1
            return self.stuck_value
        cycle = self.amp * math.sin(2 * math.pi * t / self.day + self.phase)
        value = self.base + cycle + self.drift * t + self.rng.gauss(0, self.noise)
        r = self.rng.random()
        if r < self.p_nan:
            return float("nan")
        if r < self.p_nan + self.p_outlier:
            value += self.rng.choice([-1, 1]) * self.outlier_size * self.rng.uniform(1, 3)
        value = round(value, self.dec)
        if self.rng.random() < self.p_stuck:
            self.stuck_left, self.stuck_value = self.stuck_len - 1, value
        return value


class PirSensor:
    """PIR: событие движения держится 1–3 отсчёта; днём движение чаще."""

    def __init__(self, day_seconds, rng=None):
        self.day, self.rng, self.hold = day_seconds, rng or random.Random(), 0

    def read(self, t):
        if self.hold > 0:
            self.hold -= 1
            return True
        daylight = max(0.0, math.sin(2 * math.pi * t / self.day))
        if self.rng.random() < 0.02 + 0.10 * daylight:
            self.hold = self.rng.randint(0, 2)
            return True
        return False


class Validator:
    """Проверки на устройстве: диапазон, NaN, залипание, скачок.

    Политика: NaN и выход за диапазон -> значение null + флаг (данные
    физически невозможны); stuck и spike -> значение сохраняется + флаг
    (это может быть реальным событием, окончательно решает backend).
    """

    def __init__(self, limits, max_step, stuck_n=5):
        self.limits, self.max_step, self.stuck_n = limits, max_step, stuck_n
        self.history = {k: [] for k in limits}
        self.last_ok = {}

    def check(self, name, value):
        lo, hi = self.limits[name]
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None, "nan"
        if not lo <= value <= hi:
            return None, "out_of_range"
        hist = self.history[name]
        hist.append(value)
        del hist[:-self.stuck_n]
        flag = None
        if len(hist) == self.stuck_n and len(set(hist)) == 1:
            flag = "stuck"
        prev = self.last_ok.get(name)
        if flag is None and prev is not None and abs(value - prev) > self.max_step[name]:
            flag = "spike"
        if flag is None:
            self.last_ok[name] = value
        return value, flag


class Device:
    def __init__(self, device_id, day_seconds=86400, seed=None, faults=True):
        rng = random.Random(seed)
        k = 1.0 if faults else 0.0
        common = dict(day_seconds=day_seconds, rng=rng)
        self.device_id = device_id
        self.sensors = {
            "temperature": AnalogSensor("temperature", 21.0, 4.0, 0.15, 0.05, 2,
                                        phase=-math.pi / 2, p_outlier=0.01 * k, outlier_size=15,
                                        p_nan=0.004 * k, p_stuck=0.003 * k, **common),
            "humidity": AnalogSensor("humidity", 45.0, 12.0, 0.6, -0.1, 1,
                                     phase=math.pi / 2, p_outlier=0.008 * k, outlier_size=30,
                                     p_nan=0.004 * k, p_stuck=0.003 * k, **common),
            "pressure": AnalogSensor("pressure", 965.0, 2.5, 0.06, 0.02, 2,
                                     p_outlier=0.006 * k, outlier_size=60,
                                     p_nan=0.003 * k, p_stuck=0.003 * k, **common),
        }
        self.pir = PirSensor(day_seconds, rng)
        self.validator = Validator(
            limits={"temperature": (-40, 85), "humidity": (0, 100), "pressure": (300, 1100)},
            max_step={"temperature": 3.0, "humidity": 10.0, "pressure": 5.0},
        )
        self.rng, self.battery, self.seq = rng, 100.0, 0

    def sample(self, t, now=None):
        """Один цикл: опрос датчиков -> валидация -> сообщение (dict)."""
        now = now or datetime.now(timezone.utc)
        metrics, quality = {}, {}
        for name, sensor in self.sensors.items():
            value, flag = self.validator.check(name, sensor.read(t))
            metrics[name] = value
            if flag:
                quality[name] = flag
        metrics["motion"] = self.pir.read(t)
        self.battery = max(0.0, self.battery - 0.01)
        msg = {
            "schema_version": SCHEMA_VERSION,
            "device_id": self.device_id,
            "ts": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "seq": self.seq,
            "metrics": metrics,
            "battery": round(self.battery, 1),
            "rssi": int(self.rng.gauss(-67, 4)),
        }
        if quality:
            msg["quality"] = quality
        self.seq += 1
        return msg


def encode(msg, fmt):
    if fmt == "json":
        return json.dumps(msg, separators=(",", ":"), ensure_ascii=False).encode()
    if fmt == "cbor":
        return cbor2.dumps(msg)
    raise ValueError(fmt)


def decode(data, fmt):
    return json.loads(data) if fmt == "json" else cbor2.loads(data)
