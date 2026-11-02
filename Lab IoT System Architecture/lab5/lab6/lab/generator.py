"""Генератор правдоподобной телеметрии асинхронных двигателей (1 замер/мин на устройство).

Каждый двигатель работает по графику (будни 06–22, суббота 08–14 по Астане, воскресенье – стоит),
нагрузка плавно меняется; температура обмотки следует за нагрузкой с тепловой инерцией (τ = 40 мин)
и сезонной температурой цеха; вибрация СКЗ медленно растёт из-за износа подшипника, плюс
эпизоды аномалий (~2 в месяц, 20–180 мин, 7,5–12 мм/с > порога ISO 10816 зоны D 7,1 мм/с).
"""
import io
import math

import numpy as np
import pandas as pd

MODELS = [("АИР80В2", 2.2), ("АИР100L4", 4.0), ("АИР132М4", 11.0), ("АИР160S4", 15.0), ("5АМ250М4", 90.0)]
SITES = ["astana", "almaty", "karaganda", "shymkent"]
ANOMALY_THRESHOLD = 7.1
LOCAL_OFFSET_MIN = 5 * 60          # Asia/Almaty, UTC+5


def device_rows(n):
    return [(d, f"M-{d:03d}", MODELS[d % len(MODELS)][0], SITES[d % len(SITES)], MODELS[d % len(MODELS)][1])
            for d in range(1, n + 1)]


class Generator:
    def __init__(self, devices, start, minutes, seed=42):
        self.devices = devices
        self.start = np.datetime64(start, "m")
        self.minutes = minutes
        self.rng = np.random.default_rng(seed)
        self.temp = {d: 20.0 for d in range(1, devices + 1)}
        self.power = {d: MODELS[d % len(MODELS)][1] for d in range(1, devices + 1)}
        self.wear = {d: self.rng.uniform(0.2, 1.6) for d in range(1, devices + 1)}
        # эпизоды аномалий на весь период: Пуассон, ~2 в месяц на двигатель
        self.episodes = {}
        for d in range(1, devices + 1):
            k = self.rng.poisson(minutes / (30 * 1440) * 2)
            st = np.sort(self.rng.integers(0, minutes, k))
            self.episodes[d] = [(s, s + int(self.rng.integers(20, 180)), float(self.rng.uniform(7.5, 12.0))) for s in st]
        self.anomaly_minutes = 0

    def block(self, i0, i1):
        """Минуты [i0, i1) от начала периода -> (ts datetime64[m], {device: {metric: array}})."""
        idx = np.arange(i0, i1)
        ts = self.start + idx.astype("timedelta64[m]")
        loc = idx + (self.start.astype("int64") + LOCAL_OFFSET_MIN)       # минуты локального времени от эпохи
        mod = loc % 1440
        hour = mod / 60.0
        dow = ((loc // 1440) + 3) % 7                                     # 1970-01-01 – четверг; 0 = понедельник
        doy = (loc // 1440) % 365
        ambient = 18 + 8 * np.sin(2 * np.pi * (doy - 110) / 365) + 2.5 * np.sin(2 * np.pi * (hour - 9) / 24)
        out = {}
        for d in range(1, self.devices + 1):
            r = self.rng
            sh = (d % 3) * 0.5
            run = (((dow < 5) & (hour >= 6 + sh) & (hour < 22 - sh)) | ((dow == 5) & (hour >= 8) & (hour < 14))).astype(float)
            load = np.clip(0.75 + 0.12 * np.sin(2 * np.pi * idx / (180 + 7 * d)) + r.normal(0, 0.03, idx.size), 0.4, 1.0)
            rpm = run * (1490 - 20 * load + r.normal(0, 1.5, idx.size))
            nominal = self.power[d] * 1000 / (math.sqrt(3) * 380 * 0.85)
            current = run * (nominal * (0.35 + 0.65 * load) + r.normal(0, 0.02 * nominal, idx.size))
            target = ambient + run * 45 * load
            temp = np.empty(idx.size)
            t = self.temp[d]
            a = 1 - math.exp(-1 / 40)
            for k in range(idx.size):                                       # тепловая инерция, τ = 40 мин
                t += a * (target[k] - t)
                temp[k] = t
            self.temp[d] = t
            temp = temp + r.normal(0, 0.15, idx.size)
            vib = run * (1.8 + self.wear[d] * idx / self.minutes + 0.3 * load + r.normal(0, 0.08, idx.size)) \
                + (1 - run) * np.abs(r.normal(0.05, 0.02, idx.size))
            for s, e, amp in self.episodes[d]:
                if e > i0 and s < i1:
                    lo, hi = max(s, i0) - i0, min(e, i1) - i0
                    m = run[lo:hi] > 0
                    vib[lo:hi][m] = amp + r.normal(0, 0.4, int(m.sum()))
            self.anomaly_minutes += int((vib > ANOMALY_THRESHOLD).sum())
            # разрешение датчиков: термосопротивление 0,1 °C, вибродатчик 0,01 мм/с, токовые клещи 0,1 А, энкодер 1 об/мин
            out[d] = {1: np.round(temp, 1), 2: np.round(np.abs(vib), 2), 3: np.round(np.abs(current), 1), 4: np.round(rpm, 0)}
        return ts, out


def ts_strings(ts):
    return np.char.add(np.datetime_as_string(ts, unit="s"), "+00")


def narrow_copy_text(ts, data):
    s = ts_strings(ts)
    parts = []
    for d, ms in data.items():
        for m, v in ms.items():
            parts.append(pd.DataFrame({"ts": s, "d": d, "m": m, "v": v}))
    buf = io.StringIO()
    pd.concat(parts).to_csv(buf, sep="\t", header=False, index=False)
    return buf.getvalue(), sum(len(p) for p in parts)


def wide_copy_text(ts, data):
    s = ts_strings(ts).tolist()
    lines = []
    for d, ms in data.items():
        t, v, c, r = (ms[k].tolist() for k in (1, 2, 3, 4))
        lines += [f'{a}\t{d}\t{{"temperature": {b}, "vibration_rms": {e}, "current": {f}, "rpm": {g}}}\n'
                  for a, b, e, f, g in zip(s, t, v, c, r)]
    return "".join(lines), len(lines)
