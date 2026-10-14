"""Модель сети LoRaWAN: N устройств -> коллизионная модель ALOHA -> модель шлюза.

Допущения модели (указаны в отчёте):
  * Класс A, неподтверждаемые uplink, без повторов. Каждое устройство в каждом периоде T
    выбирает случайный момент передачи (равномерно) -> поток, близкий к пуассоновскому (чистая ALOHA).
  * 3 обязательных канала EU868 (868.1 / 868.3 / 868.5 МГц), канал выбирается случайно.
  * Ортогональность SF: пакеты разных SF не мешают друг другу (идеальная ортогональность);
    коллизия возможна только при перекрытии ToA на одном канале и одном SF.
  * Capture effect: из перекрывающихся пакетов принимается тот, чья мощность на шлюзе выше
    мощности КАЖДОГО из остальных минимум на CAPTURE_DB (6 дБ). Без capture теряются все.
  * Шлюз (SX1301): 8 демодуляторов. Если в момент начала пакета все 8 заняты — пакет теряется.
  * Мощность на шлюзе: Ptx = 14 дБм, затухание log-distance (модель Bor et al., 2016):
    PL(d) = 127,41 + 20,8·lg(d/40 м) + X, X ~ N(0; 3,57 дБ). Устройства в круге R = 1 км.
    Все устройства считаются в зоне покрытия выбранного SF (чувствительность не моделируется).
"""
import heapq
import numpy as np

from airtime import time_on_air

CHANNELS = 3
CAPTURE_DB = 6.0
DEMODULATORS = 8
PTX_DBM = 14.0


def path_loss(d_m, rng):
    return 127.41 + 20.8 * np.log10(np.maximum(d_m, 1.0) / 40.0) + rng.normal(0, 3.57, size=np.shape(d_m))


def place_devices(n, rng, radius_m=1000.0):
    r = radius_m * np.sqrt(rng.random(n))          # равномерно по площади круга
    return r


def resolve(start, dur, chan, sf, power, capture=True, demods=DEMODULATORS):
    """Возвращает маску успешно принятых пакетов. Все массивы одной длины."""
    n = len(start)
    end = start + dur
    ok = np.ones(n, dtype=bool)

    # 1) коллизии внутри каждой группы (канал, SF)
    key = chan * 100 + sf
    for k in np.unique(key):
        idx = np.where(key == k)[0]
        idx = idx[np.argsort(start[idx])]
        s, e, p = start[idx], end[idx], power[idx]
        # для каждого пакета: соседи, начавшиеся до его конца и закончившиеся после его начала
        max_dur = (e - s).max()
        lo = np.searchsorted(s, s - max_dur, side="left")
        hi = np.searchsorted(s, e, side="left")
        for i in range(len(idx)):
            j = np.arange(lo[i], hi[i])
            j = j[(j != i) & (e[j] > s[i])]
            if len(j) == 0:
                continue
            if capture and np.all(p[i] - p[j] >= CAPTURE_DB):
                continue
            ok[idx[i]] = False

    # 2) ограничение шлюза: 8 параллельных демодуляторов
    if demods:
        order = np.argsort(start)
        busy = []                                   # время окончания занятых демодуляторов
        for i in order:
            while busy and busy[0] <= start[i]:
                heapq.heappop(busy)
            if len(busy) >= demods:
                ok[i] = False
            else:
                heapq.heappush(busy, end[i])
    return ok


def simulate(n_dev, sf, period_s, payload=20, periods=8, capture=True, demods=DEMODULATORS,
             seed=0, sf_per_device=None):
    """Один прогон. sf_per_device — массив SF для смешанной сети (иначе у всех одинаковый sf).
    Возвращает dict с PDR (общий и по SF) и счётчиками."""
    rng = np.random.default_rng(seed)
    dist = place_devices(n_dev, rng)
    dev_sf = np.full(n_dev, sf) if sf_per_device is None else np.asarray(sf_per_device)
    toa = {s: time_on_air(int(s), payload) for s in np.unique(dev_sf)}

    dev = np.repeat(np.arange(n_dev), periods)
    k = np.tile(np.arange(periods), n_dev)
    start = (k + rng.random(n_dev * periods)) * period_s
    s_arr = dev_sf[dev]
    dur = np.zeros(len(dev))
    for s_val, t in toa.items():
        dur[s_arr == s_val] = t
    chan = rng.integers(0, CHANNELS, size=len(dev))
    power = PTX_DBM - path_loss(dist[dev], rng)

    ok = resolve(start, dur, chan, s_arr, power, capture=capture, demods=demods)
    # краевой эффект: учитываем только пакеты из «внутренних» периодов
    inner = (start >= period_s) & (start < (periods - 1) * period_s)
    res = {"pdr": ok[inner].mean(), "sent": int(inner.sum()), "ok": int(ok[inner].sum())}
    for s in np.unique(dev_sf):
        m = inner & (s_arr == s)
        res[f"pdr_sf{int(s)}"] = ok[m].mean() if m.any() else np.nan
    return res


def aloha_test(G, pkt=1.0, horizon=20000.0, seed=0):
    """Проверка на аналитическом пределе: чистая ALOHA, 1 канал, 1 SF, без capture и без лимита шлюза.
    G — нагрузка (среднее число начатых передач за время пакета). Возвращает пропускную способность S."""
    rng = np.random.default_rng(seed)
    n = rng.poisson(G * horizon / pkt)
    start = np.sort(rng.random(n) * horizon)
    z = np.zeros(n, dtype=int)
    ok = resolve(start, np.full(n, pkt), z, z + 7, np.zeros(n), capture=False, demods=0)
    inner = (start > pkt) & (start < horizon - 2 * pkt)
    return G * ok[inner].mean()


def analytic_pdr(n_dev, sf, period_s, payload=20):
    """Без capture и лимита шлюза: PDR = exp(-2G), G = N·ToA / (T·каналы)."""
    g = n_dev * time_on_air(sf, payload) / (period_s * CHANNELS)
    return float(np.exp(-2 * g))


def analytic_max_n(sf, period_s, pdr=0.95, payload=20):
    return -np.log(pdr) / 2 * period_s * CHANNELS / time_on_air(sf, payload)
