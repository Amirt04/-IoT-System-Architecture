"""Lab 6 — Time-Series Database: все шаги по порядку.

python run_lab6.py                 # всё: schema load bench_raw cagg compress bench_comp seg2 cold retention (~15–25 мин)
python run_lab6.py load --days 400 --devices 10
python run_lab6.py bench_raw       # отдельный шаг
Шаги: schema, load, bench_raw, cagg, compress, bench_comp, seg2, cold, retention
Результаты: /results/lab6.json, /results/plans/*.txt;  затем  python plots.py
"""
import argparse
import datetime as dt
import io
import json
import os
import statistics
import time

import psycopg

from generator import ANOMALY_THRESHOLD, Generator, device_rows, narrow_copy_text, wide_copy_text

DSN = os.getenv("PG_DSN", "host=timescaledb port=5432 dbname=iot user=postgres password=postgres")
RESULTS = os.getenv("RESULTS", "/results")
HERE = os.path.dirname(os.path.abspath(__file__))
RES_FILE = os.path.join(RESULTS, "lab6.json")
os.makedirs(os.path.join(RESULTS, "plans"), exist_ok=True)
TABLES = ("telemetry_wide", "telemetry_narrow")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def load_res():
    try:
        return json.load(open(RES_FILE, encoding="utf-8"))
    except FileNotFoundError:
        return {}


def save_res(key, value):
    r = load_res()
    r[key] = value
    with open(RES_FILE, "w", encoding="utf-8") as f:
        json.dump(r, f, ensure_ascii=False, indent=1, default=str)


def conn():
    return psycopg.connect(DSN, autocommit=True)


def q(c, sql, params=None):
    with c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else None


def one(c, sql, params=None):
    r = q(c, sql, params)
    return r[0][0] if r else None


def mb(b):
    return round((b or 0) / 2**20, 1)


def meta(c):
    return {k: v for k, v in q(c, "SELECT key, value FROM lab_meta")}


def sizes(c, table):
    t, i, toast, tot = q(c, "SELECT table_bytes, index_bytes, toast_bytes, total_bytes FROM hypertable_detailed_size(%s)",
                         (table,))[0]
    return {"table_mb": mb(t), "index_mb": mb(i), "toast_mb": mb(toast), "total_mb": mb(tot),
            "chunks": one(c, "SELECT count(*) FROM show_chunks(%s)", (table,))}


# ------------------------------------------------------------------ 1. схема
def step_schema(a):
    with conn() as c:
        for v in ("cagg_1d", "cagg_1h", "cagg_1m"):
            q(c, f"DROP MATERIALIZED VIEW IF EXISTS {v} CASCADE")
        for t in TABLES + ("devices", "metrics", "lab_meta"):
            q(c, f"DROP TABLE IF EXISTS {t} CASCADE")
        q(c, "SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE job_id >= 1000")
        q(c, open(os.path.join(HERE, "schema.sql"), encoding="utf-8").read())
        with c.cursor() as cur:
            cur.executemany("INSERT INTO devices VALUES (%s,%s,%s,%s,%s)", device_rows(a.devices))
        # без статистики справочников планировщик выбирает плохой порядок соединения в LATERAL-запросе (Q1 в 600 раз медленнее)
        q(c, "ANALYZE devices")
        q(c, "ANALYZE metrics")
    log("схема создана: telemetry_wide (ts, device_id, payload JSONB), telemetry_narrow (ts, device_id, metric_id, value); "
        "chunk_time_interval = 7 дней")


# ------------------------------------------------------------------ 2. загрузка
def step_load(a):
    end = dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
    start = end - dt.timedelta(days=a.days)
    minutes = a.days * 1440
    with conn() as c:
        for t in TABLES:
            q(c, f"TRUNCATE {t}")
        q(c, "INSERT INTO lab_meta VALUES ('start', %s), ('end', %s), ('devices', %s), ('days', %s) "
             "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
          (start.isoformat(), end.isoformat(), str(a.devices), str(a.days)))
        g = Generator(a.devices, start.replace(tzinfo=None).isoformat(), minutes)
        tm = {"gen": 0.0, "telemetry_wide": 0.0, "telemetry_narrow": 0.0}
        rows = {"telemetry_wide": 0, "telemetry_narrow": 0}
        block = 7 * 1440
        log(f"загрузка: {a.devices} устройств × 1 замер/мин × {a.days} дней ({start:%Y-%m-%d} … {end:%Y-%m-%d}), "
            f"ожидается {a.devices * minutes:,} строк wide и {a.devices * minutes * 4:,} строк narrow")
        nb = (minutes + block - 1) // block
        for bi, i0 in enumerate(range(0, minutes, block)):
            t0 = time.perf_counter()
            ts, data = g.block(i0, min(i0 + block, minutes))
            wtext, wn = wide_copy_text(ts, data)
            ntext, nn = narrow_copy_text(ts, data)
            tm["gen"] += time.perf_counter() - t0
            for table, text, n, cols in (("telemetry_wide", wtext, wn, "ts, device_id, payload"),
                                         ("telemetry_narrow", ntext, nn, "ts, device_id, metric_id, value")):
                t0 = time.perf_counter()
                with c.cursor() as cur, cur.copy(f"COPY {table} ({cols}) FROM STDIN") as cp:
                    cp.write(text)
                tm[table] += time.perf_counter() - t0
                rows[table] += n
            if bi % 10 == 9 or bi == nb - 1:
                log(f"  блок {bi + 1}/{nb}: wide {rows['telemetry_wide']:,} строк ({tm['telemetry_wide']:.0f} с), "
                    f"narrow {rows['telemetry_narrow']:,} ({tm['telemetry_narrow']:.0f} с)")
        for t in TABLES:
            q(c, f"ANALYZE {t}")
        res = {"devices": a.devices, "days": a.days, "start": start, "end": end, "gen_seconds": round(tm["gen"], 1),
               "anomaly_minutes": g.anomaly_minutes}
        for t in TABLES:
            s = sizes(c, t)
            s.update(rows=rows[t], load_seconds=round(tm[t], 1), rows_per_s=round(rows[t] / tm[t]),
                     bytes_per_row=round(s["total_mb"] * 2**20 / rows[t], 1),
                     hypertable_size_mb=mb(one(c, "SELECT hypertable_size(%s)", (t,))))
            res[t] = s
            log(f"  {t}: {rows[t]:,} строк за {tm[t]:.0f} с ({s['rows_per_s']:,} строк/с), hypertable_size = "
                f"{s['hypertable_size_mb']} МБ (таблица {s['table_mb']}, индексы {s['index_mb']}, TOAST {s['toast_mb']}), "
                f"{s['bytes_per_row']} Б/строку, чанков {s['chunks']}")
        # размеры каждого чанка до сжатия — понадобятся для сравнения с Parquet
        res["chunks_raw"] = {t: [[str(n), rs, re, tb] for n, rs, re, tb in q(c, """
            SELECT c.chunk_schema || '.' || c.chunk_name, c.range_start, c.range_end, s.total_bytes
            FROM timescaledb_information.chunks c
            JOIN chunks_detailed_size(%s) s ON s.chunk_schema = c.chunk_schema AND s.chunk_name = c.chunk_name
            WHERE c.hypertable_name = %s ORDER BY c.range_start""", (t, t))] for t in TABLES}
        save_res("load", res)


# ------------------------------------------------------------------ 3. типовые запросы
def queries(c):
    m = meta(c)
    end = dt.datetime.fromisoformat(m["end"])
    d0 = (end - dt.timedelta(days=1)).replace(hour=0, minute=0)
    p = {"end": end, "d0": d0, "d1": d0 + dt.timedelta(days=1), "m30": end - dt.timedelta(days=30),
         "y1": end - dt.timedelta(days=365), "dev": 3, "thr": ANOMALY_THRESHOLD}
    Q = {
        "narrow": {
            "q1_last": """SELECT d.device_id, m.metric_id, l.ts, l.value FROM devices d CROSS JOIN metrics m
                          CROSS JOIN LATERAL (SELECT ts, value FROM telemetry_narrow t
                             WHERE t.device_id = d.device_id AND t.metric_id = m.metric_id
                             ORDER BY ts DESC LIMIT 1) l""",
            "q2_day_avg": """SELECT device_id, metric_id, avg(value) FROM telemetry_narrow
                             WHERE ts >= %(d0)s AND ts < %(d1)s GROUP BY device_id, metric_id""",
            "q3_anomalies": """SELECT device_id, count(*), min(ts), max(ts), max(value) FROM telemetry_narrow
                               WHERE metric_id = 2 AND ts > %(m30)s AND value > %(thr)s GROUP BY device_id""",
            "q4_device_year": """SELECT ts, value FROM telemetry_narrow
                                 WHERE device_id = %(dev)s AND metric_id = 1 AND ts > %(y1)s ORDER BY ts""",
        },
        "wide": {
            "q1_last": """SELECT d.device_id, l.ts, l.payload FROM devices d CROSS JOIN LATERAL
                          (SELECT ts, payload FROM telemetry_wide w WHERE w.device_id = d.device_id
                           ORDER BY ts DESC LIMIT 1) l""",
            "q2_day_avg": """SELECT device_id, avg((payload->>'temperature')::float8), avg((payload->>'vibration_rms')::float8),
                                    avg((payload->>'current')::float8), avg((payload->>'rpm')::float8)
                             FROM telemetry_wide WHERE ts >= %(d0)s AND ts < %(d1)s GROUP BY device_id""",
            "q3_anomalies": """SELECT device_id, count(*), min(ts), max(ts), max((payload->>'vibration_rms')::float8)
                               FROM telemetry_wide WHERE ts > %(m30)s AND (payload->>'vibration_rms')::float8 > %(thr)s
                               GROUP BY device_id""",
            "q4_device_year": """SELECT ts, (payload->>'temperature')::float8 FROM telemetry_wide
                                 WHERE device_id = %(dev)s AND ts > %(y1)s ORDER BY ts""",
        },
        "cagg": {
            "q2_day_avg": """SELECT device_id, metric_id, avg_value FROM cagg_1d WHERE bucket = %(d0)s""",
            "q3_anomalies": """SELECT device_id, count(*), min(bucket), max(bucket), max(max_value) FROM cagg_1m
                               WHERE metric_id = 2 AND bucket > %(m30)s AND max_value > %(thr)s GROUP BY device_id""",
            "q4_device_year": """SELECT bucket, avg_value FROM cagg_1h
                                 WHERE device_id = %(dev)s AND metric_id = 1 AND bucket > %(y1)s ORDER BY bucket""",
        },
    }
    return Q, p


def bench(c, config, schema, reps):
    Q, p = queries(c)
    out = {}
    for name, sql in Q[schema].items():
        times, n = [], 0
        for i in range(reps + 1):                       # первый прогон — прогрев кэша, не считается
            t0 = time.perf_counter()
            rows = q(c, sql, p)
            dtm = time.perf_counter() - t0
            n = len(rows)
            if i:
                times.append(dtm)
        plan = q(c, "EXPLAIN (ANALYZE, BUFFERS) " + sql, p)
        with open(os.path.join(RESULTS, "plans", f"{config}_{name}.txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(r[0] for r in plan))
        exe = [r[0] for r in plan if r[0].startswith(("Execution Time", "Planning Time"))]
        out[name] = {"median_ms": round(statistics.median(times) * 1000, 2), "min_ms": round(min(times) * 1000, 2),
                     "rows": n, "plan": exe}
        log(f"  {config:<14} {name:<15} медиана {out[name]['median_ms']:>9.2f} мс   строк {n:>7}   {' | '.join(exe)}")
    return out


def step_bench_raw(a):
    with conn() as c:
        save_res("bench_raw", {"wide": bench(c, "wide_raw", "wide", a.reps), "narrow": bench(c, "narrow_raw", "narrow", a.reps)})


# ------------------------------------------------------------------ 4. continuous aggregates
CAGG_SQL = {
    "cagg_1m": """CREATE MATERIALIZED VIEW cagg_1m WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 minute', ts) AS bucket, device_id, metric_id,
               avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value,
               sum(value) AS sum_value, count(*) AS cnt
        FROM telemetry_narrow GROUP BY 1, 2, 3 WITH NO DATA""",
    "cagg_1h": """CREATE MATERIALIZED VIEW cagg_1h WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', ts) AS bucket, device_id, metric_id,
               avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value,
               sum(value) AS sum_value, count(*) AS cnt
        FROM telemetry_narrow GROUP BY 1, 2, 3 WITH NO DATA""",
    "cagg_1d": """CREATE MATERIALIZED VIEW cagg_1d WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 day', ts) AS bucket, device_id, metric_id,
               avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value,
               sum(value) AS sum_value, count(*) AS cnt
        FROM telemetry_narrow GROUP BY 1, 2, 3 WITH NO DATA""",
}
# политики обновления: какое окно пересчитывать и как часто (для «живой» системы)
CAGG_POLICY = {"cagg_1m": ("2 hours", "1 minute", "1 minute"), "cagg_1h": ("3 days", "1 hour", "30 minutes"),
               "cagg_1d": ("7 days", "1 day", "1 hour")}


def cagg_size(c, view):
    return mb(one(c, """SELECT hypertable_size(format('%%I.%%I', materialization_hypertable_schema,
                                materialization_hypertable_name)::regclass)
                        FROM timescaledb_information.continuous_aggregates WHERE view_name = %s""", (view,)))


def step_cagg(a):
    res = {}
    with conn() as c:
        m = meta(c)
        end = dt.datetime.fromisoformat(m["end"])
        for v in ("cagg_1d", "cagg_1h", "cagg_1m"):
            q(c, f"DROP MATERIALIZED VIEW IF EXISTS {v} CASCADE")
        for v, sql in CAGG_SQL.items():
            q(c, sql)
            # 1-минутный агрегат держим только за последние 31 день (дальше — 1h и 1d)
            lo = end - dt.timedelta(days=31) if v == "cagg_1m" else None
            t0 = time.perf_counter()
            lit = f"'{lo.isoformat()}'::timestamptz" if lo else "NULL"
            q(c, f"CALL refresh_continuous_aggregate('{v}', {lit}, NULL)")
            sec = time.perf_counter() - t0
            so, eo, sch = CAGG_POLICY[v]
            q(c, "SELECT add_continuous_aggregate_policy(%s, start_offset => %s::interval, end_offset => %s::interval, "
                 "schedule_interval => %s::interval)", (v, so, eo, sch))
            rows = one(c, f"SELECT count(*) FROM {v}")
            res[v] = {"refresh_seconds": round(sec, 1), "rows": rows, "size_mb": cagg_size(c, v)}
            log(f"  {v}: обновление {sec:.1f} с, строк {rows:,}, размер {res[v]['size_mb']} МБ")
        q(c, "SELECT add_retention_policy('cagg_1m', drop_after => INTERVAL '31 days', if_not_exists => true)")
        res["bench"] = bench(c, "cagg", "cagg", a.reps)
    save_res("cagg", res)


# ------------------------------------------------------------------ 5. компрессия
def compression_stats(c, t):
    r = q(c, """SELECT total_chunks, number_compressed_chunks, before_compression_table_bytes, before_compression_index_bytes,
                       before_compression_toast_bytes, before_compression_total_bytes, after_compression_table_bytes,
                       after_compression_index_bytes, after_compression_toast_bytes, after_compression_total_bytes
                FROM hypertable_compression_stats(%s)""", (t,))[0]
    keys = ["chunks", "compressed_chunks", "before_table", "before_index", "before_toast", "before_total",
            "after_table", "after_index", "after_toast", "after_total"]
    d = dict(zip(keys, r))
    for k in keys[2:]:
        d[k] = mb(d[k])
    d["ratio"] = round(d["before_total"] / d["after_total"], 2) if d["after_total"] else None
    return d


def compress(c, t, segmentby, orderby):
    q(c, f"ALTER TABLE {t} SET (timescaledb.compress, timescaledb.compress_segmentby = '{segmentby}', "
         f"timescaledb.compress_orderby = '{orderby}')")
    t0 = time.perf_counter()
    q(c, f"SELECT count(compress_chunk(ch, if_not_compressed => true)) FROM show_chunks('{t}') ch")
    sec = time.perf_counter() - t0
    q(c, f"ANALYZE {t}")
    s = compression_stats(c, t)
    s.update(seconds=round(sec, 1), segmentby=segmentby, orderby=orderby,
             hypertable_size_mb=mb(one(c, "SELECT hypertable_size(%s)", (t,))))
    log(f"  {t} [segmentby={segmentby}, orderby={orderby}]: {s['before_total']} МБ → {s['after_total']} МБ, "
        f"коэффициент {s['ratio']}×, сжатие {sec:.0f} с")
    return s


def step_compress(a):
    with conn() as c:
        res = {t: compress(c, t, "device_id", "ts DESC") for t in TABLES}
        res["chunks_comp"] = {t: [[f"{s}.{n}", b, af] for s, n, b, af in q(c, """
            SELECT chunk_schema, chunk_name, before_compression_total_bytes, after_compression_total_bytes
            FROM chunk_compression_stats(%s) ORDER BY chunk_name""", (t,))] for t in TABLES}
        q(c, "SELECT add_compression_policy(%s, compress_after => INTERVAL '7 days', if_not_exists => true)", ("telemetry_wide",))
        q(c, "SELECT add_compression_policy(%s, compress_after => INTERVAL '7 days', if_not_exists => true)", ("telemetry_narrow",))
    save_res("compress", res)


def step_bench_comp(a):
    with conn() as c:
        save_res("bench_comp", {"wide": bench(c, "wide_comp", "wide", a.reps), "narrow": bench(c, "narrow_comp", "narrow", a.reps)})


def step_seg2(a):
    """Вариант узкой схемы: segmentby = device_id, metric_id — в сегменте одна метрика одного устройства."""
    t = "telemetry_narrow"
    with conn() as c:
        t0 = time.perf_counter()
        q(c, f"SELECT count(decompress_chunk(ch, if_compressed => true)) FROM show_chunks('{t}') ch")
        dec = time.perf_counter() - t0
        log(f"  распаковка {t}: {dec:.0f} с")
        s = compress(c, t, "device_id, metric_id", "ts DESC")
        s["decompress_seconds"] = round(dec, 1)
        s["bench"] = bench(c, "narrow_comp2", "narrow", a.reps)
        s["chunks_comp"] = [[f"{sc}.{n}", b, af] for sc, n, b, af in q(c, """
            SELECT chunk_schema, chunk_name, before_compression_total_bytes, after_compression_total_bytes
            FROM chunk_compression_stats(%s) ORDER BY chunk_name""", (t,))]
    save_res("seg2", s)


# ------------------------------------------------------------------ 6. холодный слой: Parquet в MinIO
def step_cold(a):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from minio import Minio

    mc = Minio(os.getenv("MINIO_ENDPOINT", "minio:9000"), access_key=os.getenv("MINIO_USER", "minioadmin"),
               secret_key=os.getenv("MINIO_PASSWORD", "minioadmin123"), secure=False)
    bucket = "cold-tier"
    if not mc.bucket_exists(bucket):
        mc.make_bucket(bucket)
    r = load_res()
    out = {"bucket": bucket, "tables": {}}
    with conn() as c:
        end = dt.datetime.fromisoformat(meta(c)["end"])
        cutoff = end - dt.timedelta(days=a.retention_days)
        for t in TABLES:
            ch = [x for x in r["load"]["chunks_raw"][t] if dt.datetime.fromisoformat(str(x[2])) <= cutoff]
            if not ch:
                log(f"  {t}: нет чанков старше {a.retention_days} дней")
                continue
            lo, hi = ch[0][1], ch[-1][2]
            names = {x[0] for x in ch}
            raw_bytes = sum(x[3] for x in ch)
            comp = r.get("seg2", {}).get("chunks_comp") if t == "telemetry_narrow" and "seg2" in r else \
                r.get("compress", {}).get("chunks_comp", {}).get(t, [])
            comp_bytes = sum(x[2] for x in comp if x[0] in names) if comp else None
            if t == "telemetry_narrow":
                sql = f"COPY (SELECT ts, device_id, metric_id, value FROM {t} WHERE ts >= '{lo}' AND ts < '{hi}' ORDER BY device_id, metric_id, ts) TO STDOUT WITH CSV HEADER"
            else:
                sql = (f"COPY (SELECT ts, device_id, (payload->>'temperature')::float8 AS temperature, "
                       f"(payload->>'vibration_rms')::float8 AS vibration_rms, (payload->>'current')::float8 AS current, "
                       f"(payload->>'rpm')::float8 AS rpm FROM {t} WHERE ts >= '{lo}' AND ts < '{hi}' ORDER BY device_id, ts) "
                       f"TO STDOUT WITH CSV HEADER")
            buf = io.BytesIO()
            t0 = time.perf_counter()
            with c.cursor() as cur, cur.copy(sql) as cp:
                for data in cp:
                    buf.write(data)
            csv_bytes = buf.tell()
            buf.seek(0)
            df = pd.read_csv(buf, parse_dates=["ts"])
            for col, typ in (("device_id", "int32"), ("metric_id", "int16")):
                if col in df:
                    df[col] = df[col].astype(typ)
            tbl = pa.Table.from_pandas(df, preserve_index=False)
            files, pq_bytes, sizes_codec = [], 0, {}
            for codec in ("snappy", "zstd"):
                b = io.BytesIO()
                pq.write_table(tbl, b, compression=codec)
                sizes_codec[codec] = b.tell()
            # выгрузка: по месяцам, zstd
            df["_m"] = df["ts"].dt.strftime("year=%Y/month=%m")
            for part, g in df.groupby("_m"):
                b = io.BytesIO()
                pq.write_table(pa.Table.from_pandas(g.drop(columns="_m"), preserve_index=False), b, compression="zstd")
                size = b.tell()
                b.seek(0)
                key = f"{t}/{part}/part-0.parquet"
                mc.put_object(bucket, key, b, size, content_type="application/vnd.apache.parquet")
                files.append([key, len(g), size])
                pq_bytes += size
            sec = time.perf_counter() - t0
            # проверка: читаем первый объект обратно
            obj = mc.get_object(bucket, files[0][0])
            back = pq.read_table(io.BytesIO(obj.read()))
            obj.close()
            rows = len(df)
            out["tables"][t] = {"from": lo, "to": hi, "chunks": len(ch), "rows": rows,
                                "pg_raw_mb": mb(raw_bytes), "pg_compressed_mb": mb(comp_bytes), "csv_mb": mb(csv_bytes),
                                "parquet_snappy_mb": mb(sizes_codec["snappy"]), "parquet_zstd_mb": mb(sizes_codec["zstd"]),
                                "uploaded_mb": mb(pq_bytes), "files": files, "seconds": round(sec, 1),
                                "readback_rows": back.num_rows, "readback_ok": back.num_rows == files[0][1]}
            log(f"  {t}: {len(ch)} чанков ({lo} … {hi}), {rows:,} строк: PostgreSQL {mb(raw_bytes)} МБ без сжатия / "
                f"{mb(comp_bytes)} МБ сжато, CSV {mb(csv_bytes)} МБ, Parquet snappy {mb(sizes_codec['snappy'])} МБ, "
                f"zstd {mb(sizes_codec['zstd'])} МБ → MinIO s3://{bucket}/{t}/ ({len(files)} файл.); первый файл прочитан "
                f"обратно: {back.num_rows} строк{' ✔' if back.num_rows == files[0][1] else ' ✘'}")
    save_res("cold", out)


# ------------------------------------------------------------------ 7. retention
def step_retention(a):
    res = {}
    with conn() as c:
        for t in TABLES:
            before = {"chunks": one(c, "SELECT count(*) FROM show_chunks(%s)", (t,)),
                      "min_ts": one(c, f"SELECT min(ts) FROM {t}"),
                      "size_mb": mb(one(c, "SELECT hypertable_size(%s)", (t,)))}
            job = one(c, "SELECT add_retention_policy(%s, drop_after => %s::interval, if_not_exists => true)",
                      (t, f"{a.retention_days} days"))
            if job is None:
                job = one(c, "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention' "
                             "AND hypertable_name = %s", (t,))
            t0 = time.perf_counter()
            q(c, "CALL run_job(%s)", (job,))
            sec = time.perf_counter() - t0
            after = {"chunks": one(c, "SELECT count(*) FROM show_chunks(%s)", (t,)),
                     "min_ts": one(c, f"SELECT min(ts) FROM {t}"),
                     "size_mb": mb(one(c, "SELECT hypertable_size(%s)", (t,)))}
            res[t] = {"job_id": job, "drop_after_days": a.retention_days, "before": before, "after": after,
                      "seconds": round(sec, 2)}
            log(f"  {t}: retention {a.retention_days} дн. (job {job}): чанков {before['chunks']} → {after['chunks']}, "
                f"самая старая запись {before['min_ts']:%Y-%m-%d} → {after['min_ts']:%Y-%m-%d}, "
                f"размер {before['size_mb']} → {after['size_mb']} МБ, {sec * 1000:.0f} мс")
        res["cagg_1d_min_bucket"] = one(c, "SELECT min(bucket) FROM cagg_1d")
        res["cagg_1d_rows"] = one(c, "SELECT count(*) FROM cagg_1d")
        log(f"  cagg_1d после retention по-прежнему с {res['cagg_1d_min_bucket']:%Y-%m-%d} "
            f"({res['cagg_1d_rows']} строк) — агрегаты переживают удаление сырых данных")
        res["jobs"] = [list(r) for r in q(c, """SELECT job_id, proc_name, schedule_interval::text,
                         coalesce(hypertable_name, '') FROM timescaledb_information.jobs WHERE job_id >= 1000 ORDER BY 1""")]
    save_res("retention", res)


STEPS = ["schema", "load", "bench_raw", "cagg", "compress", "bench_comp", "seg2", "cold", "retention"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("steps", nargs="*", default=STEPS)
    ap.add_argument("--days", type=int, default=400)
    ap.add_argument("--devices", type=int, default=10)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--retention-days", type=int, default=365)
    a = ap.parse_args()
    for _ in range(30):
        try:
            conn().close()
            break
        except psycopg.OperationalError:
            log("жду PostgreSQL…")
            time.sleep(2)
    t0 = time.time()
    for s in a.steps:
        log(f"===== {s} =====")
        globals()[f"step_{s}"](a)
    log(f"готово за {(time.time() - t0) / 60:.1f} мин. Графики и сводка:  python plots.py")


if __name__ == "__main__":
    main()
