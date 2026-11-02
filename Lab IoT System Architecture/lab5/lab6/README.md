# Lab 6 — Time-Series Database (PostgreSQL 16 + TimescaleDB)

```
[lab: генератор/загрузчик, бенчмарки] ──► [PostgreSQL 16 + TimescaleDB 2.17]
                                            │ hypertable, compression, CAGG, retention
                                            ├──► [continuous aggregates: 1m / 1h / 1d]
                                            ├──► [MinIO: cold tier, Parquet]
                                            └──► [Grafana :3000]
```

## Данные
10 двигателей × 1 замер/мин × 400 дней = **5,76 млн замеров** (температура, вибрация СКЗ, ток, обороты):
режим работы по сменам, тепловая инерция, износ подшипника, ~2 эпизода аномальной вибрации в месяц (> 7,1 мм/с).

## Две схемы (`lab/schema.sql`)
| Таблица | Строка | Строк |
|---|---|---|
| `telemetry_wide (ts, device_id, payload JSONB)` | один замер устройства | 5,76 млн |
| `telemetry_narrow (ts, device_id, metric_id, value DOUBLE)` | одно значение метрики | 23,04 млн |

Обе — hypertable с `chunk_time_interval = 7 days`, индекс `(device_id[, metric_id], ts DESC)`.

## Шаги (`lab/run_lab6.py`)
| Шаг | Что делает |
|---|---|
| `schema` | создаёт таблицы, справочники `devices`, `metrics` |
| `load` | генерирует и загружает данные через COPY, меряет время и `hypertable_size` |
| `bench_raw` | 4 типовых запроса × 2 схемы (медиана 5 прогонов + EXPLAIN ANALYZE в `results/plans/`) |
| `cagg` | continuous aggregates `cagg_1m` (31 день), `cagg_1h`, `cagg_1d` + политики обновления; запросы по агрегатам |
| `compress` | `compress_segmentby = device_id`, `compress_orderby = ts DESC`, коэффициент сжатия |
| `bench_comp` | те же запросы на сжатых данных |
| `seg2` | вариант узкой схемы: `segmentby = device_id, metric_id` |
| `cold` | срез старше 365 дней → Parquet (по месяцам, zstd) → MinIO `s3://cold-tier/`, сравнение размеров |
| `retention` | `add_retention_policy(365 days)`, запуск job, проверка удаления чанков |

## Запуск
```
docker compose up -d --build
docker exec lab6-lab python run_lab6.py        # все шаги, ~15–25 мин, ~4 ГБ на диске
docker exec lab6-lab python plots.py           # графики results/plots/*.png + results/summary.md
```
Grafana: http://localhost:3000 (admin / admin) — дашборд «Lab 6 — телеметрия двигателей».
MinIO: http://localhost:9001 (minioadmin / minioadmin123). PostgreSQL с ПК: `localhost:5433`, postgres / postgres, БД `iot`.
