-- Lab 6 — схема хранения телеметрии в TimescaleDB
-- справочники
CREATE TABLE IF NOT EXISTS metrics (
    metric_id smallint PRIMARY KEY,
    name      text UNIQUE NOT NULL,
    unit      text NOT NULL
);
INSERT INTO metrics VALUES
    (1, 'temperature',   '°C'),
    (2, 'vibration_rms', 'мм/с'),
    (3, 'current',       'А'),
    (4, 'rpm',           'об/мин')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS devices (
    device_id int PRIMARY KEY,
    name      text NOT NULL,
    model     text NOT NULL,
    site      text NOT NULL,
    power_kw  real NOT NULL
);

CREATE TABLE IF NOT EXISTS lab_meta (key text PRIMARY KEY, value text NOT NULL);

-- 1) ШИРОКАЯ схема: одна строка = один замер устройства, все метрики в JSONB
CREATE TABLE IF NOT EXISTS telemetry_wide (
    ts        timestamptz NOT NULL,
    device_id int         NOT NULL,
    payload   jsonb       NOT NULL
);
SELECT create_hypertable('telemetry_wide', by_range('ts', INTERVAL '7 days'), if_not_exists => true);
CREATE INDEX IF NOT EXISTS telemetry_wide_dev_ts ON telemetry_wide (device_id, ts DESC);

-- 2) УЗКАЯ схема: одна строка = одно значение одной метрики
CREATE TABLE IF NOT EXISTS telemetry_narrow (
    ts        timestamptz      NOT NULL,
    device_id int              NOT NULL,
    metric_id smallint         NOT NULL,
    value     double precision NOT NULL
);
SELECT create_hypertable('telemetry_narrow', by_range('ts', INTERVAL '7 days'), if_not_exists => true);
CREATE INDEX IF NOT EXISTS telemetry_narrow_dev_metric_ts ON telemetry_narrow (device_id, metric_id, ts DESC);
