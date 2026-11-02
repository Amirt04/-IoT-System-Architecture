-- выполняется один раз при первом запуске контейнера timescaledb (БД iot)
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- учётная запись Grafana только на чтение
CREATE ROLE grafana LOGIN PASSWORD 'grafana';
GRANT pg_read_all_data TO grafana;
