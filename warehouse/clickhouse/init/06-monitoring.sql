-- Local development credentials; both users are read-only. Override for a shared deployment.
CREATE USER IF NOT EXISTS grafana_reader IDENTIFIED WITH sha256_password BY 'grafana_local_reader';
GRANT SELECT ON mart.* TO grafana_reader;
GRANT SELECT ON ops.mart_refreshes TO grafana_reader;
ALTER USER grafana_reader SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY;
CREATE USER IF NOT EXISTS monitor_reader IDENTIFIED WITH sha256_password BY 'monitor_local_reader';
GRANT SELECT ON system.* TO monitor_reader;
GRANT SELECT ON bronze.* TO monitor_reader;
GRANT SELECT ON silver.* TO monitor_reader;
GRANT SELECT ON consistent.* TO monitor_reader;
GRANT SELECT ON ops.* TO monitor_reader;
ALTER USER monitor_reader SETTINGS readonly = 1;
