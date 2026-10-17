-- BRONZE: raw Debezium events, append-only, one row per Kafka message
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- Kafka consumer: each message is kept as one raw JSON string
CREATE TABLE IF NOT EXISTS bronze.kafka_cdc_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = '^cdc\\.ecommerce\\..*',   -- regex: every table topic, including ones added later
    kafka_group_name = 'clickhouse-cdc',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100,
    kafka_flush_interval_ms = 1000;

-- Persistent changelog of every CDC event
CREATE TABLE IF NOT EXISTS bronze.cdc_events
(
    source_table    LowCardinality(String),
    op              LowCardinality(String),   -- c / u / d / r
    lsn             UInt64,                   -- Postgres WAL position, used as row version downstream
    source_ts       DateTime64(3, 'UTC'),     -- when the change was committed in Postgres
    event_ts        DateTime64(3, 'UTC'),     -- when Debezium processed it
    before          String,                   -- raw JSON ('null' when absent)
    after           String,                   -- raw JSON ('null' when absent)
    kafka_topic     LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (source_table, event_ts, kafka_partition, kafka_offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_cdc_events TO bronze.cdc_events AS
SELECT
    JSONExtractString(raw, 'source', 'table')                     AS source_table,
    JSONExtractString(raw, 'op')                                  AS op,
    JSONExtractUInt(raw, 'source', 'lsn')                         AS lsn,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'source', 'ts_ms'), 'UTC') AS source_ts,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'ts_ms'), 'UTC') AS event_ts,
    JSONExtractRaw(raw, 'before')                                 AS before,
    JSONExtractRaw(raw, 'after')                                  AS after,
    _topic                                                        AS kafka_topic,
    _partition                                                    AS kafka_partition,
    _offset                                                       AS kafka_offset,
    now64(3)                                                      AS ingested_at
FROM bronze.kafka_cdc_events
WHERE JSONHas(raw, 'op');
