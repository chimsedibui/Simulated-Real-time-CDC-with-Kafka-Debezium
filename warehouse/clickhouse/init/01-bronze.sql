-- BRONZE: raw Debezium events, append-only, one row per Kafka message
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS consistent;
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
    kafka_handle_error_mode = 'stream',            -- unparsable messages go to bronze.ingest_errors, never dropped
    kafka_flush_interval_ms = 1000;

-- Persistent changelog of every CDC event
CREATE TABLE IF NOT EXISTS bronze.cdc_events
(
    source_table    LowCardinality(String),
    op              LowCardinality(String),   -- c / u / d / r
    lsn             UInt64,                   -- Postgres WAL position, used as row version downstream
    tx_id           UInt64,                   -- Postgres transaction id (0 for snapshot reads)
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
    if(op = 'r', 0, JSONExtractUInt(raw, 'source', 'txId'))       AS tx_id,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'source', 'ts_ms'), 'UTC') AS source_ts,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'ts_ms'), 'UTC') AS event_ts,
    JSONExtractRaw(raw, 'before')                                 AS before,
    JSONExtractRaw(raw, 'after')                                  AS after,
    _topic                                                        AS kafka_topic,
    _partition                                                    AS kafka_partition,
    _offset                                                       AS kafka_offset,
    now64(3)                                                      AS ingested_at
FROM bronze.kafka_cdc_events
WHERE _error = '' AND JSONHas(raw, 'op');

-- Messages ClickHouse could not read, from every Kafka table: kept for replay, checked by validation
CREATE TABLE IF NOT EXISTS bronze.ingest_errors
(
    kafka_topic     LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    error           String,
    raw_message     String,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (kafka_topic, kafka_partition, kafka_offset);

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_cdc_events_errors TO bronze.ingest_errors AS
SELECT _topic AS kafka_topic, _partition AS kafka_partition, _offset AS kafka_offset, _error AS error,
       _raw_message AS raw_message, now64(3) AS ingested_at
FROM bronze.kafka_cdc_events
WHERE _error != '';

-- Debezium transaction metadata (provide.transaction.metadata): one END record per committed transaction with
-- its event count per table, in commit order on a single-partition topic. Lets ClickHouse tell when every change
-- of a transaction has landed, so readers never see half of one (see consistent.open_transactions).
CREATE TABLE IF NOT EXISTS bronze.kafka_cdc_transactions
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'cdc.transaction',
    kafka_group_name = 'clickhouse-cdc-transactions',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS bronze.cdc_transactions
(
    tx_id           UInt64,
    event_count     UInt32,                   -- all change events of the transaction
    expected        UInt32,                   -- those that land in bronze.cdc_events (outbox rows go to events.*)
    outbox_changes  UInt32,                   -- outbox insert + delete per domain event
    commit_ts       DateTime64(3, 'UTC'),
    kafka_offset    UInt64,                   -- commit order (the topic has one partition)
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, tx_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_cdc_transactions TO bronze.cdc_transactions AS
SELECT
    toUInt64OrZero(splitByChar(':', JSONExtractString(raw, 'id'))[1])     AS tx_id,
    JSONExtractUInt(raw, 'event_count')                                    AS event_count,
    event_count - arraySum(arrayMap(c -> if(JSONExtractString(c, 'data_collection') = 'ecommerce.outbox_events',
                                            JSONExtractUInt(c, 'event_count'), 0),
                                    JSONExtractArrayRaw(raw, 'data_collections'))) AS expected,
    event_count - expected                                                 AS outbox_changes,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'ts_ms'), 'UTC')          AS commit_ts,
    _offset                                                                AS kafka_offset,
    now64(3)                                                               AS ingested_at
FROM bronze.kafka_cdc_transactions
WHERE _error = '' AND JSONExtractString(raw, 'status') = 'END';

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_cdc_transactions_errors TO bronze.ingest_errors AS
SELECT _topic AS kafka_topic, _partition AS kafka_partition, _offset AS kafka_offset, _error AS error,
       _raw_message AS raw_message, now64(3) AS ingested_at
FROM bronze.kafka_cdc_transactions
WHERE _error != '';
