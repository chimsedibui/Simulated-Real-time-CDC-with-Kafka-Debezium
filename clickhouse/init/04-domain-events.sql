-- BRONZE: domain events from the outbox (Debezium Outbox Event Router → events.<aggregate>)

CREATE TABLE IF NOT EXISTS bronze.kafka_domain_events
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = '^events\\..*',      -- regex subscription: every events.* topic, present and future
    kafka_group_name = 'clickhouse-events',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100,
    kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS bronze.domain_events
(
    event_id        UUID,
    event_type      LowCardinality(String),
    aggregate_type  LowCardinality(String),
    aggregate_id    String,
    payload         String,                   -- raw JSON
    occurred_at     DateTime64(6, 'UTC'),     -- commit time of the business transaction
    kafka_topic     LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (aggregate_type, aggregate_id, occurred_at, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_domain_events TO bronze.domain_events AS
SELECT
    toUUID(JSONExtractString(raw, 'eventId'))                                AS event_id,
    JSONExtractString(raw, 'eventType')                                      AS event_type,
    JSONExtractString(raw, 'aggregateType')                                  AS aggregate_type,
    JSONExtractString(raw, 'aggregateId')                                    AS aggregate_id,
    JSONExtractRaw(raw, 'payload')                                           AS payload,
    parseDateTime64BestEffort(JSONExtractString(raw, 'occurredAt'), 6, 'UTC') AS occurred_at,
    _topic                                                                   AS kafka_topic,
    _partition                                                               AS kafka_partition,
    _offset                                                                  AS kafka_offset,
    now64(3)                                                                 AS ingested_at
FROM bronze.kafka_domain_events
WHERE JSONHas(raw, 'eventId');
