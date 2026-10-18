-- Bronze is never deleted (R13/R19). Parts move to the cold volume (S3 on SeaweedFS, config.d/storage.xml) once
-- every row in them is 30 wall-clock days old; queries read both volumes transparently. Idempotent, so
-- observability/start.sh also applies it to an existing volume.
ALTER TABLE bronze.cdc_events MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE bronze.cdc_events MODIFY TTL toDateTime(event_ts) + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE bronze.domain_events MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE bronze.domain_events MODIFY TTL toDateTime(ingested_at) + INTERVAL 30 DAY TO VOLUME 'cold';
ALTER TABLE bronze.cdc_transactions MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE bronze.cdc_transactions MODIFY TTL toDateTime(ingested_at) + INTERVAL 30 DAY TO VOLUME 'cold';
