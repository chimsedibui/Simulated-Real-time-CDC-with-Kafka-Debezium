#!/usr/bin/env bash
# ClickHouse backup and restore with ClickHouse's own BACKUP/RESTORE, into warehouse/backups/ on the host (survives
# `docker compose down -v`). Run from anywhere, with the stack up.
#
#   warehouse/backup.sh backup              full backup of every warehouse database
#   warehouse/backup.sh backup --incremental only the parts changed since the latest full backup
#   warehouse/backup.sh list
#   warehouse/backup.sh restore <name>      replace the warehouse with a backup, then catch up from Kafka
#   warehouse/backup.sh restore <name> --force  offline restore, no Kafka consumers/jobs
#   warehouse/backup.sh prune <n>           keep the newest n full backups (and their incrementals)
#
# Consistency: during a backup the Kafka engine tables are detached and warehouse-jobs is stopped (seconds to a
# minute), so every table is captured at the same Kafka position. Kafka keeps the new events meanwhile.
# Replay offsets are computed AT BACKUP TIME, right after BACKUP and while the Kafka tables are still detached: for
# each partition, the first offset not in bronze (or the earliest offset Kafka still holds, if bronze has no row for
# it). They are saved in <name>.offsets, the topic IDs in <name>.topics.
# Restore: reads <name>.offsets (nothing is recomputed from the restored bronze), checks the topic IDs and that Kafka
# still holds those offsets (retention 7 days), restores the databases, moves each ClickHouse consumer group to the
# saved offsets and recreates the Kafka tables, so everything after the backup is read again from Kafka. If a check
# fails, the restore stops before replacing any database.
set -euo pipefail
cd "$(dirname "$0")/.."

# Serialize local backup/restore/prune operations, including across different working directories.
exec 9>/tmp/realtime-cdc-backup.lock
flock -n 9 || { echo "another warehouse backup operation is running" >&2; exit 1; }
DC="docker compose"
INIT=warehouse/clickhouse/init
ch() { $DC exec -T clickhouse bash -c 'clickhouse-client --user admin --password "$(cat /run/secrets/clickhouse_password)" --query "$0"' "$1"; }
in_ch() { $DC exec -T clickhouse bash -c "$1"; }
kafka() { $DC exec -T kafka "$@"; }
# Kafka's identity: the id of the transaction topic changes whenever Kafka's data is reset (the cluster id does not,
# it is fixed in kafka/compose.yml)
kafka_id() { kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic cdc.transaction 2>/dev/null | sed -n 's/.*TopicId: \([^[:space:]]*\).*/\1/p' | head -1; }
topic_ids() {
  kafka kafka-topics --bootstrap-server kafka:9092 --describe 2>/dev/null |
    sed -n 's/^Topic: \([^[:space:]]*\).*TopicId: \([^[:space:]]*\).*/\1,\2/p' |
    awk -F, '$1 ~ /^(cdc\.|events\.)/' | sort
}
valid_name() { [[ "$1" =~ ^ch-[0-9]{8}-[0-9]{6}(-incr)?$ ]] || die "invalid backup name"; }
die() { echo "error: $*" >&2; exit 1; }

# consumer group of every Kafka engine table, and the topics it reads (as a regex)
CONSUMERS=("clickhouse-cdc|^cdc\.ecommerce\." "clickhouse-cdc-transactions|^cdc\.transaction$" "clickhouse-events|^events\.")

kafka_tables() { ch "SELECT database || '.' || name FROM system.tables WHERE engine = 'Kafka'"; }
kafka_views() {   # the materialized views that read a Kafka table: recreated from the init SQL, not backed up
  ch "SELECT arrayJoin(arrayMap((d, t) -> d || '.' || t, dependencies_database, dependencies_table))
      FROM system.tables WHERE engine = 'Kafka'"
}
databases() { ch "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', 'default') ORDER BY name"; }

PAUSED=()
JOBS_RUNNING=0
pause() {
  if [[ -n "$($DC ps --status running -q warehouse-jobs)" ]]; then
    JOBS_RUNNING=1
    $DC stop warehouse-jobs >/dev/null 2>&1
  fi
  mapfile -t PAUSED < <(kafka_tables)
  for t in "${PAUSED[@]}"; do ch "DETACH TABLE $t"; done
}
resume() {
  local failed=0
  for t in "${PAUSED[@]}"; do
    ch "ATTACH TABLE $t" || { echo "error: could not re-attach $t" >&2; failed=1; }
  done
  PAUSED=()
  if (( JOBS_RUNNING )); then $DC start warehouse-jobs >/dev/null 2>&1; fi
  return "$failed"
}

latest_full() { in_ch 'cd /backups && grep -l "\"type\": \"full\"" *.json 2>/dev/null | sort | tail -1 | sed "s/\.json$//"' || true; }

backup() {
  [[ "${1:-}" == "" || "${1:-}" == "--incremental" ]] || die "unknown backup option"
  local incremental=${1:-} name base="" settings="" views db list=() t0
  name="ch-$(date -u +%Y%m%d-%H%M%S)"
  if [[ "$incremental" == "--incremental" ]]; then
    base=$(latest_full); [[ -n "$base" ]] || die "no full backup to base an incremental on"
    name="$name-incr"; settings="SETTINGS base_backup = Disk('backups', '$base')"
  fi
  mapfile -t views < <(kafka_views)
  local tail=()   # a database with EXCEPT TABLES goes last: the comma after its table list would be ambiguous
  for db in $(databases); do
    local except=() v
    for v in "${views[@]}"; do [[ "$v" == "$db".* ]] && except+=("$v"); done
    if (( ${#except[@]} )); then tail+=("DATABASE $db EXCEPT TABLES $(IFS=,; echo "${except[*]}")")
    else list+=("DATABASE $db"); fi
  done
  (( ${#tail[@]} <= 1 )) || die "Kafka tables in more than one database: ${tail[*]}"
  list+=("${tail[@]}")
  t0=$(date +%s)
  trap resume EXIT
  pause
  ch "BACKUP $(IFS=,; echo "${list[*]}") TO Disk('backups', '$name') $settings" >/dev/null
  # Save the actual replay position BEFORE resuming ingestion. For partitions with no bronze row,
  # preserve the earliest offset now; retention advancing later must cause restore to fail.
  local positions earliest
  positions=$(next_offsets)
  earliest=$(kafka kafka-get-offsets --bootstrap-server kafka:9092 --time -2 | tr ':' ',')
  awk -F, 'NR==FNR {p[$1 FS $2]=$3; next}
    $1 ~ /^(cdc\.ecommerce\.|cdc\.transaction$|events\.)/ {
      k=$1 FS $2; print $1 FS $2 FS (k in p ? p[k] : $3)
    }' <(printf '%s\n' "$positions") <(printf '%s\n' "$earliest") |
    in_ch "cat > /backups/$name.offsets"
  topic_ids | in_ch "cat > /backups/$name.topics"
  resume
  trap - EXIT
  in_ch "cat > /backups/$name.json" <<JSON
{"name": "$name", "type": "$([[ -n "$base" ]] && echo incremental || echo full)", "base": "$base",
 "created_at": "$(date -u +%FT%TZ)", "kafka_id": "$(kafka_id)",
 "databases": [$(databases | sed 's/.*/"&"/' | paste -sd,)]}
JSON
  echo "backup $name done in $(( $(date +%s) - t0 ))s ($(in_ch "du -sh /backups/$name | cut -f1"))${base:+, based on $base}"
}

list() {
  in_ch 'cd /backups 2>/dev/null && for f in $(ls *.json 2>/dev/null | sort); do
           n=${f%.json}; printf "%-28s %-12s %-8s %s\n" "$n" \
             "$(grep -o "\"type\": \"[a-z]*\"" $f | cut -d\" -f4)" "$(du -sh $n | cut -f1)" \
             "$(grep -o "\"base\": \"[^\"]*\"" $f | cut -d\" -f4)"
         done' || true
}

next_offsets() {   # topic,partition,first offset not in the restored bronze
  ch "SELECT kafka_topic, kafka_partition, max(kafka_offset) + 1 FROM (
        SELECT kafka_topic, kafka_partition, kafka_offset FROM bronze.cdc_events
        UNION ALL SELECT 'cdc.transaction', 0, kafka_offset FROM bronze.cdc_transactions
        UNION ALL SELECT kafka_topic, kafka_partition, kafka_offset FROM bronze.domain_events
        UNION ALL SELECT kafka_topic, kafka_partition, kafka_offset FROM bronze.ingest_errors)
      GROUP BY kafka_topic, kafka_partition FORMAT CSV" | tr -d '"'
}

restore() {
  local name=${1:-} force=${2:-} manifest dbs restored=() earliest latest next group regex plan
  [[ -n "$name" ]] || die "usage: $0 restore <name>"
  valid_name "$name"
  [[ "$force" == "" || "$force" == "--force" ]] || die "unknown restore option"
  manifest=$(in_ch "cat /backups/$name.json" 2>/dev/null) || die "no backup named $name (see: $0 list)"
  local backup_kafka; backup_kafka=$(sed -n 's/.*"kafka_id": "\([^"]*\)".*/\1/p' <<<"$manifest")
  if [[ "$force" != "--force" ]] && [[ -z "$backup_kafka" || "$backup_kafka" != "$(kafka_id)" ]]; then
    die "backup was taken against another Kafka (transaction topic $backup_kafka, now $(kafka_id)): data was reset.
       Restoring would mix two histories; pass --force to restore the backup as it is, without catching up"
  fi
  mapfile -t dbs < <(grep -o '"databases": \[[^]]*\]' <<<"$manifest" | grep -o '"[a-z_]*"' | tr -d '"' | grep -v databases)

  # Validate all topics and replay offsets BEFORE replacing any database.
  # --force is an OFFLINE restore: do not attach consumers or run jobs against another history.
  if [[ "$force" != "--force" ]]; then
    next=$(in_ch "cat /backups/$name.offsets" 2>/dev/null) || die "legacy backup has no saved offsets; use --force for offline restore"
    local saved_topics current_topics
    saved_topics=$(in_ch "cat /backups/$name.topics" 2>/dev/null) || die "backup has no topic identities"
    current_topics=$(topic_ids)
    [[ -n "$next" && -n "$saved_topics" ]] || die "empty backup replay metadata"
    while IFS=, read -r topic id; do
      grep -Fxq "$topic,$id" <<<"$current_topics" || die "topic $topic was deleted or recreated"
    done <<<"$saved_topics"
    earliest=$(kafka kafka-get-offsets --bootstrap-server kafka:9092 --time -2 | tr ':' ',')
    latest=$(kafka kafka-get-offsets --bootstrap-server kafka:9092 --time -1 | tr ':' ',')
    while IFS=, read -r topic partition want; do
      local first end
      first=$(awk -F, -v t="$topic" -v p="$partition" '$1==t && $2==p {print $3}' <<<"$earliest")
      end=$(awk -F, -v t="$topic" -v p="$partition" '$1==t && $2==p {print $3}' <<<"$latest")
      [[ "$first" =~ ^[0-9]+$ && "$end" =~ ^[0-9]+$ && "$want" =~ ^[0-9]+$ ]] || die "missing partition $topic:$partition"
      (( want >= first && want <= end )) || die "$topic:$partition needs $want, retained range is $first..$end; warehouse kept intact"
    done <<<"$next"
    # A topic or partition created AFTER the backup must also be replayed from its beginning.
    while IFS=, read -r topic partition first; do
      [[ "$topic" =~ ^(cdc\.ecommerce\.|cdc\.transaction$|events\.) ]] || continue
      if ! awk -F, -v t="$topic" -v p="$partition" '$1==t && $2==p {found=1} END {exit !found}' <<<"$next"; then
        (( first == 0 )) || die "new partition $topic:$partition has already lost events to retention"
        next+=$'\n'"$topic,$partition,0"
      fi
    done <<<"$earliest"
  fi

  in_ch "test -s /backups/$name/.backup" || die "backup data is missing; warehouse kept intact"
  (( ${#dbs[@]} )) || die "manifest contains no databases"
  echo "stopping warehouse-jobs and dropping ${dbs[*]}"
  $DC stop warehouse-jobs >/dev/null
  for t in $(kafka_tables); do ch "DETACH TABLE $t"; done
  for db in "${dbs[@]}"; do ch "DROP DATABASE IF EXISTS $db SYNC"; restored+=("DATABASE $db"); done
  echo "restoring $name"
  ch "RESTORE $(IFS=,; echo "${restored[*]}") FROM Disk('backups', '$name')" >/dev/null
  if [[ "$force" == "--force" ]]; then
    echo "offline restore complete; Kafka tables remain absent and warehouse-jobs stays stopped"
    return
  fi
  for entry in "${CONSUMERS[@]}"; do
    group=${entry%%|*}; regex=${entry#*|}; plan=""
    while IFS=, read -r topic partition want; do
      [[ "$topic" =~ $regex ]] && plan+="$topic,$partition,$want"$'\n'
    done <<<"$next"
    [[ -n "$plan" ]] || continue
    printf '%s' "$plan" | $DC exec -T kafka bash -c "cat > /tmp/$group.csv"
    local empty=0
    for _ in $(seq 1 30); do
      if kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group "$group" --state 2>/dev/null |
          awk 'NF && $NF ~ /^[0-9]+$/ && $NF == 0 {empty=1} END {exit !empty}'; then
        empty=1; break
      fi
      sleep 2
    done
    (( empty )) || die "consumer group $group is still active; restored warehouse stays offline"
    kafka kafka-consumer-groups --bootstrap-server kafka:9092 --group "$group" --reset-offsets \
      --from-file "/tmp/$group.csv" --execute >/dev/null
    echo "  $group: $(printf '%s' "$plan" | wc -l) partitions moved to saved replay positions"
  done

  for f in $(grep -l "ENGINE = Kafka" $INIT/*.sql); do   # Kafka tables and their views, from the init SQL
    $DC exec -T clickhouse bash -c 'clickhouse-client --user admin --password "$(cat /run/secrets/clickhouse_password)" --multiquery' < "$f"
  done
  $DC start warehouse-jobs >/dev/null
  echo "restored $name; ClickHouse is now reading the events after the backup from Kafka (watch the lag in Grafana)"
}

prune() {
  local keep=${1:-} fulls drop
  [[ "$keep" =~ ^[1-9][0-9]*$ ]] || die "usage: $0 prune <number of full backups to keep>"
  fulls=$(in_ch 'cd /backups && grep -l "\"type\": \"full\"" *.json 2>/dev/null | sort | sed "s/\.json$//"' || true)
  drop=$(head -n -"$keep" <<<"$fulls")
  [[ -n "$drop" ]] || { echo "nothing to prune"; return; }
  for b in $drop; do
    in_ch "cd /backups && for f in \$(grep -l '\"base\": \"$b\"' *.json); do rm -rf \${f%.json} \$f \${f%.json}.offsets \${f%.json}.topics; echo removed \${f%.json}; done
           rm -rf $b $b.json $b.offsets $b.topics; echo removed $b"
  done
}

case "${1:-}" in
  backup)  backup "${2:-}" ;;
  list)    list ;;
  restore) restore "${2:-}" "${3:-}" ;;
  prune)   prune "${2:-}" ;;
  *) sed -n '2,18p' "$0"; exit 2 ;;
esac
