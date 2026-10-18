"""Generate provisioned dashboards: python3 observability/grafana/generate_dashboards.py."""
import json
from pathlib import Path

ROOT = Path(__file__).parent / 'dashboards'

def dashboard(uid, title, description):
    return dict(uid=uid,title=title,description=description,schemaVersion=39,version=1,
                tags=['cdc'],timezone='Asia/Ho_Chi_Minh',refresh='30s',editable=True,
                time={'from':'now-1h','to':'now'},panels=[],templating={'list':[]})

def panel(d, title, datasource, target, kind='timeseries', unit='short', description='', width=12, height=8):
    n=len(d['panels'])
    # All panels use a simple two-column layout.
    p=dict(id=n+1,title=title,type=kind,description=description,
           gridPos=dict(x=(n%2)*12,y=(n//2)*8,w=width,h=height),
           datasource={'type':datasource[0],'uid':datasource[1]},
           targets=[dict(refId='A',**target)],
           fieldConfig={'defaults':{'unit':unit},'overrides':[]},options={})
    if d['uid']=='cdc-business':
        p['gridPos'] = dict(x=(n%4)*6,y=0,w=6,h=4) if n<4 else dict(x=((n-4)%2)*12,y=4+((n-4)//2)*8,w=12,h=8)
    if kind=='table': p['options']={'showHeader':True,'cellHeight':'sm'}
    if kind=='stat':
        p['fieldConfig']['defaults']['color']={'mode':'fixed','fixedColor':'blue'}
        p['options']={'reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False},'colorMode':'value'}
    d['panels'].append(p)

business=dashboard('cdc-business','Marketplace · Business Analytics',
 'Metrics use business time. All panels share the completed mart refresh selected below. Dates are not filtered by the wall-clock picker. NMV changes with returns; these are live metrics, not closed-day snapshots.')
business['templating']['list']=[dict(name='refresh_id',label='Completed refresh',type='query',
 datasource={'type':'grafana-clickhouse-datasource','uid':'clickhouse'},
 query='SELECT toString(refresh_id) FROM ops.mart_refreshes ORDER BY refresh_id DESC LIMIT 1',
 refresh=2,hide=0,multi=False,includeAll=False,options=[],current={})]
ch=('grafana-clickhouse-datasource','clickhouse')
def sql(q): return dict(editorType='sql',rawSql=q,format=1,pluginVersion='4.11.2',queryType='table')
for title,field,unit in [('Confirmed GMV','gmv','currencyVND'),('Net merchandise value','nmv','currencyVND'),('Orders','orders','short'),('Buyers','buyers','short')]:
 panel(business,title,ch,sql(f'SELECT {field} FROM mart.overview_kpis WHERE refresh_id = ${{refresh_id:raw}}'),'stat',unit)
for name,title in [('daily_sales','Daily sales · business dates'),('traffic_by_hour','Orders by business hour'),
 ('order_status_summary','Order pipeline'),('funnel','Purchase funnel · counts have different grains'),
 ('payment_summary','Payment attempts and refunds'),('category_sales','Category sales'),
 ('product_sales','Product sales'),('shop_performance','Shop scorecard'),('inventory_risk','Inventory risk'),
 ('customer_tiers','Customer loyalty'),('platform_pnl','Platform economics'),('delivery_performance','Carrier performance'),
 ('returns_summary','Returns by reason'),('return_rate_by_cohort','Return rate by delivery cohort'),
 ('voucher_performance','Voucher performance'),('flash_sale_performance','Flash-sale performance'),
 ('seller_settlement','Seller settlement')]:
 if name in ('daily_sales', 'traffic_by_hour', 'order_status_summary', 'funnel'):
  dimensions = {'daily_sales':('toString(order_date) AS day, placed_gmv, gmv, nmv','day','currencyVND'),
                'traffic_by_hour':('toString(hour) AS business_hour, orders','business_hour','short'),
                'order_status_summary':('status, orders','status','short'),
                'funnel':('stage, n','stage','short')}
  columns, axis, unit = dimensions[name]
  order = 'order_date' if name == 'daily_sales' else '_row'
  panel(business,title,ch,sql(f'SELECT {columns} FROM mart.{name} WHERE refresh_id = ${{refresh_id:raw}} ORDER BY {order} LIMIT 200'),'barchart',unit)
  business['panels'][-1]['options']={'xField':axis,'orientation':'auto','xTickLabelRotation':-45,'xTickLabelSpacing':100,'showValue':'never','stacking':'none','legend':{'displayMode':'list','placement':'bottom'}}
 else:
  panel(business,title,ch,sql(f'SELECT * EXCEPT (refresh_id, refreshed_at, _row) FROM mart.{name} WHERE refresh_id = ${{refresh_id:raw}} ORDER BY _row LIMIT 200'),'table')
panel(business,'Snapshot publication',ch,sql('SELECT refresh_id, finished_at, duration_ms, marts FROM ops.mart_refreshes WHERE refresh_id = ${refresh_id:raw}'),'table')

platform=dashboard('cdc-platform','CDC · Platform Health',
 'Wall-clock performance. Capacity alerts are investigation signals, not automatic scaling. Exporters expose dependency health separately from scrape health.')
prom=('prometheus','prometheus')
queries=[
 ('Scrape targets','up','short'),
 ('Dependency health','cdc_dependency_up or cdc_service_up','short'),
 ('Host CPU utilization','1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))','percentunit'),
 ('Host memory utilization','1-node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes','percentunit'),
 ('Disk free','node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs"}','bytes'),
 ('Source API requests/s','sum by(handler)(rate(http_requests_total{job="source-api"}[5m]))','reqps'),
 ('Source API p95 latency','histogram_quantile(0.95,sum by(le)(rate(http_request_duration_highr_seconds_bucket{job="source-api"}[5m])))','s'),
 ('Checkout p99 latency · SLO 300 ms','histogram_quantile(0.99,sum by(le)(rate(http_request_duration_seconds_bucket{job="source-api",handler="/checkouts",method="POST"}[5m])))','s'),
 ('Source API 5xx/s','sum(rate(http_requests_total{job="source-api",status=~"5.."}[5m]))','reqps'),
 ('Kafka topic ingress/s','sum by(topic)(rate(kafka_topic_partition_current_offset[5m]))','ops'),
 ('Kafka consumer backlog','sum by(consumergroup)(kafka_consumergroup_lag)','short'),
 ('Kafka under-replicated partitions','sum(kafka_topic_partition_under_replicated_partition)','short'),
 ('Debezium behind source','cdc_debezium_behind_source_seconds','s'),
 ('Debezium change queue free','cdc_debezium_queue_remaining_ratio','percentunit'),
 ('CDC ingest p95 lag','cdc_ingest_lag_p95_seconds','s'),
 ('CDC events/s · approximate','rate(cdc_events_total[5m])','ops'),
 ('Held transactions','cdc_transactions_held_back','short'),
 ('Oldest held transaction age','cdc_oldest_held_transaction_age_seconds','s'),
 ('Debezium connector/task running','cdc_connect_running','short'),
 ('Postgres retained WAL','cdc_postgres_slot_retained_wal_bytes','bytes'),
 ('Slot WAL left before invalidation','cdc_postgres_slot_safe_wal_bytes','bytes'),
 ('Postgres connections','cdc_postgres_connections','short'),
 ('Postgres lock waiters','cdc_postgres_lock_waiters','short'),
 ('Postgres deadlocks / 5m','increase(cdc_postgres_deadlocks_total[5m])','short'),
 ('ClickHouse memory','cdc_clickhouse_metric{metric="MemoryTracking"}','bytes'),
 ('ClickHouse active queries','cdc_clickhouse_metric{metric="Query"}','short'),
 ('ClickHouse failed queries/s','rate(cdc_clickhouse_events_total{event="FailedQuery"}[5m])','ops'),
 ('Dashboard query p95 · SLO 500 ms','cdc_clickhouse_select_p95_seconds','s'),
 ('ClickHouse max parts per partition · top 10','topk(10, cdc_clickhouse_max_parts_per_partition)','short'),
 ('Mart refresh age','time()-cdc_mart_last_success_timestamp_seconds','s'),
 ('Mart refresh duration','cdc_mart_refresh_duration_seconds','s'),
 ('Reconciliation execution age','time()-cdc_reconciliation_last_success_timestamp_seconds','s'),
 ('Reconciliation failing tables','cdc_reconciliation_failing_tables','short'),
 ('Quarantined ingestion errors','cdc_ingest_errors_total','short'),
 ('Google Chat deliveries / 5m','increase(chat_webhook_deliveries_total[5m])','short'),
 ('Active alerts','ALERTS{alertstate="firing"}','short')]
for title,expr,unit in queries:
 panel(platform,title,prom,dict(expr=expr,legendFormat='{{job}} {{service}} {{dependency}} {{handler}} {{topic}} {{consumergroup}} {{component}} {{result}} {{slot}} {{user}} {{database}}.{{table}}'),unit=unit)
for d in [business,platform]:
 (ROOT/(d['uid']+'.json')).write_text(json.dumps(d,indent=2)+'\n')
