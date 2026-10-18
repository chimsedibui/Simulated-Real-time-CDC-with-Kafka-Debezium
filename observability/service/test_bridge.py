import json
from datetime import datetime, timedelta, timezone
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import bridge


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), bridge.Handler)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/alerts'

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.worker.join()

    def post(self, payload):
        return urlopen(Request(self.url, json.dumps(payload).encode(), {'Content-Type':'application/json'}))

    def test_resolved_alert_keeps_context(self):
        message = bridge.format_message({'status':'resolved','alerts':[{
            'status':'resolved','labels':{'alertname':'Test','severity':'warning','service':'kafka'},
            'annotations':{'summary':'Recovered','description':'Backlog cleared'}}]})['text']
        self.assertIn('RESOLVED', message)
        self.assertIn('Backlog cleared', message)
        self.assertIn('kafka', message)

    def test_success_and_retry_contract(self):
        payload = {'status':'firing','alerts':[]}
        with patch('bridge.deliver') as delivery:
            self.assertEqual(self.post(payload).status, 200)
            delivery.assert_called_once_with(payload)
        with patch('bridge.deliver', side_effect=RuntimeError('SECRET_URL')):
            with self.assertRaises(HTTPError) as error:
                self.post(payload)
            self.assertEqual(error.exception.code, 503)
            self.assertNotIn(b'SECRET_URL', error.exception.read())

    def test_invalid_alert_objects_do_not_call_upstream(self):
        invalid = [{'alerts': [None]}, {'alerts': [{'labels': 'invalid'}]},
                   {'alerts': [{'annotations': []}]}, {'alerts': [], 'truncatedAlerts': 'bad'}]
        with patch('bridge.deliver') as delivery:
            for payload in invalid:
                with self.subTest(payload=payload):
                    with self.assertRaises(HTTPError) as error:
                        self.post(payload)
                    self.assertEqual(error.exception.code, 400)
            delivery.assert_not_called()

    def test_bad_payload_does_not_call_upstream(self):
        with patch('bridge.deliver') as delivery:
            with self.assertRaises(HTTPError) as error:
                self.post({'alerts':'invalid'})
            self.assertEqual(error.exception.code,400)
            delivery.assert_not_called()


class FormattingTests(unittest.TestCase):
    NOW = datetime(2026, 10, 18, 17, 10, tzinfo=timezone.utc)

    def alert(self, status='resolved', **overrides):
        result = {'status': status,
                  'startsAt': '2026-10-18T16:58:00Z',
                  'endsAt': '2026-10-18T17:06:30Z',
                  'labels': {'alertname': 'TargetDown', 'severity': 'critical',
                             'job': 'warehouse-jobs', 'instance': 'warehouse-jobs:9109'},
                  'annotations': {'summary': 'Metrics target warehouse-jobs is unreachable',
                                  'condition': 'up == 0 for 2m', 'observed_value': '0'}}
        result.update(overrides)
        return result

    def render(self, *alerts, **extra):
        return bridge.format_message({'status': 'resolved', 'alerts': list(alerts), **extra}, now=self.NOW)['text']

    def test_resolved_has_vietnam_dates_duration_target_and_recovery_context(self):
        message = self.render(self.alert())
        self.assertIn('19/10/2026 00:10:00 (UTC+7)', message)
        self.assertIn('18/10/2026 23:58:00 (UTC+7)', message)
        self.assertIn('19/10/2026 00:06:30 (UTC+7)', message)
        self.assertIn('00:08:30', message)
        self.assertIn('warehouse-jobs:9109', message)
        self.assertIn('docker compose logs --tail=100 warehouse-jobs', message)
        self.assertIn('Alertmanager đã đánh dấu RESOLVED', message)
        self.assertIn('trước khi kết luận phục hồi toàn bộ', message)
        self.assertIn('up == 0 for 2m', message)
        self.assertIn('from=', message)
        self.assertIn('&to=', message)

    def test_nanosecond_rfc3339_timestamp_and_per_alert_environment(self):
        self.assertEqual(bridge.parse_time('2026-10-18T16:58:00.123456789Z').microsecond, 123456)
        alert = self.alert()
        alert['labels']['env'] = 'staging'
        self.assertIn('Môi trường: staging', self.render(alert))

    def test_firing_does_not_treat_future_ends_at_as_recovery(self):
        message = self.render(self.alert('firing', startsAt='2026-10-18T17:00:00Z',
                                         endsAt='2026-10-19T17:00:00Z'), status='firing')
        self.assertIn('00:10:00', message)
        self.assertIn('1 FIRING | 0 RESOLVED', message)
        self.assertNotIn('Kết thúc cảnh báo:', message)
        self.assertNotIn('20/10/2026', message)

    def test_bad_missing_and_zero_timestamps_are_not_invented(self):
        for bad in [None, '', 'bad-time', '0001-01-01T00:00:00Z', '2026-10-18T17:00:00']:
            with self.subTest(value=bad):
                message = self.render(self.alert(startsAt=bad, endsAt=bad))
                self.assertIn('Không có trong payload', message)
                self.assertIn('Thời lượng cảnh báo: Không xác định', message)
                self.assertNotIn('01/01/1', message)

    def test_resolved_group_missing_one_end_uses_notification_time_for_dashboard(self):
        message = self.render(self.alert(), self.alert(endsAt='0001-01-01T00:00:00Z'))
        expected = int((self.NOW + timedelta(minutes=5)).timestamp() * 1000)
        self.assertIn(f'to={expected}', message)
        self.assertIn('Thời lượng cảnh báo: Không xác định', message)

    def test_negative_duration_is_not_reported(self):
        message = self.render(self.alert(startsAt='2026-10-19T17:00:00Z'))
        self.assertIn('Thời lượng cảnh báo: Không xác định', message)
        self.assertNotIn('from=', message)

    def test_explicit_offset_and_long_duration(self):
        message = self.render(self.alert(startsAt='2026-10-16T23:58:00+07:00',
                                         endsAt='2026-10-19T00:06:30+07:00'))
        self.assertIn('2 ngày 00:08:30', message)

    def test_generic_alert_keeps_description_labels_environment_and_runbook(self):
        alert = self.alert(labels={'alertname': 'KafkaBacklogGrowing', 'service': 'kafka',
                                  'consumergroup': 'clickhouse-cdc'},
                           annotations={'summary': 'Backlog growing', 'description': 'Investigate throughput',
                                        'runbook_url': 'https://example.com/runbook'})
        with patch.dict('os.environ', {'GRAFANA_PUBLIC_URL': 'https://grafana.example.com/',
                                       'ALERT_ENVIRONMENT': 'staging'}):
            message = self.render(alert)
        self.assertIn('Môi trường: staging', message)
        self.assertIn('Investigate throughput', message)
        self.assertIn('clickhouse-cdc', message)
        self.assertIn('https://example.com/runbook', message)
        self.assertIn('https://grafana.example.com/d/cdc-platform?', message)

    def test_common_environment_overrides_fallback_and_mixed_group_keeps_states(self):
        message = self.render(self.alert('firing'), self.alert(), status='firing',
                              commonLabels={'environment': 'production', 'project': 'cdc-demo'})
        self.assertIn('Môi trường: production | Dự án: cdc-demo', message)
        self.assertIn('1 FIRING | 1 RESOLVED', message)
        self.assertIn('[FIRING]', message)
        self.assertIn('[RESOLVED]', message)

    def test_kafka_exporter_failure_is_not_claimed_to_be_broker_failure(self):
        alert = self.alert('firing', labels={'alertname': 'TargetDown', 'job': 'kafka',
                                           'instance': 'kafka-exporter:9308'})
        message = self.render(alert, status='firing')
        self.assertIn('Kafka exporter', message)
        self.assertIn('chưa đủ kết luận Kafka broker đã dừng', message)

    def test_local_and_alertmanager_truncation_are_visible(self):
        message = self.render(*[self.alert() for _ in range(17)], truncatedAlerts=4)
        self.assertIn('Còn 6 cảnh báo', message)

    def test_oversized_description_keeps_dashboard_and_reports_truncation(self):
        alert = self.alert(labels={'alertname': 'Custom'}, annotations={'description': 'Độ trễ tăng cao ' * 5000})
        message = self.render(alert)
        self.assertLessEqual(len(message.encode('utf-8')), bridge.MAX_MESSAGE)
        self.assertIn('Nội dung được rút gọn', message)
        self.assertIn('Grafana (khoảng thời gian cảnh báo):', message)


if __name__ == '__main__':
    unittest.main()
