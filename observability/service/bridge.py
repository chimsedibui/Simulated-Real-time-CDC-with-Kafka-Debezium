"""Internal Alertmanager receiver. Secrets and upstream response bodies are never logged."""
import json
import os
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

DELIVERIES = Counter('chat_webhook_deliveries_total', 'Google Chat notification deliveries', ['result'])


VN_TIME = timezone(timedelta(hours=7))
MAX_ALERTS = 15
MAX_MESSAGE = 25000  # UTF-8 bytes; leave headroom for Chat request framing
TARGET_CONTEXT = {
    'warehouse-jobs': (
        'Mart, snapshot và đối soát có thể chậm cập nhật; cần kiểm tra job thực tế.',
        'docker compose logs --tail=100 warehouse-jobs; kiểm tra endpoint warehouse-jobs:9109/metrics, tuổi mart và reconciliation.'),
    'source-api': (
        'Mất quan sát API nghiệp vụ; kiểm tra health/API để xác định có ảnh hưởng đường ghi hay không.',
        'docker compose logs --tail=100 source-api; kiểm tra /health, /metrics và kết nối PostgreSQL.'),
    'kafka': (
        'Mất quan sát offset/consumer lag từ Kafka exporter; chưa đủ kết luận Kafka broker đã dừng.',
        'docker compose logs --tail=100 kafka-exporter; kiểm tra kết nối exporter tới kafka:9092 và tình trạng broker.'),
    'platform': (
        'Mất quan sát dependency, CDC lag, watermark và mart từ platform-exporter.',
        'docker compose logs --tail=100 platform-exporter; kiểm tra endpoint metrics và các dependency PostgreSQL/ClickHouse/Connect.'),
}


def parse_time(value):
    """Alertmanager uses RFC3339; Go's zero timestamp means unknown, not a real end."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.year > 1 and parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def display_time(value):
    return value.astimezone(VN_TIME).strftime('%d/%m/%Y %H:%M:%S') if value else 'Không có trong payload'


def elapsed(start, end):
    if not start or not end or end < start:
        return 'Không xác định'
    seconds = int((end - start).total_seconds())
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return (f'{days} ngày ' if days else '') + f'{hours:02d}:{minutes:02d}:{seconds:02d}'


def format_message(payload, *, now=None):
    """Render notification time separately from per-alert start/end and business time."""
    now = now or datetime.now(timezone.utc)
    alerts = payload.get('alerts', [])
    common = payload.get('commonLabels') or {}
    environments = {a.get('labels', {}).get('environment') or a.get('labels', {}).get('env') for a in alerts}
    environments.discard(None)
    environments.discard('')
    environment = (common.get('environment') or common.get('env')
                   or ', '.join(sorted(environments)) or os.getenv('ALERT_ENVIRONMENT', 'local'))
    project = common.get('project', 'realtime-cdc')
    status = str(payload.get('status', 'unknown')).upper()
    counts = {state: sum(a.get('status', payload.get('status')) == state for a in alerts)
              for state in ('firing', 'resolved')}
    lines = [f'[CDC] {status}', f'Môi trường: {environment} | Dự án: {project}',
             f'Thời điểm tạo thông báo: {display_time(now)} (UTC+7)',
             f'Nhóm nhận được: {counts["firing"]} FIRING | {counts["resolved"]} RESOLVED']
    starts, ends = [], []
    for alert in alerts[:MAX_ALERTS]:
        labels, note = alert.get('labels') or {}, alert.get('annotations') or {}
        state = alert.get('status', payload.get('status', 'unknown'))
        job = labels.get('job', '')
        service = labels.get('service') or job or 'platform'
        start = parse_time(alert.get('startsAt'))
        # A firing alert's endsAt may be a future timeout, never a recovery timestamp.
        end = parse_time(alert.get('endsAt')) if state == 'resolved' else None
        if start:
            starts.append(start)
        if end:
            ends.append(end)
        lines += ['', f'[{str(state).upper()}] {labels.get("severity", "info")} — {labels.get("alertname", "Alert")}',
                  f'Dịch vụ: {service}', f'Target: {labels.get("instance", "Không có trong payload")}']
        if job and job != service:
            lines.append(f'Scrape job: {job}')
        for key in ('environment', 'env', 'dependency', 'connector', 'component', 'topic',
                    'consumergroup', 'database', 'table', 'slot', 'handler', 'mountpoint', 'slo'):
            if labels.get(key):
                lines.append(f'{key}: {labels[key]}')
        lines.append(f'Bắt đầu cảnh báo: {display_time(start)} (UTC+7)')
        if state == 'resolved':
            lines += [f'Kết thúc cảnh báo: {display_time(end)} (UTC+7)',
                      f'Thời lượng cảnh báo: {elapsed(start, end)}']
        else:
            lines.append(f'Đã kéo dài đến thời điểm thông báo: {elapsed(start, now)}')
        if note.get('summary'):
            lines.append(('Tóm tắt khi cảnh báo: ' if state == 'resolved' else 'Tóm tắt: ') + str(note['summary']))
        if labels.get('alertname') == 'TargetDown':
            if state == 'resolved':
                lines.append('Diễn giải: Alertmanager đã đánh dấu RESOLVED. '
                             'Cần xác nhận target scrape lại thành công và đối chiếu job, dữ liệu nghiệp vụ '
                             'trước khi kết luận phục hồi toàn bộ.')
            else:
                lines.append('Diễn giải: Prometheus không scrape được metrics target liên tiếp theo ngưỡng rule; '
                             'chưa đủ kết luận tiến trình đã dừng hoặc dữ liệu đã mất.')
            impact, action = TARGET_CONTEXT.get(job or service, (
                'Mất quan sát metrics của target; trạng thái ứng dụng cần được kiểm tra riêng.',
                'Kiểm tra target trong Prometheus, log service/exporter và kết nối mạng nội bộ.'))
            lines += [f'Ảnh hưởng khi cảnh báo: {note.get("impact", impact)}',
                      f'Kiểm tra: {note.get("action", action)}']
        elif note.get('description'):
            lines.append(('Chi tiết khi cảnh báo: ' if state == 'resolved' else 'Chi tiết: ') + str(note['description']))
        for key, title in [('condition', 'Điều kiện'), ('observed_value', 'Giá trị rule khi cảnh báo (payload)'),
                           ('impact', 'Ảnh hưởng'), ('action', 'Kiểm tra')]:
            if note.get(key) and not (labels.get('alertname') == 'TargetDown' and key in ('impact', 'action')):
                lines.append(f'{title}: {note[key]}')
        if note.get('runbook_url'):
            lines.append(f'Runbook: {note["runbook_url"]}')
    omitted = max(0, len(alerts) - MAX_ALERTS) + int(payload.get('truncatedAlerts') or 0)
    if omitted:
        lines.append(f'\nCòn {omitted} cảnh báo bị giới hạn khỏi thông báo này; xem Alertmanager/Prometheus.')
    grafana = os.getenv('GRAFANA_PUBLIC_URL', 'http://localhost:3000').rstrip('/') + '/d/cdc-platform'
    if starts:
        finish = max(ends) if len(ends) == len(alerts) and all(a.get('status', payload.get('status')) == 'resolved' for a in alerts) else now
        if min(starts) <= finish:
            grafana += '?' + urlencode({'from': int((min(starts) - timedelta(minutes=5)).timestamp() * 1000),
                                        'to': int((finish + timedelta(minutes=5)).timestamp() * 1000)})
    footer = '\nGrafana (khoảng thời gian cảnh báo): ' + grafana
    text = '\n'.join(lines)
    if len(text.encode('utf-8')) + len(footer.encode('utf-8')) > MAX_MESSAGE:
        suffix = '\n[Nội dung được rút gọn; xem dashboard và Alertmanager để biết đầy đủ.]'
        budget = max(0, MAX_MESSAGE - len(footer.encode('utf-8')) - len(suffix.encode('utf-8')))
        text = text.encode('utf-8')[:budget].decode('utf-8', errors='ignore') + suffix
    return {'text': text + footer}


def deliver(payload):
    url = Path(os.getenv('GOOGLE_CHAT_WEBHOOK_FILE', '/run/secrets/google-chat-webhook')).read_text().strip()
    if not url.startswith('https://chat.googleapis.com/'):
        raise ValueError('Google Chat webhook is not configured')
    request = Request(url, data=json.dumps(format_message(payload)).encode(),
                      headers={'Content-Type': 'application/json'}, method='POST')
    with urlopen(request, timeout=15) as response:
        response.read(1024)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200); self.send_header('Content-Type', CONTENT_TYPE_LATEST); self.end_headers()
            self.wfile.write(generate_latest())
        elif self.path == '/health':
            self.send_response(200); self.end_headers(); self.wfile.write(b'ok')
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path != '/alerts':
            self.send_error(404); return
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if length <= 0 or length > 262144:
                self.send_error(413); return
            payload = json.loads(self.rfile.read(length))
            if (not isinstance(payload, dict) or not isinstance(payload.get('alerts'), list)
                    or not isinstance(payload.get('commonLabels', {}), dict)
                    or any(not isinstance(a, dict)
                           or not isinstance(a.get('labels', {}), dict)
                           or not isinstance(a.get('annotations', {}), dict) for a in payload['alerts'])
                    or not isinstance(payload.get('truncatedAlerts', 0), int)
                    or payload.get('truncatedAlerts', 0) < 0):
                self.send_error(400); return
        except (ValueError, TypeError):
            self.send_error(400); return
        try:
            deliver(payload)
        except Exception:
            DELIVERIES.labels('error').inc()
            # A non-2xx response lets Alertmanager retry. Never echo the credential-bearing exception.
            self.send_error(503, 'Notification delivery failed'); return
        DELIVERIES.labels('success').inc()
        self.send_response(200); self.end_headers(); self.wfile.write(b'ok')


if __name__ == '__main__':
    ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
