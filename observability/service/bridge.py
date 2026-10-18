"""Internal Alertmanager receiver. Secrets and upstream response bodies are never logged."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

DELIVERIES = Counter('chat_webhook_deliveries_total', 'Google Chat notification deliveries', ['result'])


def format_message(payload):
    lines = ['[CDC] ' + payload.get('status', 'unknown').upper()]
    for alert in payload.get('alerts', [])[:15]:
        labels, note = alert.get('labels', {}), alert.get('annotations', {})
        lines.append(f"\n[{alert.get('status', payload.get('status', 'unknown')).upper()}] "
                     f"{labels.get('severity', 'info')} — {labels.get('alertname', 'Alert')}")
        lines.append(note.get('summary', ''))
        lines.append(note.get('description', ''))
        lines.append('Service: ' + labels.get('service', labels.get('job', 'platform')))
        if note.get('runbook_url'):
            lines.append('Runbook: ' + note['runbook_url'])
    lines.append('\nGrafana: ' + os.getenv('GRAFANA_PUBLIC_URL', 'http://localhost:3000') + '/d/cdc-platform')
    return {'text': '\n'.join(lines)[:25000]}


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
            if not isinstance(payload, dict) or not isinstance(payload.get('alerts'), list):
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
