"""Read-only integration check against provisioned Grafana and its ClickHouse datasource.

Run: python3 observability/grafana/verify_business_dashboards.py
Uses local Grafana admin secret without printing it. Never starts/stops services or writes business data.
"""
import base64
import json
import os
import re
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from business_dashboards import build_dashboards

ROOT = Path(__file__).resolve().parents[2]


def quoted(value):
    return "'" + str(value).replace('\\', '\\\\').replace("'", "\\'") + "'"


def interpolate(query, variables):
    def replace(match):
        name, fmt = match.group(1), match.group(2)
        values = variables[name]
        if not isinstance(values, list):
            values = [values]
        if fmt == 'sqlstring':
            return ','.join(quoted(v) for v in values) if values else "''"
        if fmt == 'raw':
            return str(int(values[0]))
        return str(values[0])
    return re.sub(r'\$\{(\w+)(?::(\w+))?\}', replace, query)


def main():
    password = (ROOT / 'secrets/grafana_admin_password').read_text().strip()
    auth = 'Basic ' + base64.b64encode(('admin:' + password).encode()).decode()
    base = os.getenv('GRAFANA_URL', 'http://localhost:3000')
    calls = 0

    def execute(sql):
        nonlocal calls
        payload = {'queries': [{'refId': 'A', 'datasource': {'uid': 'clickhouse', 'type': 'grafana-clickhouse-datasource'},
                                'editorType': 'sql', 'rawSql': sql, 'format': 1}], 'from': 'now-1h', 'to': 'now'}
        request = Request(base + '/api/ds/query', json.dumps(payload).encode(),
                          {'Content-Type': 'application/json', 'Authorization': auth})
        try:
            with urlopen(request, timeout=30) as response:
                result = json.load(response)['results']['A']
        except HTTPError as error:
            detail = json.loads(error.read()).get('results', {}).get('A', {}).get('error', 'HTTP query failed')
            raise RuntimeError(detail) from None
        assert result.get('status', 200) == 200 and not result.get('error'), result.get('error')
        calls += 1
        frames = result.get('frames', [])
        if not frames:
            return []
        return [list(row) for row in zip(*frames[0]['data']['values'])]

    pinned = execute('SELECT toString(max(refresh_id)) FROM ops.mart_refreshes')[0][0]
    assert int(pinned) > 0, 'No published mart snapshot'
    for board in build_dashboards():
        variables = {'refresh_id': pinned}
        for variable in board['templating']['list']:
            name = variable['name']
            if name in variables:
                continue
            if variable['type'] == 'custom':
                variables[name] = variable['current']['value']
            else:
                rows = execute(interpolate(variable['query'], variables))
                variables[name] = [r[0] for r in rows] if variable.get('multi') else (rows[0][0] if rows else '')
        checked = 0
        for panel in board['panels']:
            for target in panel.get('targets', []):
                try:
                    execute(interpolate(target['rawSql'], variables))
                except Exception as error:
                    raise RuntimeError(f'{board["uid"]} / {panel["title"]}: {error}') from None
                checked += 1
        print(f'PASS {board["uid"]}: {checked} panel queries at snapshot {pinned}')
        if board['uid'] == 'cdc-business':
            for days in ('14', '30'):
                alternate = dict(variables, period_days=days)
                for panel in board['panels']:
                    for target in panel.get('targets', []):
                        if 'period_days' in target['rawSql']:
                            execute(interpolate(target['rawSql'], alternate))
                print(f'PASS cdc-business: {days}-day matched-period window')
        # Exercise dependent filters with one selection rather than only All.
        if board['uid'] in ('cdc-growth', 'cdc-sellers', 'cdc-finance'):
            single = {'refresh_id': pinned}
            for variable in board['templating']['list']:
                name = variable['name']
                if name in single:
                    continue
                if variable['type'] == 'custom':
                    single[name] = variable['current']['value']
                else:
                    rows = execute(interpolate(variable['query'], single))
                    single[name] = [rows[0][0]] if rows and variable.get('multi') else (rows[0][0] if rows else '')
            for panel in board['panels']:
                for target in panel.get('targets', []):
                    execute(interpolate(target['rawSql'], single))
            print(f'PASS {board["uid"]}: single-selection filters')
    # Ratios must use aligned grains; buyer counts are never summed from daily distinct counts.
    rows = execute(f'SELECT sum(nmv) <= sum(gmv), sum(confirmed_orders) <= sum(orders) FROM mart.daily_sales WHERE refresh_id={int(pinned)}')
    assert rows == [[True, True]], 'Daily mart metric assumptions do not hold; inspect source data'
    print(f'PASS: {calls} read-only Grafana queries; no source/warehouse mutations')


if __name__ == '__main__':
    main()
