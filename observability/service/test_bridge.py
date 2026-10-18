import json
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

    def test_bad_payload_does_not_call_upstream(self):
        with patch('bridge.deliver') as delivery:
            with self.assertRaises(HTTPError) as error:
                self.post({'alerts':'invalid'})
            self.assertEqual(error.exception.code,400)
            delivery.assert_not_called()


if __name__ == '__main__':
    unittest.main()
