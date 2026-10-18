"""Unit checks for bounded scheduler settling, without importing database-connected validation code."""
import ast
import contextlib
import io
from pathlib import Path
import unittest
from unittest.mock import Mock


def load_helpers():
    source = ast.parse(Path(__file__).with_name('check_data.py').read_text())
    helpers = ast.Module(body=[n for n in source.body if isinstance(n, ast.FunctionDef)
                              and n.name in {'time_rules_sql', 'wait_for_scheduled_rules'}], type_ignores=[])
    namespace = {'AUTO_COMPLETE_S': 259200, 'DISPUTE_WINDOW_S': 86400,
                 'JOB_GRACE_REAL_S': 60, 'SETTLE_TIMEOUT_S': 10}
    exec(compile(helpers, '<validation helpers>', 'exec'), namespace)
    return namespace


class SchedulerSettlingTests(unittest.TestCase):
    def run_wait(self, pending, times):
        ns = load_helpers()
        cursor = Mock()
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone.return_value = {'speed': 1.0}
        cursor.fetchall.side_effect = pending
        ns['pg_live'] = Mock()
        ns['pg_live'].cursor.return_value = cursor
        ns['time'] = Mock()
        ns['time'].monotonic.side_effect = times
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            ns['wait_for_scheduled_rules']()
        return ns, cursor, output.getvalue()

    def test_empty_backlog_returns_without_sleep(self):
        ns, cursor, output = self.run_wait([[]], [0])
        ns['time'].sleep.assert_not_called()
        self.assertEqual(cursor.fetchall.call_count, 1)
        self.assertEqual(output, '')

    def test_live_state_is_polled_until_jobs_catch_up(self):
        ns, cursor, output = self.run_wait([[{'rule': 'unpaid past deadline'}], []], [0, 1])
        self.assertEqual(cursor.fetchall.call_count, 2)
        ns['time'].sleep.assert_called_once_with(2)
        self.assertEqual(output, '')

    def test_timeout_preserves_failure_for_normal_validation(self):
        ns, cursor, output = self.run_wait([[{'rule': 'unpaid past deadline'}]], [0, 11])
        ns['time'].sleep.assert_not_called()
        self.assertIn('still have 1 overdue rows', output)
        self.assertEqual(cursor.fetchall.call_count, 1)


if __name__ == '__main__':
    unittest.main()
