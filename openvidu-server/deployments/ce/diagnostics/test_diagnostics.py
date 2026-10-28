import importlib.util
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("diagnostics", Path(__file__).with_name("kms-diagnostics.py"))
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class DiagnosticsTest(unittest.TestCase):
    def test_fd_snapshot_bounded_and_no_paths(self):
        result = diagnostics.fd_snapshot(os.getpid(), limit=2)
        self.assertEqual(2, result['scanned'])
        self.assertEqual(2, sum(result['kinds'].values()))
        self.assertTrue(result['truncated'])
        self.assertFalse(any('/' in key for key in result['kinds']))

    def test_fd_snapshot_deadline_and_missing_process(self):
        result = diagnostics.fd_snapshot(os.getpid(), seconds=0)
        self.assertEqual(0, result['scanned'])
        self.assertTrue(result['truncated'])
        self.assertIn('error', diagnostics.fd_snapshot(-1))

    def test_thread_snapshot_is_bounded_and_does_not_attach(self):
        with patch.object(diagnostics, 'command') as command:
            result = diagnostics.thread_snapshot(os.getpid(), limit=1)
            self.assertEqual(1, result['scanned'])
            self.assertEqual(1, sum(g['count'] for g in result['groups']))
            self.assertNotIn('kernel_stack', result['representatives'][0])
            command.assert_not_called()

    def test_thread_snapshot_deadline_and_missing_process(self):
        result = diagnostics.thread_snapshot(os.getpid(), seconds=0)
        self.assertEqual(0, result['scanned'])
        self.assertTrue(result['truncated'])
        self.assertIn('error', diagnostics.thread_snapshot(-1))

    def test_incident_thread_explosion_skips_debugger_but_keeps_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(diagnostics, 'find_pid', return_value=os.getpid()), \
                    patch.object(diagnostics, 'process_stats', return_value={'status': {'Threads': '20357'}}), \
                    patch.object(diagnostics, 'capture') as capture:
                diagnostics.incident(Path(temp), True)
                directory = next(Path(temp).glob('incident-*'))
                self.assertTrue((directory / 'kms-process.json').exists())
                self.assertTrue((directory / 'kms-maps.txt').exists())
                self.assertTrue((directory / 'kms-threads.json').exists())
                self.assertIn('20357', (directory / 'kms-backtrace.txt').read_text())
                capture.assert_not_called()

    def test_incident_saves_lightweight_evidence_before_debugger(self):
        with tempfile.TemporaryDirectory() as temp:
            def capture(path, *args, **kwargs):
                self.assertTrue((path.parent / 'kms-process.json').exists())
                self.assertTrue((path.parent / 'kms-threads.json').exists())
                self.assertTrue((path.parent / 'kms-fds.json').exists())
                self.assertTrue((path.parent / 'kms-maps.txt').exists())
            with patch.object(diagnostics, 'find_pid', return_value=os.getpid()), \
                    patch.object(diagnostics, 'capture', side_effect=capture), \
                    patch.object(diagnostics, 'resume_detached_process'):
                diagnostics.incident(Path(temp), True)

    def test_resume_only_same_detached_process(self):
        with patch.object(diagnostics, "process_identity", return_value="123"), \
                patch.object(diagnostics, "read_text", return_value="TracerPid:\t0\n"), \
                patch.object(diagnostics.os, "kill") as kill:
            diagnostics.resume_detached_process(42, "122")
            kill.assert_not_called()
            diagnostics.resume_detached_process(42, "123")
            kill.assert_called_once_with(42, diagnostics.signal.SIGCONT)

    def test_never_resume_process_still_traced(self):
        with patch.object(diagnostics, "process_identity", return_value="123"), \
                patch.object(diagnostics, "read_text", return_value="TracerPid:\t99\n"), \
                patch.object(diagnostics.os, "kill") as kill:
            diagnostics.resume_detached_process(42, "123")
            kill.assert_not_called()

    def test_watchdog_budget_with_unresponsive_stack_helper(self):
        with tempfile.TemporaryDirectory() as temp:
            script = '''
import importlib.util, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location("diagnostics", sys.argv[1])
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)
d.find_pid = lambda *args, **kwargs: 123
original = d.capture
def stuck_capture(path, args, deadline, seconds=3):
    original(path, [sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)"], deadline, seconds)
d.capture = stuck_capture
d.incident(Path(sys.argv[2]), True)
'''
            start = time.monotonic()
            result = subprocess.run(["timeout", "-k", "1s", "4s", sys.executable, "-c", script,
                                     str(Path(diagnostics.__file__).resolve()), temp],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=7)
            self.assertIn(result.returncode, (0, 124, 137))
            self.assertLess(time.monotonic() - start, 5.5)

    def test_incident_prioritizes_native_stack_without_slow_collection(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(diagnostics, "find_pid", return_value=123) as lookup, \
                    patch.object(diagnostics, "capture") as capture, \
                    patch.object(diagnostics, "process_stats", return_value={}), \
                    patch.object(diagnostics, "sample") as sample, \
                    patch.object(diagnostics, "keep_latest") as prune:
                diagnostics.incident(Path(temp), True)
                self.assertEqual("gdb", capture.call_args_list[0].args[1][0])
                lookup.assert_called_once_with(diagnostics.CONTAINERS["kms"], "kurento", timeout=0.5)
                sample.assert_not_called()
                prune.assert_not_called()
                self.assertFalse(any("docker" == call.args[1][0] for call in capture.call_args_list))

    def test_post_restart_uses_original_manifest_not_new_container(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            old_log = root / "old.log"
            old_log.write_text("original process evidence")
            incident = root / "incident-01"
            incident.mkdir()
            (incident / "sources.json").write_text(diagnostics.json.dumps({"kms-native.log": str(old_log)}))
            with patch.object(diagnostics, "capture") as capture:
                diagnostics.post_restart(root)
                self.assertEqual("original process evidence", (incident / "kms-native.log").read_text())
                self.assertTrue((incident / "collected").exists())
                self.assertEqual(1, capture.call_count)
                diagnostics.post_restart(root)
                self.assertEqual(1, capture.call_count)

    def test_post_restart_handles_interrupted_incident(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            incident = root / "incident-01"
            incident.mkdir()
            diagnostics.post_restart(root)
            self.assertIn("interrupted", (incident / "collected").read_text())

    def test_pid_selection_does_not_use_container_init_pid(self):
        with patch.object(diagnostics, "command", return_value="PID COMMAND\n12 tini\n14 kurento-media-se\n"):
            self.assertEqual(14, diagnostics.find_pid("kms", "kurento"))

    def test_missing_process_is_explicit(self):
        with patch.object(diagnostics, "command", return_value="Cannot connect to Docker daemon"):
            self.assertIsNone(diagnostics.find_pid("kms", "kurento"))
        self.assertIn("error", diagnostics.process_stats(None))

    def test_retention_leaves_other_files_alone(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ("sample-01.jsonl", "sample-02.jsonl", "sample-03.jsonl", "keep.txt"):
                (root / name).touch()
            diagnostics.keep_latest(root, "sample-*.jsonl", 2)
            self.assertEqual(["keep.txt", "sample-02.jsonl", "sample-03.jsonl"],
                             sorted(p.name for p in root.iterdir()))

    def test_capture_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.txt"
            diagnostics.capture(path, ["nonexistent-command"], time.monotonic() - 1)
            self.assertIn("budget exhausted", path.read_text())

    def test_capture_missing_tool(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.txt"
            diagnostics.capture(path, ["nonexistent-diagnostics-test-command"], time.monotonic() + 2)
            self.assertIn("No such file", path.read_text())

    def test_capture_kills_stuck_helper(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.txt"
            start = time.monotonic()
            diagnostics.capture(path, [sys.executable, "-c", "import time; time.sleep(60)"],
                                start + 0.2)
            self.assertLess(time.monotonic() - start, 3)
            self.assertIn("timed out", path.read_text())

    def test_capture_output_size_is_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "result.txt"
            diagnostics.capture(path, [sys.executable, "-c", "print('x' * 4000000)"], time.monotonic() + 3)
            self.assertLessEqual(path.stat().st_size, diagnostics.MAX_FILE)


if __name__ == "__main__":
    unittest.main()
