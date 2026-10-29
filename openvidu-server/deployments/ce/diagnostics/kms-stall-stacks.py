#!/usr/bin/env python3
"""Opt-in, independent native-cleanup stall collector. Never restarts KMS."""
import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path
import re
import resource
import signal
import subprocess
import time

CAPTURE_SECONDS = 5
MAX_BYTES = 1024 * 1024
SLOW = re.compile(r'KMS_NATIVE_CLEANUP_SLOW tid=(\d+) kind=(release|delete) '
                  r'stage=(\w+) age_ms=(\d+) stage_ms=(\d+) object=(\S+) type=(\S+)')


def read(path, limit=8192):
    try:
        with open(path) as stream:
            return stream.read(limit)
    except OSError as error:
        return 'unavailable: ' + str(error)


def identity(pid):
    try:
        return Path('/proc/%s/stat' % pid).read_text().rsplit(')', 1)[1].split()[19]
    except (OSError, IndexError):
        return None


class Detector:
    def __init__(self, threshold=60000, cooldown=1800):
        self.threshold = threshold
        self.cooldown = cooldown
        self.previous = {}
        self.last_capture = float('-inf')
        self.latched = False
        self.healthy = 0

    def feed(self, line, now):
        if 'KMS_NATIVE_CLEANUP release_queued=' in line:
            values = dict(re.findall(r'(\w+)=(\d+)', line))
            keys = ('release_pending', 'release_running', 'delete_pending', 'delete_running')
            if all(values.get(key) == '0' for key in keys):
                self.healthy += 1
                if self.healthy >= 2:
                    self.latched = False
                    self.previous.clear()
            else:
                self.healthy = 0
            return None
        match = SLOW.search(line)
        if not match:
            return None
        tid, kind, stage, age, stage_age, obj, obj_type = match.groups()
        item = dict(tid=int(tid), kind=kind, stage=stage, age_ms=int(age),
                    stage_ms=int(stage_age), object=obj[:192], type=obj_type[:64])
        key = (item['tid'], kind, item['object'])
        previous = self.previous.get(key)
        self.previous = {k: v for k, v in self.previous.items() if now - v[0] < 90}
        self.previous[key] = (now, item)
        while len(self.previous) > 32:
            del self.previous[next(iter(self.previous))]
        if (previous and 1 <= now - previous[0] < 90 and item['age_ms'] > previous[1]['age_ms']
                and item['age_ms'] >= self.threshold and not self.latched
                and now - self.last_capture >= self.cooldown):
            self.latched = True
            self.last_capture = now
            others = [v[1] for k, v in self.previous.items()
                      if k != key and now - v[0] < 40 and v[1]['age_ms'] >= self.threshold]
            return [item] + others[:3]
        return None


def resolve_threads(pid, namespace_tids):
    """Native logs contain container TIDs; GDB on the host needs host TIDs."""
    found = {}
    # Fast path when logs and collector share a PID namespace. Validate NSpid
    # even here: the same numeric host TID can belong to another container task.
    for tid in namespace_tids:
        status = read('/proc/%s/task/%s/status' % (pid, tid), 4096)
        match = re.search(r'^NSpid:\s+(.+)$', status, re.M)
        if match and int(match.group(1).split()[-1]) == tid:
            found[tid] = tid
    if len(found) == len(namespace_tids):
        return found
    deadline = time.monotonic() + 2
    with os.scandir('/proc/%s/task' % pid) as entries:
        for entry in entries:
            if time.monotonic() > deadline:
                break
            status = read(Path(entry.path) / 'status', 4096)
            match = re.search(r'^NSpid:\s+(.+)$', status, re.M)
            inner = int(match.group(1).split()[-1]) if match else int(entry.name)
            if inner in namespace_tids:
                found[inner] = int(entry.name)
            if len(found) == len(namespace_tids):
                break
    return found


def debugger(pid, tids, directory, budget=CAPTURE_SECONDS):
    # Numeric host TIDs only. Do not interpolate log content into GDB commands.
    script = directory / 'selected.gdb'
    script.write_text('''set pagination off
set confirm off
set auto-load off
set print thread-events off
set auto-solib-add off
set debug-file-directory /nonexistent
set print frame-arguments none
set sysroot /proc/%d/root
attach %d
sharedlibrary libkmscoreimpl|libkmselementsimpl|libgst|libglib|libgobject|libstdc|libpthread|libc\\.
python
import gdb
wanted = %r
for thread in gdb.selected_inferior().threads():
    if thread.ptid[1] in wanted:
        thread.switch()
        gdb.write("SELECTED_HOST_TID=%%d\\n" %% thread.ptid[1])
        gdb.execute("bt 24")
end
detach
quit
''' % (pid, pid, sorted(set(int(t) for t in tids))))
    old_identity = identity(pid)
    proc = None
    started = time.monotonic()
    cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    outcome = 'error'

    def limits():
        resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_BYTES, MAX_BYTES))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

    try:
        with (directory / 'native.txt').open('wb') as output:
            proc = subprocess.Popen(['gdb', '-nx', '-nh', '-batch', '-x', str(script)],
                                    stdout=output, stderr=subprocess.STDOUT,
                                    env=dict(os.environ, DEBUGINFOD_URLS=''),
                                    start_new_session=True, preexec_fn=limits)
            try:
                proc.wait(timeout=budget)
                outcome = 'finished' if proc.returncode == 0 else 'gdb_error'
            except subprocess.TimeoutExpired:
                outcome = 'timeout'
    finally:
        try:
            if proc and proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=.2)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=.5)
        finally:
            # Attempt resumption even if debugger termination itself times out.
            if proc and old_identity and identity(pid) == old_identity:
                status = read('/proc/%s/status' % pid)
                if re.search(r'^TracerPid:\s+0$', status, re.M):
                    try:
                        os.kill(pid, signal.SIGCONT)
                    except ProcessLookupError:
                        pass
    cpu_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    trace = read(directory / 'native.txt', MAX_BYTES)
    selected = sorted(set(int(tid) for tid in re.findall(r'SELECTED_HOST_TID=(\d+)', trace)))
    return {'outcome': outcome, 'elapsed_ms': round((time.monotonic()-started)*1000, 2),
            'requested_tids': sorted(set(tids)), 'selected_tids': selected,
            'all_selected': selected == sorted(set(tids)),
            'debugger_cpu_ms': round(1000 * (cpu_after.ru_utime + cpu_after.ru_stime -
                                           cpu_before.ru_utime - cpu_before.ru_stime), 2),
            'children_peak_rss_kib': cpu_after.ru_maxrss}


def capture(pid, items, root, native=False, budget=CAPTURE_SECONDS):
    start = identity(pid)
    if not start:
        return None
    directory = root / ('stall-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ'))
    directory.mkdir(mode=0o700)
    status = read('/proc/%s/status' % pid)
    (directory / 'process.txt').write_text(status)
    mapping = resolve_threads(pid, {item['tid'] for item in items})
    evidence = []
    for item in items:
        host_tid = mapping.get(item['tid'])
        entry = dict(item, host_tid=host_tid)
        if host_tid:
            task = Path('/proc/%s/task/%s' % (pid, host_tid))
            entry.update({name: read(task / name, 8192) for name in ('stat', 'wchan', 'stack', 'syscall')})
        evidence.append(entry)
    (directory / 'threads.json').write_text(json.dumps(evidence, indent=2))
    (directory / 'maps.txt').write_text(read('/proc/%s/maps' % pid, MAX_BYTES))
    result = {'pid': pid, 'starttime': start, 'native': 'skipped'}
    if not native:
        result['reason'] = 'native capture not enabled'
    elif not mapping or identity(pid) != start:
        result['reason'] = 'selected threads gone or process changed'
    elif not re.search(r'^TracerPid:\s+0$', status, re.M):
        result['reason'] = 'process already traced'
    elif re.search(r'^State:\s+[Tt]', status, re.M):
        result['reason'] = 'process already stopped'
    else:
        try:
            result['native'] = debugger(pid, mapping.values(), directory, budget)
        except OSError as error:
            result['native'] = {'error': str(error)}
    (directory / 'result.json').write_text(json.dumps(result, indent=2))
    # Only collector-owned files, no recursive traversal or symlink following.
    for old in sorted(root.glob('stall-*'), reverse=True)[10:]:
        if old.is_symlink() or not old.is_dir():
            continue
        for name in ('process.txt', 'threads.json', 'maps.txt', 'selected.gdb', 'native.txt', 'result.json'):
            (old / name).unlink(missing_ok=True)
        try:
            old.rmdir()
        except OSError:
            pass
    return directory


def follow(path):
    """Follow a file or newest timestamp-named log; ignore history on startup."""
    key, offset, partial = None, 0, b''
    while True:
        try:
            source = max(path.glob('*.log')) if path.is_dir() else path
            stat = source.stat()
            current = (stat.st_dev, stat.st_ino)
            if key is None:
                offset = stat.st_size
            elif current != key or stat.st_size < offset:
                offset, partial = 0, b''
            key = current
            with source.open('rb') as stream:
                # Skip an excessive backlog rather than attach for stale evidence.
                if stat.st_size - offset > 256 * 1024:
                    offset, partial = stat.st_size, b''
                stream.seek(offset)
                data = stream.read(256 * 1024)
                offset = stream.tell()
            lines = (partial + data).split(b'\n')
            partial = lines.pop()[-8192:]
            for line in lines:
                yield line[-8192:].decode('utf-8', errors='replace')
        except (OSError, ValueError):
            pass
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--log', type=Path, required=True, help='KMS log file, symlink or timestamp-named log directory')
    parser.add_argument('--container', default='openvidu_kms_1')
    parser.add_argument('--root', type=Path, default=Path('/opt/openvidu/diagnostics/native-stalls'))
    parser.add_argument('--native-stacks', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    args.root.mkdir(parents=True, exist_ok=True)
    # Lock is deliberately separate from watchdog collectors; neither waits on the other.
    with (args.root / 'collector.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        detector, process = Detector(), None
        pid, lookup_at = None, float('-inf')
        stamp = args.root / 'last-capture'
        if stamp.exists():
            try:
                age = max(0, time.time() - float(stamp.read_text()))
                detector.last_capture = time.monotonic() - age
            except ValueError:
                pass
        for line in follow(args.log):
            if 'KMS_NATIVE_CLEANUP' not in line:
                continue
            try:
                now = time.monotonic()
                if pid is None or now - lookup_at >= 30 or identity(pid) is None:
                    output = subprocess.check_output(['docker', 'top', args.container, '-eo', 'pid,comm'],
                                                     timeout=1, text=True)
                    pid = next(int(parts[0]) for row in output.splitlines()
                               if len(parts := row.split()) == 2 and parts[0].isdigit() and 'kurento' in parts[1])
                    lookup_at = now
                current = (pid, identity(pid))
                if process != current:
                    detector.previous.clear()
                    detector.latched = False
                    detector.healthy = 0
                    process = current
                items = detector.feed(line, time.monotonic())
                if items:
                    stamp.write_text(str(time.time()))
                    print(capture(pid, items, args.root, args.native_stacks), flush=True)
            except (OSError, subprocess.SubprocessError, StopIteration) as error:
                print('Collector unavailable: ' + str(error), flush=True)


if __name__ == '__main__':
    # SIGTERM interrupts a running capture so its finally block detaches/resumes.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(0)))
    main()
