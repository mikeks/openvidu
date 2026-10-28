#!/usr/bin/env python3
"""Bounded host-side telemetry. Does not change containers or restart services."""
import argparse
from collections import Counter
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import time

CONTAINERS = {"kms": "openvidu_kms_1", "ov": "openvidu_openvidu-server_1"}
MAX_FILE = 2 * 1024 * 1024
MAX_NATIVE_STACK_THREADS = 4096


def fd_snapshot(pid, seconds=0.1, limit=1024):
    deadline = time.monotonic() + seconds
    kinds = Counter()
    scanned = 0
    truncated = False
    try:
        with os.scandir('/proc/%s/fd' % pid) as entries:
            for entry in entries:
                if scanned >= limit or time.monotonic() >= deadline:
                    truncated = True
                    break
                try:
                    target = os.readlink(entry.path)
                    if target.startswith(('socket:', 'pipe:')):
                        kind = target.split(':', 1)[0]
                    elif target.startswith('anon_inode:'):
                        kind = target[:80]
                    else:
                        kind = 'file_or_other'
                except OSError:
                    kind = 'unavailable'
                kinds[kind] += 1
                scanned += 1
    except OSError as error:
        return {'error': str(error)}
    # Do not record file paths, socket addresses or descriptor contents.
    return {'scanned': scanned, 'truncated': truncated,
            'selection': 'proc directory order; not a random or full census', 'kinds': dict(kinds)}


def thread_snapshot(pid, seconds=0.25, limit=2048, kernel_stacks=False):
    """Non-pausing, bounded sample; counts describe scanned threads, not the process."""
    deadline = time.monotonic() + seconds
    groups = Counter()
    representatives = []
    scanned = 0
    truncated = False
    try:
        with os.scandir('/proc/%s/task' % pid) as tasks:
            for task in tasks:
                if scanned >= limit or time.monotonic() >= deadline:
                    truncated = True
                    break
                root = Path(task.path)
                name = read_text(root / 'comm', 128).strip()
                wait = read_text(root / 'wchan', 128).strip()
                key = (name, wait)
                if not groups[key] and len(representatives) < 12:
                    item = {'tid': task.name, 'name': name, 'wait': wait,
                            'stat': read_text(root / 'stat', 2048)}
                    if kernel_stacks:
                        item['kernel_stack'] = read_text(root / 'stack', 4096)
                    representatives.append(item)
                groups[key] += 1
                scanned += 1
    except OSError as error:
        return {'error': str(error)}
    return {'scanned': scanned, 'truncated': truncated,
            'selection': 'proc directory order; not a random or full census',
            'groups': [{'name': name, 'wait': wait, 'count': count}
                       for (name, wait), count in groups.most_common(32)],
            'representatives': representatives}


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def read_text(path, limit=65536):
    try:
        with open(path) as stream:
            return stream.read(limit)
    except OSError as error:
        return str(error)


def command(args, timeout=2):
    try:
        result = subprocess.run(args, timeout=timeout, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        return result.stdout[:65536].strip()
    except (OSError, subprocess.TimeoutExpired) as error:
        return str(error)


def find_pid(container, process, timeout=2):
    # Do not inspect environment variables or command lines (they contain secrets).
    for line in command(["docker", "top", container, "-eo", "pid,comm"], timeout=timeout).splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0].isdigit() and process in fields[1]:
            return int(fields[0])
    return None


def process_stats(pid):
    if not pid:
        return {"error": "process not found"}
    root = Path("/proc") / str(pid)
    status = {}
    for line in read_text(root / "status").splitlines():
        key, _, value = line.partition(":")
        if key in {"Name", "State", "Threads", "VmRSS", "VmSize", "VmSwap", "FDSize"}:
            status[key] = value.strip()
    try:
        status["fd_count"] = sum(1 for _ in (root / "fd").iterdir())
    except OSError:
        status["fd_count"] = None
    return {"pid": pid, "status": status, "stat": read_text(root / "stat"),
            "io": read_text(root / "io"), "limits": read_text(root / "limits")}


def process_identity(pid):
    try:
        # starttime is field 22; comm may contain spaces or parentheses.
        return Path("/proc/%s/stat" % pid).read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def resume_detached_process(pid, identity):
    if identity is None or process_identity(pid) != identity:
        return
    status = read_text("/proc/%s/status" % pid)
    if "TracerPid:\t0\n" in status:
        try:
            os.kill(pid, signal.SIGCONT)
        except ProcessLookupError:
            pass


def sample():
    result = {"utc": utc_now().isoformat(), "loadavg": read_text("/proc/loadavg"),
            "meminfo": read_text("/proc/meminfo"), "vmstat": read_text("/proc/vmstat"),
            "pressure_cpu": read_text("/proc/pressure/cpu"),
            "pressure_memory": read_text("/proc/pressure/memory"),
            "pressure_io": read_text("/proc/pressure/io"),
            "processes": {key: process_stats(find_pid(container, "java" if key == "ov" else "kurento"))
                          for key, container in CONTAINERS.items()}}
    kms = result['processes']['kms']
    if kms.get('pid'):
        kms['threads'] = thread_snapshot(kms['pid'])
        kms['descriptors'] = fd_snapshot(kms['pid'])
    return result


def keep_latest(root, pattern, count, directories=False):
    paths = sorted(root.glob(pattern), reverse=True)
    for path in paths[count:]:
        if path.is_symlink():
            continue
        if directories:
            shutil.rmtree(path)
        else:
            path.unlink()


def capture(path, args, deadline, seconds=3):
    remaining = min(seconds, deadline - time.monotonic())
    if remaining <= 0:
        path.write_text("Skipped: incident time budget exhausted\n")
        return

    def limits():
        resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_FILE, MAX_FILE))

    with path.open("wb") as output:
        try:
            proc = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT,
                                    start_new_session=True, preexec_fn=limits)
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                # Give GDB a chance to detach before enforcing the deadline.
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
                output.write(b"\nCapture timed out\n")
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                    try:
                        proc.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL)
                        proc.wait()
        except OSError as error:
            output.write(str(error).encode())


def incident(root, native_stacks):
    directory = root / ("incident-" + utc_now().strftime("%Y%m%dT%H%M%S.%fZ"))
    directory.mkdir(mode=0o700)
    # Leave time for helper termination inside the watchdog's five-second bound.
    deadline = time.monotonic() + 3
    try:
        logs = sorted(Path("/opt/openvidu/kurento-logs").glob("*.log"), key=lambda p: p.stat().st_mtime)
    except OSError:
        logs = []
    sources = {"evidence.log": "/opt/openvidu/evidence/evidence-recording.log",
               "ov-persistent.log": str(root / "ov/openvidu.log")}
    if logs:
        sources["kms-native.log"] = str(logs[-1])
    (directory / "sources.json").write_text(json.dumps(sources))
    kms_pid = find_pid(CONTAINERS["kms"], "kurento", timeout=0.5)
    (directory / "process.json").write_text(json.dumps({"utc": utc_now().isoformat(), "kms_pid": kms_pid}))
    # GDB may spend the entire deadline attaching to thousands of threads.
    # Preserve cheap evidence before attempting an all-thread native backtrace.
    threads = 0
    if kms_pid:
        stats = process_stats(kms_pid)
        (directory / "kms-process.json").write_text(json.dumps(stats))
        (directory / "kms-fds.json").write_text(json.dumps(fd_snapshot(
            kms_pid, seconds=max(0, min(0.1, deadline - time.monotonic())))))
        (directory / "kms-maps.txt").write_text(read_text("/proc/%s/maps" % kms_pid, MAX_FILE))
        (directory / "kms-threads.json").write_text(json.dumps(
            thread_snapshot(kms_pid, seconds=max(0, min(0.25, deadline - time.monotonic())),
                            kernel_stacks=True)))
        threads = int(stats.get('status', {}).get('Threads', '0'))
    if kms_pid and native_stacks and threads <= MAX_NATIVE_STACK_THREADS:
        identity = process_identity(kms_pid)
        try:
            capture(directory / "kms-backtrace.txt", ["gdb", "-nx", "-batch",
                "-ex", "set pagination off", "-ex", "set auto-load off",
                "-ex", "set print thread-events off",
                "-ex", "set debug-file-directory /nonexistent",
                "-ex", "set print frame-arguments none",
                "-ex", "set sysroot /proc/%s/root" % kms_pid,
                "-ex", "attach %s" % kms_pid, "-ex", "thread apply all bt 30",
                "-ex", "detach", "-ex", "quit"], deadline)
        finally:
            # GDB interrupted during symbol loading can detach leaving SIGSTOP set.
            resume_detached_process(kms_pid, identity)
    else:
        reason = ("thread count %s exceeds bounded attach limit %s" % (threads, MAX_NATIVE_STACK_THREADS)
                  if threads > MAX_NATIVE_STACK_THREADS else "KMS not found or native stacks disabled")
        (directory / "kms-backtrace.txt").write_text("Skipped: " + reason + "\n")
    print(directory)


def post_restart(root):
    # No debugger or old-container queries here: those processes no longer exist.
    keep_latest(root, "incident-*", 10, directories=True)
    for directory in sorted(root.glob("incident-*")):
        if directory.is_symlink() or not directory.is_dir() or (directory / "collected").exists():
            continue
        collect_logs(directory)


def collect_logs(directory):
    manifest = directory / "sources.json"
    if not manifest.exists():
        (directory / "collected").write_text("Pre-restart capture interrupted before log manifest\n")
        return
    for name, source in json.loads(manifest.read_text()).items():
        source = Path(source)
        try:
            with source.open("rb") as stream:
                stream.seek(max(0, source.stat().st_size - MAX_FILE))
                (directory / name).write_bytes(stream.read(MAX_FILE))
        except OSError as error:
            (directory / name).write_text(str(error))
    deadline = time.monotonic() + 5
    capture(directory / "kernel.log", ["dmesg", "--ctime", "--level=err,warn"], deadline)
    (directory / "collected").write_text("Collected after restart at " + utc_now().isoformat() +
                                       "; shared logs may include post-restart entries\n")


def main():
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("sample", "incident", "post-restart"))
    parser.add_argument("--root", type=Path, default=Path("/opt/openvidu/diagnostics"))
    parser.add_argument("--native-stacks", action="store_true",
                        help="Pauses KMS briefly with GDB; only use after a failed health probe")
    args = parser.parse_args()
    os.umask(0o077)
    args.root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (args.root / (args.mode + ".lock")).open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if args.mode == "incident":
            incident(args.root, args.native_stacks)
        elif args.mode == "post-restart":
            post_restart(args.root)
        else:
            path = args.root / ("sample-" + utc_now().strftime("%Y%m%d") + ".jsonl")
            if not path.exists() or path.stat().st_size < 32 * 1024 * 1024:
                with path.open("a") as output:
                    output.write(json.dumps(sample(), separators=(",", ":")) + "\n")
            keep_latest(args.root, "sample-*.jsonl", 7)


if __name__ == "__main__":
    main()
