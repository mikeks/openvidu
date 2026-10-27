# Kurento stall diagnostics

These files are deployment assets, not an automatic production deployment.
Do not run load tests or attach GDB to healthy production media sessions.

## Install the lightweight sampler

Install `kms-diagnostics.py` as `/opt/openvidu/kms-diagnostics.py` (root-owned,
mode 0750). Install both services and the sampler timer in `/etc/systemd/system/`, then:

```bash
systemctl daemon-reload
systemctl enable --now kms-diagnostics.timer
```

Requires Python 3, Docker CLI, procps, and host access to `/proc`.
Every minute it saves cumulative CPU/IO counters, memory, threads, file descriptor
count, host memory/load/pressure and the process PID. Compare deltas only within
the same PID lifetime. No media requests, DB writes or debugger attachment occur.
There are at most seven daily sample files, capped at 16 MiB each. Failure to
find the process is explicit, not represented as zero resource usage.

## Capture before watchdog destroys containers

In the existing `/opt/openvidu/kms-watchdog.sh`, insert this immediately before
`echo "Kill KMS"` in `restart_services()`. Keep the existing restart path intact:

```bash
echo "Capturing bounded KMS incident diagnostics"
timeout -k 1s 4s python3 /opt/openvidu/kms-diagnostics.py incident --native-stacks || true
```

After the existing `docker-compose up` command, launch deferred collection:

```bash
systemctl start --no-block kms-diagnostics-post.service || true
```

Capture runs only after the watchdog has already decided to restart. The outer
timeout allows four seconds plus one second to terminate helpers: **five seconds
maximum added delay under normal OS scheduling**, not a real-time guarantee if
the host itself is stuck in uninterruptible IO. KMS PID lookup has a 0.5-second
timeout. Native stacks get the remaining shared three-second working budget;
helper termination has a one-second allowance. Lightweight process state is
captured only if time remains. Missing tools never intentionally block recovery.
Timeouts can leave partial stacks; `Capture timed out` or an interrupted file
must not be interpreted as a complete dump.

Log copying, kernel-log collection and retention run in the separate systemd
service after restart, without waiting in the watchdog. The manifest selects
the old KMS log before restart, not the replacement process's log. Shared OV and
evidence log tails can include post-restart entries; correlate their timestamps.
The persistent OV logging setup below is required to preserve OV output: removed
containers' Docker logs cannot be fetched afterward. Rotated OV archives remain
in the persistent logging directory even if the copied active tail lacks the
incident. Java stacks and Docker metadata capture are omitted from this fast
path; do not mistake new-process stacks for evidence about the old stall.
Latest ten incident folders are retained when deferred collection runs (a new
pending folder can temporarily exceed this). Each output is capped at 2 MiB.
Samples plus incidents are bounded to roughly 400 MiB (normal use much less).

The native capture requires **GDB on the host**, root ptrace access, and ideally
debug symbols matching the exact KMS image and its libraries. It uses the
container's filesystem as GDB sysroot. It intentionally excludes local variables
and frame arguments. Host separate-debug-file lookup is disabled to avoid loading
mismatched host symbols and spending the short budget on them. Exported symbols
and any embedded debug information remain available. It briefly pauses KMS, so never enable native capture in the
periodic sampler. Missing symbols can leave unresolved frames; save the exact
deployed image before pruning it if further symbol analysis
is needed. Do not upgrade KMS merely to install mismatched symbols.

The updated image includes JDK tools for manual investigations, but automated
Java capture is disabled to prioritize native stacks within the short deadline.
The inspected production host lacks GDB. GDB must be installed on each host
separately before enabling native stack capture.
Until then the capture reports missing tools and still saves the other evidence.

Interrupted GDB can leave its target stopped even after detaching. The collector
resumes only the same process lifetime (PID plus start time) and only after its
tracer has detached. The watchdog still restarts the unhealthy process afterward.

Validate stack capture on an isolated test server first. Do not enable global
GStreamer TRACE logging or automatic full core dumps on production: both can
generate excessive data. Native debugger background:
[GDB thread backtraces](https://sourceware.org/gdb/current/onlinedocs/gdb/Threads.html).

## Persist OV logging across container recreation

The updated OV jar supports `OPENVIDU_DIAGNOSTICS_PATH`. Merge this into the
existing compose override (do not replace existing evidence mounts/settings):

```yaml
services:
  openvidu-server:
    environment:
      - OPENVIDU_DIAGNOSTICS_PATH=/opt/openvidu/diagnostics/ov
    volumes:
      - /opt/openvidu/diagnostics/ov:/opt/openvidu/diagnostics/ov
```

Pre-create the host directory with permissions allowing the OV container user to
write; do not make it world-readable. The rolling log keeps seven days, at most
200 MB archived plus a 20 MB active file. This is additional to the host sampler
and existing Kurento logs. Without this environment setting no new file appender
is enabled. Check that OV logging is enabled on every host (server 7 was missing
normal application output during the investigation).

The jar logs `MEDIA_LIFECYCLE cumulative ...` once per minute. These fixed-size
counters cover creations, disposal, late callbacks, duplicate subscriptions,
publisher lock timeouts, and release outcomes including retry timeouts. Counters
reset on JVM restart. They are **operation counters, not a remote object census**:
disposal may happen before creation, callbacks may arrive after a timeout, and
release retries can race with a successful original release. Do not subtract
these counters to claim an exact number of leaked KMS endpoints.

## Interpret the next incident

1. Correlate UTC timestamps across resource samples, lifecycle counters and logs.
2. Check whether thread count, RSS or file descriptors grow while room load drops.
3. Inspect native stacks for repeated mutex/futex waits, blocked IO, or a common
   hot path; correlate OS thread IDs with `threads.txt`.
4. Inspect persistent OV logs for waiting Kurento RPCs and lifecycle failures.
5. Compare late creation/release failure counts with the period before the stall.

A stack snapshot can reveal the blocked operation; it does not automatically
prove which earlier event caused it. Reproduce the implicated path in the test
environment before changing native Kurento code.

All diagnostics are private operator data. Existing logs can contain SDP,
network addresses and user identifiers. The capture does not collect environment
variables, command-line secrets, audio, or full memory dumps. Keep the diagnostics
directory root-only and redact logs before sharing them externally.

## Local checks

```bash
python3 -m unittest discover -s openvidu-server/deployments/ce/diagnostics -p 'test_*.py'
mvn -pl openvidu-server -am -Dtest=MediaEndpointLifecycleTest,DiagnosticsLoggingTest -Dsurefire.failIfNoSpecifiedTests=false test
```

## Server 6 deployment, 2026-10-19

Deployed only to video6 while drained. Application image:
`openvidu/openvidu-server:2.19.0-lifecycle-20261019`, derived from its existing
runtime image with the patched JAR (no base OS or Kurento upgrade). Automated
capture needs host GDB only; optional JDK image changes were not deployed.
JAR SHA256: `35d2d422ea56b753b5c10258c064d74d5b78086d68afca8b293c2e66e5278856`.

Rollback image: `openvidu/openvidu-server:rollback-20261019`.
Original compose file, watchdog and JAR are in
`/opt/openvidu/deployment-backups/20261019-lifecycle/` (root-only directory).
The new image, log mount and environment setting are in the new
`/opt/openvidu/docker-compose.override.yml`; the base compose file is unchanged.

Validation: 21 Java tests and 13 diagnostics tests passed. Session/token creation
and session deletion succeeded. Idle native capture took 0.78 seconds with stacks
for 40 threads and KMS remained responsive. A deliberate SIGSTOP stall at
12:13:49 Berlin was detected at 12:13:56; capture completed within that second,
watchdog logged UP at 12:13:59, and OV startup completed by 12:14:04. Deferred
collection succeeded and sampler files were written. These are idle-server
measurements, not a client media-reconnection or load benchmark. The synthetic
incident is `incident-20261019T101356.228571Z`, not a spontaneous production crash.

To roll back while drained: stop `kms-watchdog`, disable/stop the diagnostics
timer, stop the deferred collection service, move the new compose override aside,
and restore the backed-up watchdog. Recreate only the OV service from the original
compose configuration, then start the watchdog. This host's legacy Compose can
fail recreation with `ContainerConfig`; remove the stopped OV service container
with `docker-compose rm -f openvidu-server` before `up -d --no-deps openvidu-server`.
Do not remove volumes or persisted evidence. Preserve diagnostic files for review.
