---
id: 18
title: "Socket activation (stretch)"
milestone: "M6 — Service management"
area: area:ops
size: M
risk: stretch
depends_on: [16, 17]
blocks: []
status: todo
assignee:
---

# #18 — Socket activation (stretch)

## Context

Socket activation lets the service manager own the listening socket and start the engine on
first connection. Benefits: no idle daemon when unused, no startup race (clients connect to a
socket that always exists), and the service manager handles socket cleanup.

Marked **stretch** — the engine works without it. Defer if M6 is running long.

There is a genuine tension worth noting: on-demand start means the first CLI invocation pays
database-open and migration cost, and an engine that exits when idle repeatedly reopens the
database. Evaluate whether on-demand start is actually desirable, or whether socket activation
should be used purely for socket lifecycle management with `RunAtLoad`/`enable` still starting
the daemon eagerly.

## Tasks

- [ ] Add launchd `Sockets` configuration and retrieve the inherited fd via
      `launch_activate_socket`.
- [ ] Add a systemd `.socket` unit and retrieve the fd via the `LISTEN_FDS` protocol, validating
      `LISTEN_PID`.
- [ ] Abstract listener construction so the engine accepts either an inherited fd or a
      self-bound socket, with the same downstream code path.
- [ ] Skip the socket-cleanup logic from [#6](0006-lifecycle-and-single-instance.md) when the fd
      is inherited — the service manager owns it. The flock guard still applies.
- [ ] Decide and document whether the engine exits when idle. If yes, define the idle timeout
      and ensure in-flight work and background jobs block it.
- [ ] Measure cold-start latency (open + migrate + serve) and document whether it is acceptable
      for interactive CLI use.

## Acceptance criteria

- The engine starts on first connection under both launchd and systemd.
- Inherited-fd and self-bound paths produce identical behaviour after startup.
- The flock guard still prevents two instances.
- Cold-start latency is measured and documented, not assumed.
- If idle-exit is implemented, in-flight requests and running background jobs prevent it.

## Test strategy

Manual on both platforms.

- Connect with no daemon running; assert it starts and serves.
- Assert socket file lifecycle is managed by the service manager.
- Benchmark cold start against warm start; record both.
- If idle-exit is implemented, assert a long-running background job blocks it.
