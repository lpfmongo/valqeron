---
id: 21
title: "CLI watch command"
milestone: "M7 — Events"
area: area:cli
size: S
depends_on: [14, 20]
blocks: []
status: todo
assignee:
---

# #21 — CLI `watch` command

## Context

The first consumer of [#20](0020-event-service-streaming.md), and the cheapest way to validate
the streaming contract before a desktop app exists. If `watch` is awkward to implement, the
streaming API is wrong — better to learn that now than after a UI is built on it.

It is also a genuinely useful debugging tool: watching events while running commands in another
terminal makes engine behaviour observable.

Streaming does not fit the CLI's existing one-shot `{success, dry_run, data}` envelope. Use
newline-delimited JSON (one event per line) — greppable, pipeable, and standard for streams.

## Tasks

- [ ] Add `valqeron watch` streaming events as newline-delimited JSON.
- [ ] Support the filter options exposed by the service.
- [ ] Handle the resync marker visibly — do not hide it. The user must know the stream had gaps.
- [ ] Handle Ctrl-C cleanly: close the stream, exit 0.
- [ ] Handle engine shutdown mid-stream with a clear message and a distinct exit code.
- [ ] Require engine mode; fail clearly with guidance if no daemon is running (direct mode
      cannot stream).
- [ ] Flush each event immediately so piping works without buffering delays.
- [ ] Document the output format in `--help`, including that it differs from the standard
      envelope.

## Acceptance criteria

- Events from other clients' mutations appear promptly.
- Output is one valid JSON object per line, flushed immediately.
- Resync markers are visible and clearly distinguishable from events.
- Ctrl-C exits cleanly with code 0.
- Engine shutdown mid-stream produces a clear message and a distinct exit code.
- Running without a daemon fails with actionable guidance.

## Test strategy

- **Integration:** start `watch`, mutate from a second client, assert the event appears.
- **Piping:** pipe into a line-consuming process; assert events arrive without buffering delay.
- **Signals:** Ctrl-C mid-stream; assert clean exit.
- **Shutdown:** stop the engine mid-stream; assert the message and exit code.
- **Format:** assert every line parses as JSON independently.
