# resumable

> Server-sent events with a real Last-Event-ID contract and an explicit gap signal, so a reconnect never silently skips events.

`COMPACT` · **Full Stack Engineering** · Intermediate · ~5 days · Gaming - live tournament operations

**Primary language:** TypeScript
**Tags:** `streaming`, `sse`, `event-driven`, `redis`, `reliability`

---

## The problem

Live event feeds drop connections constantly on mobile networks. Most implementations reconnect and resume from 'now', so a viewer who lost signal for eight seconds simply never sees the elimination, the goal, or the price change that happened in that window - and has no way to know they missed it.

## ⭐ The differentiator

Implements the SSE `Last-Event-ID` contract properly against a **bounded ring-buffer replay log with an explicit gap signal**. If the resume point has already been evicted, the client receives a GAP event and refetches a snapshot, rather than resuming mid-stream and pretending continuity. A generic implementation ignores `Last-Event-ID` entirely; a slightly better one honours it and then lies when the buffer has rolled.

This is the sentence to lead with when someone asks you to walk through the
project. Everything else in this repo exists to make it true and to prove it.

## Data

A synthetic tournament event generator emitting ordered, monotonically-identified events, driven by a scripted disconnect harness with configurable outage lengths.

> No paid API key is required to run or demo this project. Where a paid
> service would add value it is wired as an optional enhancement behind an
> interface with an offline mock as the default implementation.

## Stack

- TypeScript, Fastify
- Redis for the per-channel replay buffer
- React client hook
- Vitest + a load harness

## Core capabilities

- Monotonic event IDs with a bounded per-channel replay buffer
- Last-Event-ID resume with explicit gap detection and snapshot fallback
- Per-connection heartbeat and idle eviction
- Client hook exposing connection state, gap events, and an events-missed count
- Load harness driving 5,000 concurrent connections with scripted drops

## Repository layout

```
src/server/
src/client/
sim/disconnect/
test/ordering/
bench/
```

## Build plan

1. Event IDs and the ring buffer first. Decide the eviction policy deliberately and write down why.
2. Resume path, then the gap path. Test the gap path hardest - it is the branch nobody exercises.
3. Load harness last, to produce the headline number.

## Testing strategy

Assert **exact event-set equality** - no gaps, no duplicates, correct order - across 1,000 scripted disconnects. Separately assert the gap path *fires* when the buffer has evicted the resume point, because a resume implementation that never exercises its own failure branch is untested where it matters.

Tests assert **correctness**, not merely that the code runs. A green suite on
this repo is a claim about behaviour under adversarial conditions; treat any
test that would pass against a deliberately broken implementation as a bug in
the test.

## Measurable outcome

> 1,000 simulated disconnects with zero lost events, and every unrecoverable gap surfaced to the client rather than hidden.

State it in these terms — business units, not technical ones — in your CV
bullet and in the first thirty seconds of describing the project.

## Interview questions this project answers

- **SSE versus WebSocket - when and why?**
- **How large should a replay buffer be, and what happens at its edge?**

## What this deliberately is *not*

- Not a message broker. Bounded replay with honest failure, nothing more.


## Run it now

```bash
npm test        # runs the suite; no install step needed
npm run demo    # the 60-second artefact
```

Requires Node 22.6+ (24 recommended). TypeScript runs natively via
type stripping - there is no build step and no `node_modules`.

## Getting started

```bash
git clone <your-fork-url> resumable
cd resumable
docker compose up -d redis
npm install
npm run dev
npm run test:ordering         # 1,000 disconnects, zero loss
```

Docker is supported but optional — every path above works on a plain
Windows/macOS/Linux laptop without a cloud account.

## Definition of done

- [ ] The differentiator above is implemented, and a test proves it
- [ ] The measurable outcome is produced by a command anyone can run
- [ ] `README` explains the one decision a generic version gets wrong
- [ ] CI runs the full suite on every push and is green on `main`
- [ ] A recruiter can see the headline artefact in under 60 seconds

## Licence

MIT — see [LICENSE](LICENSE).
