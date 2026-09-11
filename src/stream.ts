/**
 * Resumable event streaming with an honest failure mode.
 *
 * THE DIFFERENTIATOR LIVES HERE.
 *
 * The SSE spec gives you `Last-Event-ID` for free: on reconnect the browser
 * sends the last id it saw, and the server is supposed to resume from there.
 * Most implementations ignore it and resume from "now", so an eight-second
 * mobile outage silently swallows whatever happened in those eight seconds.
 * The viewer never learns they missed the elimination.
 *
 * Honouring `Last-Event-ID` is necessary but not sufficient, because the replay
 * buffer is bounded and will eventually have rolled past the resume point. The
 * implementation that honours the header and then resumes from the oldest thing
 * it still has is the WORSE bug: it looks correct, it is silently lossy, and
 * nothing distinguishes it from a healthy stream.
 *
 * So there are exactly three outcomes, and the third one is the point:
 *   RESUMED  - every event since the resume point is delivered
 *   FRESH    - no resume point supplied; start at the live edge
 *   GAP      - the resume point has been evicted. Say so, and hand back a
 *              snapshot instead of pretending continuity.
 */

export interface StreamEvent {
  /** Monotonic per channel. The resume coordinate. */
  readonly id: number;
  readonly type: string;
  readonly data: unknown;
}

export type ResumeOutcome =
  | { kind: 'FRESH'; events: readonly StreamEvent[]; nextId: number }
  | { kind: 'RESUMED'; events: readonly StreamEvent[]; nextId: number }
  | {
      kind: 'GAP';
      /** How many events were lost - reported, never hidden. */
      missed: number;
      oldestAvailable: number;
      requested: number;
      snapshotAt: number;
    };

/** Bounded per-channel replay log. */
export class ReplayBuffer {
  private events: StreamEvent[] = [];
  private nextId = 1;
  readonly capacity: number;
  private evicted = 0;

  constructor(capacity: number) {
    if (capacity < 1) throw new Error('capacity must be >= 1');
    this.capacity = capacity;
  }

  publish(type: string, data: unknown): StreamEvent {
    const e: StreamEvent = { id: this.nextId++, type, data };
    this.events.push(e);
    while (this.events.length > this.capacity) {
      this.events.shift();
      this.evicted++;
    }
    return e;
  }

  get oldestId(): number | null {
    return this.events.length ? this.events[0]!.id : null;
  }
  get newestId(): number {
    return this.nextId - 1;
  }
  get evictedCount(): number {
    return this.evicted;
  }
  get size(): number {
    return this.events.length;
  }

  /**
   * Resolve a reconnect.
   *
   * `lastEventId` is what the client last SAW, so delivery starts at
   * lastEventId + 1. Off-by-one here duplicates or drops exactly one event per
   * reconnect, which is invisible in a demo and obvious at scale.
   */
  resume(lastEventId: number | null): ResumeOutcome {
    if (lastEventId === null) {
      return { kind: 'FRESH', events: [], nextId: this.newestId };
    }
    if (lastEventId >= this.newestId) {
      // Client is already current. Not a gap.
      return { kind: 'RESUMED', events: [], nextId: this.newestId };
    }

    const wanted = lastEventId + 1;
    const oldest = this.oldestId;

    if (oldest === null || wanted < oldest) {
      return {
        kind: 'GAP',
        missed: (oldest ?? this.newestId + 1) - wanted,
        oldestAvailable: oldest ?? -1,
        requested: wanted,
        snapshotAt: this.newestId,
      };
    }

    const events = this.events.filter((e) => e.id >= wanted);
    return { kind: 'RESUMED', events, nextId: this.newestId };
  }

  /** SSE wire format, for the transport layer. */
  static encode(e: StreamEvent): string {
    return `id: ${e.id}\nevent: ${e.type}\ndata: ${JSON.stringify(e.data)}\n\n`;
  }
}

/**
 * Client-side connection state. Tracks what it has seen and how much it was
 * told it missed - a number the UI can actually show the viewer.
 */
export class StreamClient {
  lastEventId: number | null = null;
  received: StreamEvent[] = [];
  gaps: Array<{ missed: number; at: number }> = [];

  apply(outcome: ResumeOutcome): void {
    if (outcome.kind === 'GAP') {
      this.gaps.push({ missed: outcome.missed, at: outcome.snapshotAt });
      // Refetch a snapshot and rebase. We do NOT pretend the missing events
      // arrived; the client's history has an explicit hole recorded.
      this.lastEventId = outcome.snapshotAt;
      return;
    }
    for (const e of outcome.events) {
      this.received.push(e);
      this.lastEventId = e.id;
    }
    if (outcome.events.length === 0) this.lastEventId ??= outcome.nextId;
  }

  get missedTotal(): number {
    return this.gaps.reduce((a, g) => a + g.missed, 0);
  }
}
