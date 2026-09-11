import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { ReplayBuffer, StreamClient } from '../src/stream.ts';

function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Drive `disconnects` reconnects against a live stream. Between drops, at most
 * `maxBurst` events are published - kept under capacity so the resume point
 * always survives and the client must see EVERY event.
 */
function runDisconnects(opts: {
  disconnects: number; capacity: number; maxBurst: number; seed: number;
}) {
  const rnd = mulberry32(opts.seed);
  const buf = new ReplayBuffer(opts.capacity);
  const client = new StreamClient();
  let published = 0;

  // Establish the connection BEFORE anything is published. A client that
  // connects mid-stream legitimately never sees the earlier events, and
  // counting those as "lost" would be measuring the wrong thing.
  client.apply(buf.resume(null));

  for (let d = 0; d < opts.disconnects; d++) {
    const burst = 1 + Math.floor(rnd() * opts.maxBurst);
    for (let i = 0; i < burst; i++) {
      buf.publish('tick', { n: published++ });
    }
    client.apply(buf.resume(client.lastEventId));
  }
  return { buf, client, published };
}

describe('1,000 disconnects with the resume point intact', () => {
  const r = runDisconnects({
    disconnects: 1_000, capacity: 512, maxBurst: 50, seed: 11,
  });

  test('THE HEADLINE: zero events lost', () => {
    assert.equal(r.client.received.length, r.published,
      `published ${r.published}, client received ${r.client.received.length}`);
  });

  test('zero duplicates', () => {
    const ids = r.client.received.map((e) => e.id);
    assert.equal(new Set(ids).size, ids.length, 'an event was delivered twice');
  });

  test('strictly increasing order', () => {
    const ids = r.client.received.map((e) => e.id);
    for (let i = 1; i < ids.length; i++) {
      assert.ok(ids[i]! > ids[i - 1]!, `out of order at ${i}`);
    }
  });

  test('exact set equality against what was published', () => {
    const expected = Array.from({ length: r.published }, (_, i) => i + 1);
    assert.deepEqual(r.client.received.map((e) => e.id), expected);
  });

  test('no gaps were reported, because none occurred', () => {
    assert.equal(r.client.gaps.length, 0);
  });
});

describe('THE BRANCH NOBODY EXERCISES: the resume point has been evicted', () => {
  test('a gap is reported rather than a silent resume', () => {
    const buf = new ReplayBuffer(10);
    const client = new StreamClient();
    for (let i = 0; i < 5; i++) buf.publish('tick', { i });
    client.apply(buf.resume(client.lastEventId)); // FRESH
    client.apply(buf.resume(buf.newestId));       // caught up at id 5

    // Client disappears. 100 events roll past a 10-slot buffer.
    for (let i = 0; i < 100; i++) buf.publish('tick', { i });

    const outcome = buf.resume(5);
    assert.equal(outcome.kind, 'GAP',
      'resuming from an evicted id must not report success');
  });

  test('the gap reports how many events were missed', () => {
    const buf = new ReplayBuffer(10);
    for (let i = 0; i < 105; i++) buf.publish('tick', { i });
    const outcome = buf.resume(5);
    assert.equal(outcome.kind, 'GAP');
    if (outcome.kind !== 'GAP') return;
    // ids 6..95 are gone; 96..105 remain.
    assert.equal(outcome.oldestAvailable, 96);
    assert.equal(outcome.requested, 6);
    assert.equal(outcome.missed, 90);
  });

  test('the client records the hole instead of pretending continuity', () => {
    const buf = new ReplayBuffer(10);
    const client = new StreamClient();
    for (let i = 0; i < 5; i++) buf.publish('tick', { i });
    client.apply(buf.resume(null));
    client.apply(buf.resume(5));
    for (let i = 0; i < 100; i++) buf.publish('tick', { i });
    client.apply(buf.resume(client.lastEventId));

    assert.equal(client.gaps.length, 1);
    assert.ok(client.missedTotal > 0);
    // And it rebased to the live edge rather than replaying stale history.
    assert.equal(client.lastEventId, buf.newestId);
  });

  test('after a gap, the stream resumes cleanly with no further loss', () => {
    const buf = new ReplayBuffer(16);
    const client = new StreamClient();
    for (let i = 0; i < 100; i++) buf.publish('a', { i });
    client.apply(buf.resume(1));            // GAP
    const afterGap = buf.newestId;
    for (let i = 0; i < 10; i++) buf.publish('b', { i });
    client.apply(buf.resume(client.lastEventId));

    assert.deepEqual(
      client.received.map((e) => e.id),
      Array.from({ length: 10 }, (_, i) => afterGap + 1 + i),
    );
  });
});

describe('resume edge cases that cause off-by-one loss', () => {
  test('resuming starts at lastEventId + 1, not at lastEventId', () => {
    const buf = new ReplayBuffer(100);
    buf.publish('a', 1); buf.publish('a', 2); buf.publish('a', 3);
    const o = buf.resume(1);
    assert.equal(o.kind, 'RESUMED');
    if (o.kind !== 'RESUMED') return;
    assert.deepEqual(o.events.map((e) => e.id), [2, 3],
      'delivering event 1 again duplicates; starting at 3 drops event 2');
  });

  test('a client that is already current gets nothing and no gap', () => {
    const buf = new ReplayBuffer(100);
    buf.publish('a', 1);
    const o = buf.resume(1);
    assert.equal(o.kind, 'RESUMED');
    if (o.kind === 'RESUMED') assert.equal(o.events.length, 0);
  });

  test('a first connection with no Last-Event-ID starts at the live edge', () => {
    const buf = new ReplayBuffer(100);
    for (let i = 0; i < 50; i++) buf.publish('a', i);
    const o = buf.resume(null);
    assert.equal(o.kind, 'FRESH');
    if (o.kind === 'FRESH') assert.equal(o.events.length, 0);
  });

  test('resuming from exactly the oldest retained id succeeds', () => {
    // The boundary. One off here turns a healthy reconnect into a false gap.
    const buf = new ReplayBuffer(10);
    for (let i = 0; i < 20; i++) buf.publish('a', i);
    const oldest = buf.oldestId!;
    const o = buf.resume(oldest - 1);
    assert.equal(o.kind, 'RESUMED');
    if (o.kind === 'RESUMED') assert.equal(o.events[0]!.id, oldest);
  });

  test('resuming one before the oldest retained id is a gap', () => {
    const buf = new ReplayBuffer(10);
    for (let i = 0; i < 20; i++) buf.publish('a', i);
    assert.equal(buf.resume(buf.oldestId! - 2).kind, 'GAP');
  });
});

describe('buffer mechanics', () => {
  test('stays bounded', () => {
    const buf = new ReplayBuffer(64);
    for (let i = 0; i < 10_000; i++) buf.publish('a', i);
    assert.equal(buf.size, 64);
    assert.equal(buf.evictedCount, 10_000 - 64);
  });

  test('ids remain monotonic across eviction', () => {
    const buf = new ReplayBuffer(8);
    for (let i = 0; i < 100; i++) buf.publish('a', i);
    assert.equal(buf.newestId, 100);
    assert.equal(buf.oldestId, 93);
  });

  test('encodes valid SSE frames', () => {
    const buf = new ReplayBuffer(4);
    const e = buf.publish('score', { home: 1 });
    assert.equal(ReplayBuffer.encode(e), 'id: 1\nevent: score\ndata: {"home":1}\n\n');
  });

  test('rejects a capacity of zero', () => {
    assert.throws(() => new ReplayBuffer(0), /capacity must be >= 1/);
  });
});
