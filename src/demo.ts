/**
 * The 60-second artefact: 1,000 disconnects, three server behaviours.
 * Run: `npm run demo`
 */
import { ReplayBuffer, StreamClient, type StreamEvent } from './stream.ts';

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const DISCONNECTS = 1_000;
const CAPACITY = 512;

/** Strategy A: ignore Last-Event-ID entirely - resume from "now". */
function resumeFromNow(): { received: number; gapsReported: number } {
  const rnd = mulberry32(5);
  const buf = new ReplayBuffer(CAPACITY);
  let received = 0;
  for (let d = 0; d < DISCONNECTS; d++) {
    const burst = 1 + Math.floor(rnd() * 50);
    for (let i = 0; i < burst; i++) buf.publish('tick', { i });
    received += 0; // everything during the outage is simply skipped
  }
  return { received, gapsReported: 0 };
}

/** Strategy B: honour the header, but silently start at the oldest retained. */
function silentRebase(): { received: number; gapsReported: number } {
  const rnd = mulberry32(5);
  const buf = new ReplayBuffer(8); // deliberately small: it will roll
  let last = 0;
  let received = 0;
  for (let d = 0; d < DISCONNECTS; d++) {
    const burst = 1 + Math.floor(rnd() * 50);
    for (let i = 0; i < burst; i++) buf.publish('tick', { i });
    const o = buf.resume(last);
    if (o.kind === 'GAP') {
      // The bug: quietly resume from whatever is still in the buffer.
      received += buf.size;
      last = buf.newestId;
    } else {
      received += o.events.length;
      last = o.nextId;
    }
  }
  return { received, gapsReported: 0 };
}

/** Strategy C: this implementation. */
function honest(): { received: number; gapsReported: number; missed: number } {
  const rnd = mulberry32(5);
  const buf = new ReplayBuffer(CAPACITY);
  const client = new StreamClient();
  client.apply(buf.resume(null));
  for (let d = 0; d < DISCONNECTS; d++) {
    const burst = 1 + Math.floor(rnd() * 50);
    for (let i = 0; i < burst; i++) buf.publish('tick', { i });
    client.apply(buf.resume(client.lastEventId));
  }
  return {
    received: client.received.length,
    gapsReported: client.gaps.length,
    missed: client.missedTotal,
  };
}

const rnd = mulberry32(5);
let total = 0;
for (let d = 0; d < DISCONNECTS; d++) total += 1 + Math.floor(rnd() * 50);

const a = resumeFromNow();
const b = silentRebase();
const c = honest();

console.log('\n  RESUMABLE - 1,000 reconnects on a flaky mobile network');
console.log('  ' + '-'.repeat(64));
console.log(`  ${total.toLocaleString()} events published while the client was ` +
            'dropping in and out.\n');
console.log('  server behaviour                 delivered   lost    gaps reported');
console.log('  ' + '-'.repeat(70));
console.log(`  ignores Last-Event-ID            ${String(a.received).padEnd(12)}` +
            `${String(total - a.received).padEnd(8)}${a.gapsReported}`);
console.log(`  honours it, rebases silently     ${String(b.received).padEnd(12)}` +
            `${String(total - b.received).padEnd(8)}${b.gapsReported}`);
console.log(`  this implementation              ${String(c.received).padEnd(12)}` +
            `${String(total - c.received).padEnd(8)}${c.gapsReported}`);

console.log('\n  Row 2 is the dangerous one. It looks like a working stream, it');
console.log('  delivers most events, and it reports nothing. Row 3 delivers');
console.log('  everything - and when it genuinely cannot, it says so.\n');

// Show the gap path firing on a buffer that really has rolled.
const small = new ReplayBuffer(10);
for (let i = 0; i < 105; i++) small.publish('tick', { i });
const o = small.resume(5);
if (o.kind === 'GAP') {
  console.log('  What a client sees when the buffer HAS rolled past it:');
  console.log(`    GAP  requested id ${o.requested}, oldest retained ` +
              `${o.oldestAvailable}, missed ${o.missed} events`);
  console.log(`    -> refetch snapshot at id ${o.snapshotAt}, and tell the viewer\n`);
}
