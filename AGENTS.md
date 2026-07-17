# RockyDB — Agent Guide

Embedded, single-file key-value database in Java 21 (Gradle). Storage engine is a **B-Link tree**
(Lehman & Yao) with merge/absorb logic inspired by Shasha & Lanin, a PostgreSQL-style **clock-sweep
buffer pool**, and a **FIFO freelist guarded by epochs**. Package: `org.rockydb` (flat, single package).

Build/test: `./gradlew build`, `./gradlew test`. Tests: JUnit 6 + Mockito, one test class per component.

> **WARNING: there is NO WAL.** Dirty pages reach disk only on buffer eviction or `close()`.
> A crash mid-write can lose data or corrupt the tree (e.g. a split writes the right sibling
> before the left/parent — a crash between those writes leaks or loses pages). WAL is on the TODO list.
> There is also no fsync. Treat durability as "clean shutdown only".

## Layering (top → bottom)

- `Database` — multi-table facade. The **catalog is itself a B-Link tree** mapping
  `table-name → root page id`. Each table gets a `BLinkTree` with a `TableRootRef` that
  persists root-id changes back into the catalog. `createTable`/`dropTable` are serialized
  by `catalogLock`; `dropTable` walks the tree and retires every page.
- `BLinkTree` — all tree algorithms (get/insert/delete/split/merge/root-collapse) over the
  `Store` interface. `RootRef` abstracts where the root id lives (file header for the
  standalone/catalog tree, catalog entry for tables).
- `NodeStore implements Store` — node-level storage: `PageCodec` ser/de, tree latching
  (`latchForWrite`/`WriteHandle`), tree root id, bootstrap of a fresh file (root leaf +
  free-log page), and safe page recycling (`EpochManager` + `FreeLog`). Composed of a
  `DiscStore` and a `BufferedPool`.
- `BufferedPool` — raw byte page cache over `DiscStore`: fixed `Frame[]`, `pageToFrame`
  ConcurrentHashMap, `Clock` eviction. Whole API is `Frame acquire(long id)` + stats +
  flush-on-close; knows nothing about nodes.
- `DiscStore` — deliberately dumb: `FileChannel` raw page i/o (page id = file offset /
  `PAGE_SIZE`), `allocateRaw`, and the file header (first 32 bytes): root id @0, FreeLog
  head @8, epoch @16, FreeLog tail @24. No knowledge of nodes, epochs or freelist semantics.
- `FreeLog` + `EpochManager` — safe page recycling, owned directly by `NodeStore`.
- `PageCodec` — node (de)serialization, incl. the is-leaf/marked flag bits. `Node` / `LeafNode` / `BranchNode` —
  sealed hierarchy, immutable-style copy-on-write. `Value` — `byte[]` wrapper, unsigned
  lexicographic order; `Value.infinity()` (null bytes) is the +∞ fence sentinel.

## On-disk / in-memory page format

`PAGE_SIZE = 8KB`. Page header = 5 bytes: flags byte (bit0 is-leaf, bit1 marked), short elem-count,
short height. Values are length-prefixed (int, `-1` = +∞/null). Leaf: keys[], values[], link(long),
fence. Branch: keys[], pointers[] (longs), link. **Branch invariants: `keys.length == pointers.length`
and `keys[i]` is the maximum (fence) key of subtree `pointers[i]`** — fence keys, not separators.

Sizing constants in `Store`: split when body > `PAGE_SIZE - 5`; merge when body < 30% of that
(`MERGE_THRESHOLD`).

## B-Link tree invariants

- Every node has a right `link` (page id, `-1` = rightmost) and knows its `biggestKey()` (fence).
- `shouldGoRight(key)`: key > fence (and link exists) → the target has moved right due to a split.
- `marked` = tombstone of a merged-away node; marked nodes always route right via `link`
  (link then points at the survivor). Tombstones are created on merge and root collapse.
- Splits: size-balanced half split; right sibling inherits the old link; left points at right;
  the separator (promoted key) is pushed into the parent. Splitting the root creates a new root
  (height grows). Merges absorb the right sibling into the left node and remove the downlink
  from the parent; the root is collapsed when it has a single child (height shrinks).

## Concurrency model

Three independent mechanisms:

1. **Tree latches (writers).** `Store.latchForWrite(id)` returns a `WriteHandle` holding the
   frame's `treeLatch` (ReentrantLock) **and a pin** — the pin guarantees the frame isn't
   repurposed while latched, so frame↔page identity is stable. Writers use **latch coupling**
   when moving right: latch next, then close previous. Reads never take tree latches.
2. **Buffer pool (physical).** `Frame.pinCount` (AtomicInteger) — `Clock.findVictim()` CASes
   0→1 (`tryClaim`) to exclusively claim a victim; PostgreSQL-style sweep decrements `usageCount`
   (capped at `MAX_USAGE = 5`), evicting only a claimed frame with usage 0. `Frame.ioLock`
   (ReentrantReadWriteLock) guards the byte buffer: read lock for deserialize, write lock for
   serialize/evict/repurpose. `BufferedPool.acquire(id)` internally does the
   "lookup → pin → verify pageId under ioLock → retry" dance, so it always returns a pinned
   frame that provably holds that page (a pinned frame can never be repurposed). Eviction of a
   dirty frame writes it to disk. If all frames stay pinned through the sweeps,
   `BufferExhaustedException` is thrown.
3. **Epochs (logical page reuse).** Every public op wraps its body in `enterOp()/exitOp(ticket)`
   (ticket = current epoch). `EpochManager` tracks active op counts per epoch in a
   `ConcurrentSkipListMap` — ops always enter at the current epoch, which never goes
   backwards, so the minimum sits at the head and `safeEpoch()` is a `firstEntry()` lookup
   (a transient zero-count head is conservative, never unsafe). `retire(pageId)` appends `(pageId, currentEpoch)` to the `FreeLog` and bumps
   the epoch; `allocate()` pops the FIFO head only if `headEpoch < safeEpoch` — i.e. no active
   op can still hold a reference to the freed page. The epoch auto-advances every 1024 enters,
   and is persisted in the file header (+1 on open).

`FreeLog` itself is a FIFO queue over chained disk pages (per-page header: next ptr + count;
records of `(pageId, epoch)`), head/tail packed as `(pageId << 32) | slot` in the file header.
A fully consumed head page is re-appended to the tail — the queue recycles its own pages.
To amortize disk i/o it keeps two in-memory buffers (256 entries each): `retire` appends to a
write buffer flushed to the tail only when full (or on `NodeStore.close()`); `acquireSafe`
serves from a read buffer, refilled by batch-consuming up to 256 records from the head — the
advanced head is persisted **immediately** at refill, so a crash can leak consumed-but-unserved
pages but can never resurrect an already-allocated one (no double allocation). When the on-disk
queue is empty, the write buffer is transferred to the read buffer (write-buffer entries are
always the newest, so FIFO and epoch monotonicity hold; checking the read-buffer head suffices).
Crash loss is bounded to ≤ 2 × 256 pages of leaked free space, never corruption. Serialized by
one ReentrantLock; hot paths are plain memory ops, disk i/o happens once per batch. The log does
raw i/o through `DiscStore` (never the pool), so it never competes with tree pages for frames.

## Operation flows

**get(key)**: `enterOp` → read root id → `nextNode(key)` down the tree (follows right links
when key > fence) → binary search in leaf → `exitOp`. Fully latch-free at the tree level.

**insert(key, value)**: `enterOp` → descend root→leaf, pushing ancestors only when following a
**downlink** (right-link hops are not pushed) → latch leaf → latch-couple right while
`shouldGoRight` → `copyWith` (copy-on-write) → if split: write right node, rewrite left, then
insert separator into parent from the ancestor stack (or `descendToLevel` re-descent if the
stack ran dry) via `latchLiveNode` (latches a parent candidate, then follows links while it is
marked or its fence < guide key) → cascade up while parents split → new root if the root split →
`exitOp`.

**delete(key)**: descend + latch leaf as in insert → `without(key)` (null = key absent, done) →
while node is underfilled and has a right link: latch right sibling and parent, verify the two
are still adjacent downlinks in the parent (binary search both fences), merge right into left,
write a tombstone over the right node, `retire` its page, remove the downlink from the parent,
ascend and repeat → finally `collapseRootIfSingleChild` (tombstones the old root, retires it,
publishes the child as the new root) → `exitOp`.

## Code patterns to follow

- Nodes are value objects: every mutation returns a **new** node (`copyWith`, `without`,
  `mergeWithRight`, `removeDownlink`). `WriteHandle.get()` re-deserializes on each call, so the
  tree code always works on fresh snapshots and publishes with `handle.set(node)`.
- Tree ops are written as: public method = epoch bracket (`enterOp`/`try`/`finally exitOp`),
  private `*UnderEpoch` = algorithm. Keep that split.
- `BufferedPool.acquire(id)` encapsulates the "lookup → pin → verify pageId under ioLock →
  retry if repurposed" dance — callers receive a pinned frame that provably holds their page.
  Content access still requires the frame's `ioLock`; release with `frame.unpin()`.
- `-1L` is the universal "none" for links/next pointers; `Value.infinity()` for open fences.
- Sizes are computed by hand (`Node.size(...)`) to decide split/merge — keep them consistent
  with `PageCodec`'s actual layout when changing the format.
