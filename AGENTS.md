# RockyDB — Agent Guide

Embedded, single-file key-value database in Java 21 (Gradle). Storage engine is a **B-Link tree**
(Lehman & Yao) with merge/absorb logic inspired by Shasha & Lanin, PostgreSQL-style **slotted
pages** operated on as raw byte arrays (no node materialization), a PostgreSQL-style
**clock-sweep buffer pool**, and a **FIFO freelist guarded by epochs**. Package: `org.rockydb`
(flat, single package).

Build/test: `./gradlew build`, `./gradlew test`. Tests: JUnit 6 + Mockito, one test class per component.

> **WARNING: there is NO WAL.** Dirty pages reach disk only on buffer eviction or `close()`.
> A crash mid-write can lose data or corrupt the tree (e.g. a split writes the right sibling
> before the left/parent — a crash between those writes leaks or loses pages). WAL is on the TODO list.
> There is also no fsync. Treat durability as "clean shutdown only".

## Layering (top → bottom)

- `Database` — multi-table facade. The **catalog is itself a B-Link tree** mapping
  `table-name → (root page id, element count)` (16-byte values). Each table gets a
  `BLinkTree` with a `TableRootRef` that persists root-id changes back into the catalog
  and caches the element count (written back on root change and on `Database.close()`).
  `createTable`/`dropTable` are serialized
  by `catalogLock`; `dropTable` walks the tree and retires every page.
- `BLinkTree` — all tree algorithms (get/insert/delete/split/merge/root-collapse) over the
  `Store` interface, plus an O(1) `count()`. `RootRef` abstracts where the tree's
  persistent metadata lives — root id **and cached element count** (file header for the
  standalone/catalog tree, catalog entry for tables).
- `NodeStore implements Store` — page-level storage: tree latching
  (`latchForWrite`/`latchForWriteBlank` → `PageWrite`, `readPage` → `PageRef`), tree root id,
  bootstrap of a fresh file (root leaf +
  free-log page), and safe page recycling (`EpochManager` + `FreeLog`). Composed of a
  `DiscStore` and a `BufferedPool`.
- `BufferedPool` — raw byte page cache over `DiscStore`: fixed `Frame[]`, `pageToFrame`
  ConcurrentHashMap, `Clock` eviction. API is `Frame acquire(long id)` /
  `Frame acquireBlank(long id)` (skips the disk read for freshly allocated pages) + stats +
  flush-on-close; knows nothing about page contents. Loads read straight into the frame's
  byte array (no temp buffer, no copy).
- `DiscStore` — deliberately dumb: `FileChannel` raw page i/o (page id = file offset /
  `PAGE_SIZE`, reads/writes loop until the buffer is full), `allocateRaw`, and the file
  header (first 40 bytes): root id @0, FreeLog
  head @8, epoch @16, FreeLog tail @24, element count @32. No knowledge of nodes, epochs
  or freelist semantics.
- `FreeLog` + `EpochManager` — safe page recycling, owned directly by `NodeStore`.
- `Pages` / `LeafPage` / `BranchPage` — static operations **directly on the page `byte[]`**:
  binary search over the slot array, in-place insert/remove, compaction, byte-balanced
  splits, merges, tombstones. There are no node objects: tree code never deserializes a
  page; a `Value` is materialized only at the API boundary (a `get` hit, an emitted
  `entries()` entry). `Value` — `byte[]` wrapper, unsigned
  lexicographic order; `Value.infinity()` (null bytes) is the +∞ fence sentinel.

## On-disk / in-memory page format

`PAGE_SIZE = 8KB`. Pages are **slotted** (PostgreSQL-style): fixed header, slot array
growing up, tuple region growing down. Header = 21 bytes: flags byte (bit0 is-leaf,
bit1 marked, bit2 branch ∞-last-key), u16 slot count, u16 height, i64 link (-1 = rightmost),
u16 upper (tuple-region start), u16 deadBytes (fragmented space), u16 fenceOff + u16
fenceLen (leaves only; `0xFFFF` = +∞). Slot = `keyOff u16 | keyLen u16`. Leaf tuple =
`key | valueLen i32 | value`; branch tuple = `key | childPtr i64`. The leaf fence is a
standalone header-referenced tuple, never aliased to a slot key. Branch fence = key of the
last slot; the +∞ downlink of a rightmost child is stored as a 0-length key with the
`FLAG_INF_LAST` flag bit (only the last slot may carry it).
**Branch invariants: `count == keys == pointers` and `keys[i]` is the maximum (fence) key
of subtree `pointers[i]`** — fence keys, not separators.

Free space = `upper - (21 + count*4)` contiguous + `deadBytes` fragmented (reclaimed by
compaction). Sizing is **exact and O(1)** from the header — split when an insert does not
fit after one compaction; merge when live bytes < 30% (`MERGE_THRESHOLD`) of the usable
body. Mutations are preflighted: an op that reports "split needed" leaves the page
logically unmodified, and the split (`splitWithEntry`) embeds the pending entry in the
byte-balanced halves (single split, no retry loop).

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

1. **Tree latches (writers).** `Store.latchForWrite(id)` returns a `PageWrite` holding the
   frame's `treeLatch` (ReentrantLock) and a pin for the whole latch window — but **not**
   the `ioLock`: the holder mutates a private **shadow copy** of the page bytes, so
   readers keep reading the last published version concurrently and never block for the
   latch window. The pin guarantees the frame isn't repurposed while latched, so
   frame↔page identity is stable. Lock order per page is `treeLatch` → `ioLock`; across
   pages writers **latch-couple** left→right→up (latch next, then close previous), which
   keeps the acquisition order acyclic. `markDirty()` after a real mutation — it also
   **publishes the shadow immediately** (one arraycopy under the `ioLock` write lock, the
   only point readers stall): the merge path tombstones the right sibling while the
   absorbing left page is still latched, so the survivor's absorbed content must be
   visible before its own `close()` or readers routing around the tombstone could miss
   keys. `close()` re-publishes if dirty; latched-but-unmodified pages stay clean and are
   never published. Reads never take tree latches.
2. **Buffer pool (physical).** `Frame.pinCount` (AtomicInteger) — `Clock.findVictim()` CASes
   0→1 (`tryClaim`) to exclusively claim a victim; PostgreSQL-style sweep decrements `usageCount`
   (capped at `MAX_USAGE = 5`), evicting only a claimed frame with usage 0. `Frame.ioLock`
   (ReentrantReadWriteLock) guards the byte buffer: read lock held by `PageRef` for the
   whole read window, write lock only for the brief shadow-publish copy and by
   evict/repurpose. `BufferedPool.acquire(id)` internally does the
   "lookup → pin → verify pageId under ioLock → retry" dance, so it always returns a pinned
   frame that provably holds that page (a pinned frame can never be repurposed). Eviction of a
   dirty frame writes it to disk. If all frames stay pinned through the sweeps,
   `BufferExhaustedException` is thrown. Scan reads (`entries()`) skip the usage bump so a
   full-table scan does not evict the hot working set.
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

**get(key)**: `enterOp` → read root id → `nextNode(key)` down the tree (raw binary search
per page, right links when key > fence; zero allocation) → at the leaf, one `Value` copy
on a hit → `exitOp`. Fully latch-free at the tree level.

**insert(key, value)**: `enterOp` → descend root→leaf, pushing ancestors only when following a
**downlink** (right-link hops are not pushed) → latch leaf → latch-couple right while
`shouldGoRight` → `LeafPage.put` in place → if split: `splitWithEntry` rewrites left and
formats the blank-latched right sibling with the pending entry embedded in the halves,
then `insertSplit` into the parent from the ancestor stack (or `descendToLevel` re-descent
if the stack ran dry) via `latchLiveNode` (latches a parent candidate, then follows links
while it is marked or its fence < guide key) → cascade up while parents split
(`BranchPage.splitWithEntry` embeds the pending downlink change) → new root if the root
split → `exitOp`. The promoted key travels as raw bytes; nothing is materialized.

**delete(key)**: descend + latch leaf as in insert → `LeafPage.remove` (false = key absent,
done, page left clean) → while node is underfilled (O(1) header check) and has a right
link: latch right sibling and parent, verify the two are still adjacent downlinks in the
parent (raw binary search of both fences), `mergeFrom` right into left,
write a tombstone over the right page, `retire` it, `removeDownlink` from the parent,
ascend and repeat → `collapseRootIfSingleChild` (tombstones the old root, retires it,
publishes the child as the new root) — but only when this delete actually merged:
downlink removal is the only way the root can shrink to a single child, and a previous
delete always leaves the root non-collapsible → `exitOp`.

**count()**: O(1) cached counter (MyISAM-style — there is no MVCC, so a single exact count
is well-defined; InnoDB/PostgreSQL must scan because MVCC makes per-transaction counts
differ). `addValue` does an exact existence check under the leaf latch and increments only
for brand-new keys (replacing a value leaves the count unchanged); `delete` decrements only
when the key was present. The counter is an `AtomicLong` behind `RootRef`, kept in memory
during operation and persisted lazily: header @32 for the standalone/catalog tree (written
on `NodeStore.close()`, like the epoch), the 16-byte catalog entry for tables (on root
change and on `Database.close()`). Zero per-op I/O; like everything else pre-WAL, a crash
can leave the persisted value stale.

**entries()**: full-table scan returning a **lazy** `Iterator<BLinkTree.Entry>` (key, value)
in ascending key order. Descends to the leftmost leaf (leftmost downlink at every level,
routing around root-collapse/merge tombstones via `link`), then streams the leaf level one
**8KB page snapshot** (single array copy) at a time — memory is O(page), so tables larger
than RAM can be scanned; entries are materialized one at a time from the snapshot.
Scan reads skip the clock-usage bump, so a scan does not evict the hot working set.
The iterator **holds its epoch ticket for its whole lifetime** and releases it when
exhausted or when a read fails; callers must consume it fully — an abandoned iterator leaks
its ticket and stalls FreeLog recycling (accepted risk, internal use only). Between calls
the only state is `currentId` + `lastKey` + the current snapshot; concurrent
splits/merges mid-scan are tolerated (marked nodes route to their survivor, keys `<=` the
last emitted key are skipped), so stable entries are never missed or duplicated. Weak
consistency: keys inserted behind or deleted ahead of the scan position may not appear.

## Code patterns to follow

- There are **no node objects**: all tree logic is static ops on the page `byte[]`
  (`Pages`, `LeafPage`, `BranchPage`). Writers mutate the `PageWrite` shadow copy and
  call `markDirty()` (publishes it over the frame bytes immediately); readers hold a
  `PageRef` (pinned + read-locked) and close it promptly (try-with-resources) — it
  stalls a writer's publish while held.
- Mutating page ops **preflight-then-mutate**: a `SPLIT_NEEDED`/`false` result leaves the
  page logically unmodified (a compaction may have run). Only the split/merge paths
  allocate (gathered copies, a compaction scratch) — hot paths are allocation-free
  (one 8KB shadow copy per write latch excepted; reads stay allocation-free).
- Tree ops are written as: public method = epoch bracket (`enterOp`/`try`/`finally exitOp`),
  private `*UnderEpoch` = algorithm. Keep that split.
- `BufferedPool.acquire(id)` encapsulates the "lookup → pin → verify pageId under ioLock →
  retry if repurposed" dance — callers receive a pinned frame that provably holds their page.
  `acquireBlank(id)` is the same dance without the disk read, for freshly allocated pages.
- `-1L` is the universal "none" for links/next pointers; leaf +∞ fence is the `0xFFFF`
  fenceLen sentinel, branch +∞ downlink is a 0-length key + `FLAG_INF_LAST`;
  `Value.infinity()` (null bytes) is the API-level +∞.
- Sizing is exact from the page header (`Pages.liveBytes`/`contiguousFree`/`deadBytes`) —
  never recompute sizes by walking keys.
