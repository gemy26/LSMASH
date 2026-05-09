# LSMASH

A Log-Structured Merge-Tree storage engine in Go.

## Overview

A key-value store built from scratch featuring:
- In-memory MemTable (skip list)
- Immutable tiered SSTable levels
- Write-ahead logging (WAL)
- Bloom filters for fast lookups
- Tiered compaction

![](LSM%20Diagram.png)

## Project Structure

```
internal/
  engine/      - Engine API and server
  memtable/    - Skip list implementation
  sstable/     - Sorted string tables
  wal/         - Write-ahead log
  manifest/    - SSTable metadata and versioning
config/        - Configuration
```

## Components

- **Engine** - Coordinates writes and reads across memtable and SSTable levels
- **MemTable** - In-memory skip list for active writes
- **SSTable** - Immutable sorted tables on disk with bloom filters
- **WAL** - Write-ahead log for crash recovery
- **Manifest** - Tracks SSTable metadata and versions



```
┌────────────────────────────────────────┐
│ Header (28 bytes)                      │
│   MinKey · MaxKey · EntryCount         │
│   BloomSize · BloomOffset              │
├────────────────────────────────────────┤
│ Data Entries (17 bytes each)           │
│   Key (int64) · Value (int64)          │
│   Tombstone (1 byte)                   │
├────────────────────────────────────────┤
│ Bloom Filter                           │
│   Bit array (variable size)            │
└────────────────────────────────────────┘
```

- **Bloom Filter** — FNV double-hashing scheme with optimal `m` and `k` calculated from entry count and target false-positive rate (10%). Avoids unnecessary disk reads.
- **Binary Search** — Point lookups on sorted entries after bloom filter check.
- **Compaction** — Merges SSTables across adjacent levels using a multi-way merge iterator. Tombstoned entries are dropped during compaction.

### WAL (`internal/wal/`)
Write-ahead log for crash recovery. Every write is appended to the WAL before touching the memtable.

- **Binary format:** `Key (int64) | Value (int64) | Op (1 byte) | CRC32 (uint32)`
- **Integrity:** CRC32 checksum per record, verified on replay
- **Recovery:** On startup, the WAL is replayed to restore the memtable state
- **Lifecycle:** WAL is deleted after all immutable memtables are flushed to SSTables

### Manifest (`internal/mainfest/`)
Append-only JSON-lines log tracking SSTable file changes (additions and removals) across compaction events. Each record captures the operation type, affected files, and level.


