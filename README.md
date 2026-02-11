# Bitcask-Lite KV Store (Go)

A lightweight, append-only, Bitcask-style key--value store implemented
in pure Go.

## Features Completed

-   Single writer + multi-reader concurrency using RWMutex
-   Append-only log storage format
-   In-memory index mapping keys to log offsets
-   Automatic index rebuild on startup
-   Tombstone-based delete
-   Benchmarks for Put/Get and safe parallel benchmarking
-   CLI interface

## Storage Format

    [4 bytes key size][4 bytes value size][key][value]

## How to Build

    go build -o kvstore ./cmd/kvcli

## How to Run

    ./kvstore put name Alice
    ./kvstore get name
    ./kvstore del name
    ./kvstore --help


## TODO

-   Hint file for faster startup
-   Compaction
-   Segmented logs
-   Bloom filters
-   Memory-mapped reads
-   Metrics & logging enhancements
-   CLI improvements
