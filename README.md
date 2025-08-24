# Bitcask-Go

A high-performance, embeddable, **log-structured key-value store** inspired by [Bitcask](https://riak.com/assets/bitcask-intro.pdf). This project is written in Go and emphasizes simplicity, speed, and crash resilience.

## 🚀 Features
- ✅ **Append-only log** for all writes
- ✅ **In-memory hash index** (key → offset)
- ✅ **Idempotent segment loading**
- ✅ **Support for multiple data segments**
- ✅ **Efficient point lookups**
- ✅ **Tombstone creation on key deletion**
- ✅ **Crash resilient**

## 🧠 How It Works

- Data is stored in **segment files** on disk in a serialized binary format.
- An in-memory **hash index** maps keys to offsets in these segment files.
- Deletion is handled via **tombstone entries**, written just like regular records.
- If the store crashes or is restarted, segments are **replayed** to reconstruct the in-memory index on startup.

## 🛠 Installation

```bash
git clone https://github.com/201901407/bitcask-go.git
cd bitcask
go run main.go
```

## 💾 Usage
This store supports three operations: 
```
- set <key> <value>: Sets key -> value in store
- get <key>: Get value with key <key>
- delete <key>: Delete value with key <key>
- stop: Shuts down the store. All previous data is persisted on disk.
```
