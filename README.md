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

### Prerequisites
- Go 1.22.2 or higher
- Git

### Steps
```bash
git clone https://github.com/201901407/bitcask-go.git
cd bitcask
go run main.go
```

## 💾 Usage

This store supports the following operations:

| Command                  | Description                              | Example                          |
|--------------------------|------------------------------------------|----------------------------------|
| `set <key> <value>`      | Sets a key-value pair in the store       | `set name Alice`                |
| `get <key>`              | Retrieves the value for a given key      | `get name`                      |
| `delete <key>`           | Deletes the key-value pair from the store | `delete name`                   |
| `stop`                   | Gracefully shuts down the store          | `stop`                          |

## 📂 Project Structure

```
bitcask/
├── go.mod                # Go module definition
├── main.go               # Entry point for the application
├── README.md             # Project documentation
└── store/
    └── bitcask_store.go  # Core implementation of the Bitcask KV store
```