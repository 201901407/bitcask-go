# Bitcask KV Store (Go Implementation)

A high-performance, embeddable, **log-structured key-value store** inspired by [Bitcask](https://riak.com/assets/bitcask-intro.pdf). This project is written in Go and emphasizes simplicity, speed, and crash resilience.

## 🚀 Features
- ✅ **Append-only log** for all writes
- ✅ **In-memory hash index** (key → offset)
- ✅ **Idempotent segment loading**
- ✅ **Support for multiple data segments**
- ✅ **Efficient point lookups**

## 🛠 Installation

```bash
git clone https://github.com/201901407/bitcask.git
cd bitcask
go run main.go
```

## 💾 Usage
This store supports three operations: 
```
- set <key> <value>
- get <key>
- delete <key>
```
