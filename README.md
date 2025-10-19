# Simple Append-Only Key–Value Store (Project 1)

A minimal persistent key–value store that supports:

```
SET <key> <value>
GET <key>
EXIT
```

## Features

- **Append-only** persistence to `data.db` with immediate `fsync` on every `SET`.
- **Crash-safe** recovery: on startup the log is replayed to rebuild the in-memory index.
- **In-memory index without dict/map**: uses a simple list of `(key, value)` pairs; `GET` does a reverse linear scan (last-write-wins).
- **CLI** reads from `STDIN` and writes to `STDOUT`, suitable for black-box testing.

## Run

```bash
python3 kvstore.py
```

Then type commands, e.g.:

```
SET name Alice
GET name
EXIT
```

### Nonexistent Keys

`GET <missing-key>` prints `NULL`.

## Files

- `kvstore.py` — the implementation
- `data.db` — created on first run; append-only log

## Design Notes

- **No built-in maps**: to keep Project 1 simple and compliant, we avoid `dict` entirely for indexing. We store tuples in a list and scan from the end on `GET`.
- **Last write wins**: because `GET` scans from the end, the most recent `SET` is returned.
- **Durability**: each `SET` calls `flush()` and `os.fsync()` before returning `OK`.

## Example Session

```
$ python3 kvstore.py
SET a 1
OK
SET b 2
OK
GET a
1
GET c
NULL
EXIT
OK
```

Restarting the program preserves data:

```
$ python3 kvstore.py
GET b
2
EXIT
OK
```

## Gradebot / Blackbox Testing

- **Work dir**: the directory containing `kvstore.py`.
- **Command**: `python3 kvstore.py`
- The tester will pipe commands to `STDIN` and read outputs on `STDOUT`.
- Ensure `data.db` is writable in the work dir. Include your screenshot as `gradebot_screenshot.png`.

## Future Work (Project 2 Hints)

- Replace the linear-scan list with your own B+ tree or custom hash table to make `GET` O(log n) or O(1).
- Add a `COMPACT`/`MERGE` operation to rewrite a smaller snapshot plus recent tail.
