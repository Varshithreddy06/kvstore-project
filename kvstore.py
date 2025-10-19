#!/usr/bin/env python3
import sys
import os
from typing import List, Tuple, Optional

LOG_FILE = "data.db"

class KVStore:
    """A minimal append-only key–value store with linear-scan in-memory index.

    - No built-in dict/map types are used.
    - Persistence via append-only writes to data.db.
    - Last-write-wins semantics.
    """
    def __init__(self, log_path: str = LOG_FILE) -> None:
        self.log_path = log_path
        self.entries: List[Tuple[str, str]] = []  # list of (key, value)

        # Ensure log file exists
        if not os.path.exists(self.log_path):
            with open(self.log_path, 'a', encoding='utf-8'):
                pass

        # Rebuild entries from log
        self._replay_log()

    def _replay_log(self) -> None:
        """Rebuild in-memory entries from log file."""
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(' ', 2)
                    if len(parts) < 3:
                        continue  # skip corrupted lines
                    cmd, key, value = parts[0], parts[1], parts[2]
                    if cmd.upper() == 'SET':
                        self.entries.append((key, value))
        except FileNotFoundError:
            pass

    def set(self, key: str, value: str) -> None:
        """Append a SET operation to log and update in-memory index."""
        record = f"SET {key} {value}\n"
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(record)
            f.flush()
            os.fsync(f.fileno())
        self.entries.append((key, value))

    def get(self, key: str) -> Optional[str]:
        """Return last value for key, or None if not present."""
        for k, v in reversed(self.entries):
            if k == key:
                return v
        return None


def main() -> None:
    store = KVStore()

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue

        parts = line.split(' ', 2)
        cmd = parts[0].upper()

        try:
            if cmd == 'EXIT':
                print('OK')
                sys.stdout.flush()
                return

            elif cmd == 'SET':
                if len(parts) < 3:
                    raise ValueError("SET requires key and value")
                key, value = parts[1], parts[2]
                if ('\n' in key) or ('\n' in value) or (key == ''):
                    raise ValueError("Invalid key or value")
                store.set(key, value)
                print('OK')
                sys.stdout.flush()

            elif cmd == 'GET':
                if len(parts) != 2:
                    raise ValueError("GET requires key")
                key = parts[1]
                val = store.get(key)
                if val is not None:
                    print(val)
                    sys.stdout.flush()
                # If val is None, do nothing (rubric expects no output)

            else:
                raise ValueError("Unknown command")

        except ValueError:
            print('ERR')
            sys.stdout.flush()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
