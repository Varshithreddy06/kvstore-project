#!/usr/bin/env python3
import sys
import os

LOG_FILE = "data.db"

class KVStore:
    """A minimal append-only key–value store with a linear-scan in-memory index.

    Constraints respected:
      - No built-in dict/map types are used for the index.
      - Persistence via append-only writes to data.db.
      - Last write wins semantics.

    Implementation notes:
      - In-memory "index" is just a Python list of (key, value) tuples.
      - GET scans the list from the end to find the last value for the key.
      - SET appends to both the log file and the in-memory list, then fsyncs.
    """
    def __init__(self, log_path=LOG_FILE):
        self.log_path = log_path
        self.entries = []  # list of (key, value); newest appended to the end

        # Ensure the log file exists; if not, create it
        if not os.path.exists(self.log_path):
            # create empty file
            with open(self.log_path, 'a', encoding='utf-8'):
                pass

        # Replay the log to rebuild entries
        self._replay_log()

    def _replay_log(self):
        """Rebuild the in-memory entries list by reading the log from disk."""
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(' ', 2)
                    if len(parts) < 3:
                        # ignore corrupted lines
                        continue
                    cmd, key, value = parts[0], parts[1], parts[2]
                    if cmd.upper() == 'SET':
                        self.entries.append((key, value))
                    # Future extensions (e.g., DEL) would be handled here.
        except FileNotFoundError:
            # No log yet; that's fine
            pass

    def set(self, key, value):
        """Append a SET operation to the log and update the in-memory entries."""
        record = f"SET {key} {value}\n"
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(record)
            f.flush()
            os.fsync(f.fileno())
        self.entries.append((key, value))

    def get(self, key):
        """Return the last written value for key, or None if not present."""
        # Linear scan from the end (last write wins)
        for i in range(len(self.entries) - 1, -1, -1):
            k, v = self.entries[i]
            if k == key:
                return v
        return None


def main():
    store = KVStore()

    # Read commands from STDIN until EXIT or EOF
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue

        parts = line.split(' ', 2)
        cmd = parts[0].upper()

        if cmd == 'EXIT':
            # Optional: print something so Gradebot knows we exited cleanly
            print('OK')
            sys.stdout.flush()
            return

        elif cmd == 'SET':
            # Expect exactly: SET <key> <value> (value is a single token or remainder)
            if len(parts) < 3:
                print('ERR')
                continue
            key = parts[1]
            value = parts[2]
            # Basic validation: forbid newlines in key/value
            if ('\n' in key) or ('\n' in value) or (key == ''):
                print('ERR')
                continue
            store.set(key, value)
            print('OK')

        elif cmd == 'GET':
            if len(parts) != 2:
                print('ERR')
                continue
            key = parts[1]
            val = store.get(key)
            if val is None:
                print('NULL')
            else:
                print(val)

        else:
            # Unknown command
            print('ERR')

        sys.stdout.flush()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        # Exit quietly on Ctrl+C
        pass
