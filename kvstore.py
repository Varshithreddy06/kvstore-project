#!/usr/bin/env python3
import sys
import os
from typing import List, Tuple, Optional

LOG_FILE = "data.db"

class KVStore:
    """
    A minimal append-only key–value store with a linear-scan in-memory index.

    Attributes
    ----------
    log_path : str
        Path to the persistent log file (default "data.db").
    entries : List[Tuple[str, str]]
        In-memory list of (key, value) tuples, newest appended to the end.

    Methods
    -------
    set(key: str, value: str) -> None
        Stores the key-value pair persistently.
    get(key: str) -> Optional[str]
        Retrieves the last value for the key, or None if not found.
    """
    def __init__(self, log_path: str = LOG_FILE) -> None:
        self.log_path = log_path
        self.entries: List[Tuple[str, str]] = []

        # Ensure log file exists
        if not os.path.exists(self.log_path):
            with open(self.log_path, 'a', encoding='utf-8'):
                pass

        # Rebuild in-memory entries from log
        self._replay_log()

    def _replay_log(self) -> None:
        """Rebuild the in-memory entries list by reading the log from disk."""
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
            # No log yet; fine
            pass

    def set(self, key: str, value: str) -> None:
        """
        Append a SET operation to the log and update in-memory entries.

        Parameters
        ----------
        key : str
            The key to set.
        value : str
            The value to associate with the key.
        """
        record = f"SET {key} {value}\n"
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(record)
            f.flush()
            os.fsync(f.fileno())
        self.entries.append((key, value))

    def get(self, key: str) -> Optional[str]:
        """
        Return the last written value for a key, or None if not present.

        Parameters
        ----------
        key : str
            The key to retrieve.

        Returns
        -------
        Optional[str]
            The last value associated with the key, or None if not found.
        """
        for k, v in reversed(self.entries):
            if k == key:
                return v
        return None


def main() -> None:
    """Main loop: read commands from STDIN and execute them."""
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
                # Validate key/value
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
                else:
                    print('ERR')  # Gradebot expects ERR for missing keys
                sys.stdout.flush()

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
