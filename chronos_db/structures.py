"""Rich Redis-compatible data structures (Hashes, Lists, Sets) on ChronosEngine."""

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

if TYPE_CHECKING:
    from .engine import ChronosEngine


class DataStructures:
    """Manages structured data types (Hashes, Lists, Sets) on top of ChronosDB KV engine."""

    def __init__(self, engine: "ChronosEngine"):
        self.engine = engine

    # --------------------------------------------------------------------------
    # Hashes (HSET, HGET, HDEL, HGETALL, HEXISTS, HLEN)
    # --------------------------------------------------------------------------

    def hset(self, key: str, field: str, value: str) -> int:
        """
        Set field in hash to value.
        Returns 1 if field is new, 0 if field was updated.
        """
        with self.engine._lock:
            hash_dict = self._load_hash(key)
            is_new = 1 if field not in hash_dict else 0
            hash_dict[field] = str(value)
            self._save_hash(key, hash_dict)
            return is_new

    def hset_multiple(self, key: str, mapping: Dict[str, str]) -> int:
        """Set multiple fields in hash. Returns number of fields added."""
        with self.engine._lock:
            hash_dict = self._load_hash(key)
            added = 0
            for f, v in mapping.items():
                if f not in hash_dict:
                    added += 1
                hash_dict[f] = str(v)
            self._save_hash(key, hash_dict)
            return added

    def hget(self, key: str, field: str) -> Optional[str]:
        """Get value of field in hash. Returns None if field or key does not exist."""
        with self.engine._lock:
            hash_dict = self._load_hash(key)
            return hash_dict.get(field)

    def hdel(self, key: str, *fields: str) -> int:
        """Delete one or more fields from hash. Returns number of removed fields."""
        with self.engine._lock:
            hash_dict = self._load_hash(key)
            removed = 0
            for f in fields:
                if f in hash_dict:
                    del hash_dict[f]
                    removed += 1
            if removed > 0:
                if hash_dict:
                    self._save_hash(key, hash_dict)
                else:
                    self.engine.delete(key)
            return removed

    def hgetall(self, key: str) -> Dict[str, str]:
        """Return all fields and values in hash."""
        with self.engine._lock:
            return self._load_hash(key)

    def hexists(self, key: str, field: str) -> bool:
        """Check if field exists in hash."""
        with self.engine._lock:
            hash_dict = self._load_hash(key)
            return field in hash_dict

    def hlen(self, key: str) -> int:
        """Return number of fields in hash."""
        with self.engine._lock:
            return len(self._load_hash(key))

    def _load_hash(self, key: str) -> Dict[str, str]:
        val = self.engine.get(key)
        if val is None:
            return {}
        try:
            parsed = json.loads(val.decode("utf-8"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _save_hash(self, key: str, hash_dict: Dict[str, str]) -> None:
        self.engine.put(key, json.dumps(hash_dict).encode("utf-8"))

    # --------------------------------------------------------------------------
    # Lists (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
    # --------------------------------------------------------------------------

    def lpush(self, key: str, *values: str) -> int:
        """Insert elements at the head of the list. Returns new length."""
        with self.engine._lock:
            lst = self._load_list(key)
            for v in values:
                lst.insert(0, str(v))
            self._save_list(key, lst)
            return len(lst)

    def rpush(self, key: str, *values: str) -> int:
        """Insert elements at the tail of the list. Returns new length."""
        with self.engine._lock:
            lst = self._load_list(key)
            for v in values:
                lst.append(str(v))
            self._save_list(key, lst)
            return len(lst)

    def lpop(self, key: str) -> Optional[str]:
        """Remove and return first element of list."""
        with self.engine._lock:
            lst = self._load_list(key)
            if not lst:
                return None
            val = lst.pop(0)
            if lst:
                self._save_list(key, lst)
            else:
                self.engine.delete(key)
            return val

    def rpop(self, key: str) -> Optional[str]:
        """Remove and return last element of list."""
        with self.engine._lock:
            lst = self._load_list(key)
            if not lst:
                return None
            val = lst.pop()
            if lst:
                self._save_list(key, lst)
            else:
                self.engine.delete(key)
            return val

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """Get range of elements from list (inclusive indices, supports negative)."""
        with self.engine._lock:
            lst = self._load_list(key)
            if not lst:
                return []
            length = len(lst)
            s = start if start >= 0 else max(0, length + start)
            e = stop if stop >= 0 else max(0, length + stop)
            if s > e or s >= length:
                return []
            return lst[s : e + 1]

    def llen(self, key: str) -> int:
        """Return length of list."""
        with self.engine._lock:
            return len(self._load_list(key))

    def _load_list(self, key: str) -> List[str]:
        val = self.engine.get(key)
        if val is None:
            return []
        try:
            parsed = json.loads(val.decode("utf-8"))
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def _save_list(self, key: str, lst: List[str]) -> None:
        self.engine.put(key, json.dumps(lst).encode("utf-8"))

    # --------------------------------------------------------------------------
    # Sets (SADD, SMEMBERS, SREM, SISMEMBER, SCARD)
    # --------------------------------------------------------------------------

    def sadd(self, key: str, *members: str) -> int:
        """Add members to set. Returns number of new members added."""
        with self.engine._lock:
            st = self._load_set(key)
            added = 0
            for m in members:
                m_str = str(m)
                if m_str not in st:
                    st.add(m_str)
                    added += 1
            if added > 0:
                self._save_set(key, st)
            return added

    def smembers(self, key: str) -> List[str]:
        """Return all members in set (sorted)."""
        with self.engine._lock:
            return sorted(list(self._load_set(key)))

    def srem(self, key: str, *members: str) -> int:
        """Remove members from set. Returns number of removed members."""
        with self.engine._lock:
            st = self._load_set(key)
            removed = 0
            for m in members:
                m_str = str(m)
                if m_str in st:
                    st.remove(m_str)
                    removed += 1
            if removed > 0:
                if st:
                    self._save_set(key, st)
                else:
                    self.engine.delete(key)
            return removed

    def sismember(self, key: str, member: str) -> bool:
        """Check if member belongs to set."""
        with self.engine._lock:
            st = self._load_set(key)
            return str(member) in st

    def scard(self, key: str) -> int:
        """Return cardinality (size) of set."""
        with self.engine._lock:
            return len(self._load_set(key))

    def _load_set(self, key: str) -> Set[str]:
        val = self.engine.get(key)
        if val is None:
            return set()
        try:
            parsed = json.loads(val.decode("utf-8"))
            return set(parsed) if isinstance(parsed, list) else set()
        except Exception:
            return set()

    def _save_set(self, key: str, st: Set[str]) -> None:
        self.engine.put(key, json.dumps(sorted(list(st))).encode("utf-8"))
