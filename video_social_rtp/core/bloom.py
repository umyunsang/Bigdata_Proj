from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Iterable, Tuple


def _hash_item(value: str, seed: int) -> int:
    data = f"{value}|{seed}".encode("utf-8")
    digest = hashlib.sha256(data).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


@dataclass
class BloomFilter:
    size_bits: int
    num_hashes: int
    bitfield: bytearray
    count: int = 0

    def __post_init__(self) -> None:
        if self.size_bits < 8:
            self.size_bits = 8
        self.num_hashes = max(1, self.num_hashes)
        required_bytes = (self.size_bits + 7) // 8
        if len(self.bitfield) < required_bytes:
            padding = required_bytes - len(self.bitfield)
            self.bitfield.extend(b"\x00" * padding)

    @classmethod
    def create(cls, capacity: int, error_rate: float = 0.01) -> "BloomFilter":
        capacity = max(1, capacity)
        error_rate = min(max(error_rate, 1e-6), 0.5)
        m = int(-capacity * math.log(error_rate) / (math.log(2) ** 2))
        m = max(m, capacity * 8)  # guard against tiny results
        k = max(1, int(round((m / capacity) * math.log(2))))
        return cls(size_bits=m, num_hashes=k, bitfield=bytearray((m + 7) // 8), count=0)

    @classmethod
    def from_bytes(cls, *, size_bits: int, num_hashes: int, payload: bytes, count: int) -> "BloomFilter":
        return cls(size_bits=size_bits, num_hashes=num_hashes, bitfield=bytearray(payload), count=count)

    def _bit_indexes(self, value: str) -> Iterable[int]:
        for seed in range(self.num_hashes):
            yield _hash_item(value, seed) % self.size_bits

    def add(self, value: str) -> None:
        for idx in self._bit_indexes(value):
            byte_index, bit_index = divmod(idx, 8)
            self.bitfield[byte_index] |= 1 << bit_index
        self.count += 1

    def add_all(self, values: Iterable[str]) -> None:
        for value in values:
            self.add(value)

    def might_contain(self, value: str) -> bool:
        for idx in self._bit_indexes(value):
            byte_index, bit_index = divmod(idx, 8)
            if not (self.bitfield[byte_index] & (1 << bit_index)):
                return False
        return True

    def serialize(self) -> Tuple[int, int, bytes, int]:
        return self.size_bits, self.num_hashes, bytes(self.bitfield), self.count


__all__ = ["BloomFilter"]
