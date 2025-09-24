"""
Quick demo of core streaming/approx algorithms mentioned in docs:
- Reservoir Sampling (Algorithm R)
- Bloom Filter (probabilistic membership)
- HyperLogLog (approx cardinality)
This is a lightweight educational example.
"""
from __future__ import annotations
import math
import os
import random
import hashlib

# Reservoir Sampling (Algorithm R)
def reservoir_sample(iterable, k: int, seed: int | None = 42):
    rnd = random.Random(seed)
    reservoir = []
    for i, item in enumerate(iterable, start=1):
        if i <= k:
            reservoir.append(item)
        else:
            j = rnd.randint(1, i)
            if j <= k:
                reservoir[j-1] = item
    return reservoir

# Simple Bloom Filter
def _hashes(x: bytes, m: int, k: int):
    # generate k hashes using SHA256 variants
    h1 = int.from_bytes(hashlib.sha256(x).digest(), 'big')
    h2 = int.from_bytes(hashlib.md5(x).digest(), 'big')
    for i in range(k):
        yield (h1 + i * h2) % m

class BloomFilter:
    def __init__(self, m_bits: int = 1024, k_hashes: int = 4):
        self.m = m_bits
        self.k = k_hashes
        self.bits = 0
    def add(self, item: str):
        for hv in _hashes(item.encode(), self.m, self.k):
            self.bits |= (1 << hv)
    def __contains__(self, item: str):
        for hv in _hashes(item.encode(), self.m, self.k):
            if not (self.bits >> hv) & 1:
                return False
        return True

# Tiny HyperLogLog-like sketch (not production)
class HyperLogLog:
    def __init__(self, p: int = 12):
        assert 4 <= p <= 16
        self.p = p
        self.m = 1 << p
        self.registers = [0] * self.m
    def add(self, item: str):
        x = int.from_bytes(hashlib.sha1(item.encode()).digest(), 'big')
        idx = x & (self.m - 1)
        w = x >> self.p
        rho = (w.bit_length() ^ (w ^ (1 << (w.bit_length()-1)) if w else 0)).bit_length() if w else self.p
        # simpler: count leading zeros of w plus 1
        if w == 0:
            r = self.p
        else:
            r = (w.bit_length() ^ (w ^ (1 << (w.bit_length()-1)))).bit_length()
        # use a simple leading-zero count
        r = (w.bit_length() and (64 - w.bit_length())) + 1 if w else 64
        self.registers[idx] = max(self.registers[idx], r)
    def count(self) -> float:
        alpha = 0.7213 / (1 + 1.079 / self.m)
        E = alpha * self.m * self.m / sum(2.0 ** -r for r in self.registers)
        return E

if __name__ == "__main__":
    # Reservoir
    stream = range(1, 10001)
    sample = reservoir_sample(stream, k=10)
    print("Reservoir sample:", sample)

    # Bloom filter
    bf = BloomFilter(m_bits=4096, k_hashes=5)
    for i in range(1000):
        bf.add(f"user:{i}")
    print("Contains user:5?", "user:5" in bf)
    print("Contains user:9999?", "user:9999" in bf)

    # HyperLogLog (toy)
    hll = HyperLogLog(p=12)
    for i in range(50000):
        hll.add(f"item:{i}")
    print("HLL approx count:", round(hll.count()))
