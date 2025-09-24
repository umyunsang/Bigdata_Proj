from __future__ import annotations

import random
from typing import Iterable, List, TypeVar

T = TypeVar("T")


def reservoir_sample(iterable: Iterable[T], k: int = 64) -> List[T]:
    sample: List[T] = []
    n = 0
    for x in iterable:
        n += 1
        if len(sample) < k:
            sample.append(x)
        else:
            j = random.randint(1, n)
            if j <= k:
                sample[j - 1] = x
    return sample

