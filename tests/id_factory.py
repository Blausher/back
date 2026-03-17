from itertools import count
from random import randint


_id_sequence = count(randint(1_000_000, 9_000_000))


def new_id() -> int:
    return next(_id_sequence)
