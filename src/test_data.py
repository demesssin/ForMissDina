import copy
import random

DUPLICATES_COUNT = 5
DAY_MS = 86_400_000


def build_invalid_test_records(template: dict) -> list:
    """Create 5 invalid records based on a real one. Each has a unique, past open_time."""
    base_open = int(template["open_time"])
    tests = []

    def make(offset_days: int, **changes) -> dict:
        rec = copy.deepcopy(template)
        rec["open_time"] = base_open - offset_days * DAY_MS
        rec["close_time"] = rec["open_time"] + 3_599_999
        rec.update(changes)
        rec["is_test"] = True
        return rec

    tests.append(make(1000, close=-1234.5))                          # negative price
    tests.append(make(1001, symbol="FAKECOIN"))                      # symbol not allowed
    tests.append(make(1002, high=float(template["low"]) * 0.5))      # high < low
    tests.append(make(1003, volume=None))                            # missing volume
    tests.append(make(1004, close_time=base_open - 1004 * DAY_MS - 5))  # close_time before open_time
    return tests


def build_duplicates(records: list, seed: int = 42) -> list:
    rng = random.Random(seed)
    return [copy.deepcopy(r) for r in rng.sample(records, min(DUPLICATES_COUNT, len(records)))]
