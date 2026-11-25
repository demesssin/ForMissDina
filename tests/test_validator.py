from src.validator import run_self_test


def test_schema_detects_invalid_records():
    run_self_test()


if __name__ == "__main__":
    test_schema_detects_invalid_records()
    print("PASSED")
