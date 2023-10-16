import os
import datetime

DATA = {
    "items": [
        {
            "orderbook_timestamp": datetime.datetime.fromtimestamp(
                1697324643644 / 1000, datetime.timezone.utc
            ),
            "book": "btc_mxn",
            "bid": {
                "o": "k9GINvrRHPvJKD7V",
                "r": 486460,
                "a": 0.37232272,
                "v": 90.0,
                "t": 1,
                "d": 1697324643455,
                "s": "undefined",
            },
            "ask": {
                "o": "QwBFswzKUOBbYnYU",
                "r": 486890,
                "a": 0.37232272,
                "v": 100.0,
                "t": 0,
                "d": 1697324643192,
                "s": "undefined",
            },
            "spread": 10.0,
        }
    ],
    "timestamp": datetime.datetime.fromtimestamp(
        1697324643644 / 1000, datetime.timezone.utc
    ),
}


def test_persist_folder_does_not_exists():
    OUTPUT_BASE_PATH = "./tmp_{}".format(
        int(datetime.datetime.now().timestamp() * 1000000)
    )
    assert not os.path.exists(OUTPUT_BASE_PATH)
    os.environ["OUTPUT_BASE_PATH"] = OUTPUT_BASE_PATH

    EXPECTED_OUTPUT_BASE_PATH = os.path.join(OUTPUT_BASE_PATH, "2023/10/14/23")
    EXPECTED_OUTPUT_FILENAME = os.path.join(
        EXPECTED_OUTPUT_BASE_PATH, "spread_2023-10-14T23_04_03.csv"
    )
    from bidaskspread import persist

    persist_data = persist.PersistData()
    persist_data.persist(DATA)

    assert os.path.exists(EXPECTED_OUTPUT_BASE_PATH)
    assert os.path.exists(EXPECTED_OUTPUT_FILENAME)
