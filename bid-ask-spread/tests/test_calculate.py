import datetime

DATA = {
    "type": "orders",
    "book": "btc_mxn",
    "payload": {
        "bids": [
            {
                "o": "k9GINvrRHPvJKD7V",
                "r": 486460,
                "a": 0.37232272,
                "v": 90.0,
                "t": 1,
                "d": 1697324643455,
                "s": "undefined",
            }
        ],
        "asks": [
            {
                "o": "QwBFswzKUOBbYnYU",
                "r": 486890,
                "a": 0.37232272,
                "v": 100.0,
                "t": 0,
                "d": 1697324643192,
                "s": "undefined",
            }
        ],
    },
    "sent": 1697324643644,
}


def test_one_spread():
    from bidaskspread import calculate

    compute_spread = calculate.ComputeSpread()

    expected_data = {
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

    data = compute_spread.compute_spread(DATA)

    assert data == expected_data, "Spread does not match with expected value"
