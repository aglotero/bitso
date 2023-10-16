import datetime


class ComputeSpread(object):
    def __init__(self):
        """Compute the bid/ask spread"""
        pass

    def compute_spread(self, data):
        """Compute the bid/ask spread

        Giving a message from the Bitso websocket compute the spread.
        """
        best_ask = data["payload"]["asks"][0]
        best_bid = data["payload"]["bids"][0]
        spread = (best_ask["v"] - best_bid["v"]) * 100 / best_ask["v"]

        return {
            "orderbook_timestamp": datetime.datetime.fromtimestamp(
                data["sent"] / 1000, datetime.timezone.utc
            ),
            "book": data["book"],
            "bid": best_bid,
            "ask": best_ask,
            "spread": spread,
        }
