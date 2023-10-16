from . import constants
from . import calculate
from . import persist
import logging
import websocket
import json
import threading
import time
import queue
import datetime

compute_spread = calculate.ComputeSpread()
persist_data = persist.PersistData()
data_queue = queue.Queue()

logging.basicConfig(level=logging.DEBUG)


def process_queue():
    while True:
        logging.info(
            "process_queue is here! is_empty={is_empty}".format(
                is_empty=data_queue.empty()
            )
        )
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        data = []
        while not data_queue.empty():
            item = data_queue.get()
            data.append(item)

            # do not pick more items as they
            # are new from this execution
            if item["orderbook_timestamp"] > now:
                break

        if len(data) > 0:
            persist_data.persist({"items": data, "timestamp": now})

        logging.info("process_queue going to sleep")
        time.sleep(600)  # Sleep by 10 minutes


def on_message(ws, message):
    data = json.loads(message)
    if data["type"] == "orders":
        logging.info(message)
        spread = compute_spread.compute_spread(data)
        logging.info(spread)
        data_queue.put(spread)


def on_error(ws, error):
    logging.error(error)


def on_close(ws, close_status_code, close_msg):
    logging.info("### closed ###")


def on_open(ws):
    # When opening the connection to the websocket,
    # subscrive to receive book updates for btm_mxn book
    action = {
        "action": "subscribe",
        "book": constants.BOOK_TO_SUBSCRIBE,
        "type": "orders",
    }

    ws.send(json.dumps(action))


def main():
    # Starting the process_queue method
    threading.Thread(target=process_queue, daemon=True).start()

    # Defining and starting the websocket connection
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        constants.BITSO_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever()


main()
