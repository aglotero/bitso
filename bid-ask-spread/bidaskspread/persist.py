import os
import csv


class PersistData(object):
    def __init__(self):
        """Persist spread data into CSV files

        Gather the output path from the OUTPUT_BASE_PATH env var.

        If needed it will create the paths.
        """
        from . import constants

        self.base_folder = constants.OUTPUT_BASE_PATH

        if not os.path.exists(self.base_folder):
            os.makedirs(self.base_folder)

    def persist(self, data):
        """Persists a list of spreads on disk

        If needed create the directory structure:
         (OUTPUT_BASE_PATH/YEAR/MONTH/DAY/HOUR).

        """

        spread_date = data["timestamp"]

        folder_path = "{base_folder}/{year}/{month}/{day}/{hour}/".format(
            base_folder=self.base_folder,
            year=str(spread_date.year),
            month="{:02d}".format(spread_date.month),
            day="{:02d}".format(spread_date.day),
            hour="{:02d}".format(spread_date.hour),
        )

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        formated_data = []

        for item in data["items"]:
            date_iso = item["orderbook_timestamp"].isoformat()
            formated_data.append(
                {
                    "orderbook_timestamp": date_iso,
                    "book": item["book"],
                    "ask": item["ask"]["v"],
                    "bid": item["bid"]["v"],
                    "spread": item["spread"],
                }
            )

        CSV_COLUMNS = ["orderbook_timestamp", "book", "ask", "bid", "spread"]
        output_file_name = "bid_ask_spread_{timestamp}.csv".format(
            timestamp=spread_date.isoformat()[:-13].replace(":", "_"),
        )

        with open(
            os.path.join(folder_path, output_file_name), "w", newline=""
        ) as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            for item in formated_data:
                writer.writerow(item)
