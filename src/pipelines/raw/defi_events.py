import requests
import pandas as pd
import os
import sys
import time
from argparse import ArgumentParser, Namespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from spectral_data_lib.data_lakehouse import DataLakehouse
from spectral_data_lib.log_manager import Logger


from config import settings
from src.pipelines.utils import get_latest_timestamp_in_data_lake_table_for_event


PAGINATION_SIZE = settings.EXTRACTION_PAGINATION_SIZE
MAX_TIMEWINDOW_DAYS = settings.MAX_TIMEWINDOW_DAYS
NUMBER_OF_THREADS = settings.NUMBER_OF_THREADS

data_lakehouse = DataLakehouse()
logger = Logger(logger_name=__file__.split("/")[-1].split(".")[0])
event_names = settings.EVENTS_NAMES


def insert_incoming_data_to_data_lake(incoming_data_df, event_name):
    """Inserts incoming data into the Data Lakehouse table.

    Args:
        incoming_data_df (pd.DataFrame): Incoming data.
        event_name (str): Event name.

    Returns:
        None. Inserts data into the Data Lakehouse table.
    """
    logger.info(f"Inserting {incoming_data_df.shape[0]} rows into Data Lakehouse")
    data_lakehouse.write_parquet_table(
        database_name="db_raw_prod",
        table_name=f"transpose_{event_name}_events",
        source="transpose",
        data=incoming_data_df,
        layer="raw",
        partition_columns=["year", "month"],
    )


def fetch_data(event_name, transpose_api_key):
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": transpose_api_key,
    }
    offset_amount = 0
    continue_fetching = True
    incoming_data_df = pd.DataFrame([])

    start_unixtimestamp = get_latest_timestamp_in_data_lake_table_for_event(event_name, "raw")
    end_unixtimestamp = start_unixtimestamp + (MAX_TIMEWINDOW_DAYS * 86400)
    while continue_fetching:
        try:
            json_data = {
                "sql": f"""
                SELECT
                    EXTRACT(EPOCH FROM TIMESTAMP) as timestamp_unixtimestamp,
                    *
                FROM ethereum.lending_events
                    WHERE
                        category = '{event_name}'
                        AND protocol_name IN ('aave', 'compound')
                        AND contract_version = 'v2'
                        AND EXTRACT(EPOCH FROM TIMESTAMP) > {start_unixtimestamp}
                        AND EXTRACT(EPOCH FROM TIMESTAMP) <= {end_unixtimestamp}
                    ORDER BY TIMESTAMP ASC
                    LIMIT {PAGINATION_SIZE} offset '{{{{offset_amount}}}}'""",
                "parameters": {
                    "offset_amount": offset_amount,
                },
                "options": {},
            }
            response = requests.post("https://api.transpose.io/sql", headers=headers, json=json_data)
            current_results_dataframe = pd.DataFrame(response.json().get("results"))
            offset_amount += current_results_dataframe.shape[0]
            # if we're getting less than N results, we know there's no more to fetch.
            if current_results_dataframe.shape[0] < 10000:
                continue_fetching = False
            if current_results_dataframe.shape[0] == 0:
                return None
            incoming_data_df = pd.concat([incoming_data_df, current_results_dataframe], axis=0)
            logger.info(f"Fetched {event_name} up to {current_results_dataframe.iloc[-1]['timestamp']}")
        except Exception as e:
            logger.error(f"issue at {offset_amount}")
            raise e
    incoming_data_df["year"] = (
        incoming_data_df["timestamp_unixtimestamp"].apply(lambda x: pd.to_datetime(x, unit="s")).dt.year
    )
    incoming_data_df["month"] = (
        incoming_data_df["timestamp_unixtimestamp"].apply(lambda x: pd.to_datetime(x, unit="s")).dt.month
    )
    incoming_data_df.drop(columns=["timestamp_unixtimestamp"], inplace=True)
    return incoming_data_df


def update_event_table(event_name: str, transpose_api_key):
    start = time.time()

    incoming_data = fetch_data(event_name=event_name, transpose_api_key=transpose_api_key)
    if incoming_data is not None:
        insert_incoming_data_to_data_lake(incoming_data_df=incoming_data, event_name=event_name)
    else:
        logger.info(f"No new data for {event_name}")

    end = time.time()
    logger.info(f"Completed successfully. Elapsed time: {end - start}")


def get_args() -> Namespace:
    parser = ArgumentParser(description="This is for DeFi Features (Transaction and Daily basis).")

    parser.add_argument(
        "--event_name",
        "-e",
        type=str,
        required=True,
        choices=event_names,
        help="Event Name",
    )
    parser.add_argument(
        "--api_key",
        "-k",
        type=str,
        required=True,
        help="Pipeline Name",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    update_event_table(args.event_name, args.api_key)
