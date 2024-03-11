import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests


from spectral_data_lib.log_manager import Logger
from spectral_data_lib.helpers.get_secrets import get_secret
from spectral_data_lib.data_lakehouse import DataLakehouse


from src.pipelines.utils import fetch_daily_first_block_numbers_and_partitions, get_start_block_to_fetch_new_data

data_lakehouse = DataLakehouse()
logger = Logger(logger_name=__file__.split("/")[-1].split(".")[0])


def get_data_for_block_range(block_number, url, query, protocol):
    # For some reason we occasionally get errors on certain blocks when fetching so this retry mechanism will query a nearby block
    # a few times before giving up
    logger.info(f"attempting to get response for block {block_number} on protocol {protocol}")
    for i in range(0, 5):
        query_variables = {"block_number": block_number}
        if i > 0:
            logger.info(f"attempting to get response for block {block_number} on protocol {protocol} try {i}")
        try:
            response = requests.post(url=url, json={"query": query, "variables": query_variables})
            subgraph_response_dict = response.json()
            dataframe_chunk = pd.DataFrame.from_dict(subgraph_response_dict.get("data").get("first_results"))
            dataframe_chunk["block_number"] = block_number
            dataframe_chunk["protocol"] = protocol
            time.sleep(0.25)
            return dataframe_chunk
        except Exception as e:
            logger.error(f"Failed to get response for block {block_number}")


def fetch_data(api_key: str):
    param_dict = {
        "compound-v2-eth": {
            "url": f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/6tGbL7WBx287EZwGUvvcQdL6m67JGMJrma3JSTtt5SV7",
            "earliest_block": 7774386,
        },  # default value for protocol
        "aave-v2-eth": {
            "url": f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/84CvqQHYhydZzr2KSth8s1AFYpBRzUbVJXq6PWuZm9U9",
            "earliest_block": 11363052,
        },  # default value for protocol
    }
    query = open("src/pipelines/raw/queries/historical_market_state.graphql").read()

    general_blocks_list = []
    total_subgraph_dataframe_list = []
    for protocol in param_dict.keys():
        earliest_block_to_consider = get_start_block_to_fetch_new_data("the_graph_historical_market_data")
        earliest_block_to_consider = (
            param_dict[protocol]["earliest_block"]
            if pd.isna(earliest_block_to_consider)
            else earliest_block_to_consider
        )
        blocks_list = fetch_daily_first_block_numbers_and_partitions(earliest_block_to_consider)
        first_block_number = blocks_list.iloc[0].first_eth_block_of_the_day
        if protocol == "aave-v2-eth" and first_block_number != param_dict["aave-v2-eth"]["earliest_block"]:
            blocks_list = blocks_list[1:]
        elif protocol == "compound-v2-eth" and first_block_number != param_dict["compound-v2-eth"]["earliest_block"]:
            blocks_list = blocks_list[1:]
        if len(blocks_list) == 0:
            return []
        general_blocks_list.append(blocks_list)
        unique_block_numbers = blocks_list["first_eth_block_of_the_day"].unique()

        logger.info(f"Found {len(blocks_list)} blocks to query for {protocol}")
        url = param_dict[protocol]["url"]
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(get_data_for_block_range, block_number, url, query, protocol)
                for block_number in blocks_list["first_eth_block_of_the_day"].apply(lambda x: int(x)).to_list()
            ]
            for future in concurrent.futures.as_completed(futures):
                total_subgraph_dataframe_list.append(future.result())

    total_subgraph_dataframe = pd.concat(total_subgraph_dataframe_list)

    total_subgraph_dataframe = total_subgraph_dataframe[
        total_subgraph_dataframe["block_number"] != unique_block_numbers[0]
    ]
    unique_block_numbers = total_subgraph_dataframe["block_number"].unique()
    general_blocks_list = pd.concat(general_blocks_list, ignore_index=True)
    for i in range(
        0, len(unique_block_numbers)
    ):  # skiping first day because we already have the first next block day's data
        total_subgraph_dataframe.loc[
            total_subgraph_dataframe["block_number"] == unique_block_numbers[i], "year"
        ] = f"20{general_blocks_list['year'][i]}"
        total_subgraph_dataframe.loc[
            total_subgraph_dataframe["block_number"] == unique_block_numbers[i], "month"
        ] = general_blocks_list["month"][i]
    return total_subgraph_dataframe


def insert_incoming_data_into_data_lake(data: pd.DataFrame):
    data_lakehouse.write_parquet_table(
        database_name="db_raw_prod",
        table_name="the_graph_historical_market_data",
        source="the_graph",
        data=data,
        layer="raw",
        partition_columns=["year", "month"],
    )


def update_table(api_key: str):
    start = time.time()

    incoming_data = fetch_data(api_key)
    if len(incoming_data) == 0:
        logger.info("No new data to ingest. Exiting.")
        return
    logger.info(f"Incoming data size: {incoming_data.shape}")
    insert_incoming_data_into_data_lake(incoming_data)

    end = time.time()
    logger.info(f"Elapsed time: {end - start}")


if __name__ == "__main__":
    secrets = get_secret("prod/api/keys")
    update_table(secrets["SUBGRAPH_API_KEY"])
