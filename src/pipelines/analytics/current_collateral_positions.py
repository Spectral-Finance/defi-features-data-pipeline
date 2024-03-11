import time
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import pandas as pd
import requests
import numpy as np
import awswrangler as wr

from spectral_data_lib.helpers.get_secrets import get_secret
from spectral_data_lib.data_lakehouse import DataLakehouse
from spectral_data_lib.log_manager import Logger
from config import settings


data_lakehouse = DataLakehouse()
logger = Logger(logger_name=__file__.split("/")[-1].split(".")[0])


def reload_data_lake_table(new_data: pd.DataFrame):
    if wr.catalog.does_table_exist(database="db_analytics_prod", table="the_graph_current_collateral_positions"):
        wr.s3.delete_objects(
            path=f"s3://data-lakehouse-prod/analytics/the_graph/the_graph_current_collateral_positions/"
        )
    data_lakehouse.write_parquet_table(
        database_name="db_analytics_prod",
        table_name="the_graph_current_collateral_positions",
        source="the_graph",
        data=new_data,
        layer="analytics",
        partition_columns=None,
    )


def get_current_position_data(protocol, urls, list_of_addresses):
    """
    For each list of addresses, fetch all current positions these addresses hold on a specific protocol
    :param protocol:
    :param urls:
    :param list_of_addresses:
    :return:
    """
    url = urls.get(protocol)
    current_positions_for_address_list = open(
        "src/pipelines/analytics/queries/current_positions_for_address_list.graphql"
    ).read()
    total_response_dataframes_list = []
    more_to_fetch = True
    while more_to_fetch:
        query_variables = {"last_id": "", "address_list": list_of_addresses}
        response = requests.post(
            url=url, json={"query": current_positions_for_address_list, "variables": query_variables}
        )
        try:
            subgraph_response_dict = response.json()
            for result in list(subgraph_response_dict.get("data").keys()):
                if subgraph_response_dict.get("data").get(result):
                    dataframe_chunk = pd.DataFrame.from_dict(subgraph_response_dict.get("data").get(result))
                    dataframe_chunk["protocol"] = protocol
                    total_response_dataframes_list.append(dataframe_chunk)
                    if len(subgraph_response_dict.get("data").get(result)) < 1000:
                        more_to_fetch = False
                    if result == "sixth_results" and more_to_fetch:
                        print("paginating on sixth_results")
                        query_variables = {"last_id": dataframe_chunk.iloc[-1]["id"], "address_list": list_of_addresses}
                else:
                    more_to_fetch = False
                    break
        except Exception as e:
            logger.error(e)
            logger.error(subgraph_response_dict)
            logger.error(
                f"""
            Failed to parse response for addresses.
            Protocol: {protocol}
            Url: {url}
            First address: {list_of_addresses[0]}
            """
            )
    total_response_dataframe = pd.concat(total_response_dataframes_list)
    return total_response_dataframe


def divide_chunks(l, max_list_size):
    """
    Divide a list into chunks of at most max_list_size
    :param l: list of items
    :param max_list_size: int
    :return: list of lists
    """
    for i in range(0, len(l), max_list_size):
        yield l[i : i + max_list_size]


def fetch_current_data(unique_active_borrowers: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """Fetches all current positions data for all unique_active_borrowers addresses on all protocols.

    Args:
        unique_active_borrowers (pd.DataFrame): Unique active borrowers.
        api_key (str): API key for the Transpose API.

    Returns:
        pd.DataFrame: All current positions data for all unique_active_borrowers addresses on all protocols.
    """

    unique_active_borrowers["id"] = unique_active_borrowers["wallet_address"].str.lower()
    unique_active_borrowers = pd.DataFrame(unique_active_borrowers["id"])
    address_list_size = settings.CURRENT_POSITIONS_MAX_PARALLEL_REQUESTS

    address_chunk_lists = list(divide_chunks(l=unique_active_borrowers, max_list_size=address_list_size))
    address_chunk_lists = [list(address_chunk_list["id"]) for address_chunk_list in address_chunk_lists]

    urls = {
        "aave-v2-eth": f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/84CvqQHYhydZzr2KSth8s1AFYpBRzUbVJXq6PWuZm9U9",
        "compound-v2-eth": f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/6tGbL7WBx287EZwGUvvcQdL6m67JGMJrma3JSTtt5SV7",
    }
    protocol_address_chunk_list = [(i, j) for i in urls.keys() for j in address_chunk_lists]

    total_positions_list = []
    with ThreadPoolExecutor(10) as executor:
        futures = [
            executor.submit(
                get_current_position_data, protocol=protocol, urls=urls, list_of_addresses=list_of_addresses
            )
            for protocol, list_of_addresses in protocol_address_chunk_list
        ]
        for future in concurrent.futures.as_completed(futures):
            total_positions_list.append(future.result())
    total_positions_dataframe = pd.concat(total_positions_list)

    total_positions_dataframe["isCollateral"] = total_positions_dataframe["isCollateral"].astype(bool)
    total_positions_dataframe.rename(columns={"isCollateral": "is_collateral"}, inplace=True)
    total_positions_dataframe["market_id"] = total_positions_dataframe["market"].apply(lambda x: x.get("id").lower())
    total_positions_dataframe["market"] = total_positions_dataframe["market"].apply(lambda x: x.get("name"))
    total_positions_dataframe["account"] = total_positions_dataframe["account"].apply(lambda x: x.get("id").lower())
    total_positions_dataframe["balance"] = total_positions_dataframe["balance"].astype(np.float64)
    return total_positions_dataframe


def fetch_test_dataset_wallets_addresses() -> pd.DataFrame:
    return data_lakehouse.read_sql_query(
        database_name="db_sandbox_prod", query="SELECT wallet_address FROM db_sandbox_prod.test_set_wallet_addresses"
    )


def update_table(api_key: str):
    """
    Fetches current data from the API and updates the table.

    Args:
        api_key (str): API key for the Transpose API.

    Returns:
        None. Updates the table.
    """
    start = time.time()
    unique_active_borrowers = fetch_test_dataset_wallets_addresses()
    new_data = fetch_current_data(unique_active_borrowers, api_key)
    logger.info(f"New_data size: {new_data.shape}")
    reload_data_lake_table(new_data)
    end = time.time()
    logger.info(f"Elapsed time: {end - start}")


if __name__ == "__main__":
    secrets = get_secret("prod/api/keys")
    update_table(secrets["SUBGRAPH_API_KEY"])
