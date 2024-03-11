import asyncio
import time
import aiohttp
import pandas as pd
import gc

from config import settings
from spectral_data_lib.data_lakehouse import DataLakehouse
from spectral_data_lib.log_manager import Logger
from spectral_data_lib.helpers.get_secrets import get_secret


from src.pipelines.utils import fetch_daily_first_block_numbers_and_partitions, get_start_block_to_fetch_new_data


data_lakehouse = DataLakehouse()
logger = Logger(logger_name=__file__.split("/")[-1].split(".")[0])


def insert_incoming_data_into_data_lake(data: pd.DataFrame):
    data_lakehouse.write_parquet_table(
        database_name="db_raw_prod",
        table_name="the_graph_historical_account_positions",
        source="the_graph",
        data=data,
        layer="raw",
        partition_columns=["year", "month"],
    )


async def get_data_for_block_range(
    session,
    block_number,
    url,
    query,
    protocol,
    sem,
    year,
    month,
) -> pd.DataFrame:
    """Fetches data from Subgraph for a given block number and protocol.

    Args:
        session (aiohttp.ClientSession): An aiohttp session.
        block_number (int): The block number to fetch data for.
        url (str): The URL of the Subgraph.
        query (str): The GraphQL query to run.
        protocol (str): The protocol to fetch data for.
        sem (asyncio.Semaphore): An asyncio semaphore.

    Returns:
        pd.DataFrame: The data fetched from the Subgraph.
    """
    async with sem:
        logger.info(f"Attempting to get response for block {block_number} on {protocol}")
        total_block_dataframe_list = []
        more_to_fetch = True
        query_variables = {"block_number": block_number, "last_id": ""}
        while more_to_fetch:
            try:
                response = await session.post(url=url, json={"query": query, "variables": query_variables})
                try:
                    subgraph_response_dict = await response.json()
                    if (
                        not subgraph_response_dict.get("data").get("first_results")
                        or len(subgraph_response_dict.get("data").get("first_results")) == 0
                    ):
                        # No result or no more data to fetch
                        logger.info(f"No result for {block_number}")
                        more_to_fetch = False
                    else:
                        for result in list(subgraph_response_dict.get("data").keys()):
                            if subgraph_response_dict.get("data").get(result):
                                # load and append our data
                                dataframe_chunk = pd.DataFrame.from_dict(subgraph_response_dict.get("data").get(result))
                                dataframe_chunk["block_number"] = block_number
                                dataframe_chunk["protocol"] = protocol
                                dataframe_chunk["year"] = f"20{year}"
                                dataframe_chunk["month"] = month
                                total_block_dataframe_list.append(dataframe_chunk)

                                # If we have less than 1000 results in any result we're done
                                if len(subgraph_response_dict.get("data").get(result)) < 1000:
                                    more_to_fetch = False

                                # If we have 1000 results in the sixth_result key in any result we need to fetch more
                                if more_to_fetch:
                                    logger.info(
                                        f'Paginating on block number {block_number} with new last id {dataframe_chunk.iloc[-1]["id"].split("-")[0]}'
                                    )
                                    query_variables = {
                                        "block_number": block_number,
                                        "last_id": dataframe_chunk.iloc[-1]["id"],
                                    }
                                    await asyncio.sleep(0.25)

                except Exception as e:
                    logger.error(f"Failed to parse response for block {block_number}")
                    logger.error(e)
                    continue
            except Exception as e:
                logger.error(f"Failed to get response for block {block_number}")
                logger.error(e)
                continue
        return pd.concat(total_block_dataframe_list)


async def fetch_data(api_key: str):
    param_dict = {
        "compound-v2-eth": {
            "url": f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/6tGbL7WBx287EZwGUvvcQdL6m67JGMJrma3JSTtt5SV7",
            "earliest_block": 7774386,
        },
        "aave-v2-eth": {
            "url": f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/84CvqQHYhydZzr2KSth8s1AFYpBRzUbVJXq6PWuZm9U9",
            "earliest_block": 11363052,
        },
    }
    query = open("src/pipelines/raw/queries/historical_account_positions.graphql").read()

    for protocol in param_dict.keys():
        earliest_block_to_consider = get_start_block_to_fetch_new_data("the_graph_historical_account_positions")
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
        param_dict[protocol]["block_list"] = blocks_list

    sem = asyncio.Semaphore(10)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for protocol in param_dict.keys():
            url = param_dict.get(protocol).get("url")
            block_list = (
                param_dict.get(protocol)
                .get("block_list")["first_eth_block_of_the_day"]
                .apply(lambda x: int(x))
                .to_list()
            )
            # Reducing size of the list in order to process a smaller window of data
            block_list = block_list[0 : settings.ACCOUNT_POSITIONS_BLOCK_WINDOW_SIZE]
            for _, block_number in enumerate(block_list):
                year_partition = param_dict.get(protocol).get("block_list")["year"].iloc[_]
                month_partition = param_dict.get(protocol).get("block_list")["month"].iloc[_]
                tasks.append(
                    get_data_for_block_range(
                        session=session,
                        block_number=block_number,
                        url=url,
                        query=query,
                        protocol=protocol,
                        sem=sem,
                        year=year_partition,
                        month=month_partition,
                    )
                )
        print(f"There are {len(tasks)} tasks to run during this iteration")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        return results


def update_table(api_key: str):
    start = time.time()

    while True:
        incoming_data = asyncio.run(fetch_data(api_key))
        if len(incoming_data) < 1:
            logger.info("No new data to ingest. Exiting.")
            break
        incoming_data = pd.concat(incoming_data)
        logger.info(f"Incoming data size: {incoming_data.shape}")
        insert_incoming_data_into_data_lake(incoming_data)
        del incoming_data
        gc.collect()

    end = time.time()
    logger.info(f"Elapsed time: {end - start}")


if __name__ == "__main__":
    secrets = get_secret("prod/api/keys")
    update_table(secrets["SUBGRAPH_API_KEY"])
