from itertools import product
import time
import awswrangler as wr
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name=__file__.split("/")[-1].split(".")[0])


def get_last_block_number() -> int:
    """Get the last block number in the table.

    Returns:
        int: Last block number in the table.
    """

    query = """
    SELECT coalesce(max(block_number), 0) as last_block_number
    FROM db_analytics_prod.the_graph_historical_market_data_and_account_positions
    """
    df = wr.athena.read_sql_query(sql=query, database="db_analytics_prod")
    return df["last_block_number"][0]


def insert_data_into_table(address_partitions: tuple, last_block_number: int = 0) -> None:
    """Insert data into table in data lake. Insert data incrementally based on the latest block number in the table.

    Args:
        address_partitions (tuple): Tuple of address partitions to insert data for.

    Returns:
        None
    """

    insert_query = f"""
    INSERT INTO db_analytics_prod.the_graph_historical_market_data_and_account_positions
    WITH market_data_prices_by_protocol AS (
        SELECT
            hmd.input_token_price_usd,
            hmd.block_number,
            protocol
        FROM db_stage_prod.the_graph_historical_market_data AS hmd
            WHERE hmd.name in ('Aave interest bearing WETH', 'Compound Ether')
            AND hmd.block_number > {last_block_number}
    ),
    merged_market_data_and_account_positions as (
    -- we need to create this as a table and ingest incrementing data into it based on the latest block number
    select
        ap.balance,
        (ap.balance * md.input_token_price_usd) / POWER(10, md.decimals) AS balance_in_usd,
        CASE WHEN (ap.balance * md.input_token_price_usd) / POWER(10, md.decimals) = 0.0 or mdp.input_token_price_usd = 0.0 THEN 0.0
            ELSE (1 / mdp.input_token_price_usd) * ((ap.balance * md.input_token_price_usd) / POWER(10, md.decimals))
        END AS balance_in_eth,
        ap.id,
        ap.is_collateral,
        ap.market_id,
        ap.side,
        ap.account,
        md.liquidation_threshold * 0.01 as liquidation_threshold,
        md.input_token_price_usd,
        mdp.input_token_price_usd as input_token_price_usd_protocol,
        md.decimals,
        ap.protocol,
        ap.block_number,
        ap.block_timestamp,
        ap.year,
        ap.month,
        SUBSTR(ap.account, 3, 2) AS address_partition
    from db_stage_prod.the_graph_historical_account_positions as ap
    inner join db_stage_prod.the_graph_historical_market_data as md
        on md.id = ap.market_id and md.block_number = ap.block_number
    inner join market_data_prices_by_protocol as mdp
        on mdp.block_number = ap.block_number and mdp.protocol = ap.protocol
    WHERE ap.block_number > {last_block_number}
    )

    SELECT * FROM merged_market_data_and_account_positions where address_partition in {address_partitions}
    """

    wr.athena.start_query_execution(
        sql=insert_query, database="db_analytics_prod", wait=True, athena_query_wait_polling_delay=1
    )


def run_insert_in_parallell(address_partitions_chunks: list) -> None:

    with ThreadPoolExecutor() as executor:
        futures = []
        logger.info(f"Inserting data for historical market data and account positions.")
        last_block_number = get_last_block_number()
        for address_partitions in address_partitions_chunks:
            futures.append(executor.submit(insert_data_into_table, tuple(address_partitions), last_block_number))
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error executing insert in parallell - {e}")
        logger.info(f"Finished inserting data for historical market data and account positions.")


def update_table() -> None:

    address_partitions = list(map("".join, product("0123456789abcdef", repeat=2)))
    address_partitions_chunks = np.array_split(address_partitions, 3)
    run_insert_in_parallell(address_partitions_chunks=address_partitions_chunks)


if __name__ == "__main__":

    start = time.time()
    update_table()
    end = time.time()
    logger.info(f"Elapsed time: {end - start}")
