from spectral_data_lib.data_lakehouse import DataLakehouse
import pandas as pd


data_lakehouse = DataLakehouse()


def get_latest_timestamp_in_data_lake_table_for_event(event_name: str, layer: str) -> int:
    """Returns latest timestamp in the Data Lakehouse stage table
    or default start timestamp if table is empty.

    Args:
        event_name (str): Event name.

    Returns:
        int: Latest index in the Data Lakehouse table.
    """
    query = f"""SELECT COALESCE(MAX(to_unixtime(timestamp)), 0) as max_timestamp FROM db_{layer}_prod.transpose_{event_name}_events"""
    event_latest_timestamp = data_lakehouse.read_sql_query(query)
    if event_latest_timestamp.iloc[0][0] is not None:
        return int(event_latest_timestamp.iloc[0][0])
    return 1557187200


def fetch_daily_first_block_numbers_and_partitions(latest_block_number: int) -> pd.DataFrame:
    """Fetches the first block number of each day from the Ethereum blockchain
    and year, month and day to which it belongs.

    Args:
        latest_block_number (int): The latest block number in the Ethereum blockchain.

    Returns:
        pd.DataFrame: The first block number of each day.
    """
    query = f"""
    SELECT
            date_format(timestamp, '%y') AS year,
            date_format(timestamp, '%m') AS month,
            date_format(timestamp, '%d') AS day,
            MIN(number) AS first_eth_block_of_the_day
    FROM db_raw_prod.ethereum_blocks
    WHERE number >= {latest_block_number}
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3 ASC"""

    latest_existing_block_number = data_lakehouse.read_sql_query(database_name="db_raw_prod", query=query)
    if len(latest_existing_block_number) > 0:
        return latest_existing_block_number
    return None


def get_start_block_to_fetch_new_data(table_name, db_name="db_raw_prod", block_number_column="block_number") -> int:
    """Returns the latest block number in the Data Lakehouse table or None if table is empty."""
    result = data_lakehouse.read_sql_query(
        database_name="db_raw_prod",
        query=f"select max({block_number_column}) as latest_block_number from {db_name}.{table_name}",
    )
    if len(result) > 0:
        return result["latest_block_number"][0]
    return None
