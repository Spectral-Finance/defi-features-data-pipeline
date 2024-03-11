from itertools import product
import numpy as np
import awswrangler as wr
from concurrent.futures import ThreadPoolExecutor, as_completed
from argparse import ArgumentParser, Namespace

from spectral_data_lib.config import settings as sdl_settings
from spectral_data_lib.log_manager import Logger
from src.pipelines.utils import get_latest_timestamp_in_data_lake_table_for_event


from config import settings


logger = Logger(logger_name="defi_events_ingestion_pipeline")
event_names = settings.EVENTS_NAMES


def render_sql_template(sql_file_path, params: dict) -> str:
    """Render SQL template with Jinja2.

    Args:
        sql_file (str): Path to SQL file.
        params (dict): Parameters to render SQL template.

    Returns:
        str: Rendered SQL query.
    """
    # sql = read_sql_file(sql_file_path)
    sql = open(sql_file_path, "r").read()

    query = sql.format(**params)

    return query


def create_table_on_datalake(event_name: str) -> None:
    """Execute DDL for each event table.

    Args:
        event_name (str): Event name.

    Returns:
        None
    """

    ddl = open((f"src/pipelines/analytics/ddl/transpose_{event_name}_events.sql"), "r").read()
    ddl = (
        ddl.replace("target_database", sdl_settings.DATA_LAKE_ANALYTICS_DATABASE)
        .replace("bucket_name", sdl_settings.DATA_LAKE_BUCKET_S3)
        .replace("layer", "analytics")
        .replace("table_name", f"transpose_{event_name}_events")
        .replace("data_source", "transpose")
    )

    try:
        wr.athena.start_query_execution(
            sql=ddl, database=sdl_settings.DATA_LAKE_ANALYTICS_DATABASE, wait=True, athena_query_wait_polling_delay=1
        )
        logger.debug(f"Table transpose_{event_name}_events created")
    except Exception as e:
        logger.error(f"Error creating table transpose_{event_name}_events - {e}")


def insert_data_into_table(
    event_name: str,
    table_exists: bool,
    last_timestamp: int,
    token_column: str,
    quantity_column: str,
    index_column: str,
    address_partitions: str,
) -> None:
    """Insert data into table.

    Args:
        event_name (str): Event name.
        table_exists (bool): Table exists.
        last_timestamp (int): Last timestamp inserted.
        token_column (str): Token column name.
        quantity_column (str): Quantity column name.
        index_column (str): Index column name.
        address_partitions (str): Address partition.

    Returns:
        None

    """

    if not table_exists:
        logger.debug(f"Table transpose_{event_name}_events does not exist. Creating table...")
        create_table_on_datalake(event_name)
    else:
        logger.debug(f"Table transpose_{event_name}_events exists. Inserting data...")
        sql_file_path = f"src/pipelines/analytics/transformations/transformations.sql"

        params = {
            "event_name": event_name,
            "token_column": token_column,
            "last_timestamp": last_timestamp,
            "quantity_column": quantity_column,
            "index_column": index_column,
            "address_partitions": address_partitions,
        }

        query = render_sql_template(sql_file_path, params)

        try:
            wr.athena.start_query_execution(
                sql=query,
                database=sdl_settings.DATA_LAKE_ANALYTICS_DATABASE,
                wait=True,
                athena_query_wait_polling_delay=1,
            )
            logger.debug(f"Data inserted into {event_name} table")

        except Exception as e:
            logger.error(f"Error inserting data into table - {e}")


def run_insert_in_parallell(event_name: str, address_partitions_chunks: list) -> None:
    """Run insert in parallell.
    We need to execute in this way because we will ingest data into 256 partitions, as Athena has a limit of 100 partitions per query.

    Args:
        event (list): Events to be processed.
        address_partitions_chunks (list): Address partitions chunks.

    Returns:
        None
    """

    with ThreadPoolExecutor() as executor:
        futures = []
        # for event_name in event_list:
        logger.info(f"Inserting data for {event_name} events")
        table_exists = wr.catalog.does_table_exist(
            database=sdl_settings.DATA_LAKE_ANALYTICS_DATABASE, table=f"transpose_{event_name}_events"
        )
        last_timestamp = get_latest_timestamp_in_data_lake_table_for_event(event_name, "analytics")
        for address_partitions in address_partitions_chunks:
            if event_name == "liquidation":
                token_column = "liquidated_token_address"
                quantity_column = "quantity_liquidated"
                index_column = "account_address"
            else:
                token_column = "token_address"
                quantity_column = "quantity"
                index_column = "sender_address"
            futures.append(
                executor.submit(
                    insert_data_into_table,
                    event_name,
                    table_exists,
                    last_timestamp,
                    token_column,
                    quantity_column,
                    index_column,
                    tuple(address_partitions),
                )
            )
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error executing insert in parallell - {e}")
        logger.info(f"Finished inserting data for {event_name} events")


def update_event_table(event_name: str):

    address_partitions = list(map("".join, product("0123456789abcdef", repeat=2)))
    address_partitions_chunks = np.array_split(address_partitions, 3)
    run_insert_in_parallell(address_partitions_chunks=address_partitions_chunks, event_name=event_name)


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
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    update_event_table(args.event_name)
