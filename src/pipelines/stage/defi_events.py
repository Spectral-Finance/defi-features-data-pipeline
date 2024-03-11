from argparse import ArgumentParser, Namespace
import time
import awswrangler as wr

from spectral_data_lib.log_manager import Logger

from config import settings
from src.pipelines.stage.transformations.stage_tranformation_queries import general_query, liquidation_query


logger = Logger(logger_name=__file__.split("/")[-1].split(".")[0])
event_names = settings.EVENTS_NAMES


def update_event_table(event_name: str) -> None:
    """Updates the stage layer for a given event.

    Args:
        event_name (str): Event name.

    Returns:
        None. Updates the stage layer for a given event.
    """
    start = time.time()

    if event_name != "liquidation":
        query = general_query
    else:
        query = liquidation_query
    query = query.format(event_name)

    wr.athena.start_query_execution(sql=query, database="db_stage_prod", wait=True, athena_query_wait_polling_delay=1)

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
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    update_event_table(args.event_name)
