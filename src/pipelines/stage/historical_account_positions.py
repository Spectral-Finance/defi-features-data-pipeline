import time
import awswrangler as wr

from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name=__file__.split("/")[-1].split(".")[0])


def update_stage() -> None:
    table_name = "the_graph_historical_account_positions"
    update_query = f"""
        INSERT INTO db_stage_prod.{table_name}
        SELECT
            distinct
            CAST(balance AS DOUBLE) AS balance,
            id,
            iscollateral AS is_collateral,
            market.name AS market,
            LOWER(market.id) AS market_id,
            side,
            LOWER(account.id) AS account,
            block_number,
            protocol,
            timestamp as block_timestamp,
            year,
            month
        FROM db_raw_prod.{table_name}
        where block_number > (select coalesce(max(block_number), 0) from db_stage_prod.{table_name})
    """

    wr.athena.start_query_execution(
        sql=update_query, database="db_stage_prod", wait=True, athena_query_wait_polling_delay=1
    )


def update_table():
    start = time.time()

    update_stage()

    end = time.time()
    logger.info(f"Elapsed time: {end - start}")


if __name__ == "__main__":
    update_table()
