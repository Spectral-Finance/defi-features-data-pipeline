import time
import awswrangler as wr

from spectral_data_lib.log_manager import Logger


logger = Logger(logger_name="analytics_data_ingestion_pipeline")


def update_stage():
    """Updates stage layer table with new data."""
    table_name = "the_graph_historical_market_data"
    update_query = f"""
        INSERT INTO db_stage_prod.{table_name}
        SELECT
            distinct
            cast(liquidationthreshold as double) AS liquidation_threshold,
            name,
            cast(inputtokenpriceusd as Double) AS input_token_price_usd,
            id,
            inputtoken.decimals AS decimals,
            protocol,
            block_number,
            timestamp AS block_timestamp,
            year,
            month
        FROM db_raw_prod.{table_name}
        where block_number > (select COALESCE(max(block_number), 0) from db_stage_prod.{table_name})
    """
    wr.athena.start_query_execution(
        sql=update_query, database="db_stage_prod", wait=True, athena_query_wait_polling_delay=1
    )


def update_table():
    start = time.time()

    update_stage()

    end = time.time()
    logger.info(f"Completed successfully. Elapsed time: {end - start}")


if __name__ == "__main__":
    update_table()
