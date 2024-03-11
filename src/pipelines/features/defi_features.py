import pandas as pd
import awswrangler as wr
import numpy as np
from concurrent.futures import ThreadPoolExecutor

from config import settings
from spectral_data_lib.config import settings as sdl_settings
from spectral_data_lib.feature_data_documentdb.sync_mongo_connection import SyncMongoConnection
from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name="analytics_data_ingestion_pipeline")

# wr configs
wr.config.max_cache_seconds = 900
wr.config.max_cache_query_inspections = 500
wr.config.max_remote_cache_entries = 50


def delete_s3_objects(bucket: str, prefix: str) -> None:
    """Remove s3 objects with prefix

    Args:
        bucket (str): s3 bucket name
        prefix (str): s3 objects prefix

    Returns:
        None
    """

    wr.s3.delete_objects(path=f"s3://{bucket}/{prefix}")


def insert_defi_features_into_datalake(sql: str) -> None:
    """Insert defi features into datalake

    Args:
        sql (str): sql query to get defi features calculated

    Returns:
        None
    """
    try:

        response = wr.catalog.delete_table_if_exists(
            database="db_analytics_prod",
            table="defi_features",
        )

        if response:  # If true, table exists and was deleted
            delete_s3_objects(
                bucket="data-lakehouse-prod",
                prefix="analytics/features/defi_features",
            )

        wr.athena.create_ctas_table(
            sql=sql,
            database="db_analytics_prod",
            s3_output="s3://data-lakehouse-prod/analytics/features/defi_features",
            storage_format="parquet",
            ctas_database="db_analytics_prod",
            ctas_table="defi_features",
            wait=True,
        )
        logger.info("Defi features inserted into datalake.")

    except Exception as e:
        logger.error(f"Error inserting defi features into datalake: {e}")
        raise e


def get_defi_features_from_datalake(sql: str) -> None:
    """Getting defi features from datalake

    Args:
        sql (str): sql query to get defi features calculated

    Returns:
        defi_features_df (pd.DataFrame): defi features calculated
    """

    try:
        defi_features_df = wr.athena.read_sql_query(
            sql=sql,
            database="db_analytics_prod",
        )

        defi_features_df.rename({"wallet_address": "walletAddress"}, axis=1, inplace=True)

        return defi_features_df

    except Exception as e:
        logger.error(f"Error getting defi features from datalake: {e}")
        raise e


def insert_defi_features_into_features_db(defi_features: pd.DataFrame) -> None:
    """Insert defi features into features db

    Args:
        defi_features (pd.DataFrame): defi features calculated

    Returns:
        None
    """

    mongo_connection = SyncMongoConnection(addition_connection_parameters_string=settings.MONGO_RETRY_WRITE_TO_FALSE)

    try:

        mongo_connection.update_documents(
            db_name="features_db",
            collection_name="defi_features",
            key_to_match="walletAddress",
            update_collection=defi_features.to_dict(orient="records"),
            upsert=True,
        )

    except Exception as e:
        logger.error(f"Error inserting defi features into features db: {e}")
        raise e


def main():

    sdl_settings.SECRET_NAME = settings.FEATURE_DB_SECRET_NAME  # Set secret name to features db secret name
    defi_features_sql = open("src/pipelines/features/transformations/defi_features_calculation.sql", "r").read()

    insert_defi_features_into_datalake(sql=defi_features_sql)

    defi_features = get_defi_features_from_datalake(sql=defi_features_sql)

    with ThreadPoolExecutor() as executor:
        defi_features_chunks = np.array_split(defi_features, executor._max_workers)
        futures = []
        for chunk in defi_features_chunks:
            futures.append(executor.submit(insert_defi_features_into_features_db, chunk))
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error inserting defi features into features db: {e}")
        logger.info("All defi features inserted into features db.")


if __name__ == "__main__":
    main()
