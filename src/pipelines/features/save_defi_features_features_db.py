import pandas as pd
import awswrangler as wr
import numpy as np
from concurrent.futures import ThreadPoolExecutor

from config import settings
from spectral_data_lib.config import settings as sdl_settings
from spectral_data_lib.feature_data_documentdb.sync_mongo_connection import SyncMongoConnection
from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name="save_defi_features_features_db")


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


def main():

    fetch_defi_features_query = """
        SELECT *
        FROM db_analytics_prod.defi_features;
    """

    sdl_settings.SECRET_NAME = settings.FEATURE_DB_SECRET_NAME  # Set secret name to features db secret name

    # defi_features_sql = open("src/pipelines/features/transformations/fetch_all_defi_features.sql", "r").read()
    defi_features = get_defi_features_from_datalake(sql=fetch_defi_features_query)

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
        logger.info("All defi features inserted into features-db.")


if __name__ == "__main__":
    main()
