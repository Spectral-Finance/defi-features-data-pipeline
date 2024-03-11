import awswrangler as wr

from spectral_data_lib.feature_data_documentdb.sync_mongo_connection import SyncMongoConnection
from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name="save_defi_features_datalake")


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


def main():
    defi_features_sql = open("src/pipelines/features/transformations/defi_features_calculation.sql", "r").read()

    insert_defi_features_into_datalake(sql=defi_features_sql)


if __name__ == "__main__":
    main()
