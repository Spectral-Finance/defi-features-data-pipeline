from config import settings

from spectral_data_lib.data_lakehouse import DataLakehouse
from spectral_data_lib.log_manager import Logger
from spectral_data_lib.config import settings as sdl_settings


class DeFiFeaturesDataQualityPipeline(object):

    def __init__(self, table_name: str) -> None:
        self.logger = Logger(logger_name=f"Features - Data Quality Pipeline Logger")
        self.table_name = table_name
        self.target_data_lake_database = sdl_settings.DATA_LAKE_ANALYTICS_DATABASE
        self.env = settings.ENV
        self.data_lakehouse_connection = DataLakehouse()

    def check_data_quality_constraints(self) -> None:
        constraints_query = open("src/pipelines/features/data_quality/defi_features_constraints_query.sql", "r").read()
        data_quality_df_checks = self.data_lakehouse_connection.read_sql_query(
            query=constraints_query,
            database_name="db_analytics_prod",
        )

        fail_data_quality_df_checks = data_quality_df_checks[data_quality_df_checks["is_fail"] == True][
            "constraint_name"
        ]

        if fail_data_quality_df_checks.shape[0] == 0:
            self.logger.info("No data quality issues found.")
        else:
            self.logger.error(f"Data quality issues found:\nBroken constraints: {fail_data_quality_df_checks.values}")
            self.logger.warning("New DeFi features data will not be saved in features-db.")
            raise RuntimeError("Data quality issues found")


if __name__ == "__main__":
    DeFiFeaturesDataQualityPipeline(table_name="defi_features").check_data_quality_constraints()
