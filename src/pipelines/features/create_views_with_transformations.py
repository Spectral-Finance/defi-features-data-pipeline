import awswrangler as wr
import glob
from src.helpers.files import read_sql_file

from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name="defi_features_create_views_with_transformations")


def create_views_with_transformations(view_sql_path: str, defi_features_name: str) -> None:
    """Create views with transformations for defi features

    Args:
        view_sql_path (str): path to sql file with view definitions
        defi_features_name (str): name of the defi features

    Returns:
        None
    """

    sql = read_sql_file(view_sql_path)

    try:
        wr.athena.start_query_execution(sql=sql, database="db_analytics_prod", wait=True)

        logger.info(f"View created successfully - {defi_features_name}")

    except Exception as e:
        logger.error(f"Error creating view - {defi_features_name} - {e}")


def main():
    """Main function to create views with transformations for defi features

    Args:
        None

    Returns:
        None
    """

    view_sql_paths = glob.glob("src/pipelines/features/transformations/view_defi_*.sql")

    for view_sql_path in view_sql_paths:
        defi_features_name = view_sql_path.split("/")[-1].split(".")[0]
        create_views_with_transformations(view_sql_path, defi_features_name)


if __name__ == "__main__":
    main()
