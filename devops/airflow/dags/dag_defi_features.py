import os
from datetime import datetime
import sys
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from helper_dag import ecs_task_template, slack_alert


ARGS = {
    "owner": "Spectral",
    "description": "Decentrilized Finance data pipeline",
    "retry": 3,
    "start_date": datetime(2023, 11, 21),
    "depend_on_past": False,
    "on_failure_callback": slack_alert,
}

DAG_ID = "dag_defi_features"

SCHEDULE = "0 3 * * *"

TAGS = ["DEFI_FEATURES", "ELT", "DAILY"]

ENV = Variable.get("environment")

STACK_NAME = "defi-features-data-pipeline"

PROJECT = "defi_features"

STREAM_LOG_PREFIX = "defi-features-data-pipeline"

MEMORY_RESERVATION = 4096


def get_secret():

    secrets_client = boto3.client("secretsmanager", region_name="us-east-2")
    secrets = secrets_client.get_secret_value(SecretId="prod/api/keys")
    return eval(secrets["SecretString"])


event_names = ["deposit", "borrow", "repay", "liquidation", "withdraw"]
secrets = get_secret()
transpose_api_keys = {key: value for key, value in secrets.items() if key.startswith("TRANSPOSE_API_KEY_")}
event_dict = {event_names[i]: list(transpose_api_keys.values())[i] for i in range(len(transpose_api_keys))}


with DAG(DAG_ID, schedule_interval=SCHEDULE, default_args=ARGS, catchup=False, max_active_runs=1, tags=TAGS) as dag:

    start = DummyOperator(task_id="start")
    teardown = DummyOperator(task_id="teardown")

    with TaskGroup(group_id="defi_events", dag=dag) as defi_events:
        for event_name, transpose_api_key in event_dict.items():
            raw_layer = ECSOperator(
                task_id=f"raw_layer_{event_name}",
                **ecs_task_template(
                    command_list=[
                        "python",
                        "src/pipelines/raw/defi_events.py",
                        "--event",
                        event_name,
                        "--api_key",
                        transpose_api_key,
                    ],
                    stack_name=f"{STACK_NAME}-{ENV}",
                    project=PROJECT,
                    stream_log_prefix=f"raw-{STREAM_LOG_PREFIX}-{event_name}",
                    memory_reservation=MEMORY_RESERVATION,
                ),
            )

            stage_layer = ECSOperator(
                task_id=f"stage_layer_{event_name}",
                **ecs_task_template(
                    command_list=["python", "src/pipelines/stage/defi_events.py", "--event", event_name],
                    stack_name=f"{STACK_NAME}-{ENV}",
                    project=PROJECT,
                    stream_log_prefix=f"stage-{STREAM_LOG_PREFIX}-{event_name}",
                    memory_reservation=MEMORY_RESERVATION,
                ),
            )

            analytics_layer = ECSOperator(
                task_id=f"analytics_layer_{event_name}",
                **ecs_task_template(
                    command_list=["python", "src/pipelines/analytics/defi_events.py", "--event", event_name],
                    stack_name=f"{STACK_NAME}-{ENV}",
                    project=PROJECT,
                    stream_log_prefix=f"analytics-{STREAM_LOG_PREFIX}-{event_name}",
                    memory_reservation=MEMORY_RESERVATION,
                ),
            )

            raw_layer >> stage_layer >> analytics_layer

    with TaskGroup(group_id="historical_market_data", dag=dag) as historical_market:
        raw_layer = ECSOperator(
            task_id=f"raw_layer",
            **ecs_task_template(
                command_list=["python", "src/pipelines/raw/historical_market_data.py"],
                stack_name=f"{STACK_NAME}-{ENV}",
                project=PROJECT,
                stream_log_prefix=f"raw-{STREAM_LOG_PREFIX}-historical_market_data",
                memory_reservation=MEMORY_RESERVATION,
            ),
        )

        stage_layer = ECSOperator(
            task_id=f"stage_layer",
            **ecs_task_template(
                command_list=["python", "src/pipelines/stage/historical_market_data.py"],
                stack_name=f"{STACK_NAME}-{ENV}",
                project=PROJECT,
                stream_log_prefix=f"stage-{STREAM_LOG_PREFIX}-historical_market_data",
                memory_reservation=MEMORY_RESERVATION,
            ),
        )

        raw_layer >> stage_layer

    with TaskGroup(group_id="historical_account_positions", dag=dag) as historical_market_data:
        raw_layer = ECSOperator(
            task_id=f"raw_layer",
            **ecs_task_template(
                command_list=["python", "src/pipelines/raw/historical_account_positions.py"],
                stack_name=f"{STACK_NAME}-{ENV}",
                project=PROJECT,
                stream_log_prefix=f"raw-{STREAM_LOG_PREFIX}-historical_account_positions",
                memory_reservation=MEMORY_RESERVATION,
            ),
        )

        stage_layer = ECSOperator(
            task_id=f"stage_layer",
            **ecs_task_template(
                command_list=["python", "src/pipelines/stage/historical_account_positions.py"],
                stack_name=f"{STACK_NAME}-{ENV}",
                project=PROJECT,
                stream_log_prefix=f"stage-{STREAM_LOG_PREFIX}-historical_account_positions",
                memory_reservation=MEMORY_RESERVATION,
            ),
        )

        raw_layer >> stage_layer

    wait_for_previous_tasks = DummyOperator(task_id="wait_for_previous_tasks")

    current_positiion = ECSOperator(
        task_id="current_collateral_positions",
        **ecs_task_template(
            command_list=["python", "src/pipelines/analytics/current_collateral_positions.py"],
            stack_name=f"{STACK_NAME}-{ENV}",
            project=PROJECT,
            stream_log_prefix=f"analytics-{STREAM_LOG_PREFIX}",
            memory_reservation=MEMORY_RESERVATION,
        ),
    )

    merge_historical_market_date_and_account_positions = ECSOperator(
        task_id="merge_historical_market_date_and_account_positions",
        **ecs_task_template(
            command_list=["python", "src/pipelines/analytics/historical_market_data_and_account_positions.py"],
            stack_name=f"{STACK_NAME}-{ENV}",
            project=PROJECT,
            stream_log_prefix=f"analytics-{STREAM_LOG_PREFIX}",
            memory_reservation=MEMORY_RESERVATION,
        ),
    )

    save_new_wallet_features_set_datalake = ECSOperator(
        task_id="save_new_wallet_features_set_datalake",
        **ecs_task_template(
            command_list=["python", "src/pipelines/features/save_defi_features_datalake.py"],
            stack_name=f"{STACK_NAME}-{ENV}",
            project=PROJECT,
            stream_log_prefix=f"analytics-{STREAM_LOG_PREFIX}",
            memory_reservation=MEMORY_RESERVATION,
        ),
    )

    data_quality_check = ECSOperator(
        task_id="data_quality_checks",
        **ecs_task_template(
            command_list=["python", "src/pipelines/features/defi_features_data_quality_pipeline.py"],
            stack_name=f"{STACK_NAME}-{ENV}",
            project=PROJECT,
            stream_log_prefix=f"analytics-{STREAM_LOG_PREFIX}",
            memory_reservation=MEMORY_RESERVATION,
        ),
    )

    save_new_wallet_features_set_features_db = ECSOperator(
        task_id="save_new_wallet_features_set_features_db",
        **ecs_task_template(
            command_list=["python", "src/pipelines/features/save_defi_features_features_db.py"],
            stack_name=f"{STACK_NAME}-{ENV}",
            project=PROJECT,
            stream_log_prefix=f"analytics-{STREAM_LOG_PREFIX}",
            memory_reservation=MEMORY_RESERVATION,
        ),
    )

    start >> defi_events >> wait_for_previous_tasks >> current_positiion >> save_new_wallet_features_set_datalake
    start >> historical_market_data >> wait_for_previous_tasks >> merge_historical_market_date_and_account_positions
    (
        start
        >> historical_market
        >> wait_for_previous_tasks
        >> merge_historical_market_date_and_account_positions
        >> save_new_wallet_features_set_datalake
        >> data_quality_check
        >> save_new_wallet_features_set_features_db
        >> teardown
    )
