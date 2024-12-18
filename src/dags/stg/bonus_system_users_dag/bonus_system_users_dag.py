import logging
import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_users_dag.users_loader import UsersLoader  # Обновляем импорт на новый модуль
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin', 'example'],
    is_paused_upon_creation=True
)
def sprint5_example_stg_bonus_system_users_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="users_load")
    def load_users():
        users_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        users_loader.load_users()

    load_users()

stg_bonus_system_users_dag = sprint5_example_stg_bonus_system_users_dag()


