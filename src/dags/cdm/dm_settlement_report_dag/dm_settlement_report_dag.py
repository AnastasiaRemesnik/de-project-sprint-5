import logging
import pendulum
from airflow.decorators import dag, task
from examples.cdm.dm_settlement_report_dag.settlement_report_loader import SettlementReportLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)  

@dag(
    schedule_interval='*/15 * * * *',  # Планируем выполнение DAG каждый день в 2:00 ночи.
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),  # Устанавливаем дату начала выполнения DAG.
    catchup=False,  # Отключаем подхват пропущенных запусков.
    tags=['dds', 'stg', 'dm_orders'],  # Добавляем теги для удобной классификации DAG.
    is_paused_upon_creation=False  # Устанавливаем состояние активным при создании.
)
def dm_settlement_report_dag():  # Определяем функцию DAG
    pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Создаем подключение к базе данных

    @task(task_id="load_settlement_report")  # Декорируем функцию как задачу в DAG
    def load_settlement_report_task():  # Определяем функцию задачи
        loader = SettlementReportLoader(pg_dest) 
        loader.load_report_by_days() 

    load_settlement_report_task()  # Запускаем задачу загрузки продуктов

dm_settlement_report_dag_instance = dm_settlement_report_dag()  # Создаем экземпляр DAG
