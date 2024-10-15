import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.stg.bonus_system_events_dag.events_loader import EventLoader  # Импортируем EventLoader

# Инициализируем логирование
log = logging.getLogger(__name__)

# Определяем DAG
@dag(
    schedule_interval='0/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),  # Дата начала выполнения дага
    catchup=False,  # Не запускаем задним числом
    tags=['sprint5', 'stg', 'origin', 'events'],  # Теги для фильтрации
    is_paused_upon_creation=False  # Запуск дага сразу после создания
)
def sprint5_stg_bonus_system_events_dag():
    # Подключение к DWH (destination)
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов (source)
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Таск для загрузки событий из outbox
    @task(task_id="events_load")
    def load_events():
        # Создаем экземпляр EventLoader, который будет обрабатывать загрузку
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        # Вызываем метод для загрузки событий
        event_loader.load_events()

    # Инициализация таска
    load_events()

# Инициализация DAG
stg_bonus_system_events_dag = sprint5_stg_bonus_system_events_dag()
