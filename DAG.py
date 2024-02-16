from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime, timezone, timedelta
import psycopg2
import logging
import re


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'employee',
    'email': 'leight1991@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'depends_on_past': False,
    'retry_delay': 60,
    'start_date': datetime(2024, 2, 16)
}

def create_tables_if_not_exists(connection):
    with connection.cursor() as cur:
        # Создание таблицу пользователей, если она не существует
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(255)
            );
        """)
        # Создание таблицу комментариев, если она не существует
        cur.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY,
                postId INTEGER,
                body TEXT,
                userId INTEGER REFERENCES users(id),
                uploadDate TIMESTAMP WITH TIME ZONE,
                modifyDate TIMESTAMP WITH TIME ZONE
            );
        """)
        connection.commit()


def check_api_availability(url="https://dummyjson.com/comments"):
    #Проверяет доступность API.
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
        else:
            logger.error(f"API недоступно: Статус {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при проверке API: {e}")
        return False


def fetch_comments_and_users(start=100, **kwargs):
    #Загружает данные о комментариях и пользователях, начиная с указанной записи.
    all_comments = []

    if not check_api_availability():
        return all_comments

    initial_response = requests.get("https://dummyjson.com/comments")
    if initial_response.status_code == 200:
        total_records = initial_response.json()[
            'total']
    else:
        logger.error(f"Ошибка при получении общего количества комментариев: {initial_response.status_code}")
        return all_comments

    limit = 50
    for offset in range(start, total_records, limit):
        response = requests.get(f"https://dummyjson.com/comments?skip={offset}&limit={limit}")
        if response.status_code == 200:
            data = response.json()['comments']
            all_comments.extend(data)
            if len(data) < limit:
                break
        else:
            logger.error(f"Ошибка при загрузке комментариев: {response.status_code}")
            break
    logger.info(f"Возвращаем {len(all_comments)} комментариев")
    return all_comments

def clean_username(user):
    #Оставляет в username только латинские буквы и цифру 1, остальные символы удаляются.
    user['username'] = re.sub(r'[^a-zA-Z1]', '', user['username'])
    return user

def process_and_upload_data(**kwargs):
    ti = kwargs['ti']
    comments = ti.xcom_pull(task_ids='fetch_comments_and_users')
    logger.info(f"Получено {len(comments)} комментариев для обработки")
    #Обрабатывает и загружает данные, обновляя существующие записи при необходимости.
    conn = psycopg2.connect(dbname='', user='', password='', host='')  # Необходимо заполнить данные для подключения
    create_tables_if_not_exists(conn)

    with conn.cursor() as cur:
        for comment in comments:
            comment['user'] = clean_username(comment['user'])
            # Обновление или вставка пользователя
            cur.execute("""
                INSERT INTO users (id, username) VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username;
            """, (comment['user']['id'], comment['user']['username']))

            # Обновление или вставка комментария
            cur.execute("""
                INSERT INTO comments (id, postId, body, userId, uploadDate, modifyDate)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET 
                postId = EXCLUDED.postId, 
                body = EXCLUDED.body, 
                userId = EXCLUDED.userId, 
                uploadDate = EXCLUDED.uploadDate, 
                modifyDate = EXCLUDED.modifyDate;
            """, (comment['id'], comment['postId'], comment['body'], comment['user']['id'], datetime.now(timezone.utc), None))
        conn.commit()
    conn.close()

with DAG(
    dag_id='username_change',
    tags=['test, username', '10 days'],
    description='Загрузка комментариев каждые 10 дней с изменением имени',
    schedule_interval=timedelta(days=10),  # Указываем интервал 10 дней
    default_args=default_args,
) as dag:

    # Задачи
    check_api_task = PythonOperator(
        task_id='check_api_availability',
        python_callable=check_api_availability,
        dag=dag,
    )

    fetch_comments_task = PythonOperator(
        task_id='fetch_comments_and_users',
        python_callable=fetch_comments_and_users,
        op_kwargs={'start': 100},  # Начинаем с 100-й записи
        dag=dag,
    )

    process_and_upload_data_task = PythonOperator(
        task_id='process_and_upload_data',
        python_callable=process_and_upload_data,
        dag=dag,
    )

    # Установка последовательности выполнения задач
    check_api_task >> fetch_comments_task >> process_and_upload_data_task
