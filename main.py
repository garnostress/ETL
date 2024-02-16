import requests
from datetime import datetime, timezone, timedelta
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def fetch_comments(start=0, end=210, limit=50):
    all_comments = []
    total_records = min(end, 210)  # 1. Определяем общее количество записей

    for offset in range(start, total_records, limit):
        remaining_records = total_records - offset
        current_limit = min(limit, remaining_records)

        response = requests.get(f"https://dummyjson.com/comments?skip={offset}&limit={current_limit}")
        if response.status_code == 200:
            data = response.json()['comments']
            all_comments.extend(data)
        else:
            logger.error(f"Error fetching comments: {response.status_code}")
            break

        if len(all_comments) >= 210:
            break

    return all_comments


def add_dates_to_comments(comments):
    msk_timezone = timezone(timedelta(hours=3))
    upload_date = datetime.now(msk_timezone).isoformat()
    for comment in comments:
        comment['uploadDate'] = upload_date  # 2. Добавление даты загрузки
        comment['modifyDate'] = None  # 2. Добавление даты изменения (по умолчанию пусто)


def upload_users_and_comments(comments):  # 3. Загружает данные в БД Postgres
    conn = psycopg2.connect(dbname='', user='', password='', host='')  # Добавить данные для подключения к БД
    create_tables_if_not_exists(conn)
    with conn.cursor() as cur:
        for comment in comments:
            cur.execute("SELECT id FROM users WHERE id = %s", (comment['user']['id'],))
            user = cur.fetchone()
            if not user:
                cur.execute(
                    "INSERT INTO users (id, username) VALUES (%s, %s)",
                    (comment['user']['id'], comment['user']['username'])
                )
            cur.execute(
                "INSERT INTO comments (id, postId, body, userId, uploadDate, modifyDate) VALUES (%s, %s, %s, %s, %s, %s)",
                (comment['id'], comment['postId'], comment['body'], comment['user']['id'], comment['uploadDate'],
                 comment['modifyDate'])
            )
        conn.commit()
    conn.close()


comments = fetch_comments()
add_dates_to_comments(comments)
upload_users_and_comments(comments)
