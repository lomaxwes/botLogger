import os
import sys
import psycopg2
from psycopg2 import pool
import json
import time
import logging
from dotenv import load_dotenv
import traceback
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

load_dotenv()

dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")

if not all([dbname, user, password, host]):
    logger.error("Не все переменные окружения загружены.")
    sys.exit(1)

def init_db_pool():
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, dbname=dbname, user=user, password=password, host=host
        )
        logger.info("✅ Пул соединений с БД успешно создан!")
        return connection_pool
    except Exception as e:
        logger.error(f"❌ Ошибка при создании пула соединений: {e}")
        sys.exit(1)


def clean_text(value):
    if isinstance(value, str):
        return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', value)
    return value


connection_pool = init_db_pool()

def check_connection(conn):
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
            conn.commit()
        return True
    except psycopg2.Error:
        return False

def execute_query(query, params):
    conn = None
    cur = None
    try:
        conn = connection_pool.getconn()
        
        if not check_connection(conn):
            logger.warning("🔄 Соединение с БД потеряно, восстанавливаем соединение.")
            connection_pool.putconn(conn)
            conn = connection_pool.getconn()

        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        logger.info("✅ Лог успешно записан в БД.")
        
    except psycopg2.Error as e:
        logger.error(f"❌ Ошибка записи в БД: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            connection_pool.putconn(conn)

log_file_path = "/var/log/nginx/access.log"

try:
    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as log_file:
        while True:
            line = log_file.readline()
            if not line:
                time.sleep(1)
                continue

            try:
                log = json.loads(line.strip())
            except json.JSONDecodeError:
                logger.warning("❌ Некорректный JSON формат в логе.")
                continue

            query = """
                INSERT INTO nginx_logs 
                (time_local, remote_addr, request, status, body_bytes_sent, http_referer, http_user_agent) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                clean_text(log.get('time_local') or ''),
                clean_text(log.get('remote_addr') or ''),
                clean_text(log.get('request') or ''),
                log.get('status') or 0,
                log.get('body_bytes_sent') or 0,
                clean_text(log.get('http_referer', '')),
                clean_text(log.get('http_user_agent', ''))
            )
            execute_query(query, params)

except KeyboardInterrupt:
    logger.info("⏹ Программа остановлена пользователем.")

except Exception as e:
    logger.error(f"❌ Произошла непредвиденная ошибка: {e}")
    logger.error(traceback.format_exc())

finally:
    try:
        connection_pool.closeall()
        logger.info("🔌 Все соединения с БД закрыты.")
    except Exception as e:
        logger.error(f"❌ Ошибка при закрытии соединений с БД: {e}")