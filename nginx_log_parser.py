import os
import sys
import psycopg2
import json
import time
from dotenv import load_dotenv

load_dotenv()

dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")

if not all([dbname, user, password, host]):
    print("❌ Ошибка: не все переменные окружения загружены.")
    sys.exit(1)

def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host
            )
            print("✅ Успешное подключение к БД!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"🔄 Ошибка подключения: {e}. Повтор через 5 секунд...")
            time.sleep(5)

conn = connect_db()
cur = conn.cursor()

try:
    for line in sys.stdin:
        try:
            log = json.loads(line.strip())
            cur.execute(
                """
                INSERT INTO nginx_logs 
                (time_local, remote_addr, request, status, body_bytes_sent, http_referer, http_user_agent) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    log.get('time_local'),
                    log.get('remote_addr'),
                    log.get('request'),
                    log.get('status'),
                    log.get('body_bytes_sent'),
                    log.get('http_referer', ''),
                    log.get('http_user_agent', '')
                )
            )
            conn.commit()
            print("✅ Лог успешно записан в БД.")

        except json.JSONDecodeError:
            print("❌ Ошибка: некорректный JSON формат.")
        except psycopg2.Error as e:
            print(f"❌ Ошибка записи в БД: {e}")
            conn.rollback()

except KeyboardInterrupt:
    print("⏹ Программа остановлена пользователем.")

finally:
    cur.close()
    conn.close()
    print("🔌 Соединение с БД закрыто.")