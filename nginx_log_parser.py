import os
import sys
import psycopg2
import json
import time
import fcntl
from dotenv import load_dotenv
from inotify_simple import INotify, flags

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

log_file_path = "/var/log/nginx/access.log"

inotify = INotify()
watch_flags = flags.MODIFY
inotify.add_watch(log_file_path, watch_flags)

try:
    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as log_file:
        while True:
            for event in inotify.read():
                if event.mask & flags.MODIFY:
                    line = log_file.readline()
                    if not line:
                        continue

                    try:
                        log = json.loads(line.strip())
                    except json.JSONDecodeError:
                        print("❌ Ошибка: некорректный JSON формат.")
                        continue

                    try:
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
                    except psycopg2.Error as e:
                        print(f"❌ Ошибка записи в БД: {e}")
                        conn.rollback()
                        conn = connect_db()
                        cur = conn.cursor()

except KeyboardInterrupt:
    print("⏹ Программа остановлена пользователем.")

finally:
    try:
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка при закрытии соединения: {e}")
    print("🔌 Соединение с БД закрыто.")