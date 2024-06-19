import psycopg2

# SQL-запросы для создания таблиц
create_signals_table = """
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER,
    message_id INTEGER,
    channel_name TEXT,
    date TEXT,
    time TEXT,
    coin TEXT,
    trend TEXT,
    tvh TEXT,
    rvh TEXT,
    lvh TEXT,
    targets TEXT,
    stop_loss TEXT,
    leverage TEXT,
    margin TEXT
);
"""

create_verified_signals_table = """
CREATE TABLE IF NOT EXISTS verified_signals (
    id SERIAL PRIMARY KEY,
    channel_id INTEGER,
    message_id INTEGER,
    channel_name TEXT,
    date TEXT,
    time TEXT,
    coin TEXT,
    trend TEXT,
    tvh TEXT,
    rvh TEXT,
    lvh TEXT,
    targets TEXT,
    stop_loss TEXT,
    leverage TEXT,
    margin TEXT
);
"""

create_defective_signals_table = """
CREATE TABLE IF NOT EXISTS defective_signals (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER,
    message TEXT
);
"""

create_packaged_signals_table = """
CREATE TABLE IF NOT EXISTS packaged_signals (
    id SERIAL PRIMARY KEY,
    region TEXT,
    region_number TEXT,
    channel_type TEXT,
    signal_channel_id INTEGER,
    signal_message_id INTEGER,
    date TEXT
);
"""

# Функция для создания таблиц
def create_tables(conn):
    with conn.cursor() as cursor:
        cursor.execute(create_signals_table)
        cursor.execute(create_verified_signals_table)
        cursor.execute(create_defective_signals_table)
        cursor.execute(create_packaged_signals_table)
    conn.commit()

# Подключение к PostgreSQL и создание таблиц
try:
    conn = psycopg2.connect(
                host='127.0.0.1',
                port="5432",
                database='signals_parser',
                user='parser',
                password='parser'
            )
    create_tables(conn)
    print("Таблицы успешно созданы!")
except psycopg2.Error as e:
    print(f"Ошибка при подключении к PostgreSQL: {e}")
finally:
    if conn:
        conn.close()

