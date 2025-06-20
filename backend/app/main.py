from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from .moysklad import MoyskladAPI
from datetime import datetime
import os
from openpyxl import Workbook
import io

app = FastAPI()

# Настройки базы данных
DB_CONFIG = {
    "host": "87.228.99.200",  # IP-адрес, который работает
    "port": 5432,             # Порт, который работает (5432 вместо 5433)
    "dbname": "MS",           # Имя базы, к которой вы подключились
    "user": "louella",        # Ваш пользователь
    "password": "XBcMJoEO1ljb",  # Ваш пароль
    "sslmode": "verify-ca",   # Режим SSL
    "sslrootcert": "/root/.postgresql/root.crt"  # Путь к сертификату
}

# Инициализация API МойСклад
moysklad = MoyskladAPI(token="2e61e26f0613cf33fab5f31cf105302aa2d607c3")

class DateRange(BaseModel):
    start_date: str
    end_date: str

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """Инициализация таблицы в базе данных"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS demands (
                id VARCHAR(255) PRIMARY KEY,
                number VARCHAR(50),
                date TIMESTAMP,
                counterparty VARCHAR(255),
                amount NUMERIC(10, 2),
                status VARCHAR(100),
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    except Exception as e:
        print(f"Ошибка при инициализации БД: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# Инициализация БД при старте
init_db()

@app.post("/api/save-to-db")
async def save_to_db(date_range: DateRange):
    conn = None
    try:
        # Проверяем/создаём таблицу перед работой с ней
        init_db()
        
        demands = moysklad.get_demands(date_range.start_date, date_range.end_date)
        conn = get_db_connection()
        cur = conn.cursor()
        
        for demand in demands:
            cur.execute("""
                INSERT INTO demands (id, number, date, counterparty, amount, status, comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    number = EXCLUDED.number,
                    date = EXCLUDED.date,
                    counterparty = EXCLUDED.counterparty,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    comment = EXCLUDED.comment
            """, (
                demand.get("name", ""),
                demand.get("moment", ""),
                demand.get("agent", {}).get("name", ""),
                demand.get("sum", 0) / 100,
            ))
        
        conn.commit()
        return {"message": f"Успешно сохранено {len(demands)} записей"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

@app.post("/api/export/excel")
async def export_excel(date_range: DateRange):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, number, date, counterparty, amount, status, comment
            FROM demands
            WHERE date BETWEEN %s AND %s
        """, (date_range.start_date, date_range.end_date))
        
        rows = cur.fetchall()
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Отгрузки"
        
        headers = ["ID", "Номер", "Дата", "Контрагент", "Сумма", "Статус", "Комментарий"]
        ws.append(headers)
        
        for row in rows:
            ws.append(row)
        
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return {
            "file": buffer.read().hex(),
            "filename": f"demands_{date_range.start_date}_{date_range.end_date}.xlsx"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()
