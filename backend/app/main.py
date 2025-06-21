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
    "host": "87.228.99.200",
    "port": 5432,
    "dbname": "MS",
    "user": "louella",
    "password": "XBcMJoEO1ljb",
    "sslmode": "verify-ca",
    "sslrootcert": "/root/.postgresql/root.crt"
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
                store VARCHAR(255),
                project VARCHAR(255),
                sales_channel VARCHAR(255),
                amount NUMERIC(10, 2),
                cost_price NUMERIC(10, 2),
                overhead NUMERIC(10, 2),
                profit NUMERIC(10, 2),
                promo_period VARCHAR(100),
                delivery_amount NUMERIC(10, 2),
                admin_data VARCHAR(255),
                gdeslon VARCHAR(255),
                cityads VARCHAR(255),
                ozon VARCHAR(255),
                ozon_fbs VARCHAR(255),
                yamarket_fbs VARCHAR(255),
                yamarket_dbs VARCHAR(255),
                yandex_direct VARCHAR(255),
                price_ru VARCHAR(255),
                wildberries VARCHAR(255),
                gis2 VARCHAR(255),
                seo VARCHAR(255),
                programmatic VARCHAR(255),
                avito VARCHAR(255),
                multiorders VARCHAR(255),
                estimated_discount NUMERIC(10, 2),
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
        init_db()
        demands = moysklad.get_demands(date_range.start_date, date_range.end_date)
        if not demands:
            return {"message": "Нет данных для сохранения"}

        conn = get_db_connection()
        cur = conn.cursor()
        
        saved_count = 0
        
        for demand in demands:
            try:
                # Функция для безопасного извлечения атрибутов
                def get_attr_value(attrs, attr_name, default=""):
                    if not attrs:
                        return default
                    for attr in attrs:
                        if attr.get("name") == attr_name:
                            value = attr.get("value")
                            if isinstance(value, dict):
                                return value.get("name", str(value))
                            return str(value) if value is not None else default
                    return default

                attributes = demand.get("attributes", [])
                
                # Обработка накладных расходов (overhead)
                overhead_data = demand.get("overhead", {})
                overhead_sum = float(overhead_data.get("sum", 0)) / 100  # Делим на 100 для перевода в рубли
                
                # Основные данные
                values = {
                    "id": str(demand.get("id", ""))[:255],
                    "number": str(demand.get("name", ""))[:50],
                    "date": demand.get("moment", ""),
                    "counterparty": str(demand.get("agent", {}).get("name", ""))[:255],
                    "store": str(demand.get("store", {}).get("name", ""))[:255],
                    "project": str(demand.get("project", {}).get("name", "Без проекта"))[:255],
                    "sales_channel": str(demand.get("salesChannel", {}).get("name", "Без канала"))[:255],
                    "amount": float(demand.get("sum", 0)) / 100,
                    "cost_price": 0,
                    "overhead": overhead_sum,  # Используем рассчитанные накладные расходы
                    "profit": 0,
                    "status": str(demand.get("state", {}).get("name", ""))[:100],
                    "comment": str(demand.get("description", ""))[:255]
                }

                # Остальная обработка атрибутов остается без изменений
                attr_fields = {
                    "promo_period": ("Акционный период", ""),
                    "delivery_amount": ("Сумма доставки", 0),
                    "admin_data": ("Адмидат", 0),
                    "gdeslon": ("ГдеСлон", 0),
                    "cityads": ("CityAds", 0),
                    "ozon": ("Ozon", 0),
                    "ozon_fbs": ("Ozon FBS", 0),
                    "yamarket_fbs": ("Яндекс Маркет FBS", 0),
                    "yamarket_dbs": ("Яндекс Маркет DBS", 0),
                    "yandex_direct": ("Яндекс Директ", 0),
                    "price_ru": ("Price ru", 0),
                    "wildberries": ("Wildberries", 0),
                    "gis2": ("2Gis", 0),
                    "seo": ("SEO", 0),
                    "programmatic": ("Программатик", 0),
                    "avito": ("Авито", 0),
                    "multiorders": ("Мультиканальные заказы", 0),
                    "estimated_discount": ("Примеренная скидка", 0)
                }

                for field, (attr_name, default) in attr_fields.items():
                    if field.endswith("_amount") or field == "estimated_discount":
                        try:
                            values[field] = float(get_attr_value(attributes, attr_name, default))
                        except (ValueError, TypeError):
                            values[field] = 0.0
                    else:
                        values[field] = str(get_attr_value(attributes, attr_name, default))[:255]
                
                # Логирование для отладки
                print(f"Данные для сохранения: {values}")

                # SQL-запрос (без изменений)
                cur.execute("""
                    INSERT INTO demands (
                        id, number, date, counterparty, store, project, sales_channel, 
                        amount, cost_price, overhead, profit, promo_period, delivery_amount,
                        admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
                        yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                        programmatic, avito, multiorders, estimated_discount, status, comment
                    ) VALUES (
                        %(id)s, %(number)s, %(date)s, %(counterparty)s, %(store)s, %(project)s, %(sales_channel)s,
                        %(amount)s, %(cost_price)s, %(overhead)s, %(profit)s, %(promo_period)s, %(delivery_amount)s,
                        %(admin_data)s, %(gdeslon)s, %(cityads)s, %(ozon)s, %(ozon_fbs)s, %(yamarket_fbs)s,
                        %(yamarket_dbs)s, %(yandex_direct)s, %(price_ru)s, %(wildberries)s, %(gis2)s, %(seo)s,
                        %(programmatic)s, %(avito)s, %(multiorders)s, %(estimated_discount)s, %(status)s, %(comment)s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        number = EXCLUDED.number,
                        date = EXCLUDED.date,
                        counterparty = EXCLUDED.counterparty,
                        store = EXCLUDED.store,
                        project = EXCLUDED.project,
                        sales_channel = EXCLUDED.sales_channel,
                        amount = EXCLUDED.amount,
                        cost_price = EXCLUDED.cost_price,
                        overhead = EXCLUDED.overhead,
                        profit = EXCLUDED.profit,
                        promo_period = EXCLUDED.promo_period,
                        delivery_amount = EXCLUDED.delivery_amount,
                        admin_data = EXCLUDED.admin_data,
                        gdeslon = EXCLUDED.gdeslon,
                        cityads = EXCLUDED.cityads,
                        ozon = EXCLUDED.ozon,
                        ozon_fbs = EXCLUDED.ozon_fbs,
                        yamarket_fbs = EXCLUDED.yamarket_fbs,
                        yamarket_dbs = EXCLUDED.yamarket_dbs,
                        yandex_direct = EXCLUDED.yandex_direct,
                        price_ru = EXCLUDED.price_ru,
                        wildberries = EXCLUDED.wildberries,
                        gis2 = EXCLUDED.gis2,
                        seo = EXCLUDED.seo,
                        programmatic = EXCLUDED.programmatic,
                        avito = EXCLUDED.avito,
                        multiorders = EXCLUDED.multiorders,
                        estimated_discount = EXCLUDED.estimated_discount,
                        status = EXCLUDED.status,
                        comment = EXCLUDED.comment
                """, values)
                
                saved_count += 1

            except Exception as e:
                print(f"Ошибка при обработке отгрузки {demand.get('id')}: {str(e)}")
                continue
        
        conn.commit()
        return {"message": f"Успешно сохранено {saved_count} из {len(demands)} записей"}

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Критическая ошибка: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

@app.post("/api/export/excel")
async def export_excel(date_range: DateRange):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                number, date, counterparty, store, project, sales_channel,
                amount, cost_price, overhead, profit, promo_period, delivery_amount,
                admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
                yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                programmatic, avito, multiorders, estimated_discount
            FROM demands
            WHERE date BETWEEN %s AND %s
        """, (date_range.start_date, date_range.end_date))
        
        rows = cur.fetchall()
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет по отгрузкам"
        
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
            "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
            "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
            "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
            "Примеренная скидка"
        ]
        ws.append(headers)
        
        for row in rows:
            ws.append(row)
        
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return {
            "file": buffer.read().hex(),
            "filename": f"Отчет_по_отгрузкам_{date_range.start_date}_{date_range.end_date}.xlsx"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()