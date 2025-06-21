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
        conn = get_db_connection()
        cur = conn.cursor()
        
        for demand in demands:
            # Функция для извлечения значения атрибута
            def get_attribute_value(attr_name, default=""):
                for attr in demand.get("attributes", []):
                    if attr["name"] == attr_name:
                        # Если значение - объект (customentity), берем его name
                        if isinstance(attr["value"], dict) and "name" in attr["value"]:
                            return str(attr["value"]["name"])
                        return str(attr["value"] if attr["value"] is not None else default)
                return default
            
            # Функция для обрезки строк до нужной длины
            def truncate(value, max_length):
                if value is None:
                    return ""
                return str(value)[:max_length]
            
            # Обрабатываем момент (дату)
            moment = demand.get("moment", "")
            if isinstance(moment, dict):
                moment = moment.get("value", "")
            
            # Подготавливаем значения для вставки
            values = {
                "id": truncate(demand.get("id", ""), 255),
                "number": truncate(demand.get("name", ""), 50),
                "date": moment,
                "counterparty": truncate(demand.get("agent", {}).get("name", ""), 255),
                "store": truncate(demand.get("store", {}).get("name", ""), 255),
                "project": truncate(demand.get("project", {}).get("name", ""), 255),
                "sales_channel": truncate(demand.get("salesChannel", {}).get("name", ""), 255),
                "amount": float(demand.get("sum", 0)) / 100,
                "cost_price": 0,  # по умолчанию
                "overhead": 0,   # по умолчанию
                "profit": 0,     # по умолчанию
                "promo_period": truncate(get_attribute_value("Акционный период"), 100),
                "delivery_amount": float(get_attribute_value("Сумма доставки", 0)),
                "admin_data": truncate(get_attribute_value("Адмидат"), 255),
                "gdeslon": truncate(get_attribute_value("ГдеСлон"), 255),
                "cityads": truncate(get_attribute_value("CityAds"), 255),
                "ozon": truncate(get_attribute_value("Ozon"), 255),
                "ozon_fbs": truncate(get_attribute_value("Ozon FBS"), 255),
                "yamarket_fbs": truncate(get_attribute_value("Яндекс Маркет FBS"), 255),
                "yamarket_dbs": truncate(get_attribute_value("Яндекс Маркет DBS"), 255),
                "yandex_direct": truncate(get_attribute_value("Яндекс Директ"), 255),
                "price_ru": truncate(get_attribute_value("Price ru"), 255),
                "wildberries": truncate(get_attribute_value("Wildberries"), 255),
                "gis2": truncate(get_attribute_value("2Gis"), 255),
                "seo": truncate(get_attribute_value("SEO"), 255),
                "programmatic": truncate(get_attribute_value("Программатик"), 255),
                "avito": truncate(get_attribute_value("Авито"), 255),
                "multiorders": truncate(get_attribute_value("Мультиканальные заказы"), 255),
                "estimated_discount": float(get_attribute_value("Примеренная скидка", 0)),
                "status": truncate(demand.get("state", {}).get("name", ""), 100),
                "comment": truncate(demand.get("description", ""), 255)
            }
            
            cur.execute("""
                INSERT INTO demands (
                    id, number, date, counterparty, store, project, sales_channel, 
                    amount, cost_price, overhead, profit, promo_period, delivery_amount,
                    admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
                    yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                    programmatic, avito, multiorders, estimated_discount, status, comment
                )
                VALUES (
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
        
        conn.commit()
        return {"message": f"Успешно сохранено {len(demands)} записей"}
    except Exception as e:
        if conn:
            conn.rollback()
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