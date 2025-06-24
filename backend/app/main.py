from fastapi.responses import StreamingResponse
import time 
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import execute_batch
from .moysklad import MoyskladAPI
from datetime import datetime
import os
from openpyxl import Workbook
import io
import asyncio
from typing import List, Dict, Any
import logging
import uuid
from urllib.parse import quote


# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

class BatchProcessResponse(BaseModel):
    task_id: str
    status: str
    message: str

# Глобальный словарь для хранения статусов задач
tasks_status = {}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """Инициализация базы данных - создание таблиц если они не существуют"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Создаем таблицу demands, если не существует
        cur.execute("""
            CREATE TABLE IF NOT EXISTS demands (
                id VARCHAR(255) PRIMARY KEY,
                number VARCHAR(50),
                date TIMESTAMP,
                counterparty VARCHAR(255),
                store VARCHAR(255),
                project VARCHAR(255),
                sales_channel VARCHAR(255),
                amount NUMERIC(15, 2),
                cost_price NUMERIC(15, 2),
                overhead NUMERIC(15, 2),
                profit NUMERIC(15, 2),
                promo_period VARCHAR(255),
                delivery_amount NUMERIC(15, 2),
                admin_data NUMERIC(15, 2),
                gdeslon NUMERIC(15, 2),
                cityads NUMERIC(15, 2),
                ozon NUMERIC(15, 2),
                ozon_fbs NUMERIC(15, 2),
                yamarket_fbs NUMERIC(15, 2),
                yamarket_dbs NUMERIC(15, 2),
                yandex_direct NUMERIC(15, 2),
                price_ru NUMERIC(15, 2),
                wildberries NUMERIC(15, 2),
                gis2 NUMERIC(15, 2),
                seo NUMERIC(15, 2),
                programmatic NUMERIC(15, 2),
                avito NUMERIC(15, 2),
                multiorders NUMERIC(15, 2),
                estimated_discount NUMERIC(15, 2),
                status VARCHAR(100),
                comment VARCHAR(255)
            )
        """)
        
        # Создаем таблицу demand_items, если не существует
        cur.execute("""
            CREATE TABLE IF NOT EXISTS demand_items (
                id VARCHAR(255) PRIMARY KEY,
                demand_id VARCHAR(255) REFERENCES demands(id),
                demand_number VARCHAR(50),
                date TIMESTAMP,
                counterparty VARCHAR(255),
                store VARCHAR(255),
                project VARCHAR(255),
                sales_channel VARCHAR(255),
                product_name VARCHAR(255),
                quantity NUMERIC(15, 3),
                price NUMERIC(15, 2),
                amount NUMERIC(15, 2),
                cost_price NUMERIC(15, 2),
                article VARCHAR(255),
                code VARCHAR(255),
                overhead NUMERIC(15, 2),
                profit NUMERIC(15, 2),
                promo_period VARCHAR(255),
                delivery_amount NUMERIC(15, 2),
                admin_data NUMERIC(15, 2),
                gdeslon NUMERIC(15, 2),
                cityads NUMERIC(15, 2),
                ozon NUMERIC(15, 2),
                ozon_fbs NUMERIC(15, 2),
                yamarket_fbs NUMERIC(15, 2),
                yamarket_dbs NUMERIC(15, 2),
                yandex_direct NUMERIC(15, 2),
                price_ru NUMERIC(15, 2),
                wildberries NUMERIC(15, 2),
                gis2 NUMERIC(15, 2),
                seo NUMERIC(15, 2),
                programmatic NUMERIC(15, 2),
                avito NUMERIC(15, 2),
                multiorders NUMERIC(15, 2),
                estimated_discount NUMERIC(15, 2)
            )
        """)
        
        conn.commit()
        logger.info("Таблицы успешно созданы или уже существуют")
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

@app.on_event("startup")
async def startup_event():
    """Действия при старте приложения"""
    init_db()
    logger.info("Приложение запущено, база данных инициализирована")

async def process_demands_batch(demands: List[Dict[str, Any]], task_id: str):
    """Асинхронная обработка пакета отгрузок с улучшенным логированием"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        batch_size = 50
        saved_count = 0
        total_count = len(demands)
        
        logger.info(f"Начало обработки {total_count} отгрузок")
        
        demands_values = []
        items_values = []
        
        for idx, demand in enumerate(demands, 1):
            try:
                # Подготовка данных отгрузки
                demand_values = prepare_demand_data(demand)
                demands_values.append(demand_values)
                
                # Получаем позиции отгрузки
                demand_id = str(demand.get("id", ""))
                positions = moysklad.get_demand_positions(demand_id)
                
                # Подготовка данных позиций
                for position in positions:
                    item_values = prepare_demand_item_data(demand, position)
                    items_values.append(item_values)
                
                if len(demands_values) >= batch_size:
                    # Вставляем отгрузки
                    inserted = await insert_batch(cur, demands_values, "demands")
                    saved_count += inserted
                    demands_values = []
                    
                    # Вставляем позиции
                    if items_values:
                        await insert_batch(cur, items_values, "demand_items")
                        items_values = []
                    
                    # Обновляем статус задачи
                    if idx % 100 == 0:
                        logger.info(f"Обработано {idx}/{total_count} записей")
                        tasks_status[task_id] = {
                            "status": "processing",
                            "progress": f"{saved_count}/{total_count}",
                            "message": f"Обработано {idx} из {total_count}"
                        }
                    
                    time.sleep(0.5)
            
            except Exception as e:
                logger.error(f"Ошибка при обработке отгрузки {demand.get('id')}: {str(e)}")
                continue
        
        # Вставляем оставшиеся записи
        if demands_values:
            saved_count += await insert_batch(cur, demands_values, "demands")
        if items_values:
            await insert_batch(cur, items_values, "demand_items")
        
        conn.commit()
        logger.info(f"Успешно сохранено {saved_count} из {total_count} записей")
        
        tasks_status[task_id] = {
            "status": "completed",
            "progress": f"{saved_count}/{total_count}",
            "message": f"Успешно сохранено {saved_count} из {total_count} записей"
        }
        
    except Exception as e:
        logger.error(f"Критическая ошибка при обработке пакета: {str(e)}")
        if conn:
            conn.rollback()
        tasks_status[task_id] = {
            "status": "failed",
            "progress": f"{saved_count}/{total_count}",
            "message": f"Ошибка: {str(e)}"
        }
    finally:
        if conn:
            conn.close()

async def insert_batch(cur, batch_values: List[Dict[str, Any]], table_name: str):
    """Массовая вставка пакета данных в указанную таблицу"""
    try:
        if table_name == "demands":
            query = """
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
            """
        elif table_name == "demand_items":
            query = """
                INSERT INTO demand_items (
                    id, demand_id, demand_number, date, counterparty, store, project, sales_channel,
                    product_name, quantity, price, amount, cost_price, article, code, overhead, profit,
                    promo_period, delivery_amount, admin_data, gdeslon, cityads, ozon, ozon_fbs,
                    yamarket_fbs, yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                    programmatic, avito, multiorders, estimated_discount
                ) VALUES (
                    %(id)s, %(demand_id)s, %(demand_number)s, %(date)s, %(counterparty)s, %(store)s, %(project)s, %(sales_channel)s,
                    %(product_name)s, %(quantity)s, %(price)s, %(amount)s, %(cost_price)s, %(article)s, %(code)s, %(overhead)s, %(profit)s,
                    %(promo_period)s, %(delivery_amount)s, %(admin_data)s, %(gdeslon)s, %(cityads)s, %(ozon)s, %(ozon_fbs)s,
                    %(yamarket_fbs)s, %(yamarket_dbs)s, %(yandex_direct)s, %(price_ru)s, %(wildberries)s, %(gis2)s, %(seo)s,
                    %(programmatic)s, %(avito)s, %(multiorders)s, %(estimated_discount)s
                )
                ON CONFLICT (id) DO UPDATE SET
                    demand_id = EXCLUDED.demand_id,
                    demand_number = EXCLUDED.demand_number,
                    date = EXCLUDED.date,
                    counterparty = EXCLUDED.counterparty,
                    store = EXCLUDED.store,
                    project = EXCLUDED.project,
                    sales_channel = EXCLUDED.sales_channel,
                    product_name = EXCLUDED.product_name,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    amount = EXCLUDED.amount,
                    cost_price = EXCLUDED.cost_price,
                    article = EXCLUDED.article,
                    code = EXCLUDED.code,
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
                    estimated_discount = EXCLUDED.estimated_discount
            """
        else:
            raise ValueError(f"Unknown table name: {table_name}")
        
        execute_batch(cur, query, batch_values)
        return len(batch_values)
    
    except Exception as e:
        logger.error(f"Ошибка при массовой вставке в {table_name}: {str(e)}")
        return 0

def prepare_demand_item_data(demand: Dict[str, Any], position: Dict[str, Any]) -> Dict[str, Any]:
    """Подготовка данных позиции отгрузки для вставки в БД"""
    demand_id = str(demand.get("id", ""))
    attributes = demand.get("attributes", [])
    
    # Основные данные из позиции
    values = {
        "id": f"{demand_id}_{position.get('id', '')}"[:255],
        "demand_id": demand_id[:255],
        "demand_number": str(demand.get("name", ""))[:50],
        "date": demand.get("moment", ""),
        "counterparty": str(demand.get("agent", {}).get("name", ""))[:255],
        "store": str(demand.get("store", {}).get("name", ""))[:255],
        "project": str(demand.get("project", {}).get("name", "Без проекта"))[:255],
        "sales_channel": str(demand.get("salesChannel", {}).get("name", "Без канала"))[:255],
        "product_name": str(position.get("product_name", ""))[:255],
        "quantity": float(position.get("quantity", 0)),
        "price": float(position.get("price", 0)) / 100,
        "amount": (float(position.get("price", 0)) / 100) * float(position.get("quantity", 0)),
        "article": str(position.get("article", ""))[:255],
        "code": str(position.get("code", ""))[:255],
    }
    
    # Получаем себестоимость для позиции
    cost_price = moysklad.get_position_cost_price(position)
    values["cost_price"] = cost_price
    values["profit"] = values["amount"] - cost_price
    
    # Обработка накладных расходов
    overhead_data = demand.get("overhead", {})
    overhead_sum = float(overhead_data.get("sum", 0)) / 100
    values["overhead"] = overhead_sum * (values["amount"] / (float(demand.get("sum", 1)) / 100)) if float(demand.get("sum", 0)) > 0 else 0
    
    # Обработка атрибутов (аналогично prepare_demand_data)
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
        "estimated_discount": ("Примерная скидка", 0)
    }
    
    for field, (attr_name, default) in attr_fields.items():
        if field.endswith("_amount") or field == "estimated_discount":
            try:
                values[field] = float(get_attr_value(attributes, attr_name, default))
            except (ValueError, TypeError):
                values[field] = 0.0
        else:
            values[field] = str(get_attr_value(attributes, attr_name, default))[:255]
    
    return values

def prepare_demand_data(demand: Dict[str, Any]) -> Dict[str, Any]:
    """Подготовка данных отгрузки для вставки в БД"""
    demand_id = str(demand.get("id", ""))
    attributes = demand.get("attributes", [])
    
    # Обработка накладных расходов (overhead)
    overhead_data = demand.get("overhead", {})
    overhead_sum = float(overhead_data.get("sum", 0)) / 100
    
    # Получаем себестоимость
    cost_price = moysklad.get_demand_cost_price(demand_id)
    
    # Основные данные
    values = {
        "id": demand_id[:255],
        "number": str(demand.get("name", ""))[:50],
        "date": demand.get("moment", ""),
        "counterparty": str(demand.get("agent", {}).get("name", ""))[:255],
        "store": str(demand.get("store", {}).get("name", ""))[:255],
        "project": str(demand.get("project", {}).get("name", "Без проекта"))[:255],
        "sales_channel": str(demand.get("salesChannel", {}).get("name", "Без канала"))[:255],
        "amount": float(demand.get("sum", 0)) / 100,
        "cost_price": cost_price,
        "overhead": overhead_sum,
        "profit": (float(demand.get("sum", 0)) / 100) - cost_price - overhead_sum,
        "status": str(demand.get("state", {}).get("name", ""))[:100],
        "comment": str(demand.get("description", ""))[:255]
    }

    # Обработка атрибутов
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
        "estimated_discount": ("Примерная скидка", 0)
    }

    for field, (attr_name, default) in attr_fields.items():
        if field.endswith("_amount") or field == "estimated_discount":
            try:
                values[field] = float(get_attr_value(attributes, attr_name, default))
            except (ValueError, TypeError):
                values[field] = 0.0
        else:
            values[field] = str(get_attr_value(attributes, attr_name, default))[:255]
    
    return values

def get_attr_value(attrs, attr_name, default=""):
    """Безопасное извлечение атрибутов"""
    if not attrs:
        return default
    for attr in attrs:
        if attr.get("name") == attr_name:
            value = attr.get("value")
            if isinstance(value, dict):
                return value.get("name", str(value))
            return str(value) if value is not None else default
    return default

@app.post("/api/save-to-db")
async def save_to_db(date_range: DateRange, background_tasks: BackgroundTasks):
    """Запуск фоновой задачи для обработки данных"""
    try:
        task_id = str(uuid.uuid4())
        tasks_status[task_id] = {
            "status": "pending",
            "progress": "0/0",
            "message": "Задача поставлена в очередь"
        }
        
        # Запускаем фоновую задачу
        background_tasks.add_task(process_data_task, date_range, task_id)
        
        return {
            "task_id": task_id,
            "status": "started",
            "message": "Обработка данных начата. Используйте task_id для проверки статуса."
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def process_data_task(date_range: DateRange, task_id: str):
    """Фоновая задача для обработки данных"""
    try:
        tasks_status[task_id] = {
            "status": "fetching",
            "progress": "0/0",
            "message": "Получение данных из МойСклад..."
        }
        
        # Получаем данные из МойСклад
        demands = moysklad.get_demands(date_range.start_date, date_range.end_date)
        
        if not demands:
            tasks_status[task_id] = {
                "status": "completed",
                "progress": "0/0",
                "message": "Нет данных для сохранения"
            }
            return
        
        tasks_status[task_id] = {
            "status": "processing",
            "progress": f"0/{len(demands)}",
            "message": "Начало обработки данных..."
        }
        
        # Обрабатываем данные пакетами
        await process_demands_batch(demands, task_id)
    
    except Exception as e:
        logger.error(f"Ошибка в фоновой задаче: {str(e)}")
        tasks_status[task_id] = {
            "status": "failed",
            "progress": "0/0",
            "message": f"Ошибка: {str(e)}"
        }

@app.get("/api/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Проверка статуса задачи"""
    status = tasks_status.get(task_id, {
        "status": "not_found",
        "progress": "0/0",
        "message": "Задача не найдена"
    })
    return {"task_id": task_id, **status}

# Остальной код (export_excel и init_db) остается без изменений

@app.post("/api/export/excel")
async def export_excel(date_range: DateRange):
    conn = None
    buffer = None
    try:
        logger.info(f"Starting export for date range: {date_range.start_date} to {date_range.end_date}")
        
        # Convert input dates to proper format
        start_date = datetime.strptime(date_range.start_date, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        end_date = datetime.strptime(date_range.end_date, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        filename = f"report_{start_date}_to_{end_date}.xlsx"
        filename_encoded = quote(filename)

        conn = get_db_connection()
        cur = conn.cursor()
        
        wb = Workbook()
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]
        
        # Process demands sheet
        logger.info("Fetching demands data from database...")
        cur.execute("""
            SELECT ... FROM demands
            WHERE date BETWEEN %s AND %s
        """, (date_range.start_date, date_range.end_date))
        
        demands_rows = cur.fetchall()
        logger.info(f"Fetched {len(demands_rows)} demands records")
        ws_demands = wb.create_sheet("Отчет по отгрузкам")
        apply_excel_styles(ws_demands, demands_headers, demands_rows, ...)
        
        # Process items sheet
        logger.info("Fetching items data from database...")
        cur.execute("""
            SELECT ... FROM demand_items
            WHERE date BETWEEN %s AND %s
        """, (date_range.start_date, date_range.end_date))
        
        items_rows = cur.fetchall()
        logger.info(f"Fetched {len(items_rows)} items records")
        ws_items = wb.create_sheet("Отчет по товарам")
        apply_excel_styles(ws_items, items_headers, items_rows, ...)

        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"; filename*=UTF-8\'\'{filename_encoded}',
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )
    
    except Exception as e:
        logger.error(f"Export error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

@app.post("/api/export/excel/items")
async def export_excel_items(date_range: DateRange):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                demand_number, date, counterparty, store, project, sales_channel,
                product_name, quantity, price, amount, cost_price, article, code,
                overhead, profit, promo_period, delivery_amount,
                admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
                yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                programmatic, avito, multiorders, estimated_discount
            FROM demand_items
            WHERE date BETWEEN %s AND %s
            ORDER BY date DESC, demand_number
        """, (date_range.start_date, date_range.end_date))
        
        rows = cur.fetchall()
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет по товарам"
        
        # Заголовки столбцов
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Артикул", "Код",
            "Накладные расходы", "Прибыль", "Акционный период", "Сумма доставки",
            "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS", "Яндекс Маркет FBS",
            "Яндекс Маркет DBS", "Яндекс Директ", "Price ru", "Wildberries", "2Gis", "SEO",
            "Программатик", "Авито", "Мультиканальные заказы", "Примеренная скидка"
        ]
        
        # Стили для оформления (аналогично export_excel)
        from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
        from openpyxl.utils import get_column_letter
        
        header_font = Font(name='Calibri', bold=True, size=12, color='FFFFFF')
        cell_font = Font(name='Calibri', size=11)
        center_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        left_alignment = Alignment(horizontal='left', vertical='center')
        right_alignment = Alignment(horizontal='right', vertical='center')
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), 
                          top=Side(style='thin'), bottom=Side(style='thin'))
        header_fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
        money_fill = PatternFill(start_color='E6E6E6', end_color='E6E6E6', fill_type='solid')
        negative_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
        
        # Добавляем заголовки
        ws.append(headers)
        
        # Форматируем заголовки
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.alignment = center_alignment
            cell.fill = header_fill
            cell.border = thin_border
            
            column_letter = get_column_letter(col)
            ws.column_dimensions[column_letter].width = max(15, len(headers[col-1]) * 1.2)
        
        # Определяем числовые столбцы
        numeric_columns = [8, 9, 10, 11, 14, 15, 17] + list(range(18, 33))  # 8-11, 14-15, 17-32
        profit_column = 15  # Столбец с прибылью
        
        # Добавляем данные и форматируем их
        for row_idx, row in enumerate(rows, start=2):
            for col_idx, value in enumerate(row, start=1):
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                cell.font = cell_font
                cell.border = thin_border
                
                if col_idx in numeric_columns:  # Все числовые столбцы
                    try:
                        num_value = float(value) if value not in [None, ''] else 0.0
                        cell.value = num_value
                        cell.number_format = '#,##0.00'
                        cell.alignment = right_alignment
                        
                        if col_idx == profit_column and num_value < 0:
                            cell.fill = negative_fill
                        elif row_idx % 2 == 0:
                            cell.fill = money_fill
                    except (ValueError, TypeError):
                        cell.alignment = left_alignment
                elif col_idx == 2:  # Столбец с датой
                    cell.number_format = 'DD.MM.YYYY'
                    cell.alignment = center_alignment
                else:
                    cell.alignment = left_alignment
        
        # Замораживаем заголовки
        ws.freeze_panes = 'A2'
        
        # Добавляем автофильтр
        ws.auto_filter.ref = ws.dimensions
        
        # Добавляем итоговую строку
        last_row = len(rows) + 1
        ws.append([""] * len(headers))
        total_row = last_row + 1
        
        # Форматируем итоговую строку
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=total_row, column=col)
            cell.font = Font(bold=True)
            cell.border = thin_border
            
            if col in numeric_columns:
                start_col = get_column_letter(col)
                formula = f"SUM({start_col}2:{start_col}{last_row})"
                cell.value = f"=ROUND({formula}, 2)"
                cell.number_format = '#,##0.00'
                cell.alignment = right_alignment
                cell.fill = PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid')
        
        # Создаем буфер для сохранения файла
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        # Возвращаем файл как ответ
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=report_items.xlsx"}
        )
    
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

def apply_excel_styles(worksheet, headers, rows, numeric_columns, profit_column):
    """Применяет стили к листу Excel"""
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
    from openpyxl.utils import get_column_letter
    
    # Стили
    header_font = Font(name='Calibri', bold=True, size=12, color='FFFFFF')
    cell_font = Font(name='Calibri', size=11)
    center_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    left_alignment = Alignment(horizontal='left', vertical='center')
    right_alignment = Alignment(horizontal='right', vertical='center')
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), 
                       top=Side(style='thin'), bottom=Side(style='thin'))
    header_fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
    money_fill = PatternFill(start_color='E6E6E6', end_color='E6E6E6', fill_type='solid')
    negative_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
    
    # Добавляем заголовки
    worksheet.append(headers)
    
    # Форматируем заголовки
    for col in range(1, len(headers) + 1):
        cell = worksheet.cell(row=1, column=col)
        cell.font = header_font
        cell.alignment = center_alignment
        cell.fill = header_fill
        cell.border = thin_border
        
        # Автоподбор ширины столбца
        column_letter = get_column_letter(col)
        worksheet.column_dimensions[column_letter].width = max(15, len(headers[col-1]) * 1.2)
    
    # Добавляем данные и форматируем их
    for row_idx, row in enumerate(rows, start=2):
        for col_idx, value in enumerate(row, start=1):
            cell = worksheet.cell(row=row_idx, column=col_idx, value=value)
            cell.font = cell_font
            cell.border = thin_border
            
            if col_idx in numeric_columns:  # Все числовые столбцы
                try:
                    num_value = float(value) if value not in [None, ''] else 0.0
                    cell.value = num_value
                    cell.number_format = '#,##0.00'
                    cell.alignment = right_alignment
                    
                    if col_idx == profit_column and num_value < 0:
                        cell.fill = negative_fill
                    elif row_idx % 2 == 0:
                        cell.fill = money_fill
                except (ValueError, TypeError):
                    cell.alignment = left_alignment
            elif col_idx == 2:  # Столбец с датой
                cell.number_format = 'DD.MM.YYYY'
                cell.alignment = center_alignment
            else:
                cell.alignment = left_alignment
    
    # Замораживаем заголовки
    worksheet.freeze_panes = 'A2'
    
    # Добавляем автофильтр
    worksheet.auto_filter.ref = worksheet.dimensions
    
    # Добавляем итоговую строку
    last_row = len(rows) + 1
    worksheet.append([""] * len(headers))
    total_row = last_row + 1
    
    # Форматируем итоговую строку
    for col in range(1, len(headers) + 1):
        cell = worksheet.cell(row=total_row, column=col)
        cell.font = Font(bold=True)
        cell.border = thin_border
        
        if col in numeric_columns:
            start_col = get_column_letter(col)
            formula = f"SUM({start_col}2:{start_col}{last_row})"
            cell.value = f"=ROUND({formula}, 2)"
            cell.number_format = '#,##0.00'
            cell.alignment = right_alignment
            cell.fill = PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid')
        elif col == 1:
            cell.value = "Итого:"
            cell.alignment = right_alignment