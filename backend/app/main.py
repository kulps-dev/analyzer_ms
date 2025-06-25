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
        
        # Проверяем существование таблицы demands
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'demands'
            )
        """)
        demands_exists = cur.fetchone()[0]
        
        if not demands_exists:
            cur.execute("""
                CREATE TABLE demands (
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
            logger.info("Таблица demands успешно создана")
        
        # Проверяем существование таблицы demand_positions
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'demand_positions'
            )
        """)
        positions_exists = cur.fetchone()[0]
        
        if not positions_exists:
            cur.execute("""
                CREATE TABLE demand_positions (
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
                    article VARCHAR(100),
                    code VARCHAR(100),
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
            logger.info("Таблица demand_positions успешно создана")
        
        conn.commit()
        
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
    """
    Полная обработка пакета отгрузок с:
    - Подробным логированием
    - Контролем себестоимости
    - Пакетной вставкой в БД
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        batch_size = 50
        saved_count = 0
        total_count = len(demands)
        
        logger.info(f"Начало обработки {total_count} отгрузок")
        logger.debug(f"Первые 3 отгрузки: {demands[:3]}")

        demands_batch = []
        positions_batch = []
        
        for idx, demand in enumerate(demands, 1):
            demand_id = demand.get("id", "unknown")
            try:
                logger.debug(f"Обработка отгрузки {idx}/{total_count} (ID: {demand_id})")
                
                # Проверка наличия позиций
                if not demand.get("positions"):
                    logger.warning(f"Отгрузка {demand_id} не содержит позиций!")
                
                # Подготовка данных отгрузки
                demand_values = prepare_demand_data(demand)
                demands_batch.append(demand_values)
                
                # Обработка позиций с контролем себестоимости
                for position in demand.get("positions", []):
                    position_id = position.get("id", "unknown")
                    
                    # Валидация данных позиции
                    if not all(k in position for k in ["id", "quantity", "price"]):
                        logger.error(f"Позиция {position_id} содержит неполные данные! Пропускаем")
                        continue
                    
                    # Логирование себестоимости перед обработкой
                    cost_before = position.get("cost_price", "не указана")
                    logger.debug(f"Позиция {position_id} - себестоимость до обработки: {cost_before}")
                    
                    # Подготовка данных позиции
                    position_data = prepare_position_data(demand, position)
                    positions_batch.append(position_data)
                    
                    # Проверка результата
                    if position_data["cost_price"] == 0:
                        logger.warning(f"Позиция {position_id} имеет нулевую себестоимость!")
                
                # Пакетная вставка
                if len(demands_batch) >= batch_size:
                    # Вставка отгрузок
                    inserted_demands = await insert_demands_batch(cur, demands_batch)
                    saved_count += inserted_demands
                    
                    # Вставка позиций
                    inserted_positions = await insert_positions_batch(cur, positions_batch)
                    
                    logger.info(
                        f"Вставлено пакетом: {inserted_demands} отгрузок, "
                        f"{inserted_positions} позиций (всего обработано: {saved_count})"
                    )
                    
                    demands_batch = []
                    positions_batch = []
                    
                    # Обновление статуса задачи
                    if idx % 100 == 0:
                        progress = f"{saved_count}/{total_count}"
                        tasks_status[task_id] = {
                            "status": "processing",
                            "progress": progress,
                            "message": f"Обработано {idx} отгрузок"
                        }
                    
                    time.sleep(0.5)
            
            except Exception as e:
                logger.error(f"Ошибка обработки отгрузки {demand_id}: {str(e)}")
                logger.debug("Трассировка ошибки:", exc_info=True)
                continue
        
        # Вставка остатков
        if demands_batch:
            inserted = await insert_demands_batch(cur, demands_batch)
            saved_count += inserted
            logger.info(f"Вставлено остаточных {inserted} отгрузок")
        
        if positions_batch:
            inserted = await insert_positions_batch(cur, positions_batch)
            logger.info(f"Вставлено остаточных {inserted} позиций")
        
        conn.commit()
        logger.info(f"Успешно сохранено {saved_count} из {total_count} отгрузок")
        
        tasks_status[task_id] = {
            "status": "completed",
            "progress": f"{saved_count}/{total_count}",
            "message": f"Обработано {saved_count} отгрузок"
        }
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
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
        logger.debug("Соединение с БД закрыто")

async def insert_demands_batch(cur, batch_values: List[Dict[str, Any]]) -> int:
    """Массовая вставка пакета данных отгрузок"""
    try:
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
        
        execute_batch(cur, query, batch_values)
        return len(batch_values)
    
    except Exception as e:
        logger.error(f"Ошибка при массовой вставке отгрузок: {str(e)}")
        return 0

async def insert_positions_batch(cur, batch_values: List[Dict[str, Any]]) -> int:
    """Массовая вставка пакета данных позиций"""
    try:
        query = """
            INSERT INTO demand_positions (
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
        
        execute_batch(cur, query, batch_values)
        return len(batch_values)
    
    except Exception as e:
        logger.error(f"Ошибка при массовой вставке позиций: {str(e)}")
        return 0

def prepare_demand_data(demand: Dict[str, Any]) -> Dict[str, Any]:
    """Подготовка данных отгрузки для вставки в БД"""
    demand_id = str(demand.get("id", ""))
    attributes = demand.get("attributes", [])
    
    # Обработка накладных расходов (overhead)
    overhead_data = demand.get("overhead", {})
    overhead_sum = float(overhead_data.get("sum", 0)) / 100
    
    # Получаем себестоимость
    cost_price = moysklad.get_demand_cost_price(demand_id)
    demand_sum = float(demand.get("sum", 0)) / 100
    profit = demand_sum - cost_price - overhead_sum
    
    # Основные данные
    values = {
        "id": demand_id[:255],
        "number": str(demand.get("name", ""))[:50],
        "date": demand.get("moment", ""),
        "counterparty": str(demand.get("agent", {}).get("name", ""))[:255],
        "store": str(demand.get("store", {}).get("name", ""))[:255],
        "project": str(demand.get("project", {}).get("name", "Без проекта"))[:255],
        "sales_channel": str(demand.get("salesChannel", {}).get("name", "Без канала"))[:255],
        "amount": demand_sum,
        "cost_price": cost_price,
        "overhead": overhead_sum,
        "profit": profit,
        "status": str(demand.get("state", {}).get("name", ""))[:100],
        "comment": str(demand.get("description", ""))[:255],
        "promo_period": "",
        "delivery_amount": 0,
        "admin_data": 0,
        "gdeslon": 0,
        "cityads": 0,
        "ozon": 0,
        "ozon_fbs": 0,
        "yamarket_fbs": 0,
        "yamarket_dbs": 0,
        "yandex_direct": 0,
        "price_ru": 0,
        "wildberries": 0,
        "gis2": 0,
        "seo": 0,
        "programmatic": 0,
        "avito": 0,
        "multiorders": 0,
        "estimated_discount": 0
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

def prepare_position_data(demand: Dict[str, Any], position: Dict[str, Any]) -> Dict[str, Any]:
    """
    Полная подготовка данных позиции для вставки в БД
    с гарантированным учетом себестоимости и подробным логированием
    """
    # Логирование входящих данных
    logger.debug(f"Начало обработки позиции {position.get('id')} для отгрузки {demand.get('id')}")
    logger.debug(f"Данные позиции: {position}")
    
    # Проверка и получение обязательных полей
    position_id = str(position.get("id", ""))[:255]
    demand_id = str(demand.get("id", ""))[:255]
    
    # Проверка наличия себестоимости
    if "cost_price" not in position:
        logger.warning(f"Позиция {position_id} не содержит себестоимости! Будет использовано 0")
        position["cost_price"] = 0.0
    
    # Основные числовые показатели
    quantity = float(position.get("quantity", 0))
    price = float(position.get("price", 0)) / 100  # Переводим из копеек в рубли
    amount = quantity * price
    cost_price = float(position.get("cost_price", 0))
    
    # Расчет накладных расходов
    overhead_data = demand.get("overhead", {})
    overhead_sum = (float(overhead_data.get("sum", 0)) / 100) if overhead_data else 0
    demand_sum = float(demand.get("sum", 0)) / 100
    overhead_share = overhead_sum * (amount / demand_sum) if demand_sum > 0 else 0
    
    # Формируем основной словарь с данными
    values = {
        "id": position_id,
        "demand_id": demand_id,
        "demand_number": str(demand.get("name", ""))[:50],
        "date": demand.get("moment", ""),
        "counterparty": str(demand.get("agent", {}).get("name", ""))[:255],
        "store": str(demand.get("store", {}).get("name", ""))[:255],
        "project": str(demand.get("project", {}).get("name", "Без проекта"))[:255],
        "sales_channel": str(demand.get("salesChannel", {}).get("name", "Без канала"))[:255],
        "product_name": str(position.get("product_name", ""))[:255],
        "quantity": quantity,
        "price": price,
        "amount": amount,
        "cost_price": cost_price,  # Гарантированно заполнено
        "article": str(position.get("article", ""))[:100],
        "code": str(position.get("code", ""))[:100],
        "overhead": overhead_share,
        "profit": amount - cost_price - overhead_share,
        "promo_period": "",
        "delivery_amount": 0,
        "admin_data": 0,
        "gdeslon": 0,
        "cityads": 0,
        "ozon": 0,
        "ozon_fbs": 0,
        "yamarket_fbs": 0,
        "yamarket_dbs": 0,
        "yandex_direct": 0,
        "price_ru": 0,
        "wildberries": 0,
        "gis2": 0,
        "seo": 0,
        "programmatic": 0,
        "avito": 0,
        "multiorders": 0,
        "estimated_discount": 0
    }

    # Обработка атрибутов
    attributes = demand.get("attributes", [])
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
    
    logger.debug(f"Итоговые данные позиции: {values}")
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

@app.post("/api/export/excel")
async def export_excel(date_range: DateRange):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Создаем Excel файл
        wb = Workbook()
        
        # Лист с отгрузками
        await create_demands_sheet(wb, cur, date_range)
        
        # Лист с товарами
        await create_positions_sheet(wb, cur, date_range)
        
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return {
            "file": buffer.read().hex(),
            "filename": f"Отчет_по_отгрузкам_{date_range.start_date}_по_{date_range.end_date}.xlsx"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

async def create_demands_sheet(wb, cur, date_range):
    """Создает лист с отгрузками"""
    cur.execute("""
        SELECT 
            number, date, counterparty, store, project, sales_channel,
            amount, cost_price, overhead, profit, promo_period, delivery_amount,
            admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
            yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
            programmatic, avito, multiorders, estimated_discount
        FROM demands
        WHERE date BETWEEN %s AND %s
        ORDER BY date DESC
    """, (date_range.start_date, date_range.end_date))
    
    rows = cur.fetchall()
    
    ws = wb.active
    ws.title = "Отчет по отгрузкам"
    
    # Заголовки столбцов
    headers = [
        "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
        "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
        "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
        "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
        "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
        "Примерная скидка"
    ]
    
    apply_sheet_styling(ws, headers, rows, numeric_columns=[7, 8, 9, 10, 12] + list(range(13, 29)), 
                        profit_column=10, sheet_type="demands")

async def create_positions_sheet(wb, cur, date_range):
    """
    Создание листа с товарами в Excel с:
    - Группировкой по отгрузкам
    - Отображением себестоимости
    - Профессиональным форматированием
    """
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
    from openpyxl.utils import get_column_letter
    
    # Выполняем запрос с явным указанием cost_price
    query = """
        SELECT 
            d.number as demand_number, 
            d.date, 
            d.counterparty, 
            d.store, 
            d.project, 
            d.sales_channel,
            dp.product_name, 
            dp.quantity, 
            dp.price, 
            dp.amount, 
            dp.cost_price,  -- Важно: выбираем себестоимость
            dp.article, 
            dp.code,
            dp.overhead, 
            dp.profit,
            d.promo_period,
            d.delivery_amount,
            d.admin_data,
            d.gdeslon,
            d.cityads,
            d.ozon,
            d.ozon_fbs,
            d.yamarket_fbs,
            d.yamarket_dbs,
            d.yandex_direct,
            d.price_ru,
            d.wildberries,
            d.gis2,
            d.seo,
            d.programmatic,
            d.avito,
            d.multiorders,
            d.estimated_discount,
            d.cost_price as total_cost_price
        FROM demand_positions dp
        JOIN demands d ON dp.demand_id = d.id
        WHERE d.date BETWEEN %s AND %s
        ORDER BY d.number, d.date DESC
    """
    cur.execute(query, (date_range.start_date, date_range.end_date))
    rows = cur.fetchall()
    
    logger.info(f"Получено {len(rows)} позиций для выгрузки в Excel")
    
    # Создаем лист
    ws = wb.create_sheet("Отчет по товарам")
    
    # Заголовки
    headers = [
        "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
        "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Артикул", "Код",
        "Накладные расходы", "Прибыль", "Акционный период", "Сумма доставки", "Адмидат",
        "ГдеСлон", "CityAds", "Ozon", "Ozon FBS", "Яндекс Маркет FBS", "Яндекс Маркет DBS",
        "Яндекс Директ", "Price ru", "Wildberries", "2Gis", "SEO", "Программатик", "Авито",
        "Мультиканальные заказы", "Примерная скидка"
    ]
    ws.append(headers)
    
    # Стили для заголовков
    header_style = {
        'font': Font(bold=True, color="FFFFFF"),
        'fill': PatternFill("solid", fgColor="4F81BD"),
        'alignment': Alignment(horizontal="center", wrap_text=True),
        'border': Border(left=Side("thin"), right=Side("thin"), 
                      top=Side("thin"), bottom=Side("thin"))
    }
    
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        for attr, value in header_style.items():
            setattr(cell, attr, value)
        ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col-1]) * 1.2)
    
    # Заполняем данные
    current_demand = None
    row_num = 2
    
    for row in rows:
        demand_num = row[0]
        
        # Добавляем строку с номером отгрузки
        if demand_num != current_demand:
            current_demand = demand_num
            demand_row = [
                demand_num, row[1], row[2], row[3], row[4], row[5],
                "Итого по отгрузке:", "", "", row[9], row[32],  # Используем total_cost_price
                "", "", row[13], row[14], row[15], row[16], row[17],
                row[18], row[19], row[20], row[21], row[22], row[23],
                row[24], row[25], row[26], row[27], row[28], row[29],
                row[30], row[31]
            ]
            ws.append(demand_row)
            
            # Форматируем строку итогов
            for col in range(1, len(demand_row) + 1):
                cell = ws.cell(row=row_num, column=col)
                cell.font = Font(bold=True)
                if col >= 7:  # Числовые поля
                    try:
                        cell.number_format = '#,##0.00'
                        cell.alignment = Alignment(horizontal="right")
                    except:
                        pass
                if col == 7:  # "Итого по отгрузке:"
                    cell.alignment = Alignment(horizontal="right")
            
            row_num += 1
        
        # Добавляем строку с товаром
        product_row = [
            "", "", "", "", "", "",  # Пустые поля заголовка
            row[6],  # product_name
            row[7],  # quantity
            row[8],  # price
            row[9],  # amount
            row[10],  # cost_price (себестоимость позиции)
            row[11],  # article
            row[12],  # code
            "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""
        ]
        ws.append(product_row)
        
        # Форматируем числовые поля
        for col in [8, 9, 10, 11]:  # Количество, цена, сумма, себестоимость
            cell = ws.cell(row=row_num, column=col)
            cell.number_format = '#,##0.00'
            cell.alignment = Alignment(horizontal="right")
        
        row_num += 1
    
    # Добавляем итоговую строку
    ws.append([""] * len(headers))
    total_row = row_num + 1
    
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=total_row, column=col)
        if col == 1:
            cell.value = "Итого:"
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="right")
        elif col in [8, 9, 10, 11, 14, 15]:  # Числовые колонки
            letter = get_column_letter(col)
            cell.value = f"=SUM({letter}2:{letter}{row_num})"
            cell.number_format = '#,##0.00'
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="right")
    
    # Настройки листа
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = 'A2'
    
    logger.info("Лист 'Отчет по товарам' успешно сформирован")

def apply_sheet_styling(ws, headers, rows, numeric_columns, profit_column, sheet_type):
    """Применяет стили к листу Excel"""
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
    from openpyxl.utils import get_column_letter
    
    # Шрифты
    header_font = Font(name='Calibri', bold=True, size=12, color='FFFFFF')
    cell_font = Font(name='Calibri', size=11)
    
    # Выравнивание
    center_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    left_alignment = Alignment(horizontal='left', vertical='center')
    right_alignment = Alignment(horizontal='right', vertical='center')
    
    # Границы
    thin_border = Border(left=Side(style='thin'), 
                      right=Side(style='thin'), 
                      top=Side(style='thin'), 
                      bottom=Side(style='thin'))
    
    # Заливка
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
        
        # Автоподбор ширины столбца
        column_letter = get_column_letter(col)
        ws.column_dimensions[column_letter].width = max(15, len(headers[col-1]) * 1.2)
    
    # Добавляем данные и форматируем их
    for row_idx, row in enumerate(rows, start=2):
        for col_idx, value in enumerate(row, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.font = cell_font
            cell.border = thin_border
            
            # Форматирование чисел и дат
            if col_idx in numeric_columns:  # Все числовые столбцы
                try:
                    num_value = float(value) if value not in [None, ''] else 0.0
                    cell.value = num_value
                    
                    # Форматирование в зависимости от типа данных
                    if sheet_type == "positions" and col_idx in [8, 9]:  # Количество и цена
                        cell.number_format = '0.00'
                    else:
                        cell.number_format = '#,##0.00'
                    
                    cell.alignment = right_alignment
                    
                    # Проверяем отрицательную прибыль
                    if col_idx == profit_column and num_value < 0:
                        cell.fill = negative_fill
                    elif row_idx % 2 == 0:  # Зебра для читаемости
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
        
        # Суммы для числовых столбцов
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