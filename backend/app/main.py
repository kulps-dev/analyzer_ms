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
from google.oauth2.service_account import Credentials
import gspread
from fastapi import HTTPException
from pathlib import Path
from fastapi.responses import JSONResponse
from datetime import datetime
from decimal import Decimal
import json
from fastapi import Response
from fastapi.responses import StreamingResponse
from fastapi import Request
from typing import Optional, Dict, Any, List
import asyncpg
from asyncpg.transaction import Transaction
from datetime import datetime
from fastapi.responses import StreamingResponse
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter
import logging

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешает все домены (для разработки)
    allow_methods=["*"],  # Разрешает все HTTP-методы
    allow_headers=["*"],  # Разрешает все заголовки
)

# Настройки базы данных
DB_CONFIG = {
    "host": "87.228.99.200",
    "port": 5432,
    "database": "MS",  # Используем 'database' вместо 'dbname'
    "user": "louella",
    "password": "XBcMJoEO1ljb",
    "ssl": "require",  # Изменяем на 'require' вместо 'verify-ca'
    # Убираем sslrootcert, так как asyncpg использует другой подход
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

class WebhookData(BaseModel):
    accountId: str
    action: str  # "CREATE", "UPDATE", "DELETE"
    entityType: str  # "demand"
    id: str  # ID отгрузки
    meta: dict  # Метаданные вебхука
    # Другие поля, которые могут прийти
    class Config:
        extra = "allow"  # Разрешает дополнительные поля

class WebhookEvent(BaseModel):
    meta: Dict[str, Any]
    action: str
    accountId: str

class WebhookData(BaseModel):
    events: List[WebhookEvent]
    auditContext: Optional[Dict[str, Any]] = None
    
    class Config:
        extra = "allow"  # Разрешает дополнительные поля

# Глобальный словарь для хранения статусов задач
tasks_status = {}

async def get_db_connection():
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {str(e)}")
        raise

async def init_db():
    """Асинхронная инициализация базы данных"""
    conn = None
    try:
        conn = await get_db_connection()
        
        # Проверка существования таблицы demands
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'demands'
            )
        """)
        
        if not exists:
            await conn.execute("""
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
            logger.info("Таблица demands создана")
        
        # Аналогично для demand_positions
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'demand_positions'
            )
        """)
        
        if not exists:
            await conn.execute("""
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
            logger.info("Таблица demand_positions создана")
            
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {str(e)}")
        if conn:
            await conn.close()
        raise
    finally:
        if conn:
            await conn.close()

@app.on_event("startup")
async def startup_event():
    """Асинхронные действия при старте приложения"""
    await init_db()
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
        
        demands_batch = []
        positions_batch = []
        
        for idx, demand in enumerate(demands, 1):
            try:
                # Подготовка данных отгрузки
                demand_values = prepare_demand_data(demand)
                demands_batch.append(demand_values)
                
                # Подготовка данных позиций
                positions = demand.get("positions", [])
                for position in positions:
                    position_values = prepare_position_data(demand, position)
                    positions_batch.append(position_values)
                
                if len(demands_batch) >= batch_size:
                    # Вставляем отгрузки
                    inserted_demands = await insert_demands_batch(cur, demands_batch)
                    saved_count += inserted_demands
                    
                    # Вставляем позиции
                    await insert_positions_batch(cur, positions_batch)
                    
                    demands_batch = []
                    positions_batch = []
                    
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
        if demands_batch:
            saved_count += await insert_demands_batch(cur, demands_batch)
        if positions_batch:
            await insert_positions_batch(cur, positions_batch)
        
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

async def insert_demands_batch(conn, batch_values):
    """Асинхронная массовая вставка отгрузок"""
    try:
        query = """
            INSERT INTO demands (
                id, number, date, counterparty, store, project, sales_channel, 
                amount, cost_price, overhead, profit, promo_period, delivery_amount,
                admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
                yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                programmatic, avito, multiorders, estimated_discount, status, comment
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
                $26, $27, $28, $29, $30, $31
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
        
        # Преобразуем данные
        values = []
        for item in batch_values:
            try:
                row = (
                    item['id'],
                    item.get('number', ''),
                    item.get('date'),
                    item.get('counterparty', ''),
                    item.get('store', ''),
                    item.get('project', 'Без проекта'),
                    item.get('sales_channel', 'Без канала'),
                    float(item.get('amount', 0)),
                    float(item.get('cost_price', 0)),
                    float(item.get('overhead', 0)),
                    float(item.get('profit', 0)),
                    item.get('promo_period', ''),
                    float(item.get('delivery_amount', 0)),
                    float(item.get('admin_data', 0)),
                    float(item.get('gdeslon', 0)),
                    float(item.get('cityads', 0)),
                    float(item.get('ozon', 0)),
                    float(item.get('ozon_fbs', 0)),
                    float(item.get('yamarket_fbs', 0)),
                    float(item.get('yamarket_dbs', 0)),
                    float(item.get('yandex_direct', 0)),
                    float(item.get('price_ru', 0)),
                    float(item.get('wildberries', 0)),
                    float(item.get('gis2', 0)),
                    float(item.get('seo', 0)),
                    float(item.get('programmatic', 0)),
                    float(item.get('avito', 0)),
                    float(item.get('multiorders', 0)),
                    float(item.get('estimated_discount', 0)),
                    item.get('status', ''),
                    item.get('comment', '')
                )
                values.append(row)
            except Exception as e:
                logger.error(f"Ошибка подготовки строки: {str(e)}")
                continue
        
        if values:
            await conn.executemany(query, values)
            return len(values)
        return 0
    
    except Exception as e:
        logger.error(f"Ошибка при вставке отгрузок: {str(e)}")
        return 0

async def create_demands_sheet_safe(wb: Workbook, conn, date_range: DateRange):
    """Безопасная версия создания листа с отгрузками с обработкой ошибок"""
    try:
        await create_demands_sheet(wb, conn, date_range)
    except Exception as e:
        logger.error(f"Ошибка создания листа отгрузок: {str(e)}")
        ws = wb.create_sheet("Отгрузки")
        ws.append(["Ошибка при создании листа", str(e)])

async def create_positions_sheet_safe(wb: Workbook, conn, date_range: DateRange):
    """Безопасная версия создания листа с позициями с обработкой ошибок"""
    try:
        await create_positions_sheet(wb, conn, date_range)
    except Exception as e:
        logger.error(f"Ошибка создания листа товаров: {str(e)}")
        ws = wb.create_sheet("Отчет по товарам")
        ws.append(["Ошибка при создании листа", str(e)])

async def create_products_summary_sheet_safe(wb: Workbook, conn, date_range: DateRange):
    """Безопасная версия создания сводного листа с обработкой ошибок"""
    try:
        await create_products_summary_sheet(wb, conn, date_range)
    except Exception as e:
        logger.error(f"Ошибка создания сводного листа: {str(e)}")
        ws = wb.create_sheet("Сводный отчет по товарам")
        ws.append(["Ошибка при создании листа", str(e)])

async def insert_positions_batch(conn, batch_values: List[Dict[str, Any]]) -> int:
    """Исправленная массовая вставка позиций с правильным количеством параметров"""
    if not batch_values:
        return 0
        
    try:
        query = """
            INSERT INTO demand_positions (
                id, demand_id, demand_number, date, counterparty, store, project, sales_channel,
                product_name, quantity, price, amount, cost_price, article, code, overhead, profit,
                promo_period, delivery_amount, admin_data, gdeslon, cityads, ozon, ozon_fbs,
                yamarket_fbs, yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                programmatic, avito, multiorders, estimated_discount
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35
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
        
        # Подготавливаем данные для вставки
        rows_to_insert = []
        
        for pos in batch_values:
            try:
                # Обработка даты
                date_value = pos.get('date')
                if isinstance(date_value, str):
                    try:
                        date_value = datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S.%f")
                    except ValueError:
                        try:
                            date_value = datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S")
                        except (ValueError, TypeError):
                            date_value = None
                elif not isinstance(date_value, datetime):
                    date_value = None
                
                # Создаем кортеж с 35 значениями
                row_data = (
                    str(pos.get('id', ''))[:255],                           # 1
                    str(pos.get('demand_id', ''))[:255],                    # 2
                    str(pos.get('demand_number', ''))[:50],                 # 3
                    date_value,                                             # 4
                    str(pos.get('counterparty', ''))[:255],                 # 5
                    str(pos.get('store', ''))[:255],                        # 6
                    str(pos.get('project', 'Без проекта'))[:255],          # 7
                    str(pos.get('sales_channel', 'Без канала'))[:255],     # 8
                    str(pos.get('product_name', ''))[:255],                 # 9
                    float(pos.get('quantity', 0)),                         # 10
                    float(pos.get('price', 0)),                            # 11
                    float(pos.get('amount', 0)),                           # 12
                    float(pos.get('cost_price', 0)),                       # 13
                    str(pos.get('article', ''))[:100],                     # 14
                    str(pos.get('code', ''))[:100],                        # 15
                    float(pos.get('overhead', 0)),                         # 16
                    float(pos.get('profit', 0)),                           # 17
                    str(pos.get('promo_period', ''))[:255],                # 18
                    float(pos.get('delivery_amount', 0)),                  # 19
                    float(pos.get('admin_data', 0)),                       # 20
                    float(pos.get('gdeslon', 0)),                          # 21
                    float(pos.get('cityads', 0)),                          # 22
                    float(pos.get('ozon', 0)),                             # 23
                    float(pos.get('ozon_fbs', 0)),                         # 24
                    float(pos.get('yamarket_fbs', 0)),                     # 25
                    float(pos.get('yamarket_dbs', 0)),                     # 26
                    float(pos.get('yandex_direct', 0)),                    # 27
                    float(pos.get('price_ru', 0)),                         # 28
                    float(pos.get('wildberries', 0)),                      # 29
                    float(pos.get('gis2', 0)),                             # 30
                    float(pos.get('seo', 0)),                              # 31
                    float(pos.get('programmatic', 0)),                     # 32
                    float(pos.get('avito', 0)),                            # 33
                    float(pos.get('multiorders', 0)),                      # 34
                    float(pos.get('estimated_discount', 0))                # 35
                )
                
                rows_to_insert.append(row_data)
                
            except Exception as e:
                logger.error(f"Ошибка подготовки позиции {pos.get('id', 'unknown')}: {str(e)}")
                continue
        
        if rows_to_insert:
            # Вставляем данные
            await conn.executemany(query, rows_to_insert)
            logger.info(f"Успешно вставлено {len(rows_to_insert)} позиций")
            return len(rows_to_insert)
        else:
            logger.warning("Нет данных для вставки")
            return 0
    
    except Exception as e:
        logger.error(f"Ошибка при вставке позиций: {str(e)}")
        return 0

async def process_single_demand(demand: Dict) -> bool:
    """Обрабатывает одну отгрузку и сохраняет в БД"""
    conn = None
    try:
        demand_id = demand.get('id')
        if not demand_id:
            logger.error("Отсутствует ID отгрузки")
            return False
            
        logger.info(f"Обработка отгрузки {demand_id}")
        
        # Подготавливаем данные
        demand_data = prepare_demand_data(demand)
        positions_data = prepare_positions_data(demand)
        
        if not demand_data:
            logger.error("Не удалось подготовить данные отгрузки")
            return False
            
        # Сохраняем в БД
        conn = await get_db_connection()
        
        async with conn.transaction():
            # 1. Удаляем старые данные
            await conn.execute("DELETE FROM demand_positions WHERE demand_id = $1", demand_id)
            await conn.execute("DELETE FROM demands WHERE id = $1", demand_id)
            
            # 2. Вставляем новые данные отгрузки
            inserted_demands = await insert_demands_batch(conn, [demand_data])
            logger.info(f"Вставлено отгрузок: {inserted_demands}")
            
            # 3. Вставляем позиции, если они есть
            if positions_data:
                # Фильтруем None значения
                valid_positions = [pos for pos in positions_data if pos is not None]
                if valid_positions:
                    inserted_positions = await insert_positions_batch(conn, valid_positions)
                    logger.info(f"Вставлено позиций: {inserted_positions}")
                else:
                    logger.warning("Нет валидных позиций для вставки")
            else:
                logger.info("Нет позиций для вставки")
                
            logger.info(f"Данные отгрузки {demand_id} успешно обновлены")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка сохранения отгрузки {demand_id}: {str(e)}")
        return False
    finally:
        if conn:
            await conn.close()

def prepare_demand_data(demand: Dict[str, Any]) -> Dict[str, Any]:
    """Подготовка данных отгрузки для вставки в БД"""
    demand_id = str(demand.get("id", ""))
    attributes = demand.get("attributes", [])
    
    # Обработка даты
    moment = demand.get("moment")
    try:
        date = datetime.strptime(moment, "%Y-%m-%d %H:%M:%S.%f") if moment else None
    except ValueError:
        try:
            date = datetime.strptime(moment, "%Y-%m-%d %H:%M:%S") if moment else None
        except (ValueError, TypeError):
            date = None
            logger.warning(f"Не удалось распарсить дату: {moment}")

    overhead_data = demand.get("overhead", {})
    overhead_sum = float(overhead_data.get("sum", 0)) / 100
    
    cost_price = moysklad.get_demand_cost_price(demand_id)
    demand_sum = float(demand.get("sum", 0)) / 100
    profit = demand_sum - cost_price - overhead_sum
    
    values = {
        "id": demand_id[:255],
        "number": str(demand.get("name", ""))[:50],
        "date": date,
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
    """Подготовка данных позиции для вставки в БД с проверкой типов"""
    try:
        position_id = str(position.get("id", ""))
        demand_id = str(demand.get("id", ""))
        
        # Получаем себестоимость позиции
        cost_price = float(position.get("cost_price", 0.0))
        
        # Количество и цена
        quantity = float(position.get("quantity", 0))
        price = float(position.get("price", 0)) / 100
        amount = quantity * price
        
        # Накладные расходы
        overhead_data = demand.get("overhead", {})
        overhead_sum = (float(overhead_data.get("sum", 0)) / 100) if overhead_data else 0
        
        # Расчет доли накладных расходов для позиции
        demand_sum = float(demand.get("sum", 0)) / 100
        overhead_share = overhead_sum * (amount / demand_sum) if demand_sum > 0 else 0
        
        # Получаем атрибуты
        attributes = demand.get("attributes", [])
        
        # Обработка даты
        moment = demand.get("moment")
        try:
            date = datetime.strptime(moment, "%Y-%m-%d %H:%M:%S.%f") if moment else None
        except ValueError:
            try:
                date = datetime.strptime(moment, "%Y-%m-%d %H:%M:%S") if moment else None
            except (ValueError, TypeError):
                date = None
        
        return {
            "id": position_id[:255],
            "demand_id": demand_id[:255],
            "demand_number": str(demand.get("name", ""))[:50],
            "date": date,
            "counterparty": str(demand.get("agent", {}).get("name", ""))[:255],
            "store": str(demand.get("store", {}).get("name", ""))[:255],
            "project": str(demand.get("project", {}).get("name", "Без проекта"))[:255],
            "sales_channel": str(demand.get("salesChannel", {}).get("name", "Без канала"))[:255],
            "product_name": str(position.get("product_name", ""))[:255],
            "quantity": quantity,
            "price": price,
            "amount": amount,
            "cost_price": cost_price,
            "article": str(position.get("article", ""))[:100],
            "code": str(position.get("code", ""))[:100],
            "overhead": overhead_share,
            "profit": amount - cost_price - overhead_share,
            "promo_period": str(get_attr_value(attributes, "Акционный период", ""))[:255],
            "delivery_amount": float(get_attr_value(attributes, "Сумма доставки", 0)),
            "admin_data": float(get_attr_value(attributes, "Адмидат", 0)),
            "gdeslon": float(get_attr_value(attributes, "ГдеСлон", 0)),
            "cityads": float(get_attr_value(attributes, "CityAds", 0)),
            "ozon": float(get_attr_value(attributes, "Ozon", 0)),
            "ozon_fbs": float(get_attr_value(attributes, "Ozon FBS", 0)),
            "yamarket_fbs": float(get_attr_value(attributes, "Яндекс Маркет FBS", 0)),
            "yamarket_dbs": float(get_attr_value(attributes, "Яндекс Маркет DBS", 0)),
            "yandex_direct": float(get_attr_value(attributes, "Яндекс Директ", 0)),
            "price_ru": float(get_attr_value(attributes, "Price ru", 0)),
            "wildberries": float(get_attr_value(attributes, "Wildberries", 0)),
            "gis2": float(get_attr_value(attributes, "2Gis", 0)),
            "seo": float(get_attr_value(attributes, "SEO", 0)),
            "programmatic": float(get_attr_value(attributes, "Программатик", 0)),
            "avito": float(get_attr_value(attributes, "Авито", 0)),
            "multiorders": float(get_attr_value(attributes, "Мультиканальные заказы", 0)),
            "estimated_discount": float(get_attr_value(attributes, "Примерная скидка", 0))
        }
    except Exception as e:
        logger.error(f"Ошибка подготовки данных позиции: {str(e)}")
        return None

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
    """Исправленный экспорт данных в Excel файл с обработкой ошибок"""
    conn = None
    try:
        logger.info(f"Начало экспорта данных с {date_range.start_date} по {date_range.end_date}")
        
        # Подключаемся к БД
        conn = await get_db_connection()
        
        # Создаем новую книгу Excel
        wb = Workbook()
        
        # Удаляем дефолтный лист
        if wb.worksheets:
            wb.remove(wb.worksheets[0])
        
        # Создаем листы по очереди с обработкой ошибок
        await create_demands_sheet_safe(wb, conn, date_range)
        await create_positions_sheet_safe(wb, conn, date_range)
        await create_products_summary_sheet_safe(wb, conn, date_range)
        
        # Создаем временный файл
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        # Формируем имя файла
        start_date_str = date_range.start_date[:10].replace('-', '_')
        end_date_str = date_range.end_date[:10].replace('-', '_')
        filename = f"report_{start_date_str}_{end_date_str}.xlsx"
        
        # Возвращаем файл
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(output.getvalue()))
            }
        )
        
    except Exception as e:
        logger.error(f"Критическая ошибка при экспорте в Excel: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка экспорта: {str(e)}"
        )
    finally:
        if conn:
            await conn.close()

async def create_demands_sheet(wb: Workbook, conn, date_range: DateRange):
    """Создает лист с отгрузками"""
    try:
        # Преобразование строк в datetime
        try:
            start_date = datetime.strptime(date_range.start_date, "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(date_range.end_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # Если не удалось распарсить, пробуем другой формат
            start_date = datetime.strptime(date_range.start_date[:19], "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(date_range.end_date[:19], "%Y-%m-%d %H:%M:%S")

        # Получаем данные из БД
        rows = await conn.fetch(
            """
            SELECT 
                number, date, counterparty, store, project, sales_channel,
                amount, cost_price, overhead, profit, promo_period, delivery_amount,
                admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
                yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                programmatic, avito, multiorders, estimated_discount
            FROM demands
            WHERE date BETWEEN $1 AND $2
            ORDER BY date DESC
            """,
            start_date, end_date
        )
        
        ws = wb.create_sheet("Отгрузки")
        
        # Заголовки столбцов
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
            "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
            "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
            "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
            "Примерная скидка"
        ]
        
        # Добавляем заголовки
        ws.append(headers)
        
        # Применяем стили
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'),
                            top=Side(style='thin'), bottom=Side(style='thin'))
        
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center', vertical='center')
            ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col-1]) * 1.1)
        
        # Добавляем данные
        for row in rows:
            # Безопасное преобразование значений
            row_data = []
            for i, value in enumerate(row):
                if value is None:
                    row_data.append("")
                elif isinstance(value, (int, float, Decimal)):
                    row_data.append(float(value))
                elif isinstance(value, datetime):
                    row_data.append(value)
                else:
                    row_data.append(str(value))
            
            ws.append(row_data)
        
        # Форматируем числовые столбцы
        numeric_cols = [7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28]
        for col in numeric_cols:
            for row_num in range(2, ws.max_row + 1):
                cell = ws.cell(row=row_num, column=col)
                if isinstance(cell.value, (int, float)):
                    cell.number_format = '#,##0.00'
        
        # Форматируем столбец с датами
        for row_num in range(2, ws.max_row + 1):
            cell = ws.cell(row=row_num, column=2)
            if isinstance(cell.value, datetime):
                cell.number_format = 'DD.MM.YYYY HH:MM:SS'
        
        # Добавляем автофильтр
        if ws.max_row > 1:
            ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{ws.max_row}"
        
        logger.info(f"Лист 'Отгрузки' создан с {len(rows)} записями")
        
    except Exception as e:
        logger.error(f"Ошибка при создании листа с отгрузками: {str(e)}")
        raise

async def create_positions_sheet(wb: Workbook, conn, date_range: DateRange):
    """Создает лист с товарами, сгруппированными по отгрузкам с себестоимостью"""
    try:
        # Преобразование строк в datetime
        try:
            start_date = datetime.strptime(date_range.start_date, "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(date_range.end_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            start_date = datetime.strptime(date_range.start_date[:19], "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(date_range.end_date[:19], "%Y-%m-%d %H:%M:%S")

        rows = await conn.fetch(
            """
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
                dp.cost_price,
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
                d.amount as total_amount,
                d.cost_price as total_cost_price,
                d.overhead as total_overhead,
                d.profit as total_profit
            FROM demand_positions dp
            JOIN demands d ON dp.demand_id = d.id
            WHERE d.date BETWEEN $1 AND $2
            ORDER BY d.number, d.date DESC
            """,
            start_date, end_date
        )
        
        ws = wb.create_sheet("Отчет по товарам")
        
        # Заголовки столбцов
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Артикул", "Код",
            "Накладные расходы", "Прибыль", "Акционный период", "Сумма доставки", "Адмидат",
            "ГдеСлон", "CityAds", "Ozon", "Ozon FBS", "Яндекс Маркет FBS", "Яндекс Маркет DBS",
            "Яндекс Директ", "Price ru", "Wildberries", "2Gis", "SEO", "Программатик", "Авито",
            "Мультиканальные заказы", "Примерная скидка"
        ]
        
        # Добавляем заголовки
        ws.append(headers)
        
        # Стили для Excel
        header_font = Font(name='Calibri', bold=True, size=11, color='FFFFFF')
        header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
        header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'),
                             top=Side(style='thin'), bottom=Side(style='thin'))
        
        # Применяем стили к заголовкам
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border
            column_letter = get_column_letter(col)
            ws.column_dimensions[column_letter].width = max(15, len(headers[col-1]) * 1.1)
        
        # Группировка по отгрузкам
        current_demand = None
        row_num = 2
        
        # Группируем данные по отгрузкам
        demands_data = {}
        for row in rows:
            demand_number = row['demand_number']
            if demand_number not in demands_data:
                demands_data[demand_number] = {
                    'info': {
                        'number': demand_number,
                        'date': row['date'],
                        'counterparty': row['counterparty'],
                        'store': row['store'],
                        'project': row['project'],
                        'sales_channel': row['sales_channel'],
                        'total_amount': float(row['total_amount']) if row['total_amount'] else 0,
                        'total_cost_price': float(row['total_cost_price']) if row['total_cost_price'] else 0,
                        'total_overhead': float(row['total_overhead']) if row['total_overhead'] else 0,
                        'total_profit': float(row['total_profit']) if row['total_profit'] else 0,
                        'promo_period': row['promo_period'] or '',
                        'delivery_amount': float(row['delivery_amount']) if row['delivery_amount'] else 0,
                        'admin_data': float(row['admin_data']) if row['admin_data'] else 0,
                        'gdeslon': float(row['gdeslon']) if row['gdeslon'] else 0,
                        'cityads': float(row['cityads']) if row['cityads'] else 0,
                        'ozon': float(row['ozon']) if row['ozon'] else 0,
                        'ozon_fbs': float(row['ozon_fbs']) if row['ozon_fbs'] else 0,
                        'yamarket_fbs': float(row['yamarket_fbs']) if row['yamarket_fbs'] else 0,
                        'yamarket_dbs': float(row['yamarket_dbs']) if row['yamarket_dbs'] else 0,
                        'yandex_direct': float(row['yandex_direct']) if row['yandex_direct'] else 0,
                        'price_ru': float(row['price_ru']) if row['price_ru'] else 0,
                        'wildberries': float(row['wildberries']) if row['wildberries'] else 0,
                        'gis2': float(row['gis2']) if row['gis2'] else 0,
                        'seo': float(row['seo']) if row['seo'] else 0,
                        'programmatic': float(row['programmatic']) if row['programmatic'] else 0,
                        'avito': float(row['avito']) if row['avito'] else 0,
                        'multiorders': float(row['multiorders']) if row['multiorders'] else 0,
                        'estimated_discount': float(row['estimated_discount']) if row['estimated_discount'] else 0
                    },
                    'positions': []
                }
            
            # Добавляем позицию
            demands_data[demand_number]['positions'].append({
                'product_name': row['product_name'] or '',
                'quantity': float(row['quantity']) if row['quantity'] else 0,
                'price': float(row['price']) if row['price'] else 0,
                'amount': float(row['amount']) if row['amount'] else 0,
                'cost_price': float(row['cost_price']) if row['cost_price'] else 0,
                'article': row['article'] or '',
                'code': row['code'] or ''
            })
        
        # Добавляем данные в лист
        for demand_number, demand_data in demands_data.items():
            info = demand_data['info']
            
            # Строка с итогами по отгрузке
            summary_row = [
                info['number'],
                info['date'],
                info['counterparty'],
                info['store'],
                info['project'],
                info['sales_channel'],
                "Итого по отгрузке:",
                "",
                "",
                info['total_amount'],
                info['total_cost_price'],
                "",
                "",
                info['total_overhead'],
                info['total_profit'],
                info['promo_period'],
                info['delivery_amount'],
                info['admin_data'],
                info['gdeslon'],
                info['cityads'],
                info['ozon'],
                info['ozon_fbs'],
                info['yamarket_fbs'],
                info['yamarket_dbs'],
                info['yandex_direct'],
                info['price_ru'],
                info['wildberries'],
                info['gis2'],
                info['seo'],
                info['programmatic'],
                info['avito'],
                info['multiorders'],
                info['estimated_discount']
            ]
            
            ws.append(summary_row)
            
            # Форматирование строки с итогами
            for col in range(1, len(headers) + 1):
                cell = ws.cell(row=row_num, column=col)
                cell.font = Font(name='Calibri', bold=True)
                cell.fill = PatternFill(start_color='D9E1F2', end_color='D9E1F2', fill_type='solid')
                cell.border = thin_border
                
                if col in [10, 11, 14, 15, 17] + list(range(18, 34)):
                    if isinstance(cell.value, (int, float)):
                        cell.number_format = '#,##0.00'
                        cell.alignment = Alignment(horizontal='right', vertical='center')
                
                if col == 7:
                    cell.alignment = Alignment(horizontal='right', vertical='center')
                
                if col == 2 and isinstance(cell.value, datetime):
                    cell.number_format = 'DD.MM.YYYY HH:MM:SS'
            
            row_num += 1
            
            # Строки с товарами
            for position in demand_data['positions']:
                product_row = [
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    position['product_name'],
                    position['quantity'],
                    position['price'],
                    position['amount'],
                    position['cost_price'],
                    position['article'],
                    position['code'],
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    ""
                ]
                
                ws.append(product_row)
                
                # Форматирование строки с товаром
                for col in range(1, len(headers) + 1):
                    cell = ws.cell(row=row_num, column=col)
                    cell.font = Font(name='Calibri', size=10)
                    cell.border = thin_border
                    
                    if col in [8, 9, 10, 11] and isinstance(cell.value, (int, float)):
                        cell.number_format = '#,##0.00'
                        cell.alignment = Alignment(horizontal='right', vertical='center')
                
                row_num += 1
        
        # Добавляем автофильтр и замораживаем заголовки
        if ws.max_row > 1:
            ws.auto_filter.ref = ws.dimensions
        ws.freeze_panes = 'A2'
        
        logger.info(f"Лист 'Отчет по товарам' создан с {len(rows)} записями")
        
    except Exception as e:
        logger.error(f"Ошибка при создании листа с товарами: {str(e)}")
        raise

async def create_products_summary_sheet(wb: Workbook, conn, date_range: DateRange):
    """Создает лист со сводным отчетом по товарам"""
    try:
        # Преобразование строк в datetime
        try:
            start_date = datetime.strptime(date_range.start_date, "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(date_range.end_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            start_date = datetime.strptime(date_range.start_date[:19], "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(date_range.end_date[:19], "%Y-%m-%d %H:%M:%S")

        rows = await conn.fetch(
            """
            SELECT 
                dp.product_name as product,
                dp.article,
                dp.code,
                SUM(dp.quantity) as total_quantity,
                d.store,
                d.project,
                d.sales_channel,
                AVG(dp.price) as avg_price,
                SUM(dp.delivery_amount) as delivery_sum,
                SUM(dp.amount) as total_amount,
                SUM(dp.cost_price) as total_cost_price,
                SUM(dp.overhead) as total_overhead,
                SUM(dp.profit) as total_profit,
                CASE 
                    WHEN SUM(dp.amount) = 0 THEN 0 
                    ELSE (SUM(dp.profit) / SUM(dp.amount)) * 100 
                END as margin_percent
            FROM demand_positions dp
            JOIN demands d ON dp.demand_id = d.id
            WHERE d.date BETWEEN $1 AND $2
            GROUP BY dp.product_name, dp.article, dp.code, d.store, d.project, d.sales_channel
            ORDER BY dp.product_name, dp.article
            """,
            start_date, end_date
        )
        
        ws = wb.create_sheet("Сводный отчет по товарам")
        
        # Заголовки столбцов
        headers = [
            "Товар", "Артикул", "Код", "Общее количество", "Склад", "Проект", "Канал продаж",
            "Средняя цена", "Сумма оплачиваемой доставки", "Общая сумма", "Себестоимость товара",
            "Сумма накладных расходов", "Общая прибыль", "Маржинальность (%)"
        ]
        
        # Добавляем заголовки
        ws.append(headers)
        
        # Применяем стили к заголовкам
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'),
                            top=Side(style='thin'), bottom=Side(style='thin'))
        
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center', vertical='center')
            ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col-1]) * 1.1)
        
        # Добавляем данные
        for row in rows:
            # Безопасное преобразование значений
            row_data = []
            for i, value in enumerate(row):
                if value is None:
                    row_data.append("")
                elif isinstance(value, (int, float, Decimal)):
                    row_data.append(float(value))
                else:
                    row_data.append(str(value))
            
            ws.append(row_data)
        
        # Форматируем числовые столбцы
        numeric_cols = [4, 8, 9, 10, 11, 12, 13, 14]  # Индексы числовых столбцов
        for col in numeric_cols:
            for row_num in range(2, ws.max_row + 1):
                cell = ws.cell(row=row_num, column=col)
                if isinstance(cell.value, (int, float)):
                    if col == 14:  # Маржинальность в процентах
                        cell.number_format = '0.00%'
                        cell.value = cell.value / 100  # Конвертируем в десятичную дробь для процентов
                    else:
                        cell.number_format = '#,##0.00'
        
        # Добавляем автофильтр
        if ws.max_row > 1:
            ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{ws.max_row}"
        
        logger.info(f"Лист 'Сводный отчет по товарам' создан с {len(rows)} записями")
        
    except Exception as e:
        logger.error(f"Ошибка при создании сводного отчета по товарам: {str(e)}")
        # Создаем пустой лист в случае ошибки
        ws = wb.create_sheet("Сводный отчет по товарам")
        ws.append(["Ошибка при создании отчета", str(e)])

def apply_sheet_styling(ws, headers, rows, numeric_columns, profit_column, sheet_type):
    """Применяет стили к листу Excel с учетом типа листа"""
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
    from openpyxl.utils import get_column_letter
    
    # Шрифты
    header_font = Font(name='Calibri', bold=True, size=11, color='FFFFFF')
    cell_font = Font(name='Calibri', size=10)
    
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
    header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
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
        ws.column_dimensions[column_letter].width = max(15, len(headers[col-1]) * 1.1)
    
    # Добавляем данные и форматируем их
    for row_idx, row in enumerate(rows, start=2):
        for col_idx, value in enumerate(row, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.font = cell_font
            cell.border = thin_border
            
            # Форматирование чисел и дат
            if (col_idx - 1) in numeric_columns:  # Учитываем смещение на 1 для 1-based индексации
                try:
                    num_value = float(value) if value not in [None, ''] else 0.0
                    cell.value = num_value
                    
                    # Особое форматирование для маржинальности (проценты)
                    if sheet_type == "products_summary" and col_idx == 14:
                        cell.number_format = '0.00%'
                    else:
                        cell.number_format = '#,##0.00'
                    
                    cell.alignment = right_alignment
                    
                    # Проверяем отрицательную прибыль
                    if col_idx == profit_column + 1 and num_value < 0:  # +1 т.к. profit_column 0-based
                        cell.fill = negative_fill
                    elif row_idx % 2 == 0:  # Зебра для читаемости
                        cell.fill = money_fill
                except (ValueError, TypeError):
                    cell.alignment = left_alignment
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
        if (col - 1) in numeric_columns:  # Учитываем смещение на 1 для 1-based индексации
            start_col = get_column_letter(col)
            
            # Для столбца с маржинальностью используем средневзвешенное
            if sheet_type == "products_summary" and col == 14:
                total_amount_col = get_column_letter(10)  # Общая сумма (J)
                total_profit_col = get_column_letter(13)  # Общая прибыль (M)
                formula = f"=SUM({total_profit_col}2:{total_profit_col}{last_row})/SUM({total_amount_col}2:{total_amount_col}{last_row})"
                cell.value = formula
                cell.number_format = '0.00%'
            else:
                formula = f"SUM({start_col}2:{start_col}{last_row})"
                cell.value = f"=ROUND({formula}, 2)"
                cell.number_format = '#,##0.00'
            
            cell.alignment = right_alignment
            cell.fill = PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid')
        elif col == 1:
            cell.value = "Итого:"
            cell.alignment = right_alignment

# Добавьте в импорты (если нет)
import os
from pathlib import Path

# Обновите конфигурацию Google Sheets (замените текущий блок)
GOOGLE_CREDS_PATH = "/app/credentials/service-account.json"
if not Path(GOOGLE_CREDS_PATH).exists():
    logger.error(f"Google credentials file not found at {GOOGLE_CREDS_PATH}")

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

@app.post("/api/export/gsheet")
async def export_to_gsheet(date_range: DateRange):
    try:
        logger.info("Создание Google Таблицы с оформлением как в Excel...")
        
        # Проверка учетных данных
        if not os.path.exists(GOOGLE_CREDS_PATH):
            logger.error("Файл учетных данных не найден!")
            return JSONResponse(
                status_code=500,
                content={"detail": "Файл учетных данных Google не найден"}
            )

        # Инициализация Google Sheets API
        gc = gspread.service_account(filename=GOOGLE_CREDS_PATH)
        
        # Создаем новую таблицу
        title = f"Отчет по отгрузкам {date_range.start_date} - {date_range.end_date}"
        sh = gc.create(title)
        sh.share(None, perm_type='anyone', role='writer')

        # Удаляем дефолтный лист
        if len(sh.worksheets()) > 1:
            sh.del_worksheet(sh.get_worksheet(0))

        # Стили оформления
        HEADER_STYLE = {
            "backgroundColor": {"red": 0.20, "green": 0.47, "blue": 0.73},
            "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "wrapStrategy": "WRAP",
            "borders": {
                "top": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                "left": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                "right": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}}
            }
        }

        SUMMARY_ROW_STYLE = {
            "backgroundColor": {"red": 0.85, "green": 0.88, "blue": 0.94},
            "textFormat": {"bold": True},
            "borders": HEADER_STYLE["borders"]
        }

        PRODUCT_ROW_STYLE = {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "borders": {
                "top": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                "left": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                "right": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}}
            }
        }

        TOTAL_ROW_STYLE = {
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
            "textFormat": {"bold": True},
            "borders": HEADER_STYLE["borders"]
        }

        NUMBER_FORMAT = {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
            "horizontalAlignment": "RIGHT"
        }

        DATE_FORMAT = {
            "numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy hh:mm"},
            "horizontalAlignment": "CENTER"
        }

        NEGATIVE_PROFIT_STYLE = {
            "backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}
        }

        # Вспомогательная функция для преобразования данных
        def prepare_value(value):
            if isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, Decimal):
                return float(value)
            return value

        # ===== 1. ЛИСТ С ТОВАРАМИ =====
        worksheet_positions = sh.add_worksheet(title="Отчет по товарам", rows=1000, cols=33)
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Получаем данные с преобразованием типов
        cur.execute("""
            SELECT 
                d.number, d.date, d.counterparty, d.store, d.project, d.sales_channel,
                dp.product_name, dp.quantity, dp.price, dp.amount, 
                dp.cost_price, dp.article, dp.code, dp.overhead, dp.profit,
                d.promo_period, d.delivery_amount, d.admin_data,
                d.gdeslon, d.cityads, d.ozon, d.ozon_fbs,
                d.yamarket_fbs, d.yamarket_dbs, d.yandex_direct,
                d.price_ru, d.wildberries, d.gis2, d.seo,
                d.programmatic, d.avito, d.multiorders,
                d.estimated_discount
            FROM demand_positions dp
            JOIN demands d ON dp.demand_id = d.id
            WHERE d.date BETWEEN %s AND %s
            ORDER BY d.number, d.date DESC
        """, (date_range.start_date, date_range.end_date))
        
        # Преобразуем данные
        positions = []
        for row in cur.fetchall():
            positions.append([prepare_value(value) for value in row])
        
        # Заголовки
        pos_headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Артикул", "Код",
            "Накладные расходы", "Прибыль", "Акционный период", "Сумма доставки", "Адмидат",
            "ГдеСлон", "CityAds", "Ozon", "Ozon FBS", "Яндекс Маркет FBS", "Яндекс Маркет DBS",
            "Яндекс Директ", "Price ru", "Wildberries", "2Gis", "SEO", "Программатик", "Авито",
            "Мультиканальные заказы", "Примерная скидка"
        ]
        
        # Добавляем заголовки
        worksheet_positions.append_row(pos_headers)
        
        # Подготовка данных для вставки с группировкой
        if positions:
            current_demand = None
            rows_to_add = []
            batch_size = 100
            total_rows = 0
            
            for row in positions:
                demand_number = row[0]
                
                if demand_number != current_demand:
                    current_demand = demand_number
                    
                    # Получаем общую себестоимость по отгрузке
                    cur.execute("""
                        SELECT cost_price FROM demands 
                        WHERE number = %s AND date BETWEEN %s AND %s
                        LIMIT 1
                    """, (demand_number, date_range.start_date, date_range.end_date))
                    total_cost = prepare_value(cur.fetchone()[0]) if cur.rowcount > 0 else 0
                    
                    # Строка с итогами по отгрузке
                    summary_row = [
                        demand_number, row[1], row[2], row[3], row[4], row[5],
                        "Итого по отгрузке:", "", "", row[9], total_cost, "", "",
                        row[13], row[14], row[15], row[16], row[17], row[18],
                        row[19], row[20], row[21], row[22], row[23], row[24],
                        row[25], row[26], row[27], row[28], row[29], row[30],
                        row[31]
                    ]
                    rows_to_add.append([prepare_value(value) for value in summary_row])
                    total_rows += 1
                
                # Строка с товаром
                product_row = [
                    "", "", "", "", "", "",
                    row[6], row[7], row[8], row[9], row[10], row[11], row[12],
                    "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""
                ]
                rows_to_add.append([prepare_value(value) for value in product_row])
                total_rows += 1
            
            # Вставляем данные пакетами
            for i in range(0, len(rows_to_add), batch_size):
                batch = rows_to_add[i:i + batch_size]
                worksheet_positions.append_rows(batch)
        
        # Форматируем лист с товарами
        last_row = total_rows + 1 if positions else 1
        requests = []
        
        # Форматирование заголовков
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet_positions.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
                "cell": {"userEnteredFormat": HEADER_STYLE},
                "fields": "userEnteredFormat"
            }
        })
        
        # Форматирование строк с товарами
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet_positions.id,
                    "startRowIndex": 1,
                    "endRowIndex": last_row
                },
                "cell": {"userEnteredFormat": PRODUCT_ROW_STYLE},
                "fields": "userEnteredFormat"
            }
        })
        
        # Форматирование строк с итогами по отгрузке
        if positions:
            for i, row in enumerate(rows_to_add, start=1):
                if row[6] == "Итого по отгрузке:":
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": worksheet_positions.id,
                                "startRowIndex": i,
                                "endRowIndex": i + 1
                            },
                            "cell": {"userEnteredFormat": SUMMARY_ROW_STYLE},
                            "fields": "userEnteredFormat"
                        }
                    })
                    
                    # Выравнивание "Итого по отгрузке:" по правому краю
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": worksheet_positions.id,
                                "startRowIndex": i,
                                "endRowIndex": i + 1,
                                "startColumnIndex": 6,
                                "endColumnIndex": 7
                            },
                            "cell": {"userEnteredFormat": {"horizontalAlignment": "RIGHT"}},
                            "fields": "userEnteredFormat.horizontalAlignment"
                        }
                    })
        
        # Форматирование числовых столбцов
        numeric_columns = [7, 8, 9, 10, 13, 14] + list(range(16, 32))
        for col in numeric_columns:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet_positions.id,
                        "startRowIndex": 1,
                        "endRowIndex": last_row,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1
                    },
                    "cell": {"userEnteredFormat": NUMBER_FORMAT},
                    "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment"
                }
            })
        
        # Форматирование дат
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet_positions.id,
                    "startRowIndex": 1,
                    "endRowIndex": last_row,
                    "startColumnIndex": 1,
                    "endColumnIndex": 2
                },
                "cell": {"userEnteredFormat": DATE_FORMAT},
                "fields": "userEnteredFormat"
            }
        })
        
        # Подсветка отрицательной прибыли
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": worksheet_positions.id,
                        "startRowIndex": 1,
                        "endRowIndex": last_row,
                        "startColumnIndex": 14,
                        "endColumnIndex": 15
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "NUMBER_LESS",
                            "values": [{"userEnteredValue": "0"}]
                        },
                        "format": NEGATIVE_PROFIT_STYLE
                    }
                },
                "index": 0
            }
        })
        
        # Установка ширины столбцов
        column_widths = [
            {"pixelSize": 100},  # A: Номер отгрузки
            {"pixelSize": 150},  # B: Дата
            {"pixelSize": 200},  # C: Контрагент
            {"pixelSize": 120},  # D: Склад
            {"pixelSize": 120},  # E: Проект
            {"pixelSize": 150},  # F: Канал продаж
            {"pixelSize": 300},  # G: Товар
            {"pixelSize": 90},   # H: Количество
            {"pixelSize": 90},   # I: Цена
            {"pixelSize": 90},   # J: Сумма
            {"pixelSize": 110},  # K: Себестоимость
            {"pixelSize": 100},  # L: Артикул
            {"pixelSize": 80},   # M: Код
            {"pixelSize": 110},  # N: Накладные расходы
            {"pixelSize": 90},   # O: Прибыль
            {"pixelSize": 120},  # P: Акционный период
            {"pixelSize": 110},  # Q: Сумма доставки
            {"pixelSize": 90},   # R: Адмидат
            {"pixelSize": 90},   # S: ГдеСлон
            {"pixelSize": 90},   # T: CityAds
            {"pixelSize": 80},   # U: Ozon
            {"pixelSize": 100},  # V: Ozon FBS
            {"pixelSize": 130},  # W: Яндекс Маркет FBS
            {"pixelSize": 130},  # X: Яндекс Маркет DBS
            {"pixelSize": 110},  # Y: Яндекс Директ
            {"pixelSize": 90},   # Z: Price ru
            {"pixelSize": 110},  # AA: Wildberries
            {"pixelSize": 80},   # AB: 2Gis
            {"pixelSize": 80},   # AC: SEO
            {"pixelSize": 110},  # AD: Программатик
            {"pixelSize": 80},   # AE: Авито
            {"pixelSize": 140},  # AF: Мультиканальные заказы
            {"pixelSize": 120}   # AG: Примерная скидка
        ]
        
        for i, width in enumerate(column_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet_positions.id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": width,
                    "fields": "pixelSize"
                }
            })
        
        # Добавляем итоговую строку
        if positions:
            # Формулы для суммирования
            sum_formulas = [
                "", "", "", "", "", "", "Общий итог:",
                f'=SUM(H2:H{last_row})',
                f'=AVERAGE(I2:I{last_row})',
                f'=SUM(J2:J{last_row})',
                f'=SUM(K2:K{last_row})',
                "", "",
                f'=SUM(N2:N{last_row})',
                f'=SUM(O2:O{last_row})',
                "",
                f'=SUM(Q2:Q{last_row})',
                f'=SUM(R2:R{last_row})',
                f'=SUM(S2:S{last_row})',
                f'=SUM(T2:T{last_row})',
                f'=SUM(U2:U{last_row})',
                f'=SUM(V2:V{last_row})',
                f'=SUM(W2:W{last_row})',
                f'=SUM(X2:X{last_row})',
                f'=SUM(Y2:Y{last_row})',
                f'=SUM(Z2:Z{last_row})',
                f'=SUM(AA2:AA{last_row})',
                f'=SUM(AB2:AB{last_row})',
                f'=SUM(AC2:AC{last_row})',
                f'=SUM(AD2:AD{last_row})',
                f'=SUM(AE2:AE{last_row})',
                f'=SUM(AF2:AF{last_row})',
                f'=SUM(AG2:AG{last_row})'
            ]
            
            worksheet_positions.append_row(sum_formulas)
            
            # Форматирование итоговой строки
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet_positions.id,
                        "startRowIndex": last_row,
                        "endRowIndex": last_row + 1
                    },
                    "cell": {"userEnteredFormat": TOTAL_ROW_STYLE},
                    "fields": "userEnteredFormat"
                }
            })
            
            last_row += 1
        
        # Фильтры и закрепление
        requests.extend([
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": worksheet_positions.id,
                            "startRowIndex": 0,
                            "endRowIndex": last_row,
                            "startColumnIndex": 0,
                            "endColumnIndex": 33
                        }
                    }
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": worksheet_positions.id,
                        "gridProperties": {"frozenRowCount": 1}
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            }
        ])
        
        # ===== 2. ЛИСТ С ОТГРУЗКАМИ =====
        worksheet_demands = sh.add_worksheet(title="Отчет по отгрузкам", rows=1000, cols=28)
        
        # Получаем данные с преобразованием типов
        cur.execute("""
            SELECT 
                number, date, counterparty, store, project, sales_channel,
                amount, cost_price, overhead, profit, 
                promo_period, delivery_amount, admin_data,
                gdeslon, cityads, ozon, ozon_fbs,
                yamarket_fbs, yamarket_dbs, yandex_direct,
                price_ru, wildberries, gis2, seo,
                programmatic, avito, multiorders,
                estimated_discount
            FROM demands
            WHERE date BETWEEN %s AND %s
            ORDER BY date DESC
        """, (date_range.start_date, date_range.end_date))
        
        # Преобразуем данные
        demands = []
        for row in cur.fetchall():
            demands.append([prepare_value(value) for value in row])
        
        conn.close()
        
        # Заголовки
        demands_headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
            "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
            "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
            "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
            "Примерная скидка"
        ]
        
        # Добавляем заголовки и данные
        worksheet_demands.append_row(demands_headers)
        if demands:
            worksheet_demands.append_rows(demands)
        
        # Форматируем лист с отгрузками
        last_demand_row = len(demands) + 1 if demands else 1
        demand_requests = []
        
        # Форматирование заголовков
        demand_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet_demands.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
                "cell": {"userEnteredFormat": HEADER_STYLE},
                "fields": "userEnteredFormat"
            }
        })
        
        # Форматирование данных
        demand_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet_demands.id,
                    "startRowIndex": 1,
                    "endRowIndex": last_demand_row
                },
                "cell": {"userEnteredFormat": PRODUCT_ROW_STYLE},
                "fields": "userEnteredFormat"
            }
        })
        
        # Форматирование числовых столбцов
        numeric_demand_columns = [6, 7, 8, 9, 11] + list(range(12, 28))
        for col in numeric_demand_columns:
            demand_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet_demands.id,
                        "startRowIndex": 1,
                        "endRowIndex": last_demand_row,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1
                    },
                    "cell": {"userEnteredFormat": NUMBER_FORMAT},
                    "fields": "userEnteredFormat.numberFormat,userEnteredFormat.horizontalAlignment"
                }
            })
        
        # Форматирование дат
        demand_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet_demands.id,
                    "startRowIndex": 1,
                    "endRowIndex": last_demand_row,
                    "startColumnIndex": 1,
                    "endColumnIndex": 2
                },
                "cell": {"userEnteredFormat": DATE_FORMAT},
                "fields": "userEnteredFormat"
            }
        })
        
        # Подсветка отрицательной прибыли
        demand_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": worksheet_demands.id,
                        "startRowIndex": 1,
                        "endRowIndex": last_demand_row,
                        "startColumnIndex": 9,
                        "endColumnIndex": 10
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "NUMBER_LESS",
                            "values": [{"userEnteredValue": "0"}]
                        },
                        "format": NEGATIVE_PROFIT_STYLE
                    }
                },
                "index": 0
            }
        })
        
        # Добавляем итоговую строку
        if demands:
            # Формулы для суммирования
            sum_formulas = [
                "Итого:", "", "", "", "", "",
                f'=SUM(G2:G{last_demand_row})',
                f'=SUM(H2:H{last_demand_row})',
                f'=SUM(I2:I{last_demand_row})',
                f'=SUM(J2:J{last_demand_row})',
                "",
                f'=SUM(L2:L{last_demand_row})',
                f'=SUM(M2:M{last_demand_row})',
                f'=SUM(N2:N{last_demand_row})',
                f'=SUM(O2:O{last_demand_row})',
                f'=SUM(P2:P{last_demand_row})',
                f'=SUM(Q2:Q{last_demand_row})',
                f'=SUM(R2:R{last_demand_row})',
                f'=SUM(S2:S{last_demand_row})',
                f'=SUM(T2:T{last_demand_row})',
                f'=SUM(U2:U{last_demand_row})',
                f'=SUM(V2:V{last_demand_row})',
                f'=SUM(W2:W{last_demand_row})',
                f'=SUM(X2:X{last_demand_row})',
                f'=SUM(Y2:Y{last_demand_row})',
                f'=SUM(Z2:Z{last_demand_row})',
                f'=SUM(AA2:AA{last_demand_row})',
                f'=SUM(AB2:AB{last_demand_row})'
            ]
            
            worksheet_demands.append_row(sum_formulas)
            
            # Форматирование итоговой строки
            demand_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": worksheet_demands.id,
                        "startRowIndex": last_demand_row,
                        "endRowIndex": last_demand_row + 1
                    },
                    "cell": {"userEnteredFormat": TOTAL_ROW_STYLE},
                    "fields": "userEnteredFormat"
                }
            })
            
            last_demand_row += 1
        
        # Установка ширины столбцов
        demand_column_widths = [
            {"pixelSize": 100},  # A: Номер отгрузки
            {"pixelSize": 150},  # B: Дата
            {"pixelSize": 200},  # C: Контрагент
            {"pixelSize": 120},  # D: Склад
            {"pixelSize": 120},  # E: Проект
            {"pixelSize": 150},  # F: Канал продаж
            {"pixelSize": 90},   # G: Сумма
            {"pixelSize": 110},  # H: Себестоимость
            {"pixelSize": 110},  # I: Накладные расходы
            {"pixelSize": 90},   # J: Прибыль
            {"pixelSize": 120},  # K: Акционный период
            {"pixelSize": 110},  # L: Сумма доставки
            {"pixelSize": 90},   # M: Адмидат
            {"pixelSize": 90},   # N: ГдеСлон
            {"pixelSize": 90},   # O: CityAds
            {"pixelSize": 80},   # P: Ozon
            {"pixelSize": 100},  # Q: Ozon FBS
            {"pixelSize": 130},  # R: Яндекс Маркет FBS
            {"pixelSize": 130},  # S: Яндекс Маркет DBS
            {"pixelSize": 110},  # T: Яндекс Директ
            {"pixelSize": 90},   # U: Price ru
            {"pixelSize": 110},  # V: Wildberries
            {"pixelSize": 80},   # W: 2Gis
            {"pixelSize": 80},   # X: SEO
            {"pixelSize": 110},  # Y: Программатик
            {"pixelSize": 80},   # Z: Авито
            {"pixelSize": 140},  # AA: Мультиканальные заказы
            {"pixelSize": 120}   # AB: Примерная скидка
        ]
        
        for i, width in enumerate(demand_column_widths):
            demand_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet_demands.id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": width,
                    "fields": "pixelSize"
                }
            })
        
        # Фильтры и закрепление
        demand_requests.extend([
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": worksheet_demands.id,
                            "startRowIndex": 0,
                            "endRowIndex": last_demand_row,
                            "startColumnIndex": 0,
                            "endColumnIndex": 28
                        }
                    }
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": worksheet_demands.id,
                        "gridProperties": {"frozenRowCount": 1}
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            }
        ])
        
        # Объединяем все запросы
        all_requests = requests + demand_requests
        
        # Применяем все запросы
        sh.batch_update({"requests": all_requests})
        
        # Устанавливаем порядок листов (товары первыми)
        sh.reorder_worksheets([worksheet_positions, worksheet_demands])
        
        logger.info(f"Таблица создана с оформлением как в Excel: {sh.url}")
        return {"url": sh.url}
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"detail": f"Ошибка при создании таблицы: {str(e)}"}
        )


@app.post("/api/webhook")
async def handle_moysklad_webhook(webhook_data: WebhookData, background_tasks: BackgroundTasks):
    """
    Обработчик вебхуков от МойСклад для обновления данных отгрузок в реальном времени
    """
    logger.info(f"Получен вебхук с {len(webhook_data.events)} событиями")

    # Обрабатываем события асинхронно в фоновой задаче
    background_tasks.add_task(process_webhook_events, webhook_data)
    
    return {
        "status": "accepted",
        "message": "Запрос принят в обработку",
        "events_count": len(webhook_data.events)
    }

async def process_webhook_events(webhook_data: WebhookData):
    """Фоновая обработка событий вебхука"""
    processed = 0
    errors = 0
    
    for event in webhook_data.events:
        try:
            # Валидация события
            if not is_valid_demand_event(event):
                continue
                
            demand_id = extract_demand_id(event)
            if not demand_id:
                continue

            logger.info(f"Начало обработки отгрузки {demand_id}")
            
            # Получаем полные данные отгрузки
            demand = await fetch_demand_with_retry(demand_id)
            if not demand:
                errors += 1
                continue
            
            # Обрабатываем данные
            success = await process_single_demand(demand)
            
            if success:
                processed += 1
                logger.info(f"Отгрузка {demand_id} успешно обработана")
            else:
                errors += 1
                
        except Exception as e:
            errors += 1
            logger.error(f"Критическая ошибка обработки события: {str(e)}")
            continue
    
    logger.info(f"Обработка завершена. Успешно: {processed}, с ошибками: {errors}")

def is_valid_demand_event(event: WebhookEvent) -> bool:
    """Проверяет, является ли событие валидным для обработки"""
    if not event.meta:
        logger.debug("Событие без meta-данных пропущено")
        return False
        
    entity_type = event.meta.get('type')
    if entity_type != 'demand':
        logger.debug(f"Событие типа {entity_type} пропущено")
        return False
        
    if event.action not in ['CREATE', 'UPDATE', 'DELETE']:
        logger.debug(f"Неизвестное действие {event.action} пропущено")
        return False
        
    return True

def extract_demand_id(event: WebhookEvent) -> Optional[str]:
    """Извлекает ID отгрузки из события"""
    try:
        href = event.meta.get('href', '')
        return href.split('/')[-1] if href else None
    except Exception as e:
        logger.error(f"Ошибка извлечения ID отгрузки: {str(e)}")
        return None

async def fetch_demand_with_retry(demand_id: str, max_retries: int = 3) -> Optional[Dict]:
    """Получает данные отгрузки с повторными попытками"""
    for attempt in range(max_retries):
        try:
            demand = moysklad.get_demand_by_id(demand_id)
            if demand:
                return demand
                
            logger.warning(f"Попытка {attempt + 1}: данные отгрузки не получены")
            
        except Exception as e:
            logger.error(f"Попытка {attempt + 1} ошибка: {str(e)}")
        
        if attempt < max_retries - 1:
            await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
    
    logger.error(f"Не удалось получить данные отгрузки {demand_id} после {max_retries} попыток")
    return None

async def process_single_demand(demand: Dict) -> bool:
    """Обрабатывает одну отгрузку и сохраняет в БД"""
    conn = None
    try:
        demand_id = demand.get('id')
        if not demand_id:
            logger.error("Отсутствует ID отгрузки")
            return False
            
        # Подготавливаем данные
        demand_data = prepare_demand_data(demand)
        positions_data = prepare_positions_data(demand)
        
        if not demand_data:
            logger.error("Не удалось подготовить данные отгрузки")
            return False
            
        # Сохраняем в БД
        conn = await asyncpg.connect(**DB_CONFIG)
        async with conn.transaction():
            # 1. Удаляем старые данные
            await conn.execute("DELETE FROM demands WHERE id = $1", demand_id)
            await conn.execute("DELETE FROM demand_positions WHERE demand_id = $1", demand_id)
            
            # 2. Вставляем новые данные
            await insert_demands_batch(conn, [demand_data])
            
            if positions_data:
                await insert_positions_batch(conn, positions_data)
                
            logger.info(f"Данные отгрузки {demand_id} успешно обновлены")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка сохранения отгрузки {demand_id}: {str(e)}")
        return False
    finally:
        if conn:
            await conn.close()

def prepare_positions_data(demand: Dict) -> List[Dict]:
    """Подготавливает данные позиций отгрузки"""
    try:
        positions = demand.get('positions', [])
        
        # Нормализация формата позиций
        if isinstance(positions, dict):
            positions = positions.get('rows', [])
            
        if not isinstance(positions, list):
            logger.warning(f"Некорректный формат позиций: {type(positions)}")
            return []
            
        logger.info(f"Подготовка {len(positions)} позиций для отгрузки {demand.get('id', 'unknown')}")
        
        prepared_positions = []
        for i, pos in enumerate(positions):
            if isinstance(pos, dict):
                prepared_pos = prepare_position_data(demand, pos)
                if prepared_pos:
                    prepared_positions.append(prepared_pos)
                    logger.debug(f"Позиция {i+1} подготовлена: {prepared_pos.get('id', 'unknown')}")
                else:
                    logger.warning(f"Позиция {i+1} не была подготовлена")
        
        logger.info(f"Успешно подготовлено {len(prepared_positions)} из {len(positions)} позиций")
        return prepared_positions
        
    except Exception as e:
        logger.error(f"Ошибка подготовки позиций: {str(e)}")
        return []

async def update_demand_positions(conn, demand_id: str, positions: List[Dict]):
    """Асинхронное обновление позиций с проверкой данных"""
    if not positions:
        logger.info("Нет позиций для обновления")
        return

    # Удаляем старые позиции
    await conn.execute("DELETE FROM demand_positions WHERE demand_id = $1", demand_id)
    
    query = """
        INSERT INTO demand_positions (
            id, demand_id, demand_number, date, counterparty, store, 
            project, sales_channel, product_name, quantity, price, 
            amount, cost_price, article, code, overhead, profit,
            promo_period, delivery_amount, admin_data, gdeslon,
            cityads, ozon, ozon_fbs, yamarket_fbs, yamarket_dbs,
            yandex_direct, price_ru, wildberries, gis2, seo,
            programmatic, avito, multiorders, estimated_discount
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
            $31, $32, $33, $34
        )
    """
    
    values = []
    for pos in positions:
        try:
            row = (
                pos['id'],
                pos['demand_id'],
                pos['demand_number'],
                pos['date'],
                pos['counterparty'],
                pos['store'],
                pos['project'],
                pos['sales_channel'],
                pos['product_name'],
                float(pos['quantity']),
                float(pos['price']),
                float(pos['amount']),
                float(pos['cost_price']),
                pos['article'],
                pos['code'],
                float(pos.get('overhead', 0)),
                float(pos.get('profit', 0)),
                pos['promo_period'],
                float(pos.get('delivery_amount', 0)),
                float(pos.get('admin_data', 0)),
                float(pos.get('gdeslon', 0)),
                float(pos.get('cityads', 0)),
                float(pos.get('ozon', 0)),
                float(pos.get('ozon_fbs', 0)),
                float(pos.get('yamarket_fbs', 0)),
                float(pos.get('yamarket_dbs', 0)),
                float(pos.get('yandex_direct', 0)),
                float(pos.get('price_ru', 0)),
                float(pos.get('wildberries', 0)),
                float(pos.get('gis2', 0)),
                float(pos.get('seo', 0)),
                float(pos.get('programmatic', 0)),
                float(pos.get('avito', 0)),
                float(pos.get('multiorders', 0)),
                float(pos.get('estimated_discount', 0))
            )
            values.append(row)
        except Exception as e:
            logger.error(f"Ошибка подготовки позиции {pos.get('id', 'unknown')}: {str(e)}")
            continue
    
    if values:
        await conn.executemany(query, values)