import time
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import os
from openpyxl import Workbook
import io
import asyncio
from typing import List, Dict, Any, Optional
import logging
import uuid
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Настройки CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Настройки базы данных
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),  # Используем имя сервиса из docker-compose
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME", "MS"),
    "user": os.getenv("DB_USER", "louella"),
    "password": os.getenv("DB_PASSWORD", "XBcMJoEO1ljb"),
    "sslmode": "verify-ca",
    "sslrootcert": "/root/.postgresql/root.crt"
}

# Инициализация API МойСклад
from .moysklad import MoyskladAPI
moysklad = MoyskladAPI(token="2e61e26f0613cf33fab5f31cf105302aa2d607c3")

# Модели данных
class DateRange(BaseModel):
    start_date: str
    end_date: str

class BatchProcessResponse(BaseModel):
    task_id: str
    status: str
    message: str

# Глобальный словарь для хранения статусов задач
tasks_status = {}

# Стили для Excel
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

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """Инициализация базы данных - создание таблиц если они не существуют"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Создаем таблицу demands (исправленный запрос)
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
        
        # Создаем таблицу demand_positions (исправленный запрос)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS demand_positions (
                id VARCHAR(255) PRIMARY KEY,
                demand_id VARCHAR(255) REFERENCES demands(id),
                assortment_id VARCHAR(255),
                product_name VARCHAR(500),
                quantity NUMERIC(15, 3),
                price NUMERIC(15, 2),
                cost_price NUMERIC(15, 2),
                overhead NUMERIC(15, 2),
                vat NUMERIC(15, 2),
                vat_enabled BOOLEAN,
                discount NUMERIC(15, 2),
                article VARCHAR(100),
                code VARCHAR(100),
                created TIMESTAMP,
                updated TIMESTAMP
            )
        """)
        
        # Создаем индексы (исправленные запросы)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_demand_positions_demand_id 
            ON demand_positions(demand_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_demand_positions_assortment_id 
            ON demand_positions(assortment_id)
        """)
        
        conn.commit()
        logger.info("Таблицы успешно созданы или уже существуют")
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {str(e)}")
        if conn:
            conn.rollback()
        raise  # Важно пробросить исключение дальше, чтобы приложение не стартовало с нерабочей БД
    finally:
        if conn:
            conn.close()

@app.on_event("startup")
async def startup_event():
    """Действия при старте приложения"""
    init_db()
    logger.info("Приложение запущено, база данных инициализирована")

async def process_demand_positions(demand_id: str):
    """Обработка и сохранение товарных позиций отгрузки"""
    conn = None
    try:
        positions = moysklad.get_demand_positions(demand_id)
        if not positions:
            return

        conn = get_db_connection()
        cur = conn.cursor()

        batch_values = []
        for position in positions:
            product_info = {}
            assortment = position.get("assortment", {})
            if assortment.get("meta", {}).get("type") == "product":
                product_id = assortment["meta"]["href"].split("/")[-1]
                product_info = moysklad.get_product_info(product_id)

            values = {
                "id": position.get("id", ""),
                "demand_id": demand_id,
                "assortment_id": assortment.get("meta", {}).get("href", "").split("/")[-1],
                "product_name": product_info.get("name", ""),
                "quantity": position.get("quantity", 0),
                "price": float(position.get("price", 0)) / 100,
                "cost_price": 0,  # Будет заполнено отдельно
                "overhead": float(position.get("overhead", 0)) / 100,
                "vat": float(position.get("vat", 0)) / 100,
                "vat_enabled": position.get("vatEnabled", False),
                "discount": float(position.get("discount", 0)) / 100,
                "article": product_info.get("article", ""),
                "code": product_info.get("code", ""),
                "created": position.get("created", ""),
                "updated": position.get("updated", "")
            }
            batch_values.append(values)

        if batch_values:
            query = """
                INSERT INTO demand_positions (
                    id, demand_id, assortment_id, product_name, quantity, price, 
                    cost_price, overhead, vat, vat_enabled, discount, 
                    article, code, created, updated
                ) VALUES (
                    %(id)s, %(demand_id)s, %(assortment_id)s, %(product_name)s, %(quantity)s, %(price)s, 
                    %(cost_price)s, %(overhead)s, %(vat)s, %(vat_enabled)s, %(discount)s, 
                    %(article)s, %(code)s, %(created)s, %(updated)s
                )
                ON CONFLICT (id) DO UPDATE SET
                    demand_id = EXCLUDED.demand_id,
                    assortment_id = EXCLUDED.assortment_id,
                    product_name = EXCLUDED.product_name,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    cost_price = EXCLUDED.cost_price,
                    overhead = EXCLUDED.overhead,
                    vat = EXCLUDED.vat,
                    vat_enabled = EXCLUDED.vat_enabled,
                    discount = EXCLUDED.discount,
                    article = EXCLUDED.article,
                    code = EXCLUDED.code,
                    created = EXCLUDED.created,
                    updated = EXCLUDED.updated
            """
            execute_batch(cur, query, batch_values)
            conn.commit()

    except Exception as e:
        logger.error(f"Ошибка при обработке позиций отгрузки {demand_id}: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

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

async def process_demands_batch(demands: List[Dict[str, Any]], task_id: str):
    """Асинхронная обработка пакета отгрузок"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        batch_size = 50
        saved_count = 0
        total_count = len(demands)
        
        logger.info(f"Начало обработки {total_count} отгрузок")
        
        batch_values = []
        
        for idx, demand in enumerate(demands, 1):
            try:
                values = prepare_demand_data(demand)
                batch_values.append(values)
                
                if len(batch_values) >= batch_size:
                    inserted = await insert_batch(cur, batch_values)
                    saved_count += inserted
                    batch_values = []
                    
                    # Обрабатываем позиции для каждой отгрузки
                    await process_demand_positions(demand["id"])
                    
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
        
        if batch_values:
            saved_count += await insert_batch(cur, batch_values)
        
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

async def insert_batch(cur, batch_values: List[Dict[str, Any]]):
    """Массовая вставка пакета данных"""
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
        logger.error(f"Ошибка при массовой вставке: {str(e)}")
        return 0

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
    """Экспорт отчета по отгрузкам в Excel"""
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
            ORDER BY date DESC
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
            "Примерная скидка"
        ]
        
        ws.append(headers)
        
        # Форматирование заголовков
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.alignment = center_alignment
            cell.fill = header_fill
            cell.border = thin_border
            ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col-1]) * 1.2)
        
        # Добавление данных
        for row in rows:
            ws.append(row)
        
        # Применение стилей
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=len(headers)):
            for cell in row:
                cell.border = thin_border
                if cell.column in [7, 8, 9, 10, 12] + list(range(13, 29)):  # Числовые столбцы
                    cell.number_format = '#,##0.00'
                    cell.alignment = right_alignment
                    if cell.column == 10 and cell.value and cell.value < 0:  # Прибыль
                        cell.fill = negative_fill
                elif cell.column == 2:  # Дата
                    cell.number_format = 'DD.MM.YYYY'
                    cell.alignment = center_alignment
                else:
                    cell.alignment = left_alignment
        
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = ws.dimensions
        
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

@app.post("/api/export/products-excel")
async def export_products_excel(date_range: DateRange):
    """Экспорт отчета по товарам в Excel"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                d.number AS shipment_number,
                d.date,
                d.counterparty,
                d.store,
                d.project,
                d.sales_channel,
                dp.product_name,
                dp.quantity,
                dp.price,
                dp.price * dp.quantity AS sum,
                dp.cost_price,
                dp.cost_price * dp.quantity AS cost_sum,
                dp.article,
                dp.code,
                d.overhead,
                (dp.price * dp.quantity) - (dp.cost_price * dp.quantity) - 
                (d.overhead * (dp.price * dp.quantity / NULLIF(d.amount, 0))) AS profit,
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
                d.estimated_discount
            FROM demand_positions dp
            JOIN demands d ON dp.demand_id = d.id
            WHERE d.date BETWEEN %s AND %s
            ORDER BY d.date DESC
        """, (date_range.start_date, date_range.end_date))
        
        rows = cur.fetchall()
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет по товарам"
        
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Сумма себестоимости",
            "Артикул", "Код", "Накладные расходы", "Прибыль", "Акционный период",
            "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
            "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
            "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
            "Примерная скидка"
        ]
        
        ws.append(headers)
        
        # Форматирование заголовков
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.alignment = center_alignment
            cell.fill = header_fill
            cell.border = thin_border
            ws.column_dimensions[get_column_letter(col)].width = max(15, len(headers[col-1]) * 1.2)
        
        # Добавление данных
        for row in rows:
            ws.append(row)
        
        # Применение стилей
        numeric_columns = [8, 9, 10, 11, 12, 15, 18]  # Номера столбцов с числами
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=len(headers)):
            for cell in row:
                cell.border = thin_border
                if cell.column in numeric_columns:
                    cell.number_format = '#,##0.00'
                    cell.alignment = right_alignment
                    if cell.column == 15 and cell.value and cell.value < 0:  # Прибыль
                        cell.fill = negative_fill
                elif cell.column == 2:  # Дата
                    cell.number_format = 'DD.MM.YYYY'
                    cell.alignment = center_alignment
                else:
                    cell.alignment = left_alignment
        
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = ws.dimensions
        
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return {
            "file": buffer.read().hex(),
            "filename": f"Отчет_по_товарам_{date_range.start_date}_по_{date_range.end_date}.xlsx"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()