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
    """Инициализация базы данных - создание таблицы если она не существует"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Проверяем существование таблицы более безопасным способом
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'demands'
            )
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
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
            conn.commit()
            logger.info("Таблица demands успешно создана")
        else:
            logger.info("Таблица demands уже существует")
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {str(e)}")
        if conn:
            conn.rollback()
        # Не прерываем работу приложения, только логируем ошибку
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
        
        batch_size = 50  # Уменьшаем размер пакета
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
                    
                    # Обновляем статус задачи каждые 100 записей
                    if idx % 100 == 0:
                        logger.info(f"Обработано {idx}/{total_count} записей")
                        tasks_status[task_id] = {
                            "status": "processing",
                            "progress": f"{saved_count}/{total_count}",
                            "message": f"Обработано {idx} из {total_count}"
                        }
                    
                    # Делаем небольшую паузу после каждого пакета
                    time.sleep(0.5)
            
            except Exception as e:
                logger.error(f"Ошибка при обработке отгрузки {demand.get('id')}: {str(e)}")
                continue
        
        # Вставляем оставшиеся записи
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
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Получаем данные для листа "Отчет по отгрузкам"
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
        
        demands_rows = cur.fetchall()
        
        # Получаем данные для листа "Отчет по товарам"
        cur.execute("""
            SELECT 
                d.number, d.date, d.counterparty, d.store, d.project, d.sales_channel,
                p.name AS product_name, p.quantity, p.price / 100 AS price, 
                p.price / 100 * p.quantity AS sum, 
                p.cost_price / 100 AS cost_price,
                p.article, p.code,
                d.overhead / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS overhead_per_product,
                (p.price / 100 * p.quantity) - (p.cost_price / 100 * p.quantity) - 
                (d.overhead / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id)) AS profit_per_product,
                d.promo_period, 
                d.delivery_amount / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS delivery_per_product,
                d.admin_data / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS admin_data_per_product,
                d.gdeslon / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS gdeslon_per_product,
                d.cityads / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS cityads_per_product,
                d.ozon / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS ozon_per_product,
                d.ozon_fbs / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS ozon_fbs_per_product,
                d.yamarket_fbs / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS yamarket_fbs_per_product,
                d.yamarket_dbs / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS yamarket_dbs_per_product,
                d.yandex_direct / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS yandex_direct_per_product,
                d.price_ru / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS price_ru_per_product,
                d.wildberries / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS wildberries_per_product,
                d.gis2 / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS gis2_per_product,
                d.seo / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS seo_per_product,
                d.programmatic / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS programmatic_per_product,
                d.avito / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS avito_per_product,
                d.multiorders / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS multiorders_per_product,
                d.estimated_discount / (SELECT COUNT(*) FROM demand_positions WHERE demand_id = d.id) AS estimated_discount_per_product
            FROM demands d
            JOIN demand_positions p ON d.id = p.demand_id
            WHERE d.date BETWEEN %s AND %s
            ORDER BY d.date DESC, d.number
        """, (date_range.start_date, date_range.end_date))
        
        products_rows = cur.fetchall()
        
        wb = Workbook()
        
        # Стили для оформления (как в оригинальном коде)
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
        
        # Лист "Отчет по отгрузкам" (как в оригинальном коде)
        ws_demands = wb.active
        ws_demands.title = "Отчет по отгрузкам"
        demands_headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
            "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
            "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
            "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
            "Примерная скидка"
        ]
        ws_demands.append(demands_headers)
        
        # Форматирование листа "Отчет по отгрузкам" (как в оригинальном коде)
        for col in range(1, len(demands_headers) + 1):
            cell = ws_demands.cell(row=1, column=col)
            cell.font = header_font
            cell.alignment = center_alignment
            cell.fill = header_fill
            cell.border = thin_border
            ws_demands.column_dimensions[get_column_letter(col)].width = max(15, len(demands_headers[col-1]) * 1.2)
        
        numeric_columns = [7, 8, 9, 10, 12] + list(range(13, 29))
        profit_column = 10
        
        for row_idx, row in enumerate(demands_rows, start=2):
            for col_idx, value in enumerate(row, start=1):
                cell = ws_demands.cell(row=row_idx, column=col_idx, value=value)
                cell.font = cell_font
                cell.border = thin_border
                
                if col_idx in numeric_columns:
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
                elif col_idx == 2:
                    cell.number_format = 'DD.MM.YYYY'
                    cell.alignment = center_alignment
                else:
                    cell.alignment = left_alignment
        
        ws_demands.freeze_panes = 'A2'
        ws_demands.auto_filter.ref = ws_demands.dimensions
        
        # Добавляем итоговую строку для листа "Отчет по отгрузкам"
        last_row = len(demands_rows) + 1
        ws_demands.append([""] * len(demands_headers))
        total_row = last_row + 1
        
        for col in range(1, len(demands_headers) + 1):
            cell = ws_demands.cell(row=total_row, column=col)
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
        
        # Лист "Отчет по товарам"
        ws_products = wb.create_sheet(title="Отчет по товарам")
        
        products_headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Артикул", "Код",
            "Накладные расходы", "Прибыль", "Акционный период", "Сумма доставки",
            "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS", "Яндекс Маркет FBS",
            "Яндекс Маркет DBS", "Яндекс Директ", "Price ru", "Wildberries", "2Gis",
            "SEO", "Программатик", "Авито", "Мультиканальные заказы", "Примерная скидка"
        ]
        
        ws_products.append(products_headers)
        
        # Форматирование заголовков листа "Отчет по товарам"
        for col in range(1, len(products_headers) + 1):
            cell = ws_products.cell(row=1, column=col)
            cell.font = header_font
            cell.alignment = center_alignment
            cell.fill = header_fill
            cell.border = thin_border
            ws_products.column_dimensions[get_column_letter(col)].width = max(15, len(products_headers[col-1]) * 1.2)
        
        # Определяем числовые столбцы для листа "Отчет по товарам"
        products_numeric_columns = [8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
        products_profit_column = 15
        
        # Добавляем данные в лист "Отчет по товарам"
        for row_idx, row in enumerate(products_rows, start=2):
            for col_idx, value in enumerate(row, start=1):
                cell = ws_products.cell(row=row_idx, column=col_idx, value=value)
                cell.font = cell_font
                cell.border = thin_border
                
                if col_idx in products_numeric_columns:
                    try:
                        num_value = float(value) if value not in [None, ''] else 0.0
                        cell.value = num_value
                        cell.number_format = '#,##0.00'
                        cell.alignment = right_alignment
                        
                        if col_idx == products_profit_column and num_value < 0:
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
        
        ws_products.freeze_panes = 'A2'
        ws_products.auto_filter.ref = ws_products.dimensions
        
        # Добавляем итоговую строку для листа "Отчет по товарам"
        products_last_row = len(products_rows) + 1
        ws_products.append([""] * len(products_headers))
        products_total_row = products_last_row + 1
        
        for col in range(1, len(products_headers) + 1):
            cell = ws_products.cell(row=products_total_row, column=col)
            cell.font = Font(bold=True)
            cell.border = thin_border
            
            if col in products_numeric_columns:
                start_col = get_column_letter(col)
                formula = f"SUM({start_col}2:{start_col}{products_last_row})"
                cell.value = f"=ROUND({formula}, 2)"
                cell.number_format = '#,##0.00'
                cell.alignment = right_alignment
                cell.fill = PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid')
            elif col == 1:
                cell.value = "Итого:"
                cell.alignment = right_alignment
        
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