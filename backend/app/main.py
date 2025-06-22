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
from concurrent.futures import ThreadPoolExecutor

app = FastAPI()

# Настройки базы данных
DB_CONFIG = {
    "host": "87.228.99.200",
    "port": 5432,
    "dbname": "MS",
    "user": "louella",
    "password": "XBcMJoEO1ljb",
    "sslmode": "verify-ca",
    "sslrootcert": "/root/.postgresql/root.crt",
    "connect_timeout": 10,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5
}

# Пул потоков для выполнения блокирующих операций
executor = ThreadPoolExecutor(max_workers=10)

# Инициализация API МойСклад
moysklad = MoyskladAPI(token="2e61e26f0613cf33fab5f31cf105302aa2d607c3")

class DateRange(BaseModel):
    start_date: str
    end_date: str

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

async def init_db():
    """Асинхронная инициализация таблицы в базе данных"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, _sync_init_db)

def _sync_init_db():
    """Синхронная инициализация БД"""
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = True  # Включаем autocommit для DDL операций
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
        
        # Создаем индексы для ускорения запросов
        cur.execute("CREATE INDEX IF NOT EXISTS idx_demands_date ON demands(date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_demands_counterparty ON demands(counterparty)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_demands_project ON demands(project)")
        
    except Exception as e:
        print(f"Ошибка при инициализации БД: {e}")
    finally:
        if conn:
            conn.close()

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.post("/api/save-to-db")
async def save_to_db(date_range: DateRange, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_data_in_background, date_range)
    return {"message": "Запущен процесс загрузки данных. Это может занять некоторое время."}

def process_data_in_background(date_range: DateRange):
    """Фоновая обработка данных"""
    try:
        demands = moysklad.get_demands(date_range.start_date, date_range.end_date)
        if not demands:
            return

        # Разбиваем данные на пакеты по 1000 записей
        batch_size = 1000
        batches = [demands[i:i + batch_size] for i in range(0, len(demands), batch_size)]

        for batch in batches:
            process_batch(batch)
            
    except Exception as e:
        print(f"Критическая ошибка при фоновой обработке: {str(e)}")

def process_batch(batch):
    """Обработка пакета данных"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Подготавливаем данные для batch-вставки
        values_list = []
        for demand in batch:
            try:
                values = prepare_demand_data(demand)
                values_list.append(tuple(values.values()))
            except Exception as e:
                print(f"Ошибка при подготовке данных отгрузки {demand.get('id')}: {str(e)}")
                continue
        
        if not values_list:
            return

        # Используем execute_batch для пакетной вставки
        execute_batch(cur, """
            INSERT INTO demands (
                id, number, date, counterparty, store, project, sales_channel, 
                amount, cost_price, overhead, profit, promo_period, delivery_amount,
                admin_data, gdeslon, cityads, ozon, ozon_fbs, yamarket_fbs,
                yamarket_dbs, yandex_direct, price_ru, wildberries, gis2, seo,
                programmatic, avito, multiorders, estimated_discount, status, comment
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
        """, values_list)
        
        conn.commit()
        print(f"Успешно обработан пакет из {len(values_list)} записей")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Ошибка при обработке пакета: {str(e)}")
    finally:
        if conn:
            conn.close()

def prepare_demand_data(demand):
    """Подготовка данных одной отгрузки"""
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

@app.post("/api/export/excel")
async def export_excel(date_range: DateRange):
    """Экспорт данных в Excel с пагинацией"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, _sync_export_excel, date_range)

def _sync_export_excel(date_range: DateRange):
    """Синхронная функция экспорта в Excel"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(name='server_side_cursor', withhold=True)
        
        # Используем серверный курсор для обработки больших данных
        cur.itersize = 1000  # Количество строк, получаемых за один раз
        
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
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет по отгрузкам"
        
        # Заголовки столбцов
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
            "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
            "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
            "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
            "Примеренная скидка"
        ]
        
        # Добавляем заголовки
        ws.append(headers)
        
        # Обработка данных с пагинацией
        row_count = 0
        while True:
            batch = cur.fetchmany(1000)
            if not batch:
                break
                
            for row in batch:
                ws.append(row)
                row_count += 1
                
                # Лимит на 1 миллион строк (ограничение Excel)
                if row_count >= 1000000:
                    break
            
            if row_count >= 1000000:
                break
        
        # Стили для оформления
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
        
        # Определяем числовые столбцы (нумерация с 1)
        numeric_columns = [7, 8, 9, 10, 12] + list(range(13, 29))  # 7-12 и 13-28 (включительно)
        
        # Добавляем данные и форматируем их
        for row_idx, row in enumerate(rows, start=2):
            for col_idx, value in enumerate(row, start=1):
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                cell.font = cell_font
                cell.border = thin_border
                
                # Форматирование чисел и дат
                if col_idx in numeric_columns:  # Все числовые столбцы
                    try:
                        # Преобразуем значение в число, если возможно
                        num_value = float(value) if value not in [None, ''] else 0.0
                        cell.value = num_value
                        cell.number_format = '#,##0.00'
                        cell.alignment = right_alignment
                        if row_idx % 2 == 0:  # Зебра для читаемости
                            cell.fill = money_fill
                    except (ValueError, TypeError):
                        # Если не удалось преобразовать в число, оставляем как есть
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