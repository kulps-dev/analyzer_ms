import time
import io
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import execute_batch
from openpyxl import Workbook
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
    "host": "87.228.99.200",
    "port": 5432,
    "dbname": "MS",
    "user": "louella",
    "password": "XBcMJoEO1ljb",
    "sslmode": "verify-ca",
    "sslrootcert": "/root/.postgresql/root.crt"
}

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

# Вспомогательные функции
def get_db_connection():
    """Создает соединение с базой данных"""
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """Инициализация базы данных - создание таблиц если они не существуют"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Таблица demands
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
        
        # Таблица demand_items
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

# Эндпоинты
@app.on_event("startup")
async def startup_event():
    """Действия при старте приложения"""
    init_db()
    logger.info("Приложение запущено, база данных инициализирована")

@app.post("/api/save-to-db", response_model=BatchProcessResponse)
async def save_to_db(date_range: DateRange, background_tasks: BackgroundTasks):
    """Запуск фоновой задачи для обработки данных"""
    try:
        task_id = str(uuid.uuid4())
        tasks_status[task_id] = {
            "status": "pending",
            "progress": "0/0",
            "message": "Задача поставлена в очередь"
        }
        
        # Здесь должен быть вызов API МойСклад для получения данных
        # Временно используем заглушку
        background_tasks.add_task(mock_process_data_task, date_range, task_id)
        
        return {
            "task_id": task_id,
            "status": "started",
            "message": "Обработка данных начата. Используйте task_id для проверки статуса."
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
    """Экспорт данных в Excel файл"""
    try:
        # Создаем новую книгу Excel
        wb = Workbook()
        
        # Удаляем лист по умолчанию
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]
        
        # Добавляем данные
        add_demands_sheet(wb, date_range)
        add_items_sheet(wb, date_range)
        
        # Сохраняем в буфер
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        # Формируем имя файла
        filename = f"report_{date_range.start_date}_to_{date_range.end_date}.xlsx"
        
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/export/excel/items")
async def export_excel_items(date_range: DateRange):
    """Экспорт данных по товарам в Excel файл"""
    try:
        # Создаем новую книгу Excel
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет по товарам"
        
        # Добавляем данные
        add_items_data(ws, date_range)
        
        # Сохраняем в буфер
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=report_items.xlsx"}
        )
    
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Вспомогательные функции для работы с Excel
def add_demands_sheet(wb: Workbook, date_range: DateRange):
    """Добавляет лист с отгрузками в книгу Excel"""
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
        
        ws = wb.create_sheet("Отчет по отгрузкам")
        
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
            "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
            "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
            "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
            "Примерная скидка"
        ]
        
        apply_excel_styles(ws, headers, rows, numeric_columns=[6, 7, 8, 9, 11] + list(range(12, 28)), profit_column=9)
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных отгрузок: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def add_items_sheet(wb: Workbook, date_range: DateRange):
    """Добавляет лист с товарами в книгу Excel"""
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
        
        ws = wb.create_sheet("Отчет по товарам")
        
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Артикул", "Код",
            "Накладные расходы", "Прибыль", "Акционный период", "Сумма доставки",
            "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS", "Яндекс Маркет FBS",
            "Яндекс Маркет DBS", "Яндекс Директ", "Price ru", "Wildberries", "2Gis", "SEO",
            "Программатик", "Авито", "Мультиканальные заказы", "Примеренная скидка"
        ]
        
        apply_excel_styles(ws, headers, rows, numeric_columns=[7, 8, 9, 10, 13, 14, 16] + list(range(17, 32)), profit_column=14)
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных товаров: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def add_items_data(ws, date_range: DateRange):
    """Добавляет данные по товарам в указанный лист"""
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
        
        headers = [
            "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
            "Товар", "Количество", "Цена", "Сумма", "Себестоимость", "Артикул", "Код",
            "Накладные расходы", "Прибыль", "Акционный период", "Сумма доставки",
            "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS", "Яндекс Маркет FBS",
            "Яндекс Маркет DBS", "Яндекс Директ", "Price ru", "Wildberries", "2Gis", "SEO",
            "Программатик", "Авито", "Мультиканальные заказы", "Примеренная скидка"
        ]
        
        apply_excel_styles(ws, headers, rows, numeric_columns=[7, 8, 9, 10, 13, 14, 16] + list(range(17, 32)), profit_column=14)
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных товаров: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def apply_excel_styles(ws, headers, rows, numeric_columns, profit_column):
    """Применяет стили к листу Excel"""
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
    
    # Добавляем данные
    for row_idx, row in enumerate(rows, start=2):
        for col_idx, value in enumerate(row, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.font = cell_font
            cell.border = thin_border
            
            if col_idx in numeric_columns:
                try:
                    num_value = float(value) if value is not None else 0.0
                    cell.value = num_value
                    cell.number_format = '#,##0.00'
                    cell.alignment = right_alignment
                    
                    if col_idx == profit_column and num_value < 0:
                        cell.fill = negative_fill
                    elif row_idx % 2 == 0:
                        cell.fill = money_fill
                except (ValueError, TypeError):
                    cell.alignment = left_alignment
            elif col_idx == 2:  # Дата
                if value:
                    try:
                        cell.value = value.strftime('%Y-%m-%d') if isinstance(value, datetime) else value
                        cell.number_format = 'DD.MM.YYYY'
                    except:
                        pass
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
        elif col == 1:
            cell.value = "Итого:"
            cell.alignment = right_alignment

# Заглушка для тестирования (замените на реальную интеграцию с МойСклад)
async def mock_process_data_task(date_range: DateRange, task_id: str):
    """Заглушка для тестирования обработки данных"""
    try:
        tasks_status[task_id] = {
            "status": "fetching",
            "progress": "0/0",
            "message": "Получение данных из МойСклад..."
        }
        
        # Имитация задержки
        await asyncio.sleep(2)
        
        # Имитация обработки данных
        tasks_status[task_id] = {
            "status": "processing",
            "progress": "50/100",
            "message": "Обработка данных..."
        }
        
        await asyncio.sleep(3)
        
        tasks_status[task_id] = {
            "status": "completed",
            "progress": "100/100",
            "message": "Обработка завершена успешно"
        }
    
    except Exception as e:
        tasks_status[task_id] = {
            "status": "failed",
            "progress": "0/0",
            "message": f"Ошибка: {str(e)}"
        }