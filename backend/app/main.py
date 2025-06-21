from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import logging
from typing import List, Dict
from moysklad import MoyskladAPI
import os
from openpyxl import Workbook
import io

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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

# Конфигурация БД
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

class ImportStatus(BaseModel):
    status: str
    processed: int
    total: int
    message: str

# Глобальные переменные для статуса импорта
import_status = {
    "running": False,
    "processed": 0,
    "total": 0,
    "message": ""
}

def get_db_connection():
    """Получение соединения с БД"""
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """Инициализация таблицы в базе данных"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
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
        logger.error(f"Ошибка при инициализации БД: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def prepare_demand_data(demand: Dict) -> Dict:
    """Подготовка данных отгрузки для вставки в БД"""
    attributes = demand.get("attributes", [])
    attr_dict = {attr.get("name"): attr.get("value") for attr in attributes}
    
    # Обработка накладных расходов
    overhead_data = demand.get("overhead", {})
    overhead_sum = float(overhead_data.get("sum", 0)) / 100
    
    # Основные данные
    return {
        "id": str(demand.get("id", ""))[:255],
        "number": str(demand.get("name", ""))[:50],
        "date": demand.get("moment", None),
        "counterparty": str(demand.get("agent", {}).get("name", ""))[:255],
        "store": str(demand.get("store", {}).get("name", ""))[:255],
        "project": str(demand.get("project", {}).get("name", "Без проекта"))[:255],
        "sales_channel": str(demand.get("salesChannel", {}).get("name", "Без канала"))[:255],
        "amount": float(demand.get("sum", 0)) / 100,
        "cost_price": float(demand.get("cost_price", 0)),
        "overhead": overhead_sum,
        "profit": (float(demand.get("sum", 0)) / 100) - float(demand.get("cost_price", 0)) - overhead_sum,
        "promo_period": str(attr_dict.get("Акционный период", ""))[:100],
        "delivery_amount": float(attr_dict.get("Сумма доставки", 0)),
        "admin_data": str(attr_dict.get("Адмидат", ""))[:255],
        "gdeslon": str(attr_dict.get("ГдеСлон", ""))[:255],
        # ... остальные атрибуты ...
        "status": str(demand.get("state", {}).get("name", ""))[:100],
        "comment": str(demand.get("description", ""))[:255]
    }

def save_demands_batch(demands: List[Dict], batch_size: int = 500) -> int:
    """Сохранение порции данных в БД"""
    conn = None
    saved_count = 0
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Подготовка данных
        prepared_data = [prepare_demand_data(demand) for demand in demands]
        
        # SQL для вставки
        insert_sql = """
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
        
        # Выполняем батч-вставку
        execute_batch(cur, insert_sql, prepared_data, page_size=batch_size)
        conn.commit()
        saved_count = len(prepared_data)
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении батча: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    
    return saved_count

@app.on_event("startup")
async def startup_event():
    """Инициализация при старте приложения"""
    init_db()
    logger.info("Приложение запущено, БД инициализирована")

@app.post("/api/start-import")
async def start_import(date_range: DateRange, background_tasks: BackgroundTasks):
    """Запуск импорта в фоновом режиме"""
    global import_status
    
    if import_status["running"]:
        raise HTTPException(status_code=400, detail="Импорт уже выполняется")
    
    import_status = {
        "running": True,
        "processed": 0,
        "total": 0,
        "message": "Импорт начат"
    }
    
    background_tasks.add_task(run_import, date_range)
    return {"message": "Импорт начат в фоновом режиме"}

@app.get("/api/import-status")
async def get_import_status():
    """Получение статуса импорта"""
    return {
        "status": "running" if import_status["running"] else "idle",
        "processed": import_status["processed"],
        "total": import_status["total"],
        "message": import_status["message"]
    }

def run_import(date_range: DateRange):
    """Функция для фонового выполнения импорта"""
    global import_status
    
    try:
        logger.info(f"Начало импорта данных за период {date_range.start_date} - {date_range.end_date}")
        
        # Получаем все отгрузки
        demands = moysklad.get_enriched_demands(date_range.start_date, date_range.end_date)
        if not demands:
            import_status.update({
                "running": False,
                "message": "Нет данных для импорта"
            })
            return
        
        import_status["total"] = len(demands)
        
        # Разбиваем на батчи и сохраняем
        batch_size = 500
        total_saved = 0
        
        for i in range(0, len(demands), batch_size):
            batch = demands[i:i + batch_size]
            saved = save_demands_batch(batch, batch_size)
            total_saved += saved
            
            import_status.update({
                "processed": i + len(batch),
                "message": f"Обработано {i + len(batch)} из {len(demands)}"
            })
            
            logger.info(f"Сохранено {saved} записей (всего {total_saved})")
        
        import_status.update({
            "running": False,
            "message": f"Импорт завершен. Всего сохранено {total_saved} записей"
        })
        
    except Exception as e:
        logger.error(f"Ошибка при импорте: {str(e)}", exc_info=True)
        import_status.update({
            "running": False,
            "message": f"Ошибка импорта: {str(e)}"
        })

@app.post("/api/export/excel")
async def export_excel(date_range: DateRange):
    """Экспорт данных в Excel"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
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
            
            # Создание Excel файла
            wb = Workbook()
            ws = wb.active
            ws.title = "Отчет по отгрузкам"
            
            # Заголовки
            headers = [
                "Номер отгрузки", "Дата", "Контрагент", "Склад", "Проект", "Канал продаж",
                "Сумма", "Себестоимость", "Накладные расходы", "Прибыль", "Акционный период",
                "Сумма доставки", "Адмидат", "ГдеСлон", "CityAds", "Ozon", "Ozon FBS",
                "Яндекс Маркет FBS", "Яндекс Маркет DBS", "Яндекс Директ", "Price ru",
                "Wildberries", "2Gis", "SEO", "Программатик", "Авито", "Мультиканальные заказы",
                "Примеренная скидка"
            ]
            
            ws.append(headers)
            
            # Данные
            for row in rows:
                ws.append(row)
            
            # Сохранение в буфер
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            
            return {
                "file": buffer.read().hex(),
                "filename": f"Отчет_по_отгрузкам_{date_range.start_date}_по_{date_range.end_date}.xlsx"
            }
            
    except Exception as e:
        logger.error(f"Ошибка при экспорте в Excel: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()