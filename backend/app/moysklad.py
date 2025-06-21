import requests
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MoyskladAPI:
    def __init__(self, token: str):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
        self.timeout = 30  # секунды
        self.max_workers = 5  # для параллельных запросов

    def _make_request(self, url: str, params: Optional[dict] = None) -> Optional[dict]:
        """Базовый метод для выполнения запросов с обработкой ошибок"""
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout при запросе к {url}")
        except Exception as e:
            logger.error(f"Ошибка при запросе к {url}: {str(e)}")
        return None

    def get_entity_name(self, entity_url: str, default_name: str = "Не указано") -> str:
        """Получение имени связанной сущности (контрагент, склад и т.д.)"""
        if not entity_url:
            return default_name
        
        data = self._make_request(entity_url)
        return str(data.get("name", default_name)) if data else default_name

    def get_demand_cost_price(self, demand_id: str) -> float:
        """Получение себестоимости отгрузки с кешированием"""
        url = f"{self.base_url}/report/stock/byoperation"
        params = {"operation.id": demand_id, "limit": 1000}
        
        data = self._make_request(url, params)
        if not data or "rows" not in data:
            return 0.0
        
        total_cost = 0.0
        for position in data["rows"][0].get("positions", []):
            cost = float(position.get("cost", 0))
            quantity = float(position.get("quantity", 1))
            total_cost += cost * quantity
        
        return total_cost / 100  # Переводим в рубли

    def get_demands_batch(self, start_date: str, end_date: str, offset: int, limit: int = 100) -> List[dict]:
        """Получение порции отгрузок"""
        url = f"{self.base_url}/entity/demand"
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        params = {
            "filter": filter_str,
            "limit": limit,
            "offset": offset,
            "expand": "agent,store,project,salesChannel,attributes"
        }
        
        data = self._make_request(url, params)
        return data.get("rows", []) if data else []

    def get_all_demands(self, start_date: str, end_date: str) -> List[dict]:
        """Получение всех отгрузок за период с пагинацией"""
        all_demands = []
        limit = 100
        offset = 0
        total_processed = 0
        
        while True:
            demands = self.get_demands_batch(start_date, end_date, offset, limit)
            if not demands:
                break
                
            all_demands.extend(demands)
            offset += limit
            total_processed += len(demands)
            
            logger.info(f"Получено {len(demands)} записей (всего {total_processed})")
            
            # Защита от бесконечного цикла
            if len(demands) < limit or total_processed >= 100000:
                break
        
        logger.info(f"Всего получено отгрузок: {len(all_demands)}")
        return all_demands

    def enrich_demand_data(self, demand: dict) -> dict:
        """Обогащение данных отгрузки дополнительной информацией"""
        if "agent" in demand and "name" not in demand["agent"]:
            demand["agent"]["name"] = self.get_entity_name(
                demand["agent"]["meta"]["href"],
                "Неизвестный контрагент"
            )
        
        if "store" in demand and "name" not in demand["store"]:
            demand["store"]["name"] = self.get_entity_name(
                demand["store"]["meta"]["href"],
                "Неизвестный склад"
            )
        
        if "project" in demand:
            demand["project"]["name"] = self.get_entity_name(
                demand["project"]["meta"]["href"],
                "Без проекта"
            )
        
        if "salesChannel" in demand:
            demand["salesChannel"]["name"] = self.get_entity_name(
                demand["salesChannel"]["meta"]["href"],
                "Без канала"
            )
        
        return demand

    def get_enriched_demands(self, start_date: str, end_date: str) -> List[dict]:
        """Получение и обогащение всех отгрузок"""
        demands = self.get_all_demands(start_date, end_date)
        
        # Параллельное обогащение данных
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self.enrich_demand_data, demand)
                for demand in demands
            ]
            
            return [future.result() for future in as_completed(futures)]