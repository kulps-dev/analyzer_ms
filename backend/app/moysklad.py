import requests
from datetime import datetime
import logging
from typing import List, Dict, Any
import time

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MoyskladAPI:
    def __init__(self, token: str):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
        self.retry_count = 3
        self.retry_delay = 5  # Увеличиваем задержку между попытками
        self.request_delay = 0.5  # Задержка между запросами (в секундах)

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Обертка для запросов с повторными попытками и задержкой"""
        for attempt in range(self.retry_count):
            try:
                time.sleep(self.request_delay)  # Добавляем задержку перед каждым запросом
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    **kwargs
                )
                
                # Обрабатываем 429 ошибку
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                    
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt == self.retry_count - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
                time.sleep(self.retry_delay * (attempt + 1))
        raise Exception("All retry attempts failed")

    def get_paginated_data(self, url: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Получение данных с пагинацией"""
        all_data = []
        offset = 0
        limit = 1000
        
        while True:
            if params is None:
                params = {}
            
            params.update({
                "offset": offset,
                "limit": limit
            })
            
            response = self._make_request("GET", url, params=params)
            data = response.json()
            
            if "rows" not in data or not data["rows"]:
                break
                
            all_data.extend(data["rows"])
            offset += limit
            
            # Проверяем, есть ли еще данные
            if len(data["rows"]) < limit:
                break
        
        return all_data

    def get_demands(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Получить отгрузки за период с пагинацией"""
        url = f"{self.base_url}/entity/demand"
        
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        params = {
            "filter": filter_str,
            "expand": "agent,store,project,salesChannel"
        }
        
        logger.info(f"Запрос отгрузок за период с {start_date} по {end_date}")
        
        try:
            demands = self.get_paginated_data(url, params)
            logger.info(f"Получено {len(demands)} отгрузок")
            
            # Дополнительно получаем полные данные, если они не были получены через expand
            for demand in demands:
                self._enrich_demand_data(demand)
            
            return demands
        
        except Exception as e:
            logger.error(f"Ошибка при получении отгрузок: {str(e)}")
            raise

    def _enrich_demand_data(self, demand: Dict[str, Any]):
        """Обогащение данных отгрузки"""
        try:
            if "agent" in demand and "name" not in demand["agent"]:
                counterparty_url = demand["agent"]["meta"]["href"]
                counterparty_data = self._make_request("GET", counterparty_url).json()
                demand["agent"]["name"] = counterparty_data.get("name", "")
            
            if "store" in demand and "name" not in demand["store"]:
                store_url = demand["store"]["meta"]["href"]
                store_data = self._make_request("GET", store_url).json()
                demand["store"]["name"] = store_data.get("name", "")
            
            # Обработка проекта
            if "project" in demand and ("name" not in demand["project"] or not demand["project"]["name"]):
                project_url = demand["project"]["meta"]["href"]
                project_data = self._make_request("GET", project_url).json()
                demand["project"]["name"] = project_data.get("name", "Без проекта")
            
            # Обработка канала продаж
            if "salesChannel" in demand and ("name" not in demand["salesChannel"] or not demand["salesChannel"]["name"]):
                sales_channel_url = demand["salesChannel"]["meta"]["href"]
                sales_channel_data = self._make_request("GET", sales_channel_url).json()
                demand["salesChannel"]["name"] = sales_channel_data.get("name", "Без канала")
        
        except Exception as e:
            logger.error(f"Ошибка при обогащении данных отгрузки: {str(e)}")

    def get_demand_cost_price(self, demand_id: str) -> float:
        """Получить себестоимость отгрузки"""
        url = f"{self.base_url}/report/stock/byoperation"
        params = {
            "operation.id": demand_id,
            "limit": 1000
        }
        
        try:
            response = self._make_request("GET", url, params=params)
            data = response.json()
            
            total_cost = 0
            if "rows" in data and len(data["rows"]) > 0:
                for position in data["rows"][0].get("positions", []):
                    cost = position.get("cost", 0)
                    quantity = position.get("quantity", 1)
                    total_cost += cost * quantity
            
            return total_cost / 100  # Переводим в рубли
        
        except Exception as e:
            logger.error(f"Ошибка при получении себестоимости для отгрузки {demand_id}: {str(e)}")
            return 0