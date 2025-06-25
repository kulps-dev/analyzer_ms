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
        """Получение данных с пагинацией и ограничением скорости"""
        all_data = []
        offset = 0
        limit = params.get("limit", 1000) if params else 1000
        
        while True:
            if params is None:
                params = {}
            
            params.update({
                "offset": offset,
                "limit": limit
            })
            
            try:
                response = self._make_request("GET", url, params=params)
                data = response.json()
                
                if "rows" not in data or not data["rows"]:
                    break
                    
                all_data.extend(data["rows"])
                offset += limit
                
                # Делаем паузу после каждого запроса
                time.sleep(0.5)
                
                # Проверяем, есть ли еще данные
                if len(data["rows"]) < limit:
                    break
                    
            except Exception as e:
                logger.error(f"Ошибка при получении данных: {str(e)}")
                break
        
        return all_data

    def get_demands(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Получить отгрузки за период с пагинацией и позициями"""
        url = f"{self.base_url}/entity/demand"
        
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        params = {
            "filter": filter_str,
            "expand": "agent,store,project,salesChannel",
            "limit": 1000
        }
        
        logger.info(f"Запрос отгрузок за период с {start_date} по {end_date}")
        
        try:
            demands = self.get_paginated_data(url, params)
            logger.info(f"Получено {len(demands)} отгрузок")
            
            # Разбиваем на пакеты для обогащения
            batch_size = 100
            for i in range(0, len(demands), batch_size):
                batch = demands[i:i + batch_size]
                self._enrich_demand_data_batch(batch)
                
                # Получаем позиции для каждой отгрузки в пакете
                for demand in batch:
                    demand_id = demand.get("id")
                    if demand_id:
                        demand["positions"] = self.get_demand_positions(demand_id)
                
                logger.info(f"Обогащено {min(i + batch_size, len(demands))}/{len(demands)} отгрузок")
                time.sleep(1)  # Пауза между пакетами
            
            return demands
        
        except Exception as e:
            logger.error(f"Ошибка при получении отгрузок: {str(e)}")
            raise

    def get_demand_positions(self, demand_id: str) -> List[Dict[str, Any]]:
        """Получить позиции отгрузки с обогащенными данными о товарах"""
        url = f"{self.base_url}/entity/demand/{demand_id}/positions"
        
        try:
            positions = self.get_paginated_data(url)
            logger.info(f"Получено {len(positions)} позиций для отгрузки {demand_id}")
            
            # Обогащаем данные о товарах
            for position in positions:
                if "assortment" in position:
                    product_url = position["assortment"]["meta"]["href"]
                    try:
                        response = self._make_request("GET", product_url)
                        product_data = response.json()
                        position["product_name"] = product_data.get("name", "")
                        position["article"] = product_data.get("article", "")
                        position["code"] = product_data.get("code", "")
                    except Exception as e:
                        logger.warning(f"Ошибка при получении данных товара: {str(e)}")
                        position["product_name"] = ""
                        position["article"] = ""
                        position["code"] = ""
            
            return positions
        
        except Exception as e:
            logger.error(f"Ошибка при получении позиций отгрузки {demand_id}: {str(e)}")
            raise

    def get_position_cost_price(self, position: Dict[str, Any]) -> float:
        """Получить себестоимость позиции"""
        try:
            # Если в позиции есть информация о себестоимости
            if "cost" in position:
                return float(position.get("cost", 0)) / 100
            
            # Если нет, делаем запрос к API
            if "assortment" in position:
                product_url = position["assortment"]["meta"]["href"]
                response = self._make_request("GET", product_url)
                product_data = response.json()
                return float(product_data.get("costPrice", {}).get("value", 0)) / 100
            
            return 0
        except Exception as e:
            logger.error(f"Ошибка при получении себестоимости позиции: {str(e)}")
            return 0

    def get_demand_cost_price(self, demand_id: str) -> float:
        """Получить себестоимость отгрузки (сумма себестоимостей позиций)"""
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
                    total_cost += cost
            
            return total_cost / 100  # Переводим в рубли
        
        except Exception as e:
            logger.error(f"Ошибка при получении себестоимости для отгрузки {demand_id}: {str(e)}")
            return 0
            
    def get_demand_cost_data(self, demand_id: str) -> Dict[str, Any]:
    """Получить данные о себестоимости позиций отгрузки"""
    url = f"{self.base_url}/report/stock/byoperation"
    params = {
        "operation.id": demand_id,
        "limit": 1000
    }
    
    try:
        response = self._make_request("GET", url, params=params)
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка при получении данных о себестоимости для отгрузки {demand_id}: {str(e)}")
        return {"rows": []}

    def _enrich_demand_data_batch(self, demands: List[Dict[str, Any]]):
        """Пакетное обогащение данных отгрузок"""
        try:
            # Собираем все URL для пакетной обработки
            urls_to_fetch = {
                'agents': set(),
                'stores': set(),
                'projects': set(),
                'sales_channels': set()
            }

            for demand in demands:
                if "agent" in demand and "name" not in demand["agent"]:
                    urls_to_fetch['agents'].add(demand["agent"]["meta"]["href"])
                if "store" in demand and "name" not in demand["store"]:
                    urls_to_fetch['stores'].add(demand["store"]["meta"]["href"])
                if "project" in demand and demand["project"] and ("name" not in demand["project"] or not demand["project"]["name"]):
                    urls_to_fetch['projects'].add(demand["project"]["meta"]["href"])
                if "salesChannel" in demand and demand["salesChannel"] and ("name" not in demand["salesChannel"] or not demand["salesChannel"]["name"]):
                    urls_to_fetch['sales_channels'].add(demand["salesChannel"]["meta"]["href"])

            # Получаем все данные одним запросом для каждого типа
            fetched_data = {
                'agents': {},
                'stores': {},
                'projects': {},
                'sales_channels': {}
            }

            for entity_type, urls in urls_to_fetch.items():
                if urls:
                    for url in urls:
                        try:
                            response = self._make_request("GET", url)
                            data = response.json()
                            fetched_data[entity_type][url] = data.get("name", "")
                            time.sleep(0.1)  # Небольшая задержка между запросами
                        except Exception as e:
                            logger.warning(f"Ошибка при получении {entity_type} {url}: {str(e)}")
                            fetched_data[entity_type][url] = ""

            # Применяем полученные данные ко всем отгрузкам
            for demand in demands:
                if "agent" in demand and "name" not in demand["agent"]:
                    demand["agent"]["name"] = fetched_data['agents'].get(demand["agent"]["meta"]["href"], "")
                if "store" in demand and "name" not in demand["store"]:
                    demand["store"]["name"] = fetched_data['stores'].get(demand["store"]["meta"]["href"], "")
                if "project" in demand and demand["project"]:
                    demand["project"]["name"] = fetched_data['projects'].get(demand["project"]["meta"]["href"], "Без проекта")
                if "salesChannel" in demand and demand["salesChannel"]:
                    demand["salesChannel"]["name"] = fetched_data['sales_channels'].get(demand["salesChannel"]["meta"]["href"], "Без канала")

        except Exception as e:
            logger.error(f"Ошибка при пакетном обогащении данных: {str(e)}")