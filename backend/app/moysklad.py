import requests
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
import time

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MoyskladAPI:
    def __init__(self, token: str):
        """Инициализация API клиента для работы с МойСклад"""
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
        self.retry_count = 3
        self.retry_delay = 5
        self.request_delay = 0.5

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Обертка для запросов с повторными попытками и задержкой"""
        for attempt in range(self.retry_count):
            try:
                time.sleep(self.request_delay)
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    **kwargs
                )
                
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

    def get_demand_by_id(self, demand_id):
        logger.info(f"Запрос отгрузки {demand_id} с позициями...")
        url = f"{self.base_url}/entity/demand/{demand_id}"
        params = {
            'expand': 'positions,positions.assortment',
            'limit': 1000
        }
        try:
            response = self._make_request("GET", url, params=params)
            data = response.json()
            logger.info(f"Данные отгрузки: {data.get('name')}, позиций: {len(data.get('positions', []))}")
            logger.debug(f"Первая позиция: {data.get('positions', [])[:1]}")
            return data
        except Exception as e:
            logger.error(f"Ошибка получения отгрузки: {str(e)}")
            raise

    def get_paginated_data(self, url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Получение данных с пагинацией"""
        all_data = []
        offset = 0
        limit = params.get("limit", 1000) if params else 1000
        
        while True:
            if params is None:
                params = {}
            
            params.update({"offset": offset, "limit": limit})
            
            try:
                response = self._make_request("GET", url, params=params)
                data = response.json()
                
                if "rows" not in data or not data["rows"]:
                    break
                    
                all_data.extend(data["rows"])
                offset += limit
                time.sleep(0.5)
                
                if len(data["rows"]) < limit:
                    break
                    
            except Exception as e:
                logger.error(f"Ошибка при получении данных: {str(e)}")
                break
        
        return all_data

    def get_demands(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Получить отгрузки за период"""
        url = f"{self.base_url}/entity/demand"
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        params = {
            "filter": filter_str,
            "expand": "agent,store,project,salesChannel",
            "limit": 1000
        }
        
        logger.info(f"Запрос отгрузок с {start_date} по {end_date}")
        
        try:
            demands = self.get_paginated_data(url, params)
            logger.info(f"Получено {len(demands)} отгрузок")
            
            batch_size = 100
            for i in range(0, len(demands), batch_size):
                batch = demands[i:i + batch_size]
                self._enrich_demand_data_batch(batch)
                
                for demand in batch:
                    demand_id = demand.get("id")
                    if demand_id:
                        demand["positions"] = self.get_demand_positions(demand_id)
                
                logger.info(f"Обогащено {min(i + batch_size, len(demands))}/{len(demands)}")
                time.sleep(1)
            
            return demands
        
        except Exception as e:
            logger.error(f"Ошибка при получении отгрузок: {str(e)}")
            raise

    def get_demand_positions(self, demand_id: str) -> List[Dict[str, Any]]:
        """Получить позиции отгрузки"""
        url = f"{self.base_url}/entity/demand/{demand_id}/positions"
        
        try:
            positions = self.get_paginated_data(url)
            logger.info(f"Получено {len(positions)} позиций для отгрузки {demand_id}")
            
            cost_data = self._get_positions_cost_data(demand_id)
            
            for position in positions:
                if "assortment" in position:
                    product_url = position["assortment"]["meta"]["href"]
                    try:
                        response = self._make_request("GET", product_url)
                        product_data = response.json()
                        position.update({
                            "product_name": product_data.get("name", ""),
                            "article": product_data.get("article", ""),
                            "code": product_data.get("code", ""),
                            "cost_price": cost_data.get(product_url.split("/")[-1], 0) / 100
                        })
                    except Exception as e:
                        logger.warning(f"Ошибка при получении данных товара: {str(e)}")
                        position.update({
                            "product_name": "",
                            "article": "",
                            "code": "",
                            "cost_price": 0.0
                        })
                else:
                    position["cost_price"] = 0.0
                
            return positions
            
        except Exception as e:
            logger.error(f"Ошибка при получении позиций: {str(e)}")
            raise

    def _get_positions_cost_data(self, demand_id: str) -> Dict[str, float]:
        """Получить себестоимости позиций"""
        url = f"{self.base_url}/report/stock/byoperation"
        params = {"operation.id": demand_id, "limit": 1000}
        
        try:
            response = self._make_request("GET", url, params=params)
            data = response.json()
            cost_data = {}
            
            if "rows" in data and len(data["rows"]) > 0:
                for position in data["rows"][0].get("positions", []):
                    if "cost" in position:
                        meta_href = position.get("meta", {}).get("href", "")
                        if meta_href:
                            product_id = meta_href.split("/")[-1]
                            cost_data[product_id] = float(position["cost"])
            
            return cost_data
        
        except Exception as e:
            logger.error(f"Ошибка при получении себестоимостей: {str(e)}")
            return {}

    def get_demand_cost_price(self, demand_id: str) -> float:
        """Получить общую себестоимость отгрузки"""
        url = f"{self.base_url}/report/stock/byoperation"
        params = {"operation.id": demand_id, "limit": 1000}
        
        try:
            response = self._make_request("GET", url, params=params)
            data = response.json()
            total_cost = 0.0
            
            if "rows" in data and len(data["rows"]) > 0:
                for position in data["rows"][0].get("positions", []):
                    if "cost" in position:
                        total_cost += float(position["cost"]) / 100
            
            return total_cost
        
        except Exception as e:
            logger.error(f"Ошибка при получении себестоимости: {str(e)}")
            return 0.0

    def _enrich_demand_data_batch(self, demands: List[Dict[str, Any]]):
        """Пакетное обогащение данных отгрузок"""
        try:
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
                if "project" in demand and demand["project"] and not demand["project"].get("name"):
                    urls_to_fetch['projects'].add(demand["project"]["meta"]["href"])
                if "salesChannel" in demand and demand["salesChannel"] and not demand["salesChannel"].get("name"):
                    urls_to_fetch['sales_channels'].add(demand["salesChannel"]["meta"]["href"])

            fetched_data = {k: {} for k in urls_to_fetch}

            for entity_type, urls in urls_to_fetch.items():
                for url in urls:
                    try:
                        response = self._make_request("GET", url)
                        fetched_data[entity_type][url] = response.json().get("name", "")
                        time.sleep(0.1)
                    except Exception as e:
                        logger.warning(f"Ошибка при получении {entity_type}: {str(e)}")
                        fetched_data[entity_type][url] = ""

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
            logger.error(f"Ошибка при обогащении данных: {str(e)}")