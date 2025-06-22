import requests
import io
from openpyxl import Workbook
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Optional, Union
import logging

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('moysklad')

class MoyskladAPI:
    def __init__(self, token: str):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
        
        # Настройка сессии с повторными попытками и пулом соединений
        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=100,
            pool_maxsize=100,
            pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Общий метод для выполнения запросов"""
        try:
            response = self.session.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed to {url}: {str(e)}")
            raise

    def get_counterparty(self, counterparty_url: str) -> Dict:
        """Получить информацию о контрагенте по URL"""
        return self._make_request(counterparty_url)

    def get_store(self, store_url: str) -> Dict:
        """Получить информацию о складе по URL"""
        return self._make_request(store_url)

    def get_project(self, project_url: str) -> Dict:
        """Получить информацию о проекте по URL"""
        return self._make_request(project_url)

    def get_sales_channel(self, sales_channel_url: str) -> Dict:
        """Получить информацию о канале продаж по URL"""
        return self._make_request(sales_channel_url)

    def get_demand_cost_price(self, demand_id: str) -> float:
        """Получить себестоимость отгрузки"""
        url = f"{self.base_url}/report/stock/byoperation"
        params = {
            "operation.id": demand_id,
            "limit": 1000
        }
        
        try:
            data = self._make_request(url, params=params)
            
            total_cost = 0.0
            if "rows" in data and len(data["rows"]) > 0:
                for position in data["rows"][0].get("positions", []):
                    cost = position.get("cost", 0)
                    quantity = position.get("quantity", 1)
                    total_cost += cost * quantity
            
            return total_cost / 100  # Переводим в рубли
        
        except Exception as e:
            logger.error(f"Ошибка при получении себестоимости для отгрузки {demand_id}: {str(e)}")
            return 0.0

    def get_demands(self, start_date: str, end_date: str) -> List[Dict]:
        """Получить отгрузки за период с пагинацией"""
        url = f"{self.base_url}/entity/demand"
        
        # Нормализация дат
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        all_demands = []
        offset = 0
        limit = 1000  # Максимальный лимит API
        
        logger.info(f"Начало загрузки отгрузок с {start_date} по {end_date}")
        
        while True:
            params = {
                "filter": filter_str,
                "limit": limit,
                "offset": offset,
                "expand": "agent,store,project,salesChannel"
            }
            
            try:
                logger.debug(f"Запрос отгрузок: offset={offset}, limit={limit}")
                data = self._make_request(url, params=params)
                
                demands = data.get("rows", [])
                if not demands:
                    logger.debug("Нет больше данных для загрузки")
                    break
                    
                all_demands.extend(demands)
                offset += len(demands)
                
                logger.info(f"Загружено {len(all_demands)} отгрузок...")
                
                # Проверяем, есть ли еще данные
                if len(demands) < limit:
                    logger.debug("Получено меньше данных, чем лимит - конец пагинации")
                    break
                    
            except Exception as e:
                logger.error(f"Ошибка при получении отгрузок (offset {offset}): {str(e)}")
                break
        
        # Дополнительная обработка данных
        logger.info(f"Всего загружено {len(all_demands)} отгрузок. Начинаем обработку...")
        
        for i, demand in enumerate(all_demands):
            try:
                # Обработка контрагента
                if "agent" in demand and "name" not in demand["agent"]:
                    try:
                        counterparty_url = demand["agent"]["meta"]["href"]
                        counterparty_data = self.get_counterparty(counterparty_url)
                        demand["agent"]["name"] = counterparty_data.get("name", "")
                    except Exception as e:
                        logger.warning(f"Ошибка при получении контрагента: {e}")
                        demand["agent"]["name"] = "Не удалось получить"
                
                # Обработка склада
                if "store" in demand and "name" not in demand["store"]:
                    try:
                        store_url = demand["store"]["meta"]["href"]
                        store_data = self.get_store(store_url)
                        demand["store"]["name"] = store_data.get("name", "")
                    except Exception as e:
                        logger.warning(f"Ошибка при получении склада: {e}")
                        demand["store"]["name"] = "Не удалось получить"
                
                # Обработка проекта
                if "project" in demand and ("name" not in demand["project"] or not demand["project"]["name"]):
                    try:
                        project_url = demand["project"]["meta"]["href"]
                        project_data = self.get_project(project_url)
                        demand["project"]["name"] = project_data.get("name", "Без проекта")
                    except Exception as e:
                        logger.warning(f"Ошибка при получении проекта: {e}")
                        demand["project"] = {"name": "Без проекта"}
                
                # Обработка канала продаж
                if "salesChannel" in demand and ("name" not in demand["salesChannel"] or not demand["salesChannel"]["name"]):
                    try:
                        sales_channel_url = demand["salesChannel"]["meta"]["href"]
                        sales_channel_data = self.get_sales_channel(sales_channel_url)
                        demand["salesChannel"]["name"] = sales_channel_data.get("name", "Без канала")
                    except Exception as e:
                        logger.warning(f"Ошибка при получении канала продаж: {e}")
                        demand["salesChannel"] = {"name": "Без канала"}
                
                # Логирование прогресса
                if (i + 1) % 1000 == 0:
                    logger.info(f"Обработано {i + 1} из {len(all_demands)} отгрузок")
            
            except Exception as e:
                logger.error(f"Критическая ошибка при обработке отгрузки {demand.get('id')}: {str(e)}")
                continue
        
        logger.info("Обработка отгрузок завершена")
        return all_demands