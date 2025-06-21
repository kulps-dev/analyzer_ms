import requests
import io
from openpyxl import Workbook
from datetime import datetime
import logging

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Создаем обработчик для вывода в консоль
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class MoyskladAPI:
    def __init__(self, token: str):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }

    def get_counterparty(self, counterparty_url: str):
        """Получить информацию о контрагенте по URL"""
        response = requests.get(counterparty_url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_store(self, store_url: str):
        """Получить информацию о складе по URL"""
        response = requests.get(store_url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_project(self, project_url: str):
        """Получить информацию о проекте по URL"""
        response = requests.get(project_url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_sales_channel(self, sales_channel_url: str):
        """Получить информацию о канале продаж по URL"""
        response = requests.get(sales_channel_url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_demand_cost_price(self, demand_id: str):
        """Получить себестоимость отгрузки"""
        url = f"{self.base_url}/report/stock/byoperation"
        params = {
            "operation.id": demand_id,
            "limit": 1000
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            total_cost = 0
            if "rows" in data and len(data["rows"]) > 0:
                for position in data["rows"][0].get("positions", []):
                    cost = position.get("cost", 0)
                    quantity = position.get("quantity", 1)
                    total_cost += cost * quantity
            
            return total_cost / 100  # Переводим в рубли
        
        except Exception as e:
            print(f"Ошибка при получении себестоимости для отгрузки {demand_id}: {str(e)}")
            return 0

    def get_demands(self, start_date: str, end_date: str):
        """Получить отгрузки за период с пагинацией и батчингом"""
        url = f"{self.base_url}/entity/demand"
        all_demands = []
        offset = 0
        limit = 100  # Оптимальный размер страницы
        max_requests = 100  # Лимит запросов для защиты от бесконечного цикла
        request_count = 0
        
        # Подготовка фильтра
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        while request_count < max_requests:
            params = {
                "filter": filter_str,
                "limit": limit,
                "offset": offset,
                "expand": "agent,store,project,salesChannel,attributes"
            }
            
            try:
                logger.info(f"Запрос данных: offset={offset}, limit={limit}")
                response = requests.get(url, headers=self.headers, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                demands = data.get("rows", [])
                
                if not demands:
                    break
                    
                all_demands.extend(demands)
                offset += limit
                request_count += 1
                
                # Если получено меньше запрошенного, значит это последняя страница
                if len(demands) < limit:
                    break
                    
            except requests.exceptions.Timeout:
                logger.error(f"Timeout при запросе отгрузок (offset={offset})")
                break
            except Exception as e:
                logger.error(f"Ошибка при получении отгрузок: {str(e)}")
                break
        
        logger.info(f"Всего получено отгрузок: {len(all_demands)}")
        return all_demands
        
        # Дополнительно получаем полные данные, если они не были получены через expand
        for demand in demands:
            if "agent" in demand and "name" not in demand["agent"]:
                try:
                    counterparty_url = demand["agent"]["meta"]["href"]
                    counterparty_data = self.get_counterparty(counterparty_url)
                    demand["agent"]["name"] = counterparty_data.get("name", "")
                except Exception as e:
                    print(f"Ошибка при получении контрагента: {e}")
                    demand["agent"]["name"] = "Не удалось получить"
            
            if "store" in demand and "name" not in demand["store"]:
                try:
                    store_url = demand["store"]["meta"]["href"]
                    store_data = self.get_store(store_url)
                    demand["store"]["name"] = store_data.get("name", "")
                except Exception as e:
                    print(f"Ошибка при получении склада: {e}")
                    demand["store"]["name"] = "Не удалось получить"
            
            # Обработка проекта
            if "project" in demand and ("name" not in demand["project"] or not demand["project"]["name"]):
                try:
                    project_url = demand["project"]["meta"]["href"]
                    project_data = self.get_project(project_url)
                    demand["project"]["name"] = project_data.get("name", "Без проекта")
                except Exception as e:
                    print(f"Ошибка при получении проекта: {e}")
                    demand["project"] = {"name": "Без проекта"}
            
            # Обработка канала продаж
            if "salesChannel" in demand and ("name" not in demand["salesChannel"] or not demand["salesChannel"]["name"]):
                try:
                    sales_channel_url = demand["salesChannel"]["meta"]["href"]
                    sales_channel_data = self.get_sales_channel(sales_channel_url)
                    demand["salesChannel"]["name"] = sales_channel_data.get("name", "Без канала")
                except Exception as e:
                    print(f"Ошибка при получении канала продаж: {e}")
                    demand["salesChannel"] = {"name": "Без канала"}
        
        return demands