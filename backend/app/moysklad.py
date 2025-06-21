# moysklad.py

import requests
import io
from openpyxl import Workbook
from datetime import datetime
import time

class MoyskladAPI:
    def __init__(self, token: str):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.retry_delay = 5  # seconds
        self.max_retries = 3

    def _make_request(self, url, params=None, method='GET'):
        for attempt in range(self.max_retries):
            try:
                if method == 'GET':
                    response = self.session.get(url, params=params, timeout=30)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (attempt + 1))

    def get_counterparty(self, counterparty_url: str):
        """Получить информацию о контрагенте по URL"""
        return self._make_request(counterparty_url)

    def get_store(self, store_url: str):
        """Получить информацию о складе по URL"""
        return self._make_request(store_url)

    def get_project(self, project_url: str):
        """Получить информацию о проекте по URL"""
        return self._make_request(project_url)

    def get_sales_channel(self, sales_channel_url: str):
        """Получить информацию о канале продаж по URL"""
        return self._make_request(sales_channel_url)

    def get_demand_cost_price(self, demand_id: str):
        """Получить себестоимость отгрузки"""
        url = f"{self.base_url}/report/stock/byoperation"
        params = {
            "operation.id": demand_id,
            "limit": 1000
        }
        
        try:
            data = self._make_request(url, params=params)
            
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
        """Получить отгрузки за период с пагинацией"""
        url = f"{self.base_url}/entity/demand"
        
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        all_demands = []
        offset = 0
        limit = 1000
        
        while True:
            params = {
                "filter": filter_str,
                "limit": limit,
                "offset": offset,
                "expand": "agent,store,project,salesChannel"
            }
            
            try:
                response = self._make_request(url, params=params)
                demands = response.get("rows", [])
                
                if not demands:
                    break
                
                all_demands.extend(demands)
                offset += limit
                
                # Если получено меньше запрошенного количества, значит это последняя страница
                if len(demands) < limit:
                    break
                    
            except Exception as e:
                print(f"Ошибка при получении отгрузок (offset {offset}): {str(e)}")
                break
        
        # Дополнительно получаем полные данные, если они не были получены через expand
        for demand in all_demands:
            try:
                if "agent" in demand and "name" not in demand["agent"]:
                    counterparty_url = demand["agent"]["meta"]["href"]
                    counterparty_data = self.get_counterparty(counterparty_url)
                    demand["agent"]["name"] = counterparty_data.get("name", "")
                
                if "store" in demand and "name" not in demand["store"]:
                    store_url = demand["store"]["meta"]["href"]
                    store_data = self.get_store(store_url)
                    demand["store"]["name"] = store_data.get("name", "")
                
                if "project" in demand and ("name" not in demand["project"] or not demand["project"]["name"]):
                    project_url = demand["project"]["meta"]["href"]
                    project_data = self.get_project(project_url)
                    demand["project"]["name"] = project_data.get("name", "Без проекта")
                
                if "salesChannel" in demand and ("name" not in demand["salesChannel"] or not demand["salesChannel"]["name"]):
                    sales_channel_url = demand["salesChannel"]["meta"]["href"]
                    sales_channel_data = self.get_sales_channel(sales_channel_url)
                    demand["salesChannel"]["name"] = sales_channel_data.get("name", "Без канала")
            
            except Exception as e:
                print(f"Ошибка при обработке отгрузки {demand.get('id')}: {str(e)}")
                continue
        
        return all_demands