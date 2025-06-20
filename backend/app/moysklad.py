import requests
import io
from openpyxl import Workbook
from datetime import datetime

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

    def get_demands(self, start_date: str, end_date: str):
        """Получить отгрузки за период"""
        url = f"{self.base_url}/entity/demand"
        
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        params = {
            "filter": filter_str,
            "limit": 1000,
            "expand": "agent,store,project,salesChannel"
        }
        
        print(f"Отправляемый запрос: {url}?{requests.models.RequestEncodingMixin._encode_params(params)}")
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        demands = response.json()["rows"]
        
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