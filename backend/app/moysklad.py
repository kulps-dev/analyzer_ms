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

    def get_stock_by_operation(self, operation_id: str):
    """Получить данные о себестоимости товаров в отгрузке"""
    url = f"{self.base_url}/report/stock/byoperation?operation.id={operation_id}"
    response = requests.get(url, headers=self.headers)
    response.raise_for_status()
    return response.json()

    def get_demands(self, start_date: str, end_date: str):
        """Получить отгрузки за период с информацией о себестоимости"""
        url = f"{self.base_url}/entity/demand"
        
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        params = {
            "filter": filter_str,
            "limit": 1000,
            "expand": "agent,store,project,salesChannel,attributes"
        }
        
        print(f"Отправляемый запрос: {url}?{requests.models.RequestEncodingMixin._encode_params(params)}")
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        demands = response.json()["rows"]
        
        # Дополнительно получаем полные данные, если они не были получены через expand
        for demand in demands:
            # ... (существующий код обработки контрагентов, складов и т.д.)
            
            # Добавляем обработку себестоимости
            try:
                operation_id = demand["id"]
                stock_data = self.get_stock_by_operation(operation_id)
                
                # Считаем общую себестоимость для отгрузки
                total_cost = 0
                if "rows" in stock_data and len(stock_data["rows"]) > 0:
                    for position in stock_data["rows"][0].get("positions", []):
                        total_cost += position.get("cost", 0)
                
                # Добавляем себестоимость в данные отгрузки
                demand["costPrice"] = total_cost / 100  # Переводим в рубли
            except Exception as e:
                print(f"Ошибка при получении себестоимости для отгрузки {demand.get('id')}: {e}")
                demand["costPrice"] = 0
        
        return demands