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
            "Accept": "application/json"
        }

    def get_demands(self, start_date: str, end_date: str):
        """Получить отгрузки за период с точным временем"""
        url = f"{self.base_url}/entity/demand"
        
        # Удаляем возможное существующее время
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        # Форматируем даты в ISO 8601 с 'T' разделителем
        start_with_time = f"{start_date} 00:00:00"
        end_with_time = f"{end_date} 23:59:59"
        
        params = {
            "filter": f"moment>={start_with_time};moment<={end_with_time}",
            "limit": 1000
        }
        
        print(f"Request URL: {url}?{requests.models.RequestEncodingMixin._encode_params(params)}")  # Для отладки
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["rows"]

    def get_demands_excel(self, start_date: str, end_date: str):
        """Сформировать Excel файл с отгрузками за период"""
        demands = self.get_demands(start_date, end_date)
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Отгрузки"
        
        headers = [
            "ID", "Номер", "Дата", "Контрагент", 
            "Сумма", "Статус", "Комментарий"
        ]
        ws.append(headers)
        
        for demand in demands:
            row = [
                demand.get("id", ""),
                demand.get("name", ""),
                demand.get("moment", ""),
                demand.get("agent", {}).get("name", ""),
                demand.get("sum", 0) / 100,
                demand.get("state", {}).get("name", ""),
                demand.get("description", "")
            ]
            ws.append(row)
        
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return buffer.read().hex()