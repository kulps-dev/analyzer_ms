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
        """Получить список отгрузок за период"""
        url = f"{self.base_url}/entity/demand"
        params = {
            "filter": f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["rows"]

    def get_demands_excel(self, start_date: str, end_date: str):
        """Сформировать Excel файл с отгрузками за период"""
        demands = self.get_demands(start_date, end_date)
        
        # Создаем Excel файл
        wb = Workbook()
        ws = wb.active
        ws.title = "Отгрузки"
        
        # Заголовки
        headers = [
            "ID", "Номер", "Дата", "Контрагент", 
            "Сумма", "Статус", "Комментарий"
        ]
        ws.append(headers)
        
        # Данные
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
        
        # Сохраняем в буфер
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        
        return buffer.read().hex()