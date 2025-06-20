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

    def get_demands(self, start_date: str, end_date: str):
        """Получить отгрузки за период"""
        url = f"{self.base_url}/entity/demand"
        
        # Убедимся, что даты в формате YYYY-MM-DD (без времени)
        start_date = start_date.split(' ')[0].split('T')[0]
        end_date = end_date.split(' ')[0].split('T')[0]
        
        #Формат с пробелом
        filter_str = f"moment>={start_date} 00:00:00;moment<={end_date} 23:59:59"
        
        params = {
            "filter": filter_str,
            "limit": 1000,
            "expand": "agent"  # Добавляем expand для автоматического получения данных агента
        }
        
        print(f"Отправляемый запрос: {url}?{requests.models.RequestEncodingMixin._encode_params(params)}")
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        demands = response.json()["rows"]
        
        # Дополнительно получаем полные данные контрагентов, если они не были получены через expand
        for demand in demands:
            if "agent" in demand and "name" not in demand["agent"]:
                try:
                    counterparty_url = demand["agent"]["meta"]["href"]
                    counterparty_data = self.get_counterparty(counterparty_url)
                    demand["agent"]["name"] = counterparty_data.get("name", "")
                except Exception as e:
                    print(f"Ошибка при получении контрагента: {e}")
                    demand["agent"]["name"] = "Не удалось получить"
        
        return demands

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