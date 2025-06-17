import aiohttp
from datetime import date
from typing import Optional

class MoyskladAPI:
    def __init__(self, api_token: str):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }

    async def get_shipments(
        self,
        start_date: date,
        end_date: date,
        project: Optional[str] = None,
        channel: Optional[str] = None
    ):
        url = f"{self.base_url}/entity/demand"
        
        # Формируем параметры запроса
        params = {
            "filter": f"moment>={start_date.isoformat()};moment<={end_date.isoformat()}",
            "limit": 1000
        }
        
        if project:
            params["filter"] += f";project={project}"
        if channel:
            params["filter"] += f";salesChannel={channel}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"API error: {response.status} - {error_text}")
                
                data = await response.json()
                return self._process_shipments(data["rows"])
    
    def _process_shipments(self, shipments):
        """Обработка данных отгрузок для фронтенда"""
        processed = []
        for shipment in shipments:
            processed.append({
                "id": shipment.get("id"),
                "name": shipment.get("name"),
                "moment": shipment.get("moment"),
                "project": shipment.get("project", {}).get("name") if shipment.get("project") else None,
                "sales_channel": shipment.get("salesChannel", {}).get("name") if shipment.get("salesChannel") else None,
                "sum": shipment.get("sum", 0) / 100,  # Конвертация копеек в рубли
                "positions": [
                    {
                        "name": pos.get("assortment", {}).get("name"),
                        "quantity": pos.get("quantity"),
                        "price": pos.get("price", 0) / 100
                    }
                    for pos in shipment.get("positions", {}).get("rows", [])
                ]
            })
        return processed