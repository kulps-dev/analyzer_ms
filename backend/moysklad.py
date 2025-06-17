import requests
from config import Config
from models import db, Demand, DemandPosition
from datetime import datetime

class MoyskladAPI:
    def __init__(self):
        self.base_url = Config.MOYSKLAD_API_URL
        self.headers = {
            'Authorization': f'Bearer {Config.MOYSKLAD_TOKEN}',
            'Accept-Encoding': 'gzip'
        }
    
    def fetch_demand(self, demand_id):
        url = f'{self.base_url}/entity/demand/{demand_id}'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def save_demand_to_db(self, demand_data):
        # Проверяем, существует ли уже такая отгрузка
        existing_demand = Demand.query.get(demand_data['id'])
        
        if existing_demand:
            # Обновляем существующую запись
            self._update_demand(existing_demand, demand_data)
        else:
            # Создаем новую запись
            self._create_demand(demand_data)
    
    def _create_demand(self, demand_data):
        demand = Demand(
            id=demand_data['id'],
            name=demand_data.get('name'),
            moment=datetime.fromisoformat(demand_data['moment']),
            sum=demand_data['sum'] / 100,  # Конвертируем копейки в рубли
            project_id=demand_data.get('project', {}).get('meta', {}).get('href', '').split('/')[-1],
            project_name=demand_data.get('project', {}).get('name'),
            organization_id=demand_data['organization']['meta']['href'].split('/')[-1],
            organization_name=demand_data['organization'].get('name'),
            state=demand_data.get('state', {}).get('name'),
            created=datetime.fromisoformat(demand_data['created']),
            updated=datetime.fromisoformat(demand_data['updated']),
            vat_sum=demand_data.get('vatSum', 0) / 100,
            payed_sum=demand_data.get('payedSum', 0) / 100,
            shipped_sum=demand_data.get('shippedSum', 0) / 100
        )
        
        db.session.add(demand)
        
        # Сохраняем позиции
        for position in demand_data.get('positions', {}).get('rows', []):
            self._create_position(position, demand.id)
        
        db.session.commit()
    
    def _update_demand(self, demand, demand_data):
        demand.name = demand_data.get('name')
        demand.moment = datetime.fromisoformat(demand_data['moment'])
        demand.sum = demand_data['sum'] / 100
        demand.project_id = demand_data.get('project', {}).get('meta', {}).get('href', '').split('/')[-1]
        demand.project_name = demand_data.get('project', {}).get('name')
        demand.organization_name = demand_data['organization'].get('name')
        demand.state = demand_data.get('state', {}).get('name')
        demand.updated = datetime.fromisoformat(demand_data['updated'])
        demand.vat_sum = demand_data.get('vatSum', 0) / 100
        demand.payed_sum = demand_data.get('payedSum', 0) / 100
        demand.shipped_sum = demand_data.get('shippedSum', 0) / 100
        
        # Обновляем позиции
        DemandPosition.query.filter_by(demand_id=demand.id).delete()
        for position in demand_data.get('positions', {}).get('rows', []):
            self._create_position(position, demand.id)
        
        db.session.commit()
    
    def _create_position(self, position_data, demand_id):
        position = DemandPosition(
            id=position_data['id'],
            demand_id=demand_id,
            quantity=position_data['quantity'],
            price=position_data['price'] / 100,
            vat=position_data.get('vat', 0),
            assortment_id=position_data['assortment']['meta']['href'].split('/')[-1],
            assortment_name=position_data['assortment'].get('name'),
            assortment_type=position_data['assortment']['meta']['type']
        )
        db.session.add(position)