from flask import Flask, jsonify
import requests
from flask_cors import CORS
from models import db, Demand
from datetime import datetime
import json

app = Flask(__name__)
CORS(app)

# Конфигурация БД
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:password@db:5432/moysklad'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2/entity/demand"
MOYSKLAD_TOKEN = "eba6f80476e5a056ef25f953a117d660be5d5687"

def process_data(raw_data):
    """Пример обработки данных"""
    rows = raw_data.get('rows', [])
    total = len(rows)
    sum_amount = sum(row.get('sum', 0) for row in rows)
    
    return {
        'total_demands': total,
        'total_amount': sum_amount,
        'first_demand_date': rows[0].get('created') if total > 0 else None,
        'analysis_date': datetime.utcnow().isoformat()
    }

@app.route('/api/demand', methods=['GET'])
def get_demand():
    try:
        headers = {
            "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
            "Accept-Encoding": "gzip"
        }
        response = requests.get(MOYSKLAD_API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Сохраняем в БД
        for item in data.get('rows', []):
            existing = Demand.query.filter_by(moysklad_id=item['id']).first()
            if not existing:
                processed = process_data({'rows': [item]})
                new_demand = Demand(
                    moysklad_id=item['id'],
                    data=item,
                    processed=processed
                )
                db.session.add(new_demand)
        
        db.session.commit()
        
        # Получаем общую статистику
        total_in_db = Demand.query.count()
        
        return jsonify({
            "status": "success",
            "saved_items": len(data.get('rows', [])),
            "total_in_database": total_in_db,
            "last_processed": datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/processed-data', methods=['GET'])
def get_processed_data():
    """Получение агрегированных данных"""
    try:
        total_demands = Demand.query.count()
        last_demand = Demand.query.order_by(Demand.created.desc()).first()
        
        if not last_demand:
            return jsonify({"error": "No data available"}), 404
            
        return jsonify({
            "total_demands": total_demands,
            "last_demand_date": last_demand.created.isoformat(),
            "sample_processed_data": last_demand.processed
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.before_first_request
def create_tables():
    """Создание таблиц при первом запросе"""
    db.create_all()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)