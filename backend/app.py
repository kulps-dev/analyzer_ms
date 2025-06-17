from flask import Flask, jsonify, request
from models import db
from moysklad import MoyskladAPI
from config import Config

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

# Инициализация базы данных
with app.app_context():
    db.create_all()

ms_api = MoyskladAPI()

@app.route('/api/demand/<demand_id>', methods=['GET'])
def get_demand(demand_id):
    try:
        demand_data = ms_api.fetch_demand(demand_id)
        ms_api.save_demand_to_db(demand_data)
        return jsonify({'status': 'success', 'data': demand_data})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/export/excel', methods=['GET'])
def export_excel():
    try:
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        project = request.args.get('project')
        channel = request.args.get('channel')
        
        # Здесь должна быть логика экспорта в Excel
        # Пока возвращаем заглушку
        return jsonify({
            'status': 'success',
            'message': 'Экспорт в Excel будет реализован позже',
            'params': {
                'start_date': start_date,
                'end_date': end_date,
                'project': project,
                'channel': channel
            }
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)