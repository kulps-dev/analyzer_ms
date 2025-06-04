from flask import Flask, jsonify, make_response
import requests
from flask_cors import CORS
import json
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2/entity/demand"
MOYSKLAD_TOKEN = "eba6f80476e5a056ef25f953a117d660be5d5687"
DATA_DIR = "/data"  # Директория для сохранения данных

# Создаем директорию, если ее нет
os.makedirs(DATA_DIR, exist_ok=True)

def save_data_to_file(data):
    """Сохраняет данные в файл с временной меткой"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DATA_DIR}/moysklad_data_{timestamp}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filename

def process_data(data):
    """Обработка данных (пример)"""
    # Здесь можно добавить любую логику обработки
    processed = {
        "total_orders": len(data.get("rows", [])),
        "first_order": data.get("rows", [{}])[0] if data.get("rows") else None,
        "original_data_size": len(json.dumps(data))
    }
    return processed

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
        
        # Сохраняем сырые данные
        save_data_to_file(data)
        
        # Обрабатываем данные
        processed_data = process_data(data)
        
        return jsonify({
            "status": "success",
            "original_data": data,
            "processed_data": processed_data
        })
        
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/processed-data', methods=['GET'])
def get_processed_data():
    """Новый endpoint для получения обработанных данных"""
    try:
        # Получаем список всех сохраненных файлов
        files = sorted([f for f in os.listdir(DATA_DIR) if f.startswith('moysklad_data_')])
        if not files:
            return jsonify({"error": "No data available"}), 404
            
        # Берем последний файл
        latest_file = files[-1]
        with open(os.path.join(DATA_DIR, latest_file), 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Обрабатываем данные
        processed_data = process_data(data)
        
        return jsonify({
            "filename": latest_file,
            "processed_data": processed_data
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)