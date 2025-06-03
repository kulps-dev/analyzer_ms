from flask import Flask, jsonify, make_response
import requests

app = Flask(__name__)

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2/entity/demand"
MOYSKLAD_TOKEN = "eba6f80476e5a056ef25f953a117d660be5d5687"

@app.route('/api/demand', methods=['GET'])
def get_demand():
    try:
        headers = {
            "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
            "Accept-Encoding": "gzip"
        }
        
        response = requests.get(MOYSKLAD_API_URL, headers=headers)
        response.raise_for_status()
        
        # Возвращаем JSON для отображения на странице
        return jsonify(response.json())
        
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/demand/download', methods=['GET'])
def download_demand():
    try:
        headers = {
            "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
            "Accept-Encoding": "gzip"
        }
        
        response = requests.get(MOYSKLAD_API_URL, headers=headers)
        response.raise_for_status()
        
        # Создаём ответ с файлом для скачивания
        response = make_response(response.text)
        response.headers['Content-Disposition'] = 'attachment; filename=moysklad_demand.txt'
        response.headers['Content-type'] = 'text/plain'
        return response
        
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)