from flask import Flask, jsonify
import requests
import logging

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2/entity/demand"
MOYSKLAD_TOKEN = "eba6f80476e5a056ef25f953a117d660be5d5687"

@app.route('/api/demand', methods=['GET'])
def get_demand():
    try:
        logger.info("Making request to MoySklad API")
        
        headers = {
            "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
            "Accept": "application/json",
            "Accept-Encoding": "gzip"
        }
        
        response = requests.get(
            MOYSKLAD_API_URL,
            headers=headers,
            timeout=10
        )
        
        logger.info(f"API response status: {response.status_code}")
        
        if response.status_code != 200:
            error_msg = f"MoySklad API error: {response.status_code} - {response.text}"
            logger.error(error_msg)
            return jsonify({"error": error_msg}), 502
            
        return jsonify(response.json())
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)