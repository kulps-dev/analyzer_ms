import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Настройки базы данных
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_SSL_MODE = os.getenv('DB_SSL_MODE', 'verify-ca')
    DB_ROOT_CERT = os.getenv('DB_ROOT_CERT', '/app/root.crt')
    
    SQLALCHEMY_DATABASE_URI = (
        f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        f'?sslmode={DB_SSL_MODE}&sslrootcert={DB_ROOT_CERT}'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Настройки МойСклад
    MOYSKLAD_API_URL = 'https://api.moysklad.ru/api/remap/1.2'
    MOYSKLAD_TOKEN = os.getenv('MOYSKLAD_TOKEN')
    
    # Настройки приложения
    SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here')