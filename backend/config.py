import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Настройки базы данных
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'moysklad')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
    
    SQLALCHEMY_DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Настройки МойСклад
    MOYSKLAD_API_URL = 'https://api.moysklad.ru/api/remap/1.2'
    MOYSKLAD_TOKEN = os.getenv('MOYSKLAD_TOKEN', 'eba6f80476e5a056ef25f953a117d660be5d568')
    
    # Настройки приложения
    SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here')