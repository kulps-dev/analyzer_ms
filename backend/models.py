from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Demand(db.Model):
    __tablename__ = 'demands'
    
    id = db.Column(db.Integer, primary_key=True)
    moysklad_id = db.Column(db.String(255), unique=True)
    created = db.Column(db.DateTime, default=datetime.utcnow)
    data = db.Column(db.JSON)  # Для хранения всей информации о документе
    processed = db.Column(db.JSON)  # Для обработанных данных
    
    def __repr__(self):
        return f'<Demand {self.moysklad_id}>'