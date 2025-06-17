from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Demand(db.Model):
    __tablename__ = 'demands'
    
    id = db.Column(db.String(36), primary_key=True)
    name = db.Column(db.String(255))
    moment = db.Column(db.DateTime)
    sum = db.Column(db.Numeric(15, 2))
    project_id = db.Column(db.String(36))
    project_name = db.Column(db.String(255))
    organization_id = db.Column(db.String(36))
    organization_name = db.Column(db.String(255))
    state = db.Column(db.String(50))
    created = db.Column(db.DateTime)
    updated = db.Column(db.DateTime)
    vat_sum = db.Column(db.Numeric(15, 2))
    payed_sum = db.Column(db.Numeric(15, 2))
    shipped_sum = db.Column(db.Numeric(15, 2))
    
    positions = db.relationship('DemandPosition', backref='demand', lazy=True)

class DemandPosition(db.Model):
    __tablename__ = 'demand_positions'
    
    id = db.Column(db.String(36), primary_key=True)
    demand_id = db.Column(db.String(36), db.ForeignKey('demands.id'))
    quantity = db.Column(db.Float)
    price = db.Column(db.Numeric(15, 2))
    vat = db.Column(db.Integer)
    assortment_id = db.Column(db.String(36))
    assortment_name = db.Column(db.String(255))
    assortment_type = db.Column(db.String(50))