from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from .moysklad import MoyskladAPI
from pydantic import BaseModel
from datetime import date
from typing import Optional

app = FastAPI(title="МойСклад Анализатор Отгрузок")

# Настройки CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Инициализация API МойСклад
moysklad = MoyskladAPI(api_token="eba6f80476e5a056ef25f953a117d660be5d568")

class ExportRequest(BaseModel):
    start_date: date
    end_date: date
    project: Optional[str] = None
    channel: Optional[str] = None

@app.get("/api/health")
async def health_check():
    return {"status": "ok"}

@app.post("/api/export/excel")
async def export_to_excel(request: ExportRequest):
    try:
        # Получаем данные из МойСклад
        data = await moysklad.get_shipments(
            start_date=request.start_date,
            end_date=request.end_date,
            project=request.project,
            channel=request.channel
        )
        
        # Здесь должна быть логика создания Excel файла
        # Пока возвращаем данные для фронтенда
        return {
            "status": "success",
            "data": data,
            "filename": f"отгрузки_{request.start_date}_{request.end_date}.xlsx"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/export/google-sheets")
async def export_to_google_sheets(request: ExportRequest):
    try:
        # Получаем данные из МойСклад
        data = await moysklad.get_shipments(
            start_date=request.start_date,
            end_date=request.end_date,
            project=request.project,
            channel=request.channel
        )
        
        # Здесь должна быть логика отправки в Google Sheets
        # Пока возвращаем фиктивную ссылку
        return {
            "status": "success",
            "url": "https://docs.google.com/spreadsheets/d/example",
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))