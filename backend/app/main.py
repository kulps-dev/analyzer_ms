from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from .moysklad import MoyskladAPI

app = FastAPI()

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Инициализация API МойСклад
moysklad = MoyskladAPI(token="2e61e26f0613cf33fab5f31cf105302aa2d607c3")

class DateRange(BaseModel):
    start_date: str
    end_date: str

@app.post("/export/excel")
async def export_excel(date_range: DateRange):
    try:
        # Получаем данные из МойСклад
        excel_data = moysklad.get_demands_excel(
            start_date=date_range.start_date,
            end_date=date_range.end_date
        )
        
        # Возвращаем файл Excel
        return {
            "file": excel_data,
            "filename": f"demands_{date_range.start_date}_{date_range.end_date}.xlsx"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))