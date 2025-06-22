FROM python:3.9-slim

WORKDIR /app

# Устанавливаем зависимости для psycopg2 и gunicorn
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn==20.1.0  # Явно устанавливаем gunicorn

COPY ./app ./app

# Копируем конфигурацию gunicorn
COPY gunicorn_conf.py /app/gunicorn_conf.py

# Изменяем команду запуска
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "/app/gunicorn_conf.py", "app.main:app"]