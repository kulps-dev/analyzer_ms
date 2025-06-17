# Фронтенд
FROM nginx:alpine as frontend
COPY ./frontend /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf
EXPOSE 80

# Бэкенд
FROM python:3.9-slim as backend
WORKDIR /app
COPY ./backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./backend /app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Итоговый образ
FROM nginx:alpine
COPY --from=frontend /usr/share/nginx/html /usr/share/nginx/html
COPY --from=frontend /etc/nginx/nginx.conf /etc/nginx/nginx.conf
COPY nginx_backend.conf /etc/nginx/conf.d/default.conf
EXPOSE 80